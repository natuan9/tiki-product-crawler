import pytest
import json
import csv
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, mock_open, MagicMock
from configparser import ConfigParser
import requests
from bs4 import BeautifulSoup
import concurrent.futures
from tenacity import RetryError

# Import các hàm cần test
from main import (
    load_crawler_config,
    load_product_ids,
    load_progress,
    save_progress,
    get_already_crawled_ids,
    get_failed_ids,
    clean_description,
    fetch_product,
    crawl_batch,
    filter_products_to_process,
    error_log
)


class TestProgressManagement:
    """Test case for progress management function"""

    def test_load_progress_file_exists(self):
        """Test load progress from existing file"""
        progress_data = {
            "processed_ids": ["123", "456"],
            "current_batch": 2,
            "processed_count": 50,
            "start_time": 1234567890
        }

        with patch("os.path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(progress_data))):
                progress = load_progress()

                assert progress == progress_data

    def test_load_progress_file_not_exists(self):
        """Test loading progress when file does not exist"""
        with patch("os.path.exists", return_value=False):
            with patch("time.time", return_value=1234567890):
                progress = load_progress()

                expected = {
                    "processed_ids": [],
                    "current_batch": 0,
                    "processed_count": 0,
                    "start_time": 1234567890
                }
                assert progress == expected

    def test_load_progress_corrupted_file(self):
        """Test loading progress when file is corrupted"""
        with patch("os.path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data="invalid json")):
                with patch("time.time", return_value=1234567890):
                    progress = load_progress()

                    expected = {
                        "processed_ids": [],
                        "current_batch": 0,
                        "processed_count": 0,
                        "start_time": 1234567890
                    }
                    assert progress == expected


class TestFetchProduct:
    """Test cases for fetch_product function"""

    @patch('main.API_URL', 'https://api.example.com/product/{0}')
    def test_fetch_product_success(self):
        """Test fetching product successfully"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "123",
            "name": "Test Product",
            "url_key": "test-product",
            "price": 29.99,
            "description": "<p>Test description</p>",
            "images": ["image1.jpg", "image2.jpg"]
        }

        mock_session = Mock()
        mock_session.get.return_value = mock_response

        result = fetch_product("123", mock_session)

        assert result["id"] == "123"
        assert result["name"] == "Test Product"
        assert result["price"] == 29.99
        assert result["description"] == "Test description"
        assert result["images"] == ["image1.jpg", "image2.jpg"]

    @patch('main.API_URL', 'https://api.example.com/product/{0}')
    def test_fetch_product_404(self):
        """Test fetching product when not found"""
        mock_response = Mock()
        mock_response.status_code = 404

        mock_session = Mock()
        mock_session.get.return_value = mock_response

        # Clear error_log before testing
        error_log.clear()

        result = fetch_product("123", mock_session)

        assert result is None
        assert len(error_log) == 1
        assert error_log[0][0] == "123"
        assert "HTTP 404" in error_log[0][1]

    @patch('main.API_URL', 'https://api.example.com/product/{0}')
    def test_fetch_product_rate_limit(self):
        """Test fetching product when rate limited"""
        mock_response = Mock()
        mock_response.status_code = 429

        mock_session = Mock()
        mock_session.get.return_value = mock_response

        with pytest.raises(RetryError):
            fetch_product("123", mock_session)


class TestFilterProductsToProcess:
    """Test cases for filter_products_to_process function"""

    def test_filter_basic(self):
        """Test basic filter"""
        all_ids = ["123", "456", "789", "101", "202"]
        already_crawled = {"123", "456"}
        failed_ids = {"789"}

        result = filter_products_to_process(all_ids, already_crawled, failed_ids, retry_failed=False)

        assert result == ["101", "202"]

    def test_filter_with_retry_failed(self):
        """Test filtering with retry failed IDs"""
        all_ids = ["123", "456", "789", "101", "202"]
        already_crawled = {"123", "456"}
        failed_ids = {"789"}

        result = filter_products_to_process(all_ids, already_crawled, failed_ids, retry_failed=True)

        assert result == ["789", "101", "202"]

    def test_filter_all_processed(self):
        """Test when all filters have been processed"""
        all_ids = ["123", "456", "789"]
        already_crawled = {"123", "456", "789"}
        failed_ids = set()

        result = filter_products_to_process(all_ids, already_crawled, failed_ids, retry_failed=False)

        assert result == []

    def test_filter_empty_input(self):
        """Test with empty input"""
        result = filter_products_to_process([], set(), set(), retry_failed=False)

        assert result == []


class TestCrawlBatch:
    """Test case for crawl_batch function"""

    @patch('main.OUTPUT_DIR', '/tmp/test_output')
    @patch('main.MAX_WORKERS', 2)
    def test_crawl_batch_success(self):
        """Test crawl batch successfully"""
        product_ids = ["123", "456"]
        batch_index = 1
        progress = {"processed_count": 0, "current_batch": 0}

        # Mock fetch_product to return a successful response
        def mock_fetch_product(pid, session):
            return {
                "id": pid,
                "name": f"Product {pid}",
                "url_key": f"product-{pid}",
                "price": 29.99,
                "description": "Test description",
                "images": []
            }

        # Mock file operations
        mock_file = mock_open()

        with patch('main.fetch_product', side_effect=mock_fetch_product):
            with patch('builtins.open', mock_file):
                with patch('main.save_progress') as mock_save_progress:
                    with patch('os.path.exists', return_value=False):
                        crawl_batch(product_ids, batch_index, progress)

        # Verify file was written
        mock_file.assert_called()

        # Verify progress was saved
        mock_save_progress.assert_called()

        # Verify progress was updated
        assert progress["processed_count"] == 2
        assert progress["current_batch"] == 2


# Fixtures cho testing
@pytest.fixture
def temp_directory():
    """Create a temp directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_config_file(temp_directory):
    """Create config file for testing"""
    config_content = """
[crawler]
api_url = https://api.example.com/product/{0}
batch_size = 10
max_workers = 5
output_dir = ./output
rate_limit = 30
time_period = 60
progress_file = crawler_progress.json
"""
    config_file = os.path.join(temp_directory, "test_config.ini")
    with open(config_file, "w") as f:
        f.write(config_content)
    return config_file


@pytest.fixture
def sample_product_csv(temp_directory):
    """Create product csv file for testing"""
    csv_content = "product_id\n12345\n67890\n11111\n22222\n"
    csv_file = os.path.join(temp_directory, "test_products.csv")
    with open(csv_file, "w") as f:
        f.write(csv_content)
    return csv_file


# Integration tests
class TestIntegration:
    """Integration tests for all"""

    def test_full_workflow_simulation(self, temp_directory, sample_product_csv):
        """Test full workflow"""
        # Setup
        os.environ['OUTPUT_DIR'] = temp_directory

        # Load product IDs
        product_ids = load_product_ids(sample_product_csv)
        assert len(product_ids) == 4

        # Simulate some products already crawled
        already_crawled = {"12345", "67890"}
        failed_ids = {"11111"}

        # Filter products to process
        to_process = filter_products_to_process(
            product_ids, already_crawled, failed_ids, retry_failed=False
        )

        # Should only have 22222 left to process
        assert to_process == ["22222"]

        # Test with retry failed
        to_process_with_retry = filter_products_to_process(
            product_ids, already_crawled, failed_ids, retry_failed=True
        )

        # Should have both 11111 and 22222
        assert set(to_process_with_retry) == {"11111", "22222"}


if __name__ == "__main__":
    pytest.main(["-v", __file__])