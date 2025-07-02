import logging
from ratelimit import limits, sleep_and_retry
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import requests
import json
import concurrent.futures
from tqdm import tqdm
import os
import re
import csv
from bs4 import BeautifulSoup
import time
from typing import Tuple, Set
import glob
from configparser import ConfigParser


logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO
)

def load_crawler_config(filename="crawler.ini", section="crawler"):
    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(section):
        raise Exception(f"Section {section} not found in {filename}")

    config = {}
    for key, value in parser.items(section):
        # Try to convert numbers
        if value.isdigit():
            config[key] = int(value)
        else:
            try:
                config[key] = float(value)
            except ValueError:
                config[key] = value
    return config

crawler_config = load_crawler_config()
logging.info(f"Crawler config loaded: {crawler_config}")


API_URL = crawler_config['api_url']
BATCH_SIZE = crawler_config['batch_size']
MAX_WORKERS = crawler_config['max_workers']
OUTPUT_DIR = crawler_config['output_dir']
RATE_LIMIT = crawler_config['rate_limit']
TIME_PERIOD = crawler_config['time_period']
PROGRESS_FILE = crawler_config['progress_file']

# Global list to track errors
error_log: list[Tuple[str, str]] = []

# === Create output directory if it doesn't exist ===
os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_product_ids(file_path: str) -> list:
    product_ids = []
    with open(file_path, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            if row and row[0].strip().isdigit():
                product_ids.append(row[0].strip())
    return product_ids


def load_progress() -> dict:
    """Load crawling progress from file"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
                logging.info(f"Loaded progress: processed {progress.get('processed_count', 0)} products")
                return progress
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning("Could not load progress file, starting from scratch")
    return {"processed_ids": [], "current_batch": 0, "processed_count": 0, "start_time": time.time()}


def save_progress(progress: dict):
    """Save crawling progress to file"""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def get_already_crawled_ids() -> Set[str]:
    """Get set of product IDs that have already been successfully crawled"""
    crawled_ids = set()

    # Check all existing batch files
    batch_files = glob.glob(f"{OUTPUT_DIR}/products_batch_*.json")

    for batch_file in batch_files:
        try:
            with open(batch_file, "r", encoding="utf-8") as f:
                products = json.load(f)
                for product in products:
                    if product and product.get("id"):
                        crawled_ids.add(str(product["id"]))
        except (json.JSONDecodeError, FileNotFoundError):
            logging.warning(f"Could not read batch file: {batch_file}")

    logging.info(f"Found {len(crawled_ids)} already crawled products")
    return crawled_ids


def get_failed_ids() -> Set[str]:
    """Get set of product IDs that failed in previous runs"""
    failed_ids = set()
    error_file = os.path.join(OUTPUT_DIR, "errors_log.csv")

    if os.path.exists(error_file):
        try:
            with open(error_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # Skip header
                for row in reader:
                    if row and len(row) >= 1:
                        failed_ids.add(row[0].strip())
        except Exception as e:
            logging.warning(f"⚠️  Could not read error log: {e}")

    return failed_ids


# === Clean and normalize product description ===
def clean_description(html_desc: str) -> str:
    soup = BeautifulSoup(html_desc or "", "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = re.sub(r'\s+', ' ', text)  # Remove extra spaces
    return text.strip()


@sleep_and_retry
@limits(calls=RATE_LIMIT, period=TIME_PERIOD)
@retry(
    wait=wait_exponential(multiplier=2, min=2, max=10),  # Exponential backoff: 2s, 4s, 8s...
    stop=stop_after_attempt(3),  # Retry max 3 times
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
def fetch_product(product_id: str, session: requests.Session):
    print("Test session: ", session)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    response = session.get(API_URL.format(product_id), headers=headers, timeout=10)

    if response.status_code == 200:
        data = response.json()
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "url_key": data.get("url_key"),
            "price": data.get("price"),
            "description": clean_description(data.get("description")),
            "images": data.get("images", [])
        }

    elif response.status_code == 429:
        raise requests.exceptions.RequestException("HTTP 429 Too Many Requests")

    else:
        error_log.append((product_id, f"HTTP {response.status_code}"))
        return None


# === Crawl and save a batch of products ===
def crawl_batch(product_ids: list, batch_index: int, progress: dict):
    results = []
    processed_in_batch = 0

    with requests.Session() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            def worker(pid):
                try:
                    return fetch_product(pid, session)
                except Exception as e:
                    error_log.append((pid, str(e)))
                    return None
            for product in tqdm(executor.map(worker, product_ids), total=len(product_ids),
                                desc=f"Batch {batch_index}"):
                if product:
                    results.append(product)
                    processed_in_batch += 1

                    # Update progress every 10 products
                    if processed_in_batch % 10 == 0:
                        progress["processed_count"] += 10
                        progress["current_batch"] = batch_index
                        save_progress(progress)

    # Save results to a JSON file
    batch_file = f"{OUTPUT_DIR}/products_batch_{batch_index:03d}.json"
    with open(batch_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Update final progress for this batch
    remaining_processed = processed_in_batch % 10
    if remaining_processed > 0:
        progress["processed_count"] += remaining_processed

    progress["current_batch"] = batch_index + 1
    save_progress(progress)

    # Save batch-level errors
    if error_log:
        error_file = os.path.join(OUTPUT_DIR, "errors_log.csv")
        write_header = not os.path.exists(error_file)

        with open(error_file, "a", encoding="utf-8") as f:
            if write_header:
                f.write("product_id,error_reason\n")
            for pid, reason in error_log:
                f.write(f"{pid},{reason}\n")
                logging.warning(f"❌ Error fetching product {pid}: {reason}")

        error_log.clear()  # reset for next batch

    logging.info(f"✅ Completed batch {batch_index}: {len(results)} products saved to {batch_file}")


def filter_products_to_process(product_ids: list, already_crawled: Set[str], failed_ids: Set[str],
                               retry_failed: bool = False) -> list:
    """Filter out already processed products"""
    to_process = []

    for pid in product_ids:
        if pid not in already_crawled:
            if retry_failed or pid not in failed_ids:
                to_process.append(pid)

    return to_process


def main():
    # Load progress
    progress = load_progress()
    start_time = progress.get("start_time", time.time())

    # Load product IDs
    all_product_ids = load_product_ids("product_ids.csv")
    logging.info(f"Loaded {len(all_product_ids)} total product IDs from CSV")

    # Get already processed products
    already_crawled = get_already_crawled_ids()
    failed_ids = get_failed_ids()

    # Filter products that still need to be processed
    # Set retry_failed=True if you want to retry previously failed products
    products_to_process = filter_products_to_process(all_product_ids, already_crawled, failed_ids, retry_failed=False)

    if not products_to_process:
        logging.info("All products have already been processed!")
        return

    logging.info(f"Resuming crawl: {len(products_to_process)} products remaining to process")
    logging.info(
        f"Progress: {len(already_crawled)}/{len(all_product_ids)} products completed ({len(already_crawled) / len(all_product_ids) * 100:.1f}%)")

    # Start processing from where we left off
    current_batch = progress.get("current_batch", 0)

    for i in range(0, len(products_to_process), BATCH_SIZE):
        batch = products_to_process[i:i + BATCH_SIZE]
        batch_index = current_batch + (i // BATCH_SIZE)

        logging.info(f"Processing batch {batch_index}: {len(batch)} products")
        crawl_batch(batch, batch_index, progress)

    end_time = time.time()
    total_elapsed_minutes = (end_time - start_time) / 60
    logging.info(f"\nCrawling completed! Total time: {total_elapsed_minutes:.2f} minutes")

    # Clean up progress file when done
    final_crawled = get_already_crawled_ids()
    if len(final_crawled) >= len(all_product_ids):
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            logging.info("Removed progress file - crawling completed!")


if __name__ == "__main__":
    main()