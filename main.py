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
from typing import Tuple

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    level=logging.INFO
)

# === Configuration ===
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
BATCH_SIZE = 1000
MAX_WORKERS = 20
OUTPUT_DIR = "products_output"
# Set rate limit: max 5 requests per second
RATE_LIMIT = 50
TIME_PERIOD = 1  # seconds

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
def fetch_product(product_id: str):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    response = requests.get(API_URL.format(product_id), headers=headers, timeout=10)

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
def crawl_batch(product_ids: list, batch_index: int):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for product in tqdm(executor.map(fetch_product, product_ids), total=len(product_ids), desc=f"Batch {batch_index}"):
            if product:
                results.append(product)

    # Save results to a JSON file
    with open(f"{OUTPUT_DIR}/products_batch_{batch_index:03d}.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def main():
    start_time = time.time() # Start the timer

    product_ids = load_product_ids("product_ids.csv")
    total = len(product_ids)
    logging.info(f"Total products to process: {total}")

    for i in range(0, total, BATCH_SIZE):
        batch = product_ids[i:i + BATCH_SIZE]
        crawl_batch(batch, i // BATCH_SIZE)

    end_time = time.time() # End the timer
    elapsed_minutes = (end_time - start_time)/60
    logging.info(f"\n✅ Total time taken: {elapsed_minutes:.2f} minutes")

    # Save error log if there are errors
    if error_log:
        error_file = os.path.join(OUTPUT_DIR, "errors_log.csv")
        with open(error_file, "w", encoding="utf-8") as f:
            f.write("product_id,error_reason\n")
            for pid, reason in error_log:
                f.write(f"{pid},{reason}\n")
        logging.info(f"\n⚠️  Logged {len(error_log)} errors to {error_file}")

if __name__ == "__main__":
    main()