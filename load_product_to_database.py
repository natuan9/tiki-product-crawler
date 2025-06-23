import os
import json
import glob
import psycopg2
from configparser import ConfigParser
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load database config
def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    return config

# Connect to DB and insert product data
def insert_products():
    config = load_config()
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id BIGINT PRIMARY KEY,
            name TEXT,
            url_key TEXT,
            price NUMERIC,
            description TEXT,
            images JSONB
        );
    """)

    product_dir = "products_output"
    json_files = sorted(glob.glob(os.path.join(product_dir, "products_batch_*.json")))

    inserted_count = 0

    for file in json_files:
        with open(file, 'r', encoding='utf-8') as f:
            products = json.load(f)
            for p in products:
                try:
                    cursor.execute("""
                        INSERT INTO products (id, name, url_key, price, description, images)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        p.get("id"),
                        p.get("name"),
                        p.get("url_key"),
                        p.get("price"),
                        p.get("description"),
                        json.dumps(p.get("images", []))
                    ))
                    inserted_count += 1
                except Exception as e:
                    logging.warning(f"Failed to insert product {p.get('id')}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"✅ Done: Inserted {inserted_count} products into database.")

if __name__ == "__main__":
    insert_products()
