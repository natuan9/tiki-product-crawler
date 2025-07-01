import os
import json
import glob
import logging
import psycopg2
from configparser import ConfigParser
from psycopg2 import DatabaseError, IntegrityError, InterfaceError, OperationalError

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

def insert_products():
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
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
                conn.commit()

                product_dir = "products_output"
                json_files = sorted(glob.glob(os.path.join(product_dir, "products_batch_*.json")))

                inserted_count = 0

                for file in json_files:
                    try:
                        with open(file, 'r', encoding='utf-8') as f:
                            products = json.load(f)
                    except json.JSONDecodeError as e:
                        logging.error(f"❌ Failed to parse JSON file {file}: {e}")
                        continue

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
                        except IntegrityError as e:
                            logging.warning(f"⚠️ Integrity error for product {p.get('id')}: {e}")
                            conn.rollback()
                        except DatabaseError as e:
                            logging.error(f"❌ Database error for product {p.get('id')}: {e}")
                            conn.rollback()
                        except Exception as e:
                            logging.exception(f"❌ Unexpected error for product {p.get('id')}")
                            conn.rollback()

                conn.commit()
                logging.info(f"✅ Done: Inserted {inserted_count} products into database.")

    except OperationalError as e:
        logging.critical(f"❌ Could not connect to the database: {e}")
    except Exception as e:
        logging.exception("❌ Unexpected top-level error")

if __name__ == "__main__":
    insert_products()
