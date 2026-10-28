"""Build data/sample from the public Jaffle Shop repository.

- Reference tables (stores, customers, products, supplies) are saved as-is.
- Orders and items are limited to the first N months (config: months_to_keep)
  and split into one file per month, e.g. orders_2016_09.csv / items_2016_09.csv.
  Each monthly file simulates a new incremental batch arriving.

Run once:  python -m src.ingestion.download_data
"""
import csv
import io
import time

import requests

from src.pipeline.db import PROJECT_ROOT, load_config
from src.pipeline.logger import get_logger

log = get_logger("download")

REFERENCE_FILES = ["raw_stores.csv", "raw_customers.csv", "raw_products.csv", "raw_supplies.csv"]
MAX_ATTEMPTS = 3


def fetch_csv(url):
    """Download a CSV as text, retrying with exponential backoff (2s, 4s)."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            return response.content.decode("utf-8-sig")
        except requests.RequestException as exc:
            if attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"Download failed after {MAX_ATTEMPTS} attempts: {url}") from exc
            wait = 2 ** attempt
            log.warning("Download attempt %s failed for %s (%s); retrying in %ss", attempt, url, exc, wait)
            time.sleep(wait)


def read_rows(text):
    """Return (header, rows) from CSV text."""
    reader = csv.reader(io.StringIO(text))
    header = next(reader)
    return header, list(reader)


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    config = load_config()
    base_url = config["source"]["base_url"]
    months_to_keep = config["source"]["months_to_keep"]
    out_dir = PROJECT_ROOT / config["paths"]["sample_data"]
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Reference tables: saved unchanged.
    for file_name in REFERENCE_FILES:
        text = fetch_csv(f"{base_url}/{file_name}")
        (out_dir / file_name).write_text(text, encoding="utf-8", newline="")
        log.info("Saved %s (%s rows)", file_name, len(read_rows(text)[1]))

    # 2. Orders: keep the first N months, one file per month.
    order_header, orders = read_rows(fetch_csv(f"{base_url}/raw_orders.csv"))
    ordered_at = order_header.index("ordered_at")
    months = sorted({row[ordered_at][:7] for row in orders})[:months_to_keep]

    order_month = {}  # order id -> "YYYY-MM", used to split items
    for month in months:
        month_rows = [row for row in orders if row[ordered_at][:7] == month]
        order_month.update({row[0]: month for row in month_rows})
        file_name = f"orders_{month.replace('-', '_')}.csv"
        write_csv(out_dir / file_name, order_header, month_rows)
        log.info("Saved %s (%s rows)", file_name, len(month_rows))

    # 3. Items: keep only items of the kept orders, split by the order's month.
    item_header, items = read_rows(fetch_csv(f"{base_url}/raw_items.csv"))
    order_id = item_header.index("order_id")
    for month in months:
        month_rows = [row for row in items if order_month.get(row[order_id]) == month]
        file_name = f"items_{month.replace('-', '_')}.csv"
        write_csv(out_dir / file_name, item_header, month_rows)
        log.info("Saved %s (%s rows)", file_name, len(month_rows))

    log.info("Sample data ready in %s (%s months: %s to %s)", out_dir, len(months), months[0], months[-1])


if __name__ == "__main__":
    main()
