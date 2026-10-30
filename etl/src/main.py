"""Runs the e-commerce sales pipeline end to end and prints what it produced.

    .venv/bin/python etl/src/main.py
"""

from extract import (
    CUSTOMERS_FILE,
    ORDERS_FILE,
    PRODUCTS_FILE,
    read_csv_in_chunks,
    read_csv_raw,
)
from load import (
    PROCESSED_DIR,
    save_csv,
    save_parquet,
    save_partitioned_by_month,
    save_to_sqlite,
)
from transform import (
    build_fact_table,
    clean_customers,
    clean_orders,
    deduplicate_customers,
    parse_price,
    revenue_by_category_month,
    split_valid_invalid,
    top_customers,
)


def main() -> None:
    warehouse = PROCESSED_DIR / "warehouse.db"

    # --- stage ---
    # The raw order export is copied into the warehouse batch by batch: the
    # first batch replaces the staging table, the rest are appended, and only
    # one batch is held in memory at a time.
    staged = batches = 0
    for batches, chunk in enumerate(read_csv_in_chunks(ORDERS_FILE), start=1):
        staged += save_to_sqlite(
            chunk, warehouse, "raw_orders",
            if_exists="replace" if batches == 1 else "append",
        )
    print(f"stage     {staged} raw orders in {batches} batches")

    # --- extract ---
    raw_orders = read_csv_raw(ORDERS_FILE)
    raw_customers = read_csv_raw(CUSTOMERS_FILE)
    raw_products = read_csv_raw(PRODUCTS_FILE)
    print(f"extract   {len(raw_orders)} orders, {len(raw_customers)} customers, "
          f"{len(raw_products)} products")

    # --- transform ---
    orders = clean_orders(raw_orders)
    customers = deduplicate_customers(clean_customers(raw_customers))
    products = raw_products.assign(unit_cost=lambda d: parse_price(d["unit_cost"]))
    print(f"clean     {len(orders)} orders after dedup, {len(customers)} unique customers")

    valid, invalid = split_valid_invalid(orders)
    print(f"validate  {len(valid)} accepted, {len(invalid)} quarantined")

    fact = build_fact_table(valid, customers, products)
    assert len(fact) == len(valid), "left join multiplied rows"
    print(f"join      {len(fact)} fact rows, "
          f"{fact['full_name'].isna().sum()} from unknown customers")

    # --- load ---
    save_csv(fact, PROCESSED_DIR / "fact_orders.csv")
    save_parquet(fact, PROCESSED_DIR / "fact_orders.parquet")
    save_csv(invalid, PROCESSED_DIR / "quarantine_orders.csv")
    save_to_sqlite(fact, warehouse, "fact_orders")
    partitions = save_partitioned_by_month(fact, PROCESSED_DIR / "fact_orders_by_month")
    print(f"load      written to {PROCESSED_DIR} ({len(partitions)} monthly partitions)")

    # --- results ---
    completed = fact[fact["status"] == "completed"]
    print(f"\ntotal completed revenue: {completed['revenue'].sum():,.2f}\n")
    print("top customers:")
    print(top_customers(fact, n=5).to_string(index=False))
    print("\nrevenue by category, first months:")
    print(revenue_by_category_month(fact).head(8).to_string(index=False))


if __name__ == "__main__":
    main()
