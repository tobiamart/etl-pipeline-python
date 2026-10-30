# ETL pipeline – e-commerce sales data

A small extract–transform–load pipeline in Python and pandas that turns three
messy CSV exports (orders, customers, products) into a clean fact table, writes it
to CSV, Parquet and SQLite, and answers a few reporting questions on top of it.

The source data is deliberately dirty — mixed decimal separators, three date
layouts, inconsistent casing and whitespace, duplicate rows, repeated customers,
orders pointing at customers that do not exist — and the pipeline is built around
handling that explicitly rather than letting pandas guess.

## Structure

```
etl/
  data/
    orders.csv  customers.csv  products.csv   raw input
    processed/                                output: fact table, quarantine, SQLite
                                              warehouse, monthly Parquet partitions
  src/
    extract.py     read the raw files, every column as text
    transform.py   parse, clean, deduplicate, validate, join, aggregate
    load.py        write CSV, Parquet, SQLite, month-partitioned Parquet
    main.py        runs the whole pipeline and prints a summary
```

## Run

Requires Python 3.10+.

```bash
python -m venv .venv
.venv/bin/pip install pandas pyarrow        # Windows: .venv\Scripts\pip
.venv/bin/python etl/src/main.py
```

Output on the bundled data:

```
stage     246 raw orders in 5 batches
extract   246 orders, 44 customers, 12 products
clean     240 orders after dedup, 40 unique customers
validate  215 accepted, 25 quarantined
join      215 fact rows, 21 from unknown customers
load      written to etl/data/processed (12 monthly partitions)

total completed revenue: 250,204.33
```

followed by the top five customers and revenue by category and month.

## How it works

### Extract — raw stays raw

Every column is read as `str`. Type conversion is an explicit decision of the
transformation layer, not a side effect of `read_csv`: inferred dtypes would turn
identifiers like `007` into integers and reduce prices written as `248,03` to
missing values.

### Stage — batch by batch

Before anything is transformed, the raw order export is copied into a
`raw_orders` staging table in the SQLite warehouse with `read_csv_in_chunks`:
the file is streamed in batches of 50 rows, the first batch replaces the table and
the rest are appended, so only one batch is held in memory at a time. This is the
same mechanism an incremental load would use for exports larger than memory.

### Transform — typed, deduplicated, validated

- **Prices** accept both `248.03` and `248,03`; unparseable values become `NaN`
  rather than `0`, so "free" and "unknown" stay different.
- **Dates** come in ISO dates, ISO timestamps and `DD/MM/YYYY`. Each layout is
  matched explicitly instead of letting pandas infer a format per row, which would
  read `05/02/2024` month-first and `16/12/2024` day-first in the same column.
- **Quantities** are nullable `Int64` — whole units, with blanks preserved.
- **Statuses, e-mails, country names** are trimmed and normalised (`Poland`, `pl`
  → `PL`).
- **Duplicate orders** are dropped while still in text form; **repeated customers**
  keep the row with the latest signup date (the pandas equivalent of
  `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) = 1`).
- **Validation** splits orders into accepted and rejected. Rejected rows (missing or
  non-positive quantity, missing price, unparseable date) go to
  `quarantine_orders.csv` instead of being silently dropped, so they can be
  inspected and replayed once the source is fixed.

### Join and aggregate — a star schema

Orders are left-joined with the customer and product dimensions into a fact table
with `revenue` and `order_month`. Left joins keep orders from unknown customers —
an inner join would drop them and understate revenue — and `main.py` asserts that
the join did not multiply rows. Reports are computed on completed orders only:
revenue by category and month, and a top-customer ranking that keeps unknown
customers in the totals (`dropna=False`).

### Load — four destinations

- `fact_orders.csv` and `quarantine_orders.csv`
- `fact_orders.parquet`, which keeps `Int64` and datetime types intact
- `warehouse.db` — `fact_orders` written with `replace` (full reload), next to the
  `raw_orders` staging table built with `append`
- `fact_orders_by_month/` — Hive-style partitions
  (`order_month=2024-01/data.parquet`, …), so a query filtered by month reads a
  single file instead of scanning the dataset; `pd.read_parquet` on the directory
  reassembles the full table with `order_month` restored from the paths
