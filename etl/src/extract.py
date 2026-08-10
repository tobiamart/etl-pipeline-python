"""Extraction layer for the e-commerce sales pipeline.

Loads the raw CSV exports into DataFrames without interpreting their
contents. Every column is read as text so that type conversion remains an
explicit decision of the transformation layer rather than a side effect of
parsing: inferred dtypes would silently coerce identifiers such as "007"
into integers and reduce the mixed decimal separators found in
``unit_price`` to missing values.
"""

from collections.abc import Iterator
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

ORDERS_FILE = DATA_DIR / "orders.csv"
CUSTOMERS_FILE = DATA_DIR / "customers.csv"
PRODUCTS_FILE = DATA_DIR / "products.csv"

# Applied to every read so that the raw layer stays a faithful copy of the source.
_READ_OPTIONS = {"dtype": str, "encoding": "utf-8"}


def read_csv_raw(path: str | Path) -> pd.DataFrame:
    """Read a CSV file into a DataFrame of untyped string columns.

    Args:
        path: Location of the CSV file.

    Returns:
        The file contents, with every column typed as text and empty fields
        represented as missing values.

    Raises:
        FileNotFoundError: If the file does not exist.
        pandas.errors.ParserError: If the file is not valid CSV.
    """
    return pd.read_csv(Path(path), **_READ_OPTIONS)


def read_csv_in_chunks(path: str | Path, chunksize: int = 50) -> Iterator[pd.DataFrame]:
    """Stream a CSV file in fixed-size batches.

    Batches are produced lazily, so only one is held in memory at a time.
    This keeps extraction viable for exports that exceed available memory,
    at the cost of requiring downstream steps to be incremental.

    Args:
        path: Location of the CSV file.
        chunksize: Number of rows per batch.

    Yields:
        Consecutive row batches, each with the columns of the source file.
    """
    yield from pd.read_csv(Path(path), chunksize=chunksize, **_READ_OPTIONS)
