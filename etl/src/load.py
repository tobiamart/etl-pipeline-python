"""Load layer for the e-commerce sales pipeline.

Writes the transformed tables to their destinations. In a cloud deployment
these would be a data lake and a warehouse; here they are a local directory
and a SQLite file, but the mechanics are the same — create the target, write
without the index, release the connection.

Each writer returns what the caller needs for logging: the path it wrote, or
the number of rows it inserted.
"""

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Literal

import pandas as pd

PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

#: How a writer behaves when its target table already exists.
IfExists = Literal["fail", "replace", "append"]


def _prepare(path: str | Path) -> Path:
    """Normalise a destination path and create its parent directory.

    Args:
        path: Destination file, as ``str`` or ``Path``.

    Returns:
        The path as a ``Path``, with its parent directory guaranteed to exist.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def save_csv(df: pd.DataFrame, path: str | Path) -> Path:
    """Write a DataFrame to CSV.

    The index is omitted: it carries no meaning after :mod:`transform` resets
    it, and writing it would add an unnamed column that comes back as
    "Unnamed: 0" on every subsequent read.

    Args:
        df: Table to write.
        path: Destination file; parent directories are created as needed.

    Returns:
        The path written to.
    """
    path = _prepare(path)
    df.to_csv(path, index=False, encoding="utf-8")
    return path


def save_parquet(df: pd.DataFrame, path: str | Path) -> Path:
    """Write a DataFrame to Parquet.

    Parquet stores the schema alongside the data, so ``Int64`` and datetime
    columns survive a round trip unchanged — unlike CSV, which flattens
    everything to text and forces the reader to guess types back.

    Args:
        df: Table to write.
        path: Destination file; parent directories are created as needed.

    Returns:
        The path written to.
    """
    path = _prepare(path)
    df.to_parquet(path, index=False)
    return path


def save_to_sqlite(
    df: pd.DataFrame,
    db_path: str | Path,
    table_name: str,
    if_exists: IfExists = "replace",
) -> int:
    """Write a DataFrame to a table in a SQLite database.

    Note that ``sqlite3.connect`` used directly as a context manager commits
    or rolls back the transaction but leaves the connection open, so the
    connection is closed explicitly here.

    Args:
        df: Table to write.
        db_path: Destination database file; parent directories are created
            as needed.
        table_name: Target table.
        if_exists: What to do when the table already exists. ``"replace"``
            suits a full reload, ``"append"`` an incremental one.

    Returns:
        The number of rows written.
    """
    db_path = _prepare(db_path)
    with closing(sqlite3.connect(db_path)) as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        conn.commit()
    return len(df)


def save_partitioned_by_month(df: pd.DataFrame, out_dir: str | Path) -> list[Path]:
    """Write the fact table as one Parquet file per month.

    Produces the Hive-style layout that data lakes and query engines expect::

        out_dir/order_month=2024-01/data.parquet
        out_dir/order_month=2024-02/data.parquet

    Because the partition key lives in the directory name, a query filtering
    on a month reads one file instead of scanning the whole dataset. The
    column itself is dropped from each file — its value is already encoded in
    the path, and readers reconstruct it from there.

    Args:
        df: Fact table containing an ``order_month`` column.
        out_dir: Root directory of the partitioned dataset.

    Returns:
        The paths written, sorted.
    """
    out_dir = Path(out_dir)
    written = [
        save_parquet(
            group.drop(columns="order_month"),
            out_dir / f"order_month={month}" / "data.parquet",
        )
        for month, group in df.groupby("order_month")
    ]
    return sorted(written)
