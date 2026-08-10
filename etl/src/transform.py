"""Transformation layer for the e-commerce sales pipeline.

Turns the raw string columns produced by :mod:`extract` into typed,
deduplicated tables, then joins them into a single fact table ready for
reporting.

Functions come in three groups: column parsers that convert one Series at a
time, table cleaners that compose those parsers, and aggregations that answer
business questions. Every function returns a new object rather than mutating
its input, so steps can be reordered or rerun without hidden state.
"""

import pandas as pd

#: Maps the country spellings found in the raw export to ISO 3166-1 alpha-2.
COUNTRY_MAP = {
    "pl": "PL",
    "poland": "PL",
    "de": "DE",
    "germany": "DE",
    "cz": "CZ",
    "czechia": "CZ",
}

#: Order status that represents realised revenue.
COMPLETED = "completed"

#: Columns whose values are matched against other tables and must not carry
#: stray whitespace.
_ORDER_KEYS = ["order_id", "customer_id", "product_id"]
_CUSTOMER_KEYS = ["customer_id", "full_name"]


# ===========================================================================
# Column parsers
# ===========================================================================
def strip_text_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Trim surrounding whitespace from the given text columns.

    Columns absent from the frame are ignored, so the same column list can be
    reused across sources with slightly different schemas.

    Args:
        df: Source frame; left unmodified.
        columns: Names of the columns to trim.

    Returns:
        A copy of ``df`` with the requested columns trimmed.
    """
    present = [col for col in columns if col in df.columns]
    return df.assign(**{col: df[col].str.strip() for col in present})


def parse_price(series: pd.Series) -> pd.Series:
    """Convert a text price column to floats.

    Accepts both decimal separators used by the source system ("248.03" and
    "248,03") and tolerates surrounding whitespace. Values that cannot be
    interpreted become missing rather than zero, preserving the distinction
    between "cost nothing" and "unknown".

    Args:
        series: Raw price column.

    Returns:
        The prices as floats, with unparseable entries as ``NaN``.
    """
    cleaned = series.str.strip().str.replace(",", ".", regex=False)
    return pd.to_numeric(cleaned, errors="coerce")


def parse_quantity(series: pd.Series) -> pd.Series:
    """Convert a text quantity column to nullable integers.

    ``Int64`` rather than ``int64`` because the column contains blanks, and
    rather than ``float64`` because quantities are whole units.

    Args:
        series: Raw quantity column.

    Returns:
        The quantities as ``Int64``, with blank entries as ``pd.NA``.
    """
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def parse_dates(series: pd.Series) -> pd.Series:
    """Parse a date column written in several formats.

    The export mixes ISO dates ("2024-11-26"), ISO timestamps
    ("2024-02-10 00:00:00") and day-first dates ("16/12/2024"). Rather than
    letting pandas infer a format per row — which resolves "05/02/2024" as
    month-first and "16/12/2024" as day-first within the same column — each
    layout is matched explicitly, widest first.

    Args:
        series: Raw date column.

    Returns:
        The dates as ``datetime64``, with unparseable entries as ``NaT``.
    """
    cleaned = series.str.strip()
    parsed = pd.to_datetime(cleaned, format="ISO8601", errors="coerce")

    remaining = parsed.isna() & cleaned.notna()
    if remaining.any():
        parsed[remaining] = pd.to_datetime(
            cleaned[remaining], format="%d/%m/%Y", errors="coerce"
        )
    return parsed


def normalize_status(series: pd.Series) -> pd.Series:
    """Fold status spellings into a single lowercase form.

    Args:
        series: Raw status column.

    Returns:
        Statuses trimmed and lowercased, e.g. "COMPLETED" and " completed "
        both become "completed".
    """
    return series.str.strip().str.lower()


def normalize_email(series: pd.Series) -> pd.Series:
    """Normalise e-mail addresses for use as a comparable identifier.

    Args:
        series: Raw e-mail column.

    Returns:
        Addresses trimmed and lowercased.
    """
    return series.str.strip().str.lower()


def normalize_country(series: pd.Series) -> pd.Series:
    """Map country names to ISO 3166-1 alpha-2 codes via :data:`COUNTRY_MAP`.

    Args:
        series: Raw country column.

    Returns:
        Two-letter country codes; values missing from the mapping become
        ``NaN`` so that unknown spellings surface instead of passing through.
    """
    return series.str.strip().str.lower().map(COUNTRY_MAP)


# ===========================================================================
# Table cleaning
# ===========================================================================
def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Type and deduplicate the raw order export.

    Rows duplicated across every column are dropped first, while the frame is
    still all-text: comparing strings is cheaper than comparing parsed values,
    and it spares the parsers the duplicate work.

    Args:
        df: Raw orders as returned by the extraction layer.

    Returns:
        A typed copy with a fresh index.
    """
    return (
        df.drop_duplicates()
        .pipe(strip_text_columns, _ORDER_KEYS)
        .assign(
            quantity=lambda d: parse_quantity(d["quantity"]),
            unit_price=lambda d: parse_price(d["unit_price"]),
            order_date=lambda d: parse_dates(d["order_date"]),
            status=lambda d: normalize_status(d["status"]),
        )
        .reset_index(drop=True)
    )


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Type and normalise the raw customer export.

    Args:
        df: Raw customers as returned by the extraction layer.

    Returns:
        A typed copy with trimmed keys, normalised e-mail addresses, ISO
        country codes and parsed signup dates.
    """
    return df.pipe(strip_text_columns, _CUSTOMER_KEYS).assign(
        email=lambda d: normalize_email(d["email"]),
        country=lambda d: normalize_country(d["country"]),
        signup_date=lambda d: parse_dates(d["signup_date"]),
    )


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per customer: the one with the latest signup date.

    The SQL equivalent is ``ROW_NUMBER() OVER (PARTITION BY customer_id
    ORDER BY signup_date DESC) = 1``. Ties are broken by e-mail so that the
    result does not depend on the sort algorithm's stability.

    Expects ``signup_date`` to have been parsed by :func:`clean_customers`.
    On unparsed text the sort is lexicographic and silently picks the wrong
    row — "29/03/2024" sorts above "2024-07-02" despite being earlier.

    Args:
        df: Cleaned customers, possibly containing repeated ``customer_id``.

    Returns:
        One row per ``customer_id``, sorted by that column, index reset.
    """
    return (
        df.sort_values(["signup_date", "email"], ascending=False)
        .drop_duplicates(subset="customer_id", keep="first")
        .sort_values("customer_id")
        .reset_index(drop=True)
    )


def split_valid_invalid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partition orders into those fit for reporting and those to quarantine.

    An order is rejected when its quantity is missing or non-positive, its
    unit price is missing, or its date could not be parsed. Rejected rows are
    returned rather than dropped so they can be inspected and replayed once
    the source is fixed.

    Args:
        df: Cleaned orders.

    Returns:
        A ``(valid, invalid)`` pair whose lengths sum to ``len(df)``, both
        with a fresh index.
    """
    # `quantity <= 0` yields NA for missing quantities, not False, so the
    # comparison is closed with fillna before combining the conditions.
    rejected = (
        df["quantity"].isna()
        | (df["quantity"] <= 0).fillna(True)
        | df["unit_price"].isna()
        | df["order_date"].isna()
    )
    return (
        df[~rejected].reset_index(drop=True),
        df[rejected].reset_index(drop=True),
    )


# ===========================================================================
# Joins and aggregations
# ===========================================================================
def build_fact_table(
    orders: pd.DataFrame, customers: pd.DataFrame, products: pd.DataFrame
) -> pd.DataFrame:
    """Join orders with their customer and product context.

    Uses left joins so that orders referencing unknown customers survive with
    empty customer attributes; an inner join would silently drop them and
    understate revenue. Both right-hand keys must be unique — otherwise the
    join multiplies rows and every downstream total is inflated.

    Args:
        orders: Valid, cleaned orders.
        customers: Deduplicated customers.
        products: Product catalogue with a numeric ``unit_cost``.

    Returns:
        One row per order, with the customer and product columns appended
        plus ``revenue`` (quantity times unit price) and ``order_month``
        (the order date as a "YYYY-MM" string).
    """
    return (
        orders.merge(customers, on="customer_id", how="left")
        .merge(products, on="product_id", how="left")
        .assign(
            revenue=lambda d: d["quantity"] * d["unit_price"],
            order_month=lambda d: d["order_date"].dt.to_period("M").astype(str),
        )
    )


def revenue_by_category_month(fact: pd.DataFrame) -> pd.DataFrame:
    """Aggregate realised revenue by product category and month.

    Args:
        fact: Fact table from :func:`build_fact_table`.

    Returns:
        Columns ``category``, ``order_month`` and ``revenue``, covering
        completed orders only, sorted by month then category.
    """
    completed = fact[fact["status"] == COMPLETED]
    return (
        completed.groupby(["category", "order_month"])
        .agg(revenue=("revenue", "sum"))
        .reset_index()
        .sort_values(["order_month", "category"])
        .reset_index(drop=True)
    )


def top_customers(fact: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    """Rank customers by realised revenue.

    Groups with ``dropna=False`` so that orders placed by customers missing
    from the customer export — which have no ``full_name`` — stay in the
    ranking instead of being silently excluded from the totals.

    Args:
        fact: Fact table from :func:`build_fact_table`.
        n: Number of customers to return.

    Returns:
        Columns ``customer_id``, ``full_name``, ``revenue`` and
        ``orders_count`` for the ``n`` highest-earning customers, ordered by
        revenue descending.
    """
    completed = fact[fact["status"] == COMPLETED]
    return (
        completed.groupby(["customer_id", "full_name"], dropna=False)
        .agg(revenue=("revenue", "sum"), orders_count=("order_id", "count"))
        .reset_index()
        .nlargest(n, "revenue")
        .reset_index(drop=True)
    )
