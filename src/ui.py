"""What every Streamlit page needs from the database, in one place.

These four lived in dashboard.py while it was the only page. pages/Screener.py needs the
same connection handling, the same per-day FX cache and the same "a missing number is a
dash, never a 0.00" rule — and a second copy of `query` is how a retry, a timeout or a cache
TTL ends up applying to one page and not the other.

Streamlit's caches are keyed per function, so hanging them here also means both pages share
one cache instead of each warming its own.
"""

import pandas as pd
import streamlit as st

from src import db, fx


@st.cache_data(ttl=300)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Every read goes through here, so there is one place that opens a connection and one
    place the cache hangs off. Parameters stay a tuple bound by psycopg — never an f-string
    into the SQL, even though the only user input is a ticker from a fixed list."""
    with db.connect() as connection:
        cursor = connection.execute(sql, params)
        columns = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


@st.cache_data(ttl=3600)
def fx_rate(base: str, quote: str) -> float:
    """The rate the valuation ratios need, cached for the page as well as in the database.

    src/fx.py already caches per day in fx_rate, so this is only about the rerun: Streamlit
    re-executes this whole file on every widget change, and opening a connection to read a
    number that cannot have moved is the same waste the query cache above exists to avoid.

    The screener calls this once per distinct currency PAIR, not once per company: fx.rate
    answers the 1.0 case before it touches the connection at all, and there are seven pairs
    across all 221 companies.
    """
    with db.connect() as connection:
        return fx.rate(connection, base, quote)


def numeric(frame: pd.DataFrame) -> pd.DataFrame:
    """Postgres numeric arrives as Decimal, which pandas keeps as object dtype — that
    silently disables formatting and sorting in st.dataframe. NULL becomes NaN, not 0."""
    return frame.apply(pd.to_numeric, errors="coerce")


def fmt(value, suffix: str = "", decimals: int = 2) -> str:
    """A missing number is shown as a dash, never as 0.00. Whether it is None from a NULL or
    NaN from a division that was not defined, "we do not have this" is the same statement and
    a zero would read as a measurement."""
    return "--" if pd.isna(value) else f"{float(value):,.{decimals}f}{suffix}"
