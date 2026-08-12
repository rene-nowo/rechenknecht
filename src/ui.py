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

from src import db, fx, screener, yahoo_source

# One query, shared by every page that ranks companies. The valuation inputs come from
# v_valuation (which contains no formula at all) and the quality metrics from the stored
# 7-year averages; the pivot is selection, not arithmetic — metric_average's primary key is
# (cik, concept, window_years), so each `max(value) filter` picks exactly one row.
#
# `exists (... fact ... source = 'edgar')` is the "has EDGAR data" test: company rows are
# created before either source runs, so an ETF (spy, qqq) or a ticker whose filings could not
# be parsed sits in `company` with no facts at all. Those are not companies with missing
# metrics, they are companies with nothing to screen on.
ROWS = """
    select v.ticker,
           v.name,
           v.sic_description,
           v.sic_code,
           v.price,
           v.currency,
           v.price_as_of,
           v.reporting_currency,
           v.avg_eps,
           v.book_value_per_share,
           v.conservative_book_value_per_share,
           m.roa,
           m.ebit_margin,
           m.equity_ratio
    from v_valuation v
             left join (select cik,
                               max(value) filter (where concept = 'RoA')          as roa,
                               max(value) filter (where concept = 'EBIT-margin')  as ebit_margin,
                               max(value) filter (where concept = 'equity-ratio') as equity_ratio
                        from metric_average
                        where window_years = 7
                        group by cik) m on m.cik = v.cik
    where exists (select 1 from fact f where f.cik = v.cik and f.source = 'edgar')
"""

# The two narrow reads behind the industry deep-dive. Both are RAW ROWS: the medians and the
# yearly means are taken in pandas by src/screener.py, never here — `select avg(...) group by`
# would be a second definition of an aggregate the page also computes, which is exactly the
# drift the no-arithmetic-in-SQL rule above exists to prevent.
#
# Filtered by TICKER rather than by SIC code, because which companies are in the selected
# industry is decided by screener.industry_label() over the frame ON SCREEN — including the
# sidebar's three filters. Re-deriving that set in SQL would let the chart show companies the
# table above it does not.
FACT_HISTORY = """
    select c.ticker, f.fiscal_year, f.concept, f.value
    from fact f
             join company c on c.cik = f.cik
    where f.source = 'edgar'
      and c.ticker = any(%s)
      and f.concept = any(%s)
"""

MACRO_HISTORY = """
    select date, value
    from macro_series
    where series_id = %s
      and date >= %s
    order by date
"""

# Decimal out of Postgres is object dtype in pandas, which silently disables both sorting and
# number formatting in st.dataframe — and a screener that cannot sort is not one. Only these
# columns: to_numeric over the whole frame would blank out the ticker and the name.
NUMBERS = [
    "sic_code", "price", "avg_eps", "book_value_per_share",
    "conservative_book_value_per_share", "roa", "ebit_margin", "equity_ratio",
]


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


def valuation_frame() -> pd.DataFrame:
    """Every screenable company with its ratios already computed — the one loader.

    Two pages rank these companies now (Screener per company, Opportunities per industry and
    per candidate), and both need the identical frame: the same query, the same numeric
    coercion, the same one-rate-per-currency-PAIR conversion. A second copy of ROWS is how the
    two pages end up disagreeing about which companies exist.

    NO RATIO IS COMPUTED HERE either — add_valuation calls src/metrics.py valuation() per row,
    the same function the ingest calls and test_excel_parity.py pins against the workbook.
    Cheap enough to run per rerun (0.03 s for 6,767 rows, measured 2026-08-12), so it carries
    no cache of its own beyond the one on query() below it.
    """
    frame = query(ROWS)
    if frame.empty:
        return frame

    frame[NUMBERS] = numeric(frame[NUMBERS])
    rates = {pair: fx_rate(*pair) for pair in screener.currency_pairs(frame)}
    return screener.add_valuation(frame, rates)


def fact_history(tickers: tuple, concepts: tuple) -> pd.DataFrame:
    """The stored EDGAR facts for a set of companies — one row per company, year and concept.

    Raw rows on purpose; screener.median_by_year() does the aggregating. Cheap because it is
    narrow: the deep-sea shipping industry's 30 companies over two concepts are 351 rows
    (measured 2026-08-12), not a scan of the 43,383-row concept.
    """
    frame = query(FACT_HISTORY, (list(tickers), list(concepts)))
    frame[screener.VALUE] = pd.to_numeric(frame[screener.VALUE], errors="coerce")
    return frame


def macro_history(series_id: str, since_year: int) -> pd.DataFrame:
    """One FRED series from `since_year` on, daily rows as stored.

    Bounded by the first fiscal year the industry has facts for, so the overlay cannot draw a
    macro line across years the fundamentals side has nothing in.
    """
    frame = query(MACRO_HISTORY, (series_id, f"{since_year}-01-01"))
    frame[screener.VALUE] = pd.to_numeric(frame[screener.VALUE], errors="coerce")
    return frame


@st.cache_data(ttl=1800, show_spinner="Fetching recent prices from Yahoo…")
def price_frame(tickers: tuple, period: str = "2y") -> pd.DataFrame:
    """Recent daily closes, fetched LIVE from Yahoo and deliberately never stored.

    A display fetch, not an ingest: nothing here writes to `quote`, and no page number is
    computed from it. The cache is what keeps it from re-hitting Yahoo on every widget
    change — Streamlit re-runs the whole page body on each interaction, and this is the only
    thing on the deep-dive that leaves the database.

    `tickers` is a tuple rather than a list so the cache can key on it.
    """
    return yahoo_source.price_history(tickers, period=period)


def numeric(frame: pd.DataFrame) -> pd.DataFrame:
    """Postgres numeric arrives as Decimal, which pandas keeps as object dtype — that
    silently disables formatting and sorting in st.dataframe. NULL becomes NaN, not 0."""
    return frame.apply(pd.to_numeric, errors="coerce")


def fmt(value, suffix: str = "", decimals: int = 2) -> str:
    """A missing number is shown as a dash, never as 0.00. Whether it is None from a NULL or
    NaN from a division that was not defined, "we do not have this" is the same statement and
    a zero would read as a measurement."""
    return "--" if pd.isna(value) else f"{float(value):,.{decimals}f}{suffix}"
