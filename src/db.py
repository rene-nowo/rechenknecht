"""Database access for Rechenknecht (Supabase Postgres).

The connection URL comes from the environment as RK_DB_URL, e.g.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
"""

import datetime
import math
import os
import pathlib
from urllib.parse import quote, unquote, urlparse

import pandas as pd
import psycopg

SCHEMA_PATH = pathlib.Path(__file__).parent.parent / "db" / "schema.sql"

# Supabase hands out db.<ref>.supabase.co, which resolves to IPv6 only unless the paid IPv4
# add-on is enabled. A host without an IPv6 route fails with "Cannot assign requested
# address", which reads like the database is down rather than unreachable. The session
# pooler is IPv4, so rewrite to it instead of making every caller remember.
POOLER_REGION = os.environ.get("RK_SUPABASE_REGION", "eu-west-1")
POOLER_PREFIX = os.environ.get("RK_SUPABASE_POOLER_PREFIX", "aws-1")


def pooler_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or ""
    if not (host.startswith("db.") and host.endswith(".supabase.co")):
        return url

    project_ref = host.split(".")[1]
    password = quote(unquote(parsed.password or ""), safe="")
    return (
        f"postgresql://postgres.{project_ref}:{password}"
        f"@{POOLER_PREFIX}-{POOLER_REGION}.pooler.supabase.com:5432/postgres"
    )


def connect() -> psycopg.Connection:
    url = os.environ.get("RK_DB_URL")
    if not url:
        raise RuntimeError("RK_DB_URL is not set (see the module docstring for how to get it)")
    return psycopg.connect(pooler_url(url), connect_timeout=30)


def apply_schema(connection: psycopg.Connection) -> None:
    connection.execute(SCHEMA_PATH.read_text())
    connection.commit()


def upsert_company(
    connection: psycopg.Connection,
    cik: str,
    ticker: str,
    name: str = None,
    sic_description: str = None,
    fiscal_year_end_month: int = None,
    reporting_currency: str = None,
    sic_code: int = None,
) -> None:
    """Upsert the company row. Every optional field is coalesced, so the bare
    (cik, ticker) call ingest.py makes before either source runs cannot wipe metadata a
    previous run established — including the reporting currency, which is only knowable
    after the filing has been parsed."""
    connection.execute(
        """
        insert into company (cik, ticker, name, sic_description, fiscal_year_end_month,
                             reporting_currency, sic_code, updated_at)
        values (%s, %s, %s, %s, %s, %s, %s, now())
        on conflict (cik) do update
            set ticker                = excluded.ticker,
                name                  = coalesce(excluded.name, company.name),
                sic_description       = coalesce(excluded.sic_description, company.sic_description),
                fiscal_year_end_month = coalesce(excluded.fiscal_year_end_month, company.fiscal_year_end_month),
                reporting_currency    = coalesce(excluded.reporting_currency, company.reporting_currency),
                sic_code              = coalesce(excluded.sic_code, company.sic_code),
                updated_at            = now()
        """,
        (cik, ticker, name, sic_description, fiscal_year_end_month, reporting_currency, sic_code),
    )


def write_facts(
    connection: psycopg.Connection,
    cik: str,
    source: str,
    frame: pd.DataFrame,
    accession: str = None,
) -> int:
    """Store one analysis frame (rows = concepts, columns = fiscal years).

    Non-year columns such as 7_YEAR_AVG are skipped. That used to DROP information rather
    than merely deduplicate it, because KGV, KBGV, KGBV_conservative and RoI are only ever
    written into the average column (src/metrics.py add_price_ratios) and no table stored a
    market price. write_metric_averages and write_quote below now persist both halves, so the
    skip here is once again a plain deduplication: everything left in the average column is a
    mean over the year columns and is genuinely derivable.
    """
    rows = []
    for column in frame.columns:
        year = str(column)
        if not (len(year) == 4 and year.isdigit()):
            continue
        for concept in frame.index:
            value = frame.loc[concept, column]
            if pd.isna(value):
                continue
            rows.append((cik, source, int(year), str(concept), float(value), accession))

    if not rows:
        return 0

    with connection.cursor() as cursor:
        cursor.executemany(
            """
            insert into fact (cik, source, fiscal_year, concept, value, accession)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (cik, source, fiscal_year, concept) do update
                set value       = excluded.value,
                    accession   = excluded.accession,
                    ingested_at = now()
            """,
            rows,
        )
    return len(rows)


def write_quote(
    connection: psycopg.Connection,
    cik: str,
    price: float,
    currency: str = "USD",
    source: str = "yahoo",
    as_of: datetime.date = None,
) -> None:
    """Store today's price. Without this the valuation ratios cannot be recomputed from
    storage at all — they were derived at ingest time and the price then discarded.

    updated_at is bumped explicitly on conflict, not left to its column default (which only
    fires on insert): this function is only ever called after a genuine live fetch, so every
    call here is real news, and market_price_of()'s same-day cache check is what a caller
    skips this function for instead."""
    connection.execute(
        """
        insert into quote (cik, as_of, price, currency, source)
        values (%s, %s, %s, %s, %s)
        on conflict (cik, as_of) do update
            set price      = excluded.price,
                currency   = excluded.currency,
                source     = excluded.source,
                updated_at = now()
        """,
        (cik, as_of or datetime.date.today(), float(price), currency, source),
    )


def latest_quote(connection: psycopg.Connection, cik: str):
    """The most recent stored quote for a company, or None. market_price_of() uses this to
    skip a live fetch entirely when today's price is already on file — Finnhub's free tier is
    rate-limited (60 req/min) and a rerun during development must not re-spend it on tickers
    it already has today's price for."""
    row = connection.execute(
        """
        select price, currency, source, updated_at
        from quote
        where cik = %s
        order by as_of desc
        limit 1
        """,
        (cik,),
    ).fetchone()
    if row is None:
        return None
    price, currency, source, updated_at = row
    return {"price": float(price), "currency": currency, "source": source, "updated_at": updated_at}


def latest_fx_rate(connection: psycopg.Connection, base: str, quote: str):
    """The most recent stored rate for a currency pair, or None.

    Same shape and the same two jobs as latest_quote above: src/fx.py rate() uses it to skip
    a live fetch when today's rate is already on file, and falls back to it when the fetch
    fails — a rate from a few days ago is off by a fraction of a percent, while an
    unconverted CNY/USD mix is off by a factor of seven.
    """
    row = connection.execute(
        """
        select rate, as_of, source
        from fx_rate
        where base = %s and quote = %s
        order by as_of desc
        limit 1
        """,
        (base, quote),
    ).fetchone()
    if row is None:
        return None
    rate, as_of, source = row
    return {"rate": float(rate), "as_of": as_of, "source": source}


def write_fx_rate(
    connection: psycopg.Connection,
    base: str,
    quote: str,
    rate: float,
    source: str,
    as_of: datetime.date = None,
) -> None:
    """Store one day's rate for a pair. One row per pair per day, like quote — a second
    fetch on the same day is an update, not a second row."""
    connection.execute(
        """
        insert into fx_rate (as_of, base, quote, rate, source)
        values (%s, %s, %s, %s, %s)
        on conflict (as_of, base, quote) do update
            set rate       = excluded.rate,
                source     = excluded.source,
                fetched_at = now()
        """,
        (as_of or datetime.date.today(), base, quote, float(rate), source),
    )


def write_metric_averages(
    connection: psycopg.Connection,
    cik: str,
    frame: pd.DataFrame,
    avg_column: str,
    window_years: int = 7,
) -> int:
    """Persist the average column src/metrics.py add_averages produced. Returns rows written.

    This is the other half of what write_facts drops. write_facts keeps only 4-digit-year
    columns, so KGV, KBGV, KGBV_conservative and RoI — which exist ONLY in the average column
    — were computed on every single ingest and thrown away.

    Non-finite values are skipped rather than stored: an average over a window with no data
    is NaN and a ratio with a zero denominator is NaN, and neither is a measurement. Storing
    them would make "the value is missing" and "the value is nan" two different states in
    every consumer.
    """
    rows = [
        (cik, str(concept), window_years, float(frame.loc[concept, avg_column]))
        for concept in frame.index
        if math.isfinite(_as_float(frame.loc[concept, avg_column]))
    ]
    if not rows:
        return 0

    with connection.cursor() as cursor:
        cursor.executemany(
            """
            insert into metric_average (cik, concept, window_years, value)
            values (%s, %s, %s, %s)
            on conflict (cik, concept, window_years) do update
                set value       = excluded.value,
                    computed_at = now()
            """,
            rows,
        )
    return len(rows)


def _as_float(value) -> float:
    """NaN for anything that is not a number, so one isfinite() call covers None, NaN, inf
    and a stray string alike."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def log_run(
    connection: psycopg.Connection,
    ticker: str,
    source: str,
    status: str,
    started_at,
    facts_written: int = 0,
    error: str = None,
) -> None:
    connection.execute(
        """
        insert into ingest_run (ticker, source, status, error, facts_written, started_at)
        values (%s, %s, %s, %s, %s, %s)
        """,
        (ticker, source, status, (error or None), facts_written, started_at),
    )
