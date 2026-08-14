"""Was the screener's own signal predictive? A point-in-time backtest, as a report script.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    python backtest_report.py --cutoff 2024-08-13 --limit 50     # sanity run, minutes
    python backtest_report.py --cutoff 2024-08-13                # the real one, ~2 hours

NO RATIO AND NO SIGNAL IS DEFINED HERE. src/metrics.py valuation() computes the ratios and
src/screener.py add_valuation()/signal() bucket them, called exactly as pages/Screener.py
calls them — the entire point of the exercise is to score the signal that actually ships, not
a second implementation of it that might flatter itself.

What this script does own is the TIME axis, and it does it in one pass:

  1. rebuild each company's valuation inputs from only the fiscal years that had plausibly
     been published by the cutoff (src/backtest.py),
  2. apply screener.industry_mask() with the live page's own defaults, because signal()'s
     cheapness half is a quartile OF THE FRAME IT IS GIVEN — score a different universe and
     the green bucket stops meaning what it means on the page,
  3. fetch one price RANGE per company, covering the cutoff and today in a single request,
  4. score once, and roll the realized forward return up per bucket.

Selection deliberately does NOT depend on any price. An earlier design used the price already
stored in `quote` to decide who was worth a fetch — but that is information from AFTER the
cutoff, and it silently dropped the companies that have since been delisted (730 of them,
measured), which are exactly the returns a backtest must not lose.
"""

import argparse
import datetime
import logging
import math
import pathlib
import time

import pandas as pd

from src import backtest, db, fx, metrics, screener, yahoo_source

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

REPORTS = pathlib.Path(__file__).parent / "reports"

# Between Yahoo calls. Same discipline as ingest.py's SEC_PAUSE_SECONDS and for a sharper
# reason: this endpoint is unofficial and shared with ingest.py's own routine price lookups,
# so tripping its rate limiter degrades the ingest too, not just this run.
PAUSE_SECONDS = 0.3

# Padding on the left of the fetched range. The cutoff may be a weekend, a holiday or a halt,
# and Yahoo has no row for it rather than interpolating one — nearest_close() needs at least
# one real trading day at or before the cutoff to find.
WINDOW_DAYS = 7

# The valuation inputs, and nothing else: ~200,000 rows rather than a scan of the whole table.
# Same narrow-concept filter src/ui.py FACT_HISTORY uses.
PIT_CONCEPTS = list(backtest.AVERAGED_CONCEPTS) + list(backtest.LATEST_CONCEPTS)

# `exists (... source = 'edgar')` is the "has EDGAR data" test src/ui.py:49 already uses:
# company rows are created before either source runs, so an ETF or a ticker whose filings
# could not be parsed sits in `company` with nothing to screen on.
#
# price_as_of is carried for ONE reason: it is the has-a-quote-row test. v_valuation's quote
# join is a lateral `order by as_of desc limit 1` (db/schema.sql:322-326), so a NULL here means
# the company has no quote at all — and writing a cutoff-dated row for such a company would
# make a two-year-old price its CURRENT price on the live Screener. Measured: 1,144 of the
# 4,216 eligible companies are in that state.
COMPANIES_SQL = """
    select v.cik,
           v.ticker,
           v.sic_code,
           v.sic_description,
           v.fiscal_year_end_month,
           v.reporting_currency,
           v.currency,
           v.price_as_of
    from v_valuation v
    where exists (select 1 from fact f where f.cik = v.cik and f.source = 'edgar')
"""

FACTS_SQL = """
    select f.cik, f.fiscal_year, f.concept, f.value
    from fact f
    where f.source = 'edgar'
      and f.concept = any(%s)
"""

# Postgres numeric arrives as Decimal, which pandas keeps as object dtype — arithmetic on it
# raises rather than propagating NaN. Only these columns, for the reason src/ui.py NUMBERS
# gives: to_numeric over the whole frame would blank the ticker and the industry text.
NUMERIC_COMPANY_COLUMNS = ["sic_code", "fiscal_year_end_month"]

CUTOFF_PRICE, TODAY_PRICE, TODAY_DATE = "cutoff_price", "today_price", "today_date"
CUTOFF_DATE = "cutoff_date"

# What a stored signal is called. screener's constants ARE the glyphs (src/screener.py:104),
# which is right for a page and wrong for a WHERE clause — `signal = 'CHEAP_AND_STRONG'`
# is a query someone will actually type, `signal = '🟢'` is not. The names are the
# constants' own, so grep leads from a stored row straight back to the definition.
SIGNAL_NAMES = {
    screener.CHEAP_AND_STRONG: "CHEAP_AND_STRONG",
    screener.MIXED: "MIXED",
    screener.RICH_AND_WEAK: "RICH_AND_WEAK",
    screener.UNKNOWN: "UNKNOWN",
}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cutoff",
        required=True,
        type=datetime.date.fromisoformat,
        help="the point-in-time date, YYYY-MM-DD. Required on purpose: a guessed cutoff "
             "produces a report about a different date that looks exactly the same.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="sanity run over a uniform random subset of the eligible pool (default: all of it)",
    )
    parser.add_argument(
        "--lag-months", type=int, default=backtest.DEFAULT_LAG_MONTHS,
        help="months after a fiscal year ends before its filing counts as public "
             f"(default: {backtest.DEFAULT_LAG_MONTHS})",
    )
    return parser.parse_args(argv)


def query(connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    """One place that turns a cursor into a DataFrame.

    Deliberately NOT src/ui.py query(): that one is @st.cache_data-decorated and assumes a live
    Streamlit ScriptRunContext, which a CLI script does not have. Same five lines, no decorator,
    rather than importing Streamlit into a script that has no page.
    """
    cursor = connection.execute(sql, params)
    return pd.DataFrame(cursor.fetchall(), columns=[column.name for column in cursor.description])


def eligible_pool(
    companies: pd.DataFrame,
    facts: pd.DataFrame,
    cutoff: datetime.date,
    lag_months: int = backtest.DEFAULT_LAG_MONTHS,
) -> pd.DataFrame:
    """Which companies were candidates on the cutoff date, and what their inputs were.

    Three filters, in this order and for three different reasons:

      * point_in_time_frame() drops the fiscal years that were not public yet, and any company
        left with nothing (its first filing lands after the cutoff — it was not a candidate),
      * industry_mask() drops SIC 6000-6799, because pages/Screener.py hides them by default
        and signal()'s quartile has to be taken over the same universe the page scores or the
        report's buckets mean something the page's do not,
      * no average EPS or no book value means no Graham number, which means ⚪ no matter what
        price comes back — spending a Yahoo call on it buys nothing.

    Indexed by cik, carrying both the company metadata and the point-in-time inputs, ready for
    a price column and then screener.add_valuation().
    """
    months = companies.set_index("cik")["fiscal_year_end_month"]
    inputs = backtest.point_in_time_frame(facts, months, cutoff, lag_months)

    pool = companies.set_index("cik").join(inputs, how="inner")
    pool = pool[screener.industry_mask(pool)]
    return pool.dropna(subset=["avg_eps", metrics.BOOK_VALUE_PER_SHARE])


def fetch_prices(pool: pd.DataFrame, cutoff: datetime.date, today: datetime.date,
                 pause_seconds: float = PAUSE_SECONDS) -> tuple:
    """The cutoff close and today's close for every company in the pool — plus the whole
    fetched series, as (frame, {cik: series}).

    ONE request per ticker. The range covers both dates, so a 4,216-company pool costs 4,216
    calls rather than 8,432 — and each date's ACTUAL trading day comes back alongside its
    price, because a company delisted since the cutoff has a "today" close that is really
    months old. That is the correct return endpoint (it is what you would have realized) but
    it is not a two-year holding period, and the count is printed rather than buried.

    The series used to be reduced to those two points and discarded — ~498 of ~500 fetched
    trading days thrown away per company, per run. It is handed back whole so
    store_close_history() can persist it; the resolved cutoff trading day is kept for the
    same reason (backtest_result stores WHICH day the cutoff price really is).

    One ticker's failure is never the run's failure, the same rule price_history() follows:
    it keeps a NaN price, becomes ⚪ UNKNOWN, and shows up as the gap between COMPANIES and
    RATED in the rollup instead of silently vanishing from the denominator.
    """
    rows, history = [], {}
    for position, (cik, company) in enumerate(pool.iterrows(), start=1):
        series = yahoo_source.closes(
            company["ticker"],
            start=cutoff - datetime.timedelta(days=WINDOW_DAYS),
            # `end` is exclusive in yfinance's history(), so today's own close needs +1 day.
            end=today + datetime.timedelta(days=1),
        )
        cutoff_price, cutoff_date = yahoo_source.nearest_close(series, cutoff)
        today_price, today_date = yahoo_source.nearest_close(series, today)

        if cutoff_price is None:
            logger.warning("no cutoff price for %s — it stays in the frame as unknown", company["ticker"])

        rows.append({"cik": cik, CUTOFF_PRICE: cutoff_price, CUTOFF_DATE: cutoff_date,
                     TODAY_PRICE: today_price, TODAY_DATE: today_date})
        history[cik] = series

        if position % 250 == 0:
            print(f"  … {position}/{len(pool)} priced")
        time.sleep(pause_seconds)

    return pd.DataFrame(rows).set_index("cik"), history


def store_cutoff_quotes(connection, pool: pd.DataFrame, cutoff: datetime.date) -> int:
    """Persist the cutoff price — and ONLY where doing so is provably inert.

    v_valuation and db.latest_quote() both take the newest quote, so an as_of=cutoff row is
    invisible to the live pages *as long as a newer row already exists*. For a company with no
    quote at all it would become the latest one, and a two-year-old price would show up on the
    Screener as today's — where a stale low price reads as a cheap stock. Measured: 1,144 of
    the 4,216 eligible companies have no quote row, so this guard is not theoretical.

    price_as_of comes from the same query that built the pool: it IS the "has a quote row"
    test, done once in SQL rather than as one db.latest_quote() round trip per company.

    Today's price is never written. It would overwrite whatever the routine ingest recorded
    today with a number this script fetched for a different purpose.
    """
    written = 0
    for cik, company in pool.iterrows():
        if pd.isna(company[CUTOFF_PRICE]) or pd.isna(company["price_as_of"]):
            continue
        currency = company["currency"] if isinstance(company["currency"], str) else "USD"
        db.write_quote(connection, cik, float(company[CUTOFF_PRICE]), currency,
                       source="yahoo", as_of=cutoff)
        # Committed per row rather than once at the end: the fetch loop runs for hours, and a
        # transaction held open that long blocks vacuum and risks losing everything to one
        # late failure.
        connection.commit()
        written += 1
    return written


def historical_closes(series, exclude: datetime.date) -> list:
    """A fetched Close series as plain (date, price) pairs, minus the `exclude` date.

    The tz-aware yfinance timestamps become the calendar dates quote.as_of is keyed on —
    the same strip nearest_close() does internally. `exclude` is the trading day
    nearest_close(series, today) resolved: today's own close (or a delisted company's last
    one), which belongs to the routine ingest, never to this script.

    NaN prices pass through untouched — deciding what is storable is db.write_quotes' job,
    the same single-guard rule write_facts follows.
    """
    if series is None or len(series) == 0:
        return []

    dates = pd.DatetimeIndex(pd.to_datetime(series.index))
    if dates.tz is not None:
        dates = dates.tz_localize(None)

    return [(date, float(price))
            for date, price in zip(dates.normalize().date, series)
            if date != exclude]


def store_close_history(connection, pool: pd.DataFrame, history: dict) -> int:
    """Persist every fetched daily close — under exactly store_cutoff_quotes' two guards.

    Same first guard, same reason: a company with no quote row at all (price_as_of is NaN —
    1,144 of the 4,216 eligible, mostly delisted) gets NOTHING written, because any row here
    would become its latest quote and a stale close would surface on the live Screener as
    its current price. Same second guard, wider application: the single most recent close is
    excluded, so today's row stays whatever the routine ingest recorded today.

    One batched write and one commit per company. Per company rather than one transaction at
    the end for store_cutoff_quotes' documented reason (the run takes hours); batched rather
    than ~500 write_quote calls because that is ~2M statements across a full run.
    """
    written = 0
    for cik, company in pool.iterrows():
        if pd.isna(company["price_as_of"]):
            continue
        closes = historical_closes(history.get(cik), company[TODAY_DATE])
        if not closes:
            continue
        currency = company["currency"] if isinstance(company["currency"], str) else "USD"
        written += db.write_quotes(connection, cik, closes, currency, source="yahoo")
        connection.commit()
    return written


def store_backtest_results(connection, valued: pd.DataFrame, signal: pd.Series,
                           returns: pd.Series, cutoff: datetime.date) -> int:
    """One backtest_result row per company that got a real cutoff price.

    Pure persistence of what the run already computed: the point-in-time inputs the pool
    carries, the three ratios add_valuation() appended, both price endpoints with their
    resolved trading days, and the realized return. A company without a cutoff price is
    skipped — no valuation was scored and no return exists, so there is nothing a later
    `select * from backtest_result` could learn from it.

    NaN becomes NULL, never Postgres numeric 'NaN' — the write_facts/write_metric_averages
    rule. The signal is stored under its constant NAME (see SIGNAL_NAMES).
    """
    def value(x):
        return float(x) if math.isfinite(db._as_float(x)) else None

    def day(x):
        return None if pd.isna(x) else x

    rows = []
    for cik, company in valued.iterrows():
        if pd.isna(company[CUTOFF_PRICE]):
            continue
        rows.append((
            cik, cutoff, SIGNAL_NAMES[signal.loc[cik]],
            value(company["avg_eps"]),
            value(company[metrics.BOOK_VALUE_PER_SHARE]),
            value(company[metrics.CONSERVATIVE_BOOK_VALUE_PER_SHARE]),
            value(company["roa"]),
            value(company["ebit_margin"]),
            value(company["equity_ratio"]),
            value(company[metrics.GRAHAM_MARGIN]),
            value(company[metrics.KGV]),
            value(company[metrics.RO_I]),
            value(company[CUTOFF_PRICE]),
            day(company[CUTOFF_DATE]),
            value(company[TODAY_PRICE]),
            day(company[TODAY_DATE]),
            value(returns.loc[cik]),
        ))

    written = db.write_backtest_results(connection, rows)
    connection.commit()
    return written


def score(connection, pool: pd.DataFrame) -> tuple:
    """The signal at the cutoff — screener's own functions, called once, unmodified —
    as (valued frame, signal series).

    The cutoff price becomes `price` and the rest is exactly the frame pages/Screener.py hands
    these two functions. fx.rate() is called WITHOUT as_of: it always fetches Frankfurter's
    /latest, and passing a date would write today's rate into fx_rate stamped with that date,
    so every later reader would believe it was the historical one. Using today's rate for a
    2024 valuation is a documented approximation; poisoning the cache is not.

    The valued frame — pool plus the KGV/RoI/Graham-margin columns add_valuation() computed —
    used to be dropped here with only the signal surviving. It is returned so
    store_backtest_results() can persist the numbers the signal was derived from.
    """
    priced = pool.assign(price=pool[CUTOFF_PRICE])
    rates = {pair: fx.rate(connection, *pair) for pair in screener.currency_pairs(priced)}
    valued = screener.add_valuation(priced, rates)
    return valued, screener.signal(valued)


def write_report(rollup: pd.DataFrame, cutoff: datetime.date, directory=REPORTS) -> pathlib.Path:
    """The rollup as a CSV, so a two-hour run can be reread without being repeated."""
    directory = pathlib.Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"backtest_{cutoff.isoformat()}.csv"
    rollup.to_csv(target, index=False)
    return target


def main(argv=None) -> None:
    args = parse_args(argv)
    today = datetime.date.today()

    with db.connect() as connection:
        companies = query(connection, COMPANIES_SQL)
        companies[NUMERIC_COMPANY_COLUMNS] = companies[NUMERIC_COMPANY_COLUMNS].apply(
            pd.to_numeric, errors="coerce"
        )
        facts = query(connection, FACTS_SQL, (PIT_CONCEPTS,))
        facts[screener.VALUE] = pd.to_numeric(facts[screener.VALUE], errors="coerce")

        pool = eligible_pool(companies, facts, args.cutoff, args.lag_months)
        sampled = backtest.sample_universe(pool, size=args.limit)
        print(
            f"universe {len(companies)}  ->  eligible at {args.cutoff} {len(pool)}"
            f"  ->  priced {len(sampled)}"
            f"   (lag {args.lag_months}mo)"
        )

        prices, history = fetch_prices(sampled, args.cutoff, today)
        sampled = sampled.join(prices)
        stored = store_cutoff_quotes(connection, sampled, args.cutoff)
        closes_written = store_close_history(connection, sampled, history)

        valued, signal = score(connection, sampled)

        returns = pd.Series(
            [backtest.forward_return(cutoff, now)
             for cutoff, now in zip(sampled[CUTOFF_PRICE], sampled[TODAY_PRICE])],
            index=sampled.index,
            dtype="float64",
        )
        results_written = store_backtest_results(connection, valued, signal, returns, args.cutoff)

    rollup = backtest.backtest_rollup(sampled, signal, returns)

    print()
    print(rollup.to_string(index=False))
    print()

    # A company delisted since the cutoff has a "today" price that is really its last traded
    # close. Its return is the one you would actually have realized, so it is kept — but the
    # holding period is shorter than the headline, and that is worth one line rather than a
    # column nobody reads.
    stale = int((pd.to_datetime(sampled[TODAY_DATE]) < pd.Timestamp(today) - pd.Timedelta(days=14)).sum())
    print(f"cutoff quotes written: {stored} of {len(sampled)} "
          f"(the rest had no existing quote to shadow, so writing one would not be inert)")
    print(f"daily closes written: {closes_written} (full fetched range, minus each company's "
          f"most recent close, under the same no-quote-row guard)")
    print(f"backtest results written: {results_written} of {len(sampled)} "
          f"(the rest had no cutoff price — nothing scoreable to keep)")
    print(f"last close older than 14 days: {stale} "
          f"(delisted or halted — their return is real but the holding period is shorter)")
    print(f"report: {write_report(rollup, args.cutoff)}")


if __name__ == "__main__":
    main()
