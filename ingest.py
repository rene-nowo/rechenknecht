"""Ingest companies from both sources into Postgres.

Per ticker: pull the annual-report XBRL from EDGAR (10-K, or 20-F for a foreign private
issuer that reports under us-gaap) and run the filing parser, pull the same concepts
from Yahoo, push both through the SAME ratio code (src/metrics.py), and store each side
under its own `source`. Comparing them is then a view, not a script.

Filings are deleted per ticker in a `finally`, whatever the outcome — at ~4.2 MB per filing
and 4+ filings per company, keeping them costs ~17 GB per 1000 companies while the database
holds the same information in a few MB. Pass --keep-filings while the parser is still
changing and you want to re-parse offline; app_edgar.py takes the same flag.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    export RK_FINNHUB_KEY="$(op read 'op://Server/Finnhub/credential')"  # optional, see market_price_of()
    python ingest.py --limit 20
"""

import argparse
import datetime as dt
import logging
import os
import pathlib
import time

import pandas as pd
import psycopg
import requests
import yfinance as yf

from app_edgar import search_edgar_data, sic_code
from src import db, fx, yahoo_source
from src.Edgar_api import EDGAR_API, discard_filings, generate_cik_format
from src.metrics import add_averages, add_sheet_ratios
from src.RechenknechtBeta import RechenknechtBeta

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("ingest")

# One instance: constructing EDGAR_API parses the 12k-row ticker/CIK map every time.
_edgar = EDGAR_API()


def cik_for(ticker: str) -> str:
    return generate_cik_format(str(_edgar.cik_map[1][ticker.lower()]))


def safe_rollback(connection: psycopg.Connection) -> psycopg.Connection:
    """connection.rollback(), or a fresh connection if the old one is beyond rolling back.

    Supabase's pooler closes an idle/long-lived session out from under a multi-hour ingest
    (measured live: `psycopg.OperationalError: server closed the connection unexpectedly`
    mid-run) — and rollback() ON that dead connection raises too. An exception raised while
    already handling one is not caught by that same except block, so every per-ticker error
    handler that called connection.rollback() directly was exactly as fatal to the whole
    batch as the original error. Call this instead, and reassign: `connection =
    safe_rollback(connection)` — the loop keeps going on a working connection either way.
    """
    try:
        connection.rollback()
        return connection
    except Exception:
        try:
            connection.close()
        except Exception:
            pass
        return db.connect()

TICKER_MAP = pathlib.Path(__file__).parent / "maps" / "ticker-cik_map.txt"

# The SEC asks for no more than 10 requests/second. One company costs roughly 5 requests
# (submissions + one per filing), so this is polite rather than tight.
SEC_PAUSE_SECONDS = 0.5

# Optional: a Finnhub API key (op://Server/Finnhub/credential) makes market_price_of() try a
# real, documented API before falling back to yfinance's undocumented scrape. Unset is fine —
# every ticker just takes the yfinance path, same as before this existed.
FINNHUB_KEY = os.environ.get("RK_FINNHUB_KEY")
FINNHUB_PAUSE_SECONDS = 1.1  # free tier is 60 req/min; this stays under that with margin


def load_tickers(limit: int, only: str = None) -> list:
    if only:
        return [t.strip().lower() for t in only.split(",") if t.strip()]
    frame = pd.read_csv(TICKER_MAP, sep="\t", header=None, names=["ticker", "cik", "result"])
    return [str(t).lower() for t in frame["ticker"].head(limit)]


def finnhub_price(ticker: str):
    """(price, 'USD') from Finnhub's free /quote endpoint, or (None, None).

    Free-tier coverage is US-listed stocks; anything outside that, and any symbol Finnhub
    simply has no data for (fl — verified, it has none there either), comes back as its
    documented no-data signal `c == 0` rather than an HTTP error, so that is the only check.
    """
    try:
        response = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": ticker.upper(), "token": FINNHUB_KEY},
            timeout=10,
        )
        response.raise_for_status()
        price = response.json().get("c")
    except requests.RequestException as error:
        logger.debug("finnhub failed for %s: %s", ticker, error)
        return None, None
    return (float(price), "USD") if price else (None, None)


def market_price_of(connection, ticker: str, stock: yf.Ticker, retries: int = 3, backoff_seconds: float = 1.0):
    """(price, currency, source, from_cache), or (None, None, None, False) when there is no
    quote to be had.

    Checks storage first: a quote already written today is reused, not re-fetched — Finnhub's
    free tier is 60 req/min, and a rerun during development (the exact case a --tickers X
    re-ingest exists for) must not re-spend that on tickers it already has today's price for.
    from_cache tells the caller to skip write_quote(); the row (and its updated_at) is already
    correct, and re-writing it would either be a no-op or, worse, bump updated_at to a time no
    live fetch actually happened at.

    Otherwise: Finnhub first when RK_FINNHUB_KEY is configured — it is a real, documented API,
    not yfinance's unofficial scrape. yfinance is the fallback — for whatever Finnhub's free
    tier does not cover (non-US listings), and for the case no key is configured at all. The
    source is returned rather than assumed by the caller — quote.source recording "yahoo" for
    a price Finnhub actually supplied would be exactly the kind of silently wrong data this
    file exists to stop producing. Currency is likewise only knowable HERE, not hardcoded by
    the caller.

    The yfinance path is retried: fast_info is Yahoo's unofficial, undocumented endpoint, and
    a single attempt measurably was not enough — one ordinary ingest run turned 19 of 21
    tickers priceless, every one of them ingestable again on the very next, unretried attempt.
    A genuinely delisted ticker (fl) still exhausts every attempt and correctly falls through
    to (None, None, None, False); the retries cost it a few idle seconds, nothing else.
    """
    cached = db.latest_quote(connection, cik_for(ticker))
    if cached is not None and cached["updated_at"].date() == dt.date.today():
        return cached["price"], cached["currency"], cached["source"], True

    if FINNHUB_KEY:
        price, currency = finnhub_price(ticker)
        if price is not None:
            return price, currency, "finnhub", False

    for attempt in range(retries):
        try:
            price = float(stock.fast_info["lastPrice"])
            currency = str(stock.fast_info["currency"] or "USD")
            return price, currency, "yahoo", False
        except Exception as error:  # delisted, throttled, or Yahoo simply has no quote
            logger.debug("no price (attempt %d/%d): %s", attempt + 1, retries, error)
            if attempt + 1 < retries:
                time.sleep(backoff_seconds * (attempt + 1))
    return None, None, None, False


def ingest_edgar(connection, ticker: str, price: float, quote_currency: str = None) -> int:
    """Download the annual-report XBRL, parse it, store the facts. Returns rows written.

    quote_currency is the currency `price` is quoted in — the valuation ratios divide one by
    the other, and for a foreign private issuer the two are not the same currency.
    """
    file_list, name, industry, sic = search_edgar_data(ticker)
    if not file_list:
        raise LookupError(f"no annual-report XBRL filings found for {ticker}")

    cik = cik_for(ticker)

    fiscal_year_end_month = None
    try:
        fiscal_year_end_month = int(str(file_list[0][1]).split("-")[1])
    except (IndexError, ValueError):
        pass

    # Committed on its own, BEFORE anything can fail. This used to sit in the same
    # transaction as the facts, so the caller's rollback threw the metadata away and left the
    # bare (cik, ticker) stub behind. sec_check.py then reads
    # coalesce(fiscal_year_end_month, 12), and fiscal_year_label(2023-01-28, 1) is "2022"
    # while (…, 12) is "2023" — so every company with a Jan–Jun fiscal year end whose EDGAR
    # ingest failed got adjudicated against the wrong fiscal year. Foot Locker is one.
    db.upsert_company(connection, cik, ticker, name, industry, fiscal_year_end_month, sic_code=sic)
    connection.commit()

    # A price is only needed for the valuation ratios; without one the sheet ratios still
    # compute, so a delisted ticker is still worth ingesting. NaN, not a fake 1.0: that
    # fallback used to flow straight into RO_I/KGV/KBGV (avg_eps / 1.0 × 100 reads as a real
    # 300% RoI) and metric_average would persist it as if it meant something. NaN is not
    # None, so RechenknechtBeta does not try to fetch a live price on our behalf, and
    # write_metric_averages' isfinite() check already drops it — the same "we do not know"
    # the dashboard shows for a priceless company today.
    # None, not "USD". That literal was never read from the filing and was silently correct
    # only while every ingested company was a US 10-K filer that really does report in USD.
    # With 20-F in scope it is provably wrong — Alibaba reports CNY, MUFG JPY, HDFC INR —
    # and it fails silently, storing foreign-currency financials labelled USD, which
    # corrupts every valuation ratio while looking normal in the dashboard.
    rechner = RechenknechtBeta(
        name, "", None, ticker, industry, file_list,
        market_price=price if price is not None else float("nan"),
        # The per-share figures below come out of the filing in its reporting currency while
        # the price is in the trading currency, so KGV/RoI/KBGV/Graham need the rate between
        # them. A callable, because the reporting currency is a result of the parse that is
        # about to happen, not an input to it — and for a US filer it resolves to a hard 1.0
        # without touching either the database or the network.
        fx_rate=lambda reporting_currency: fx.rate(connection, reporting_currency, quote_currency),
    )

    # Now that the filing has been parsed, the reporting currency is known. Coalesced by
    # upsert_company, so this adds to the row written above rather than replacing it.
    # Deliberately NOT written to quote.currency: that column describes the price, which for
    # every one of these ADRs really is USD.
    db.upsert_company(connection, cik, ticker, reporting_currency=rechner.currency)

    newest_accession = str(file_list[0][0].name).split("_")[1]
    written = db.write_facts(connection, cik, "edgar", rechner.df, accession=newest_accession)

    # The averages, which write_facts drops on the floor because an average is not a fiscal
    # year. Only the EDGAR frame is stored: metric_average has no `source` column, and the
    # whole valuation path is filing-derived (v_company_year and v_valuation both read
    # source = 'edgar'), so a Yahoo average landing on the same primary key would silently
    # pair one set of books with the other's book value.
    db.write_metric_averages(connection, cik, rechner.df, "7_YEAR_AVG", window_years=7)
    return written


def ingest_yahoo(connection, ticker: str, stock: yf.Ticker, price: float) -> int:
    facts = yahoo_source.load_facts(ticker, stock=stock)
    add_sheet_ratios(facts)
    add_averages(facts, price or 1.0, time_span=7)
    facts = facts.round(decimals=2)

    return db.write_facts(connection, cik_for(ticker), "yahoo", facts)


def refresh_quotes(connection, tickers: list) -> None:
    """Update the price only. The EDGAR/Yahoo facts underneath do not change intra-day, so
    re-pulling a decade of filings to fix a flaky price lookup would be a full-scan wearing a
    delta's costume — see CLAUDE.md's Delta Law."""
    for index, ticker in enumerate(tickers, start=1):
        stock = yf.Ticker(ticker)
        price, currency, source, from_cache = market_price_of(connection, ticker, stock)
        if price is None:
            print(f"[{index}/{len(tickers)}] {ticker}  no price (still)")
        elif from_cache:
            print(f"[{index}/{len(tickers)}] {ticker}  price={price} {currency} ({source}, cached)")
        else:
            db.write_quote(connection, cik_for(ticker), price, currency, source)
            connection.commit()
            print(f"[{index}/{len(tickers)}] {ticker}  price={price} {currency} ({source})")
        # This loop has no SEC/Yahoo work between tickers to pace it naturally, unlike
        # main()'s loop — without this, a large batch bursts past Finnhub's 60 req/min. Not
        # needed for a cache hit: that path never touches Finnhub at all.
        if FINNHUB_KEY and not from_cache:
            time.sleep(FINNHUB_PAUSE_SECONDS)


def refresh_sic_codes(connection, tickers: list) -> None:
    """Fill in company.sic_code from the submissions API, without re-ingesting anything.

    The numeric SIC code arrives in the SAME response search_edgar_data already reads the
    company name and sicDescription out of, so a company ingested before the column existed
    only needs that one request back — not the ~5 requests, the filing downloads and the full
    XBRL parse a re-ingest costs. Delta Law: the work per run is one HTTP GET per ticker.

    Deliberately does NOT call discard_filings afterwards, unlike every other path here:
    get_all_data creates filings/<ticker>/ as a side effect, but for a ticker whose filings
    are already on disk (--keep-filings) that same call would delete real downloads to clean
    up a directory it did not create.
    """
    for index, ticker in enumerate(tickers, start=1):
        try:
            data = _edgar.get_all_data(ticker)
            code = sic_code(data)
            if code is None:
                print(f"[{index}/{len(tickers)}] {ticker}  no SIC code in the submissions data")
                continue
            db.upsert_company(
                connection, cik_for(ticker), ticker,
                sic_description=data["sicDescription"], sic_code=code,
            )
            connection.commit()
            print(f"[{index}/{len(tickers)}] {ticker}  sic={code} {data['sicDescription']}")
        except Exception as error:
            connection = safe_rollback(connection)
            print(f"[{index}/{len(tickers)}] {ticker}  FAILED  {type(error).__name__}: {str(error)[:90]}")
        time.sleep(SEC_PAUSE_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20, help="how many tickers from the map")
    parser.add_argument("--tickers", help="comma separated tickers instead of the map")
    parser.add_argument("--keep-filings", action="store_true", help="do not delete filings after parsing")
    parser.add_argument("--apply-schema", action="store_true", help="create tables and views first")
    parser.add_argument(
        "--quotes-only", action="store_true",
        help="refresh price quotes for already-ingested companies, skip EDGAR/Yahoo facts",
    )
    parser.add_argument(
        "--sic-only", action="store_true",
        help="backfill company.sic_code from the submissions API, no filings, no facts",
    )
    args = parser.parse_args()

    with db.connect() as connection:
        if args.apply_schema:
            db.apply_schema(connection)
            print("schema applied\n")

        # Both refresh modes take the same ticker list — the ones named on the command line,
        # or every company already stored — so they share the resolution instead of each
        # carrying its own copy of it.
        if args.quotes_only or args.sic_only:
            tickers = (
                [t.strip().lower() for t in args.tickers.split(",") if t.strip()]
                if args.tickers
                else [row[0] for row in connection.execute("select ticker from company order by ticker")]
            )
            what, refresh = (
                ("quotes", refresh_quotes) if args.quotes_only else ("SIC codes", refresh_sic_codes)
            )
            print(f"refreshing {what} for {len(tickers)} tickers: {', '.join(tickers)}\n")
            refresh(connection, tickers)
            print("\ndone")
            return

        tickers = load_tickers(args.limit, args.tickers)
        print(f"ingesting {len(tickers)} tickers: {', '.join(tickers)}\n")

        for index, ticker in enumerate(tickers, start=1):
            # try/finally, not a trailing call: the `continue` below, anything escaping
            # log_run/commit, and Ctrl-C all used to leave the downloaded filings on disk —
            # which is how "filings are discarded after parsing" became untrue in practice.
            try:
                # Unguarded until now: cik_for(ticker) raises a bare KeyError for any ticker
                # not in the SEC's map (a batch list built from an upstream source can contain
                # one — "nan", the str() of a missing value, took down a live 20-hour overnight
                # run at 32% here). One bad ticker must skip, not crash the remaining thousands.
                try:
                    stock = yf.Ticker(ticker)
                    # Reads only (the quote cache check): safe before upsert_company below even
                    # for a ticker that has never been ingested, since a SELECT has no foreign
                    # key to violate — only the write a few lines down needs the company row to
                    # already exist.
                    price, currency, price_source, price_cached = market_price_of(connection, ticker, stock)
                except Exception as error:
                    print(f"[{index}/{len(tickers)}] {ticker}  FAILED  {type(error).__name__}: {str(error)[:90]}")
                    continue
                print(f"[{index}/{len(tickers)}] {ticker}  price={price}" + ("  (cached)" if price_cached else ""))

                # Register the company before either source runs. It used to be created only
                # inside the EDGAR path, so whenever EDGAR failed the Yahoo facts had no parent
                # row and died on a foreign key violation — a second, misleading failure.
                try:
                    db.upsert_company(connection, cik_for(ticker), ticker)
                    # Alongside the company, not inside either source: a price is worth
                    # keeping even when both parsers fail, and it is what makes the
                    # valuation ratios recomputable later instead of frozen at ingest time.
                    # Skipped on a cache hit — the row is already correct and a rewrite would
                    # only bump updated_at to a time no live fetch actually happened at.
                    if price is not None and not price_cached:
                        db.write_quote(connection, cik_for(ticker), price, currency, price_source)
                    connection.commit()
                except Exception as error:
                    connection = safe_rollback(connection)
                    print(f"    skipped: {type(error).__name__}: {error}")
                    continue

                for source, run in (("edgar", ingest_edgar), ("yahoo", ingest_yahoo)):
                    started_at = dt.datetime.now(dt.timezone.utc)
                    try:
                        written = (
                            run(connection, ticker, price, currency)
                            if source == "edgar"
                            else run(connection, ticker, stock, price)
                        )
                        db.log_run(connection, ticker, source, "ok", started_at, written)
                        print(f"    {source:6s} ok      {written} facts")
                    except Exception as error:
                        connection = safe_rollback(connection)
                        # log_run itself can still be the thing that discovers the pooler
                        # already dropped this connection (safe_rollback's rollback() can
                        # succeed locally without proving the socket is alive) — an
                        # exception here is not caught by this except block, and with no
                        # outer handler it used to escape and take the whole batch down
                        # with it. One bad ticker must skip, not crash the remaining
                        # thousands, so this gets the same guard as every other write here.
                        try:
                            db.log_run(connection, ticker, source, "failed", started_at, 0,
                                       f"{type(error).__name__}: {error}")
                        except Exception as log_error:
                            connection = safe_rollback(connection)
                            print(f"    {source:6s} FAILED  {type(error).__name__}: {str(error)[:90]}"
                                  f"  (log_run also failed, connection reset: {log_error})")
                        else:
                            print(f"    {source:6s} FAILED  {type(error).__name__}: {str(error)[:90]}")
                    try:
                        connection.commit()
                    except Exception as commit_error:
                        connection = safe_rollback(connection)
                        print(f"    commit failed, connection reset: {commit_error}")

                time.sleep(SEC_PAUSE_SECONDS)
            finally:
                if not args.keep_filings:
                    discard_filings(ticker)

    print("\ndone")


if __name__ == "__main__":
    main()
