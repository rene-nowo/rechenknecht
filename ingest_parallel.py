"""ingest.py's per-ticker work, driven by a thread pool instead of a serial loop.

Same sources, same parsers, same tables — this file owns ONLY the concurrency: how many
tickers run at once, how the shared network budgets are spent, and how the per-thread state
that ingest.py never needed is kept apart. Every piece of domain logic is imported from
ingest.py rather than restated, so the two entry points cannot drift on what a fact is.

Why threads and not processes: per ticker the work is ~10-15 sequential HTTP round-trips
(1 SEC submissions call, 5-10 filing downloads, 3+ Yahoo calls) plus an XBRL parse. Measured
on the live serial run: ~17% of one core on a 32-core box, i.e. the loop spends its time
waiting on sockets. Threads overlap that wait; processes would too, but would also each need
their own token budget, and the SEC ceiling below is per IP — a budget that only works if
every worker waits on the SAME object.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    export RK_FINNHUB_KEY="$(op read 'op://Server/Finnhub/credential')"
    python ingest_parallel.py --tickers jpm,bac,gs --workers 6
"""

import argparse
import datetime as dt
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import yfinance as yf

import app_edgar
import ingest
from ingest import cik_for, ingest_edgar, ingest_yahoo, market_price_of, safe_rollback
from src import db
from src.Edgar_api import EDGAR_API, discard_filings

# SEC fair access is "no more than 10 requests/second", counted PER IP — so the ceiling is
# shared by every thread here AND by any other ingest already running on this machine. 5/s
# leaves room for a serial ingest.py running alongside this one (it spends ~2/s) without the
# two of them together crossing 10.
SEC_REQUESTS_PER_SECOND = 5.0

# Yahoo publishes no limit and answers 429 under concurrent load from one IP. This paces the
# calls that reach it (the quote lookup and the fundamentals pull).
MARKET_REQUESTS_PER_SECOND = 2.0

# How long every thread stands still after one 429, so a throttle answer slows the whole
# process down rather than just the thread unlucky enough to receive it.
THROTTLE_PENALTY_SECONDS = 10.0


class RateLimiter:
    """A start-time budget shared by all threads: at most `per_second` acquisitions a second.

    Deliberately not a token bucket with a burst allowance — a bucket that has been idle
    hands out N requests at once, which is exactly the shape a per-IP ceiling punishes.
    Spacing the starts evenly can never burst. The sleep happens outside the lock, so threads
    queue for a slot rather than for each other's waiting.
    """

    def __init__(self, per_second: float) -> None:
        self._interval = 1.0 / per_second
        self._lock = threading.Lock()
        self._next_at = 0.0
        self.throttle_events = 0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self._interval
        if wait > 0:
            time.sleep(wait)

    def penalize(self, seconds: float) -> None:
        """Push every thread's next slot back. Called on a 429: the answer is about the IP,
        not about the one request that happened to receive it."""
        with self._lock:
            self.throttle_events += 1
            self._next_at = max(self._next_at, time.monotonic()) + seconds


SEC_LIMITER = RateLimiter(SEC_REQUESTS_PER_SECOND)
MARKET_LIMITER = RateLimiter(MARKET_REQUESTS_PER_SECOND)


class _GatedRequests:
    """`requests` with .get() gated by a limiter, to be put in another module's namespace.

    Gating here rather than around search_edgar_data() is what makes the budget exact: the
    number of SEC requests per ticker is not fixed (one submissions call plus one per annual
    filing, plus get_all_data's own retries), so a per-ticker gate would have to guess it.
    Everything other than .get is delegated, so this stays a drop-in for the real module.
    """

    def __init__(self, limiter: RateLimiter, real, penalty: float) -> None:
        self._limiter = limiter
        self._real = real
        self._penalty = penalty

    def get(self, *args, **kwargs):
        self._limiter.acquire()
        response = self._real.get(*args, **kwargs)
        if response.status_code in (429, 503):
            self._limiter.penalize(self._penalty)
        return response

    def __getattr__(self, name):
        return getattr(self._real, name)


class _ThreadLocalEdgar:
    """One EDGAR_API per thread, behind the attribute surface of a single instance.

    app_edgar holds ONE module-global EDGAR_API, and that object carries per-ticker state
    between two calls: get_all_data() assigns self.ticker/self.cik, and get_file() reads them
    back to build the archive URL and the download path. Two threads sharing it interleave —
    one ticker's CIK with another's accession number, which SEC answers 404 (verified live:
    .../data/70858/000162828026008131/jpm-...xml -> 404 while the same accession under CIK
    19617 -> 200), and one ticker's filing written into another ticker's directory, which the
    other thread's discard_filings() then deletes out from under it. The result is not corrupt
    facts but randomly missing companies, logged as if they had no XBRL at all.

    Swapping the global for this proxy keeps that state per thread. Constructing EDGAR_API
    re-parses the 12k-row CIK map, which is why it happens once per thread and not per call.
    """

    def __init__(self) -> None:
        self._local = threading.local()

    def __getattr__(self, name):
        instance = getattr(self._local, "edgar", None)
        if instance is None:
            instance = EDGAR_API()
            self._local.edgar = instance
        return getattr(instance, name)


_connections = threading.local()
_print_lock = threading.Lock()
# Every connection ever handed out, so the main thread can close them at the end — it cannot
# reach another thread's thread-local storage, and a pooled Supabase session left open is a
# slot nothing else can use until it times out.
_all_connections = []
_all_connections_lock = threading.Lock()


def thread_connection() -> "db.psycopg.Connection":
    """This thread's own database connection, opened on first use.

    A psycopg connection is not safe to share across threads, and the serial ingest passes
    exactly one around. One per worker rather than a pool: the pool size and the worker count
    would be the same number, so a pool would only add a way for them to disagree.
    """
    connection = getattr(_connections, "connection", None)
    if connection is None:
        connection = db.connect()
        _connections.connection = connection
        with _all_connections_lock:
            _all_connections.append(connection)
    return connection


def rollback() -> "db.psycopg.Connection":
    """safe_rollback() for this thread, storing the result back — and PROVING the connection
    works before handing it back.

    safe_rollback treats "rollback() did not raise" as "the connection is alive". That holds
    for the serial ingest, whose connection is never idle more than ~7s, and is false here:
    a worker spends 15-60s downloading and parsing filings with its session idle, long enough
    for Supabase's pooler to reap it, and psycopg's rollback() on an idle connection with no
    open transaction never touches the socket — so it returns cleanly on a dead one. The
    deadness then surfaces on the next real query, which is db.log_run() INSIDE the error
    handler, where the exception escapes the handler, propagates through the future, and
    kills the whole pool. Measured: a 6-worker run died at 29/30 exactly this way
    (ingest_parallel.py:224 -> db.py:286 -> psycopg OperationalError).

    One round-trip settles it. safe_rollback is still what runs first, so its own recovery is
    kept rather than reimplemented.
    """
    previous = thread_connection()
    connection = safe_rollback(previous)
    try:
        connection.execute("select 1")
        connection.rollback()
    except Exception:
        try:
            connection.close()
        except Exception:
            pass
        connection = db.connect()
    _connections.connection = connection
    if connection is not previous:
        with _all_connections_lock:
            _all_connections.append(connection)
    return connection


def process_ticker(ticker: str, index: int, total: int, keep_filings: bool) -> bool:
    """One ticker, start to finish. Returns whether EDGAR facts were written.

    The body mirrors ingest.main()'s per-ticker block because that block is a loop body, not
    a function — everything it CALLS is imported. Kept in the same order for the same reasons
    documented there: the company row is registered before either source runs so a failing
    EDGAR pass cannot leave the Yahoo facts without a parent row, and the filings are
    discarded in a finally whatever the outcome.
    """
    lines = [f"[{index}/{total}] {ticker}"]
    edgar_ok = False
    try:
        connection = thread_connection()
        try:
            stock = yf.Ticker(ticker)
            MARKET_LIMITER.acquire()
            price, currency, price_source, price_cached = market_price_of(connection, ticker, stock)
        except Exception as error:
            lines.append(f"    FAILED  {type(error).__name__}: {str(error)[:90]}")
            return False
        lines[0] += f"  price={price}" + ("  (cached)" if price_cached else "")

        try:
            db.upsert_company(connection, cik_for(ticker), ticker)
            if price is not None and not price_cached:
                db.write_quote(connection, cik_for(ticker), price, currency, price_source)
            connection.commit()
        except Exception as error:
            connection = rollback()
            lines.append(f"    skipped: {type(error).__name__}: {error}")
            return False

        for source, run in (("edgar", ingest_edgar), ("yahoo", ingest_yahoo)):
            started_at = dt.datetime.now(dt.timezone.utc)
            try:
                if source == "edgar":
                    written = run(connection, ticker, price, currency)
                else:
                    MARKET_LIMITER.acquire()
                    written = run(connection, ticker, stock, price)
                db.log_run(connection, ticker, source, "ok", started_at, written)
                lines.append(f"    {source:6s} ok      {written} facts")
                if source == "edgar":
                    edgar_ok = True
            except Exception as error:
                lines.append(f"    {source:6s} FAILED  {type(error).__name__}: {str(error)[:90]}")
                # Recording the failure must not become a second, fatal one. ingest.py's rule
                # is "one bad ticker must skip, not crash the remaining thousands" — under a
                # pool that rule needs teeth here, because an exception raised in this handler
                # travels out through the future and takes every other worker with it.
                try:
                    connection = rollback()
                    db.log_run(connection, ticker, source, "failed", started_at, 0,
                               f"{type(error).__name__}: {error}")
                except Exception as logging_error:
                    lines.append(f"    {source:6s} (could not log: {type(logging_error).__name__})")
            try:
                connection.commit()
            except Exception:
                connection = rollback()
        return edgar_ok
    except Exception as error:
        # Last line of defence: thread_connection() itself can fail, and anything that
        # escapes here would surface only when main() consumes the futures — by which point
        # the pool is already dead and the tickers still queued never ran at all.
        lines.append(f"    ABORTED  {type(error).__name__}: {str(error)[:90]}")
        return False
    finally:
        if not keep_filings:
            discard_filings(ticker)
        with _print_lock:
            print("\n".join(lines), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers", required=True, help="comma separated tickers")
    parser.add_argument("--workers", type=int, default=6, help="concurrent tickers")
    parser.add_argument("--keep-filings", action="store_true", help="do not delete filings after parsing")
    parser.add_argument("--sec-rate", type=float, default=SEC_REQUESTS_PER_SECOND,
                        help="shared SEC requests/second across ALL workers")
    parser.add_argument("--market-rate", type=float, default=MARKET_REQUESTS_PER_SECOND,
                        help="shared Yahoo/Finnhub calls/second across ALL workers")
    args = parser.parse_args()

    global SEC_LIMITER, MARKET_LIMITER
    SEC_LIMITER = RateLimiter(args.sec_rate)
    MARKET_LIMITER = RateLimiter(args.market_rate)

    # Both swaps are process-local and happen before any worker starts. app_edgar.edgar is
    # the object with the per-ticker state; Edgar_api.requests is every SEC call it makes.
    app_edgar.edgar = _ThreadLocalEdgar()
    import src.Edgar_api as edgar_module
    edgar_module.requests = _GatedRequests(SEC_LIMITER, edgar_module.requests, THROTTLE_PENALTY_SECONDS)
    # contra: Finnhub (ingest.requests, 60 req/min) and yfinance's internal HTTP are paced by
    # MARKET_LIMITER at the CALL boundary, not per request — one market_price_of() call can
    # cost up to 4 requests (1 Finnhub + 3 yfinance retries). Ceiling: a batch where most
    # tickers exhaust the retries can still burst past 60/min. Upgrade path: gate
    # ingest.requests with a _GatedRequests too and give yfinance a rate-limited session.

    tickers = [t.strip().lower() for t in args.tickers.split(",") if t.strip()]
    print(f"ingesting {len(tickers)} tickers with {args.workers} workers "
          f"(SEC {args.sec_rate}/s, market {args.market_rate}/s shared)\n")

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(
            lambda item: process_ticker(item[1], item[0], len(tickers), args.keep_filings),
            enumerate(tickers, start=1),
        ))
    elapsed = time.monotonic() - started

    for connection in _all_connections:
        try:
            connection.close()
        except Exception:
            pass

    ok = sum(1 for r in results if r)
    print(f"\ndone  {len(tickers)} tickers in {elapsed:.1f}s "
          f"({len(tickers) / (elapsed / 60):.2f}/min, {ok} with edgar facts)")
    print(f"throttle events: SEC {SEC_LIMITER.throttle_events}, market {MARKET_LIMITER.throttle_events}")


if __name__ == "__main__":
    main()
