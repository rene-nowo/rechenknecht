"""The parts of the backtest report that can go wrong without the database or Yahoo.

Everything here is either an argument the script must never guess, or the row filter that
decides WHICH universe the signal is scored over — and that second one is silent when it is
wrong: skip screener.industry_mask() and the report still prints four tidy buckets, computed
over 1,362 banks and REITs that pages/Screener.py hides by default, i.e. over a different
signal than the one the report claims to be validating.

The fetch loop and the DB reads are verified manually against the real Supabase DB (see the
plan's `done_means`), the same way ingest.py/sec_check.py/ingest_macro.py are.
"""

import datetime

import pandas as pd
import pytest

import backtest_report
from src import backtest, metrics, screener

CUTOFF = datetime.date(2024, 8, 13)
TODAY = datetime.date(2026, 8, 12)

COMPANY_COLUMNS = [
    "cik", "ticker", "sic_code", "sic_description", "fiscal_year_end_month",
    "reporting_currency", "currency", "price_as_of",
]


def companies(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=COMPANY_COLUMNS)


def company(cik, ticker, sic_code=3571) -> tuple:
    return (cik, ticker, sic_code, "Whatever", 12, None, "USD", CUTOFF)


def facts(*rows) -> pd.DataFrame:
    return pd.DataFrame(
        list(rows), columns=["cik", screener.FISCAL_YEAR, screener.CONCEPT, screener.VALUE]
    )


def complete(cik, year=2022) -> list:
    """The two concepts a company needs before it can ever produce a Graham margin."""
    return [
        (cik, year, metrics.EPS, 5.0),
        (cik, year, metrics.BOOK_VALUE_PER_SHARE, 50.0),
    ]


# ── the arguments the script must never guess ────────────────────────────────────────────


def test_the_cutoff_is_parsed_into_a_real_date():
    assert backtest_report.parse_args(["--cutoff", "2024-08-13"]).cutoff == CUTOFF


def test_a_missing_cutoff_fails_fast_instead_of_defaulting():
    """There is no sensible default cutoff. A script that picked one would silently produce a
    report about a different date than the operator believed, and the number it prints looks
    exactly the same either way."""
    with pytest.raises(SystemExit):
        backtest_report.parse_args([])


def test_a_malformed_cutoff_is_rejected_rather_than_coerced():
    with pytest.raises(SystemExit):
        backtest_report.parse_args(["--cutoff", "13.08.2024"])


def test_limit_and_lag_months_are_real_flags_with_the_documented_defaults():
    """--lag-months exposes the one approximation the whole report rests on, so it has to be
    steerable without editing code; --limit is what makes a sanity run possible at all."""
    default = backtest_report.parse_args(["--cutoff", "2024-08-13"])

    assert default.limit is None
    assert default.lag_months == backtest.DEFAULT_LAG_MONTHS

    given = backtest_report.parse_args(["--cutoff", "2024-08-13", "--limit", "50", "--lag-months", "12"])

    assert given.limit == 50
    assert given.lag_months == 12


# ── which universe the signal is scored over ─────────────────────────────────────────────


def test_banks_and_reits_are_out_of_the_pool_exactly_as_the_live_page_hides_them():
    """F2. pages/Screener.py:119-129 applies industry_mask BEFORE computing the signal, and
    screener.signal() takes its cheapness quartile from whatever frame it is handed
    (src/screener.py:294) — so scoring a universe that still contains SIC 6000-6799 produces a
    quartile the live page would never produce, and the report's 🟢 stops meaning the page's 🟢.
    """
    pool = backtest_report.eligible_pool(
        companies(company("bank", "jpm", sic_code=6021), company("tech", "msft", sic_code=3571)),
        facts(*complete("bank"), *complete("tech")),
        CUTOFF,
    )

    assert list(pool["ticker"]) == ["msft"]


def test_an_uncategorized_company_stays_in_the_pool():
    """industry_mask's uncategorized=True default, which pages/Screener.py also ships on
    (line 91). A company ingested before company.sic_code existed must not vanish out of both
    the financial filter and the tech filter without a word — measured, 43 of the eligible
    pool are in exactly that state."""
    pool = backtest_report.eligible_pool(
        companies(company("nosic", "xyz", sic_code=None)), facts(*complete("nosic")), CUTOFF
    )

    assert list(pool["ticker"]) == ["xyz"]


def test_a_company_without_point_in_time_inputs_is_not_in_the_pool():
    """No EPS or no book value at the cutoff means no Graham number, which means ⚪ forever —
    fetching a price for it spends a Yahoo call to learn nothing. 5,578 of 6,773 companies
    clear this bar (measured); the rest are not candidates on that date."""
    pool = backtest_report.eligible_pool(
        companies(company("full", "aaa"), company("eps_only", "bbb")),
        facts(*complete("full"), ("eps_only", 2022, metrics.EPS, 5.0)),
        CUTOFF,
    )

    assert list(pool["ticker"]) == ["aaa"]


def test_a_fiscal_year_that_is_not_public_yet_keeps_a_company_out_of_the_pool():
    """The point-in-time filter reaching all the way through to the pool: the same company,
    the same rows, two lag settings. With a 6-month lag its FY2023 was public by 2024-06-30;
    with a 24-month lag it was not, so it has no inputs at the cutoff and is not a candidate.
    A --lag-months that changed nothing here would be a decorative flag."""
    both = (companies(company("a", "aaa")), facts(*complete("a", year=2023)), CUTOFF)

    assert len(backtest_report.eligible_pool(*both, lag_months=6)) == 1
    assert len(backtest_report.eligible_pool(*both, lag_months=24)) == 0


def test_the_pool_carries_every_column_add_valuation_and_signal_read():
    """The pool is handed straight to screener.add_valuation() once a price is attached, so a
    missing column here is an AttributeError 4,216 Yahoo calls into a two-hour run."""
    pool = backtest_report.eligible_pool(
        companies(company("a", "aaa")), facts(*complete("a")), CUTOFF
    )

    assert {"ticker", "currency", "reporting_currency"} <= set(pool.columns)
    assert set(backtest.PIT_COLUMNS) <= set(pool.columns)


# ── the persistence the run leaves behind ────────────────────────────────────────────────
#
# Same stub-connection pattern as test/test_db.py: the functions' whole job is deciding
# WHICH rows reach the database, so a connection that records what it was asked to insert
# tests exactly that.


class _Cursor:
    def __init__(self):
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, rows):
        self.rows.extend(rows)


class _Connection:
    def __init__(self):
        self._cursor = _Cursor()
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    @property
    def rows(self):
        return self._cursor.rows


def priced_pool(*rows) -> pd.DataFrame:
    """One row per company: (cik, price_as_of, currency, today_date) — the columns the
    close-history store consults."""
    frame = pd.DataFrame(list(rows), columns=["cik", "price_as_of", "currency", backtest_report.TODAY_DATE])
    return frame.set_index("cik")


def closes(*pairs) -> pd.Series:
    """A yahoo-shaped Close series: tz-aware DatetimeIndex, float values."""
    dates, prices = zip(*pairs)
    index = pd.to_datetime(list(dates)).tz_localize("America/New_York")
    return pd.Series(list(prices), index=index, dtype="float64")


def test_fetch_prices_keeps_the_series_and_the_resolved_cutoff_date(monkeypatch):
    """The fetch already had both in hand and threw them away: the cutoff's ACTUAL trading
    day (discarded as `_`) and the whole fetched range (reduced to two points). Both are what
    the persistence functions below consume."""
    series = closes(("2024-08-12", 10.0), ("2024-08-13", 11.0), ("2026-08-12", 12.0))
    monkeypatch.setattr(backtest_report.yahoo_source, "closes", lambda *a, **k: series)
    pool = pd.DataFrame({"ticker": ["aaa"]}, index=pd.Index(["c1"], name="cik"))

    frame, history = backtest_report.fetch_prices(pool, CUTOFF, TODAY, pause_seconds=0)

    assert frame.loc["c1", backtest_report.CUTOFF_PRICE] == 11.0
    assert frame.loc["c1", backtest_report.CUTOFF_DATE] == datetime.date(2024, 8, 13)
    assert frame.loc["c1", backtest_report.TODAY_PRICE] == 12.0
    assert frame.loc["c1", backtest_report.TODAY_DATE] == datetime.date(2026, 8, 12)
    assert history["c1"] is series


def test_the_most_recent_close_is_never_written_to_quote():
    """The existing store_cutoff_quotes guarantee — today's price belongs to the routine
    ingest, never to this script — applied to the whole range: every fetched close is stored
    EXCEPT the one nearest_close(series, today) resolved, and the tz-aware yahoo timestamps
    arrive as plain dates."""
    connection = _Connection()
    pool = priced_pool(("c1", CUTOFF, "USD", datetime.date(2026, 8, 12)))
    history = {"c1": closes(("2024-08-12", 10.0), ("2024-08-13", 11.0), ("2026-08-12", 12.0))}

    written = backtest_report.store_close_history(connection, pool, history)

    assert written == 2
    assert connection.rows == [
        ("c1", datetime.date(2024, 8, 12), 10.0, "USD", "yahoo"),
        ("c1", datetime.date(2024, 8, 13), 11.0, "USD", "yahoo"),
    ]


def test_a_company_with_no_quote_row_gets_no_history():
    """The other store_cutoff_quotes guard, and the one that keeps the claim "this changes
    nothing on the live Screener" true: for a company with no quote at all, ANY row written
    here would become its latest quote, and a delisted company's stale close would surface as
    its current price. Measured: 1,144 of the 4,216 eligible companies are in that state."""
    connection = _Connection()
    pool = priced_pool(("c1", None, "USD", datetime.date(2026, 8, 12)))
    history = {"c1": closes(("2024-08-12", 10.0), ("2026-08-12", 12.0))}

    written = backtest_report.store_close_history(connection, pool, history)

    assert written == 0
    assert connection.rows == []
    assert connection.commits == 0


def test_close_history_commits_per_company_and_falls_back_to_usd():
    """One commit per company, the same long-run discipline store_cutoff_quotes documents —
    and the same currency fallback it uses, because `currency` comes from the quote row and a
    company can have one without a currency the frame kept as a string."""
    connection = _Connection()
    pool = priced_pool(
        ("c1", CUTOFF, float("nan"), datetime.date(2026, 8, 12)),
        ("c2", CUTOFF, "EUR", datetime.date(2026, 8, 12)),
    )
    history = {
        "c1": closes(("2024-08-12", 10.0), ("2026-08-12", 12.0)),
        "c2": closes(("2024-08-12", 20.0), ("2026-08-12", 22.0)),
    }

    written = backtest_report.store_close_history(connection, pool, history)

    assert written == 2
    assert connection.commits == 2
    assert connection.rows == [
        ("c1", datetime.date(2024, 8, 12), 10.0, "USD", "yahoo"),
        ("c2", datetime.date(2024, 8, 12), 20.0, "EUR", "yahoo"),
    ]


def valued(cik="c1", **overrides) -> pd.DataFrame:
    """One fully-populated row of the frame score() hands back — pool columns plus the three
    ratios add_valuation appends."""
    row = {
        "avg_eps": 5.0,
        metrics.BOOK_VALUE_PER_SHARE: 50.0,
        metrics.CONSERVATIVE_BOOK_VALUE_PER_SHARE: 40.0,
        "roa": 9.0,
        "ebit_margin": 14.0,
        "equity_ratio": 36.0,
        metrics.GRAHAM_MARGIN: 0.2,
        metrics.KGV: 12.0,
        metrics.RO_I: 8.5,
        backtest_report.CUTOFF_PRICE: 100.0,
        backtest_report.CUTOFF_DATE: datetime.date(2024, 8, 13),
        backtest_report.TODAY_PRICE: 110.0,
        backtest_report.TODAY_DATE: TODAY,
    }
    row.update(overrides)
    return pd.DataFrame([row], index=pd.Index([cik], name="cik"))


def test_a_complete_backtest_result_row_carries_every_column_in_schema_order():
    connection = _Connection()
    frame = valued()

    written = backtest_report.store_backtest_results(
        connection, frame,
        signal=pd.Series([screener.MIXED], index=frame.index),
        returns=pd.Series([0.1], index=frame.index),
        cutoff=CUTOFF,
    )

    assert written == 1
    assert connection.commits == 1
    assert connection.rows == [(
        "c1", CUTOFF, "MIXED",
        5.0, 50.0, 40.0, 9.0, 14.0, 36.0,
        0.2, 12.0, 8.5,
        100.0, datetime.date(2024, 8, 13), 110.0, TODAY, 0.1,
    )]


def test_the_signal_is_stored_as_its_constant_name_not_the_glyph():
    """The brief's own acceptance query is `where signal = 'CHEAP_AND_STRONG'` — and
    screener's constants ARE the glyphs (src/screener.py:104), so what signal() returns is
    mapped back to the constant's NAME before it is stored."""
    connection = _Connection()
    frame = valued()

    backtest_report.store_backtest_results(
        connection, frame,
        signal=pd.Series([screener.CHEAP_AND_STRONG], index=frame.index),
        returns=pd.Series([0.1], index=frame.index),
        cutoff=CUTOFF,
    )

    assert connection.rows[0][2] == "CHEAP_AND_STRONG"


def test_a_company_without_a_cutoff_price_is_not_stored():
    """No cutoff price means no return and no scoreable valuation — nothing meaningful to
    keep. It stays visible in the printed rollup's COMPANIES-vs-RATED gap instead."""
    connection = _Connection()
    frame = valued(**{backtest_report.CUTOFF_PRICE: float("nan")})

    written = backtest_report.store_backtest_results(
        connection, frame,
        signal=pd.Series([screener.UNKNOWN], index=frame.index),
        returns=pd.Series([float("nan")], index=frame.index),
        cutoff=CUTOFF,
    )

    assert written == 0
    assert connection.rows == []


def test_missing_inputs_are_stored_as_null_not_nan():
    """Postgres numeric accepts 'NaN' happily — the exact poison the measured Infinity
    incident put in `fact`. A NaN ratio (an ADS-mismatch ticker's blanked margin, a failed
    today-price) becomes SQL NULL, the one spelling of "missing" every consumer understands."""
    connection = _Connection()
    frame = valued(**{
        metrics.GRAHAM_MARGIN: float("nan"),
        metrics.KGV: float("nan"),
        backtest_report.TODAY_PRICE: float("nan"),
        backtest_report.TODAY_DATE: None,
    })

    backtest_report.store_backtest_results(
        connection, frame,
        signal=pd.Series([screener.UNKNOWN], index=frame.index),
        returns=pd.Series([float("nan")], index=frame.index),
        cutoff=CUTOFF,
    )

    row = connection.rows[0]
    assert row[9] is None      # graham_margin
    assert row[10] is None     # kgv
    assert row[14] is None     # today_price
    assert row[15] is None     # today_price_as_of
    assert row[16] is None     # forward_return


# ── the report file ──────────────────────────────────────────────────────────────────────


def test_the_reports_directory_is_created_on_demand(tmp_path):
    """A generated artifact directory that is gitignored does not exist on a fresh clone, and
    the run that would create it is the two-hour one — failing at the last step, after every
    Yahoo call has already been spent, is the expensive way to find that out."""
    rollup = backtest.backtest_rollup(
        pd.DataFrame(index=pd.Index(["c0"], name="cik")),
        pd.Series([screener.MIXED], index=["c0"]),
        pd.Series([0.1], index=["c0"]),
    )
    target = tmp_path / "reports"

    written = backtest_report.write_report(rollup, CUTOFF, directory=target)

    assert target.is_dir()
    assert written == target / "backtest_2024-08-13.csv"
    assert list(pd.read_csv(written)[screener.SIGNAL]) == list(screener.SIGNAL_ORDER)
