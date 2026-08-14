"""What write_facts is allowed to put in the `fact` table.

No database: write_facts' whole job is deciding WHICH cells of an analysis frame become rows,
so a stub connection that records what it was asked to insert tests exactly that, the same way
test_metrics.py stubs yf.Ticker and test_ingest_macro.py stubs the requests response.

The case that matters is Infinity. MEASURED 2026-08-13 against the real Supabase DB: `fact`
held 3,788 non-finite values (1,345 edgar / 494 companies, 2,443 yahoo / 793) — an EBIT-margin
over zero revenue, an EPS over a zero share count. Postgres numeric accepts 'Infinity' happily,
and pd.isna(inf) is False, so the old guard waved them straight through.

They are not harmless noise downstream: metrics.valuation() turns an infinite average EPS into
an INFINITE Graham margin, which is the largest value in any frame, so screener.signal() puts
that company in the top cheapness quartile and the Screener reports it 🟢 — the one bucket the
whole page exists to make trustworthy. write_metric_averages already refused to store a
non-finite value (math.isfinite, src/db.py); this pins the same rule for the fact table.
"""

import datetime

import pandas as pd

from src import db


class _Cursor:
    """The three things write_facts touches on a psycopg cursor."""

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

    def cursor(self):
        return self._cursor

    @property
    def rows(self):
        return self._cursor.rows


def stored(frame: pd.DataFrame) -> dict:
    """(concept, year) -> value for everything write_facts decided to persist."""
    connection = _Connection()
    db.write_facts(connection, "0000000001", "edgar", frame)
    return {(concept, year): value for _, _, year, concept, value, _ in connection.rows}


def test_an_infinite_value_is_not_stored():
    """THE fix. A division by zero at ingest time is not a measurement, and once it is in the
    table it outranks every real company on cheapness."""
    frame = pd.DataFrame({"2022": [1.5, float("inf")], "2023": [2.5, float("-inf")]},
                         index=["EPS", "book_value_per_share"])

    assert stored(frame) == {("EPS", 2022): 1.5, ("EPS", 2023): 2.5}


def test_a_missing_value_is_still_not_stored():
    """The behaviour that already worked, pinned so the new guard cannot regress it — a NULL
    and a NaN both mean "this company never reported it"."""
    frame = pd.DataFrame({"2022": [1.5, float("nan")]}, index=["EPS", "RoA"])

    assert stored(frame) == {("EPS", 2022): 1.5}


def test_every_finite_value_is_still_stored():
    """The MUST-ALLOW direction, and the one a guard-only test suite would miss entirely: a
    filter that rejects everything also rejects every infinity and would look green here
    without it. Negative numbers and zero are ordinary results — a loss-making year and a
    company with no goodwill — and must survive."""
    frame = pd.DataFrame({"2022": [-1.5, 0.0, 1e12], "2023": [2.5, -0.001, 3.0]},
                         index=["EPS", "RoA", "revenue"])

    assert len(stored(frame)) == 6
    assert stored(frame)[("EPS", 2022)] == -1.5
    assert stored(frame)[("RoA", 2022)] == 0.0


def test_the_average_column_is_still_skipped():
    """write_facts keeps only 4-digit-year columns; 7_YEAR_AVG is derivable from them and is
    persisted separately by write_metric_averages. Pinned because the new guard sits in the
    same loop as that check."""
    frame = pd.DataFrame({"2022": [1.5], "7_YEAR_AVG": [1.5]}, index=["EPS"])

    assert stored(frame) == {("EPS", 2022): 1.5}


# ── write_quotes: many days for one company, in one statement ────────────────────────────


def test_write_quotes_stores_every_finite_close():
    """The bulk sibling of write_quote: a whole (date, price) history in one executemany
    rather than ~500 round trips per company — the shape write_macro_observations already
    uses for FRED's (date, value) pairs."""
    connection = _Connection()

    written = db.write_quotes(
        connection,
        "0000000001",
        [(datetime.date(2024, 8, 12), 10.0), (datetime.date(2024, 8, 13), 11.5)],
        currency="USD",
    )

    assert written == 2
    assert connection.rows == [
        ("0000000001", datetime.date(2024, 8, 12), 10.0, "USD", "yahoo"),
        ("0000000001", datetime.date(2024, 8, 13), 11.5, "USD", "yahoo"),
    ]


def test_write_quotes_skips_a_non_finite_close():
    """Yahoo's Close column can carry NaN rows, and Postgres numeric would happily store
    'NaN' — the same class of poison as the measured Infinity incident in `fact`. The same
    math.isfinite rule the other writers follow applies here."""
    connection = _Connection()

    written = db.write_quotes(
        connection,
        "0000000001",
        [(datetime.date(2024, 8, 12), float("nan")), (datetime.date(2024, 8, 13), 11.5)],
    )

    assert written == 1
    assert connection.rows == [("0000000001", datetime.date(2024, 8, 13), 11.5, "USD", "yahoo")]


def test_write_quote_still_writes_its_single_row():
    """write_quote now delegates to write_quotes so the upsert SQL has ONE definition; the
    single-row contract every existing caller relies on must survive the delegation."""
    connection = _Connection()

    db.write_quote(connection, "0000000001", 12.0, "EUR", source="finnhub",
                   as_of=datetime.date(2024, 8, 13))

    assert connection.rows == [("0000000001", datetime.date(2024, 8, 13), 12.0, "EUR", "finnhub")]
