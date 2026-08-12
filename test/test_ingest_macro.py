"""The two things in the macro path that can go wrong quietly.

Neither is caught by looking at the row count afterwards, which is why they are pinned here:

  * a dropped observation looks identical to a date FRED never published, so a "." that got
    parsed into a number instead of being skipped shows up as a plausible extra row and only
    surfaces years later as a fake zero inside an average,
  * apply_macro_schema exists solely to NOT run the rest of schema.sql — it has to be
    startable while the multi-hour company ingest is writing to `company` and `quote`. The
    day someone appends a table below the macro block in schema.sql and this quietly starts
    shipping an ALTER TABLE along with it, nothing else notices.

No network and no database: the fetch is replaced where it is exercised, and the extraction
reads the real schema.sql off disk because that file is exactly what is under test.
"""

import ingest_macro
from src import db


class _Response:
    """The two attributes fetch_series touches on a requests response, nothing more."""

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


# A real T10Y2Y-shaped slice: two trading days, then a weekend FRED marks with ".", then the
# negative spread that made this series worth pulling in the first place.
OBSERVATIONS = [
    {"date": "2022-07-01", "value": "0.06"},
    {"date": "2022-07-02", "value": "."},
    {"date": "2022-07-03", "value": "."},
    {"date": "2022-07-05", "value": "-0.05"},
]


def test_freds_no_data_marker_is_dropped_not_parsed(monkeypatch):
    monkeypatch.setattr(
        ingest_macro.requests, "get",
        lambda *args, **kwargs: _Response({"observations": OBSERVATIONS}),
    )

    rows = ingest_macro.fetch_series("T10Y2Y", "key")

    assert rows == [("2022-07-01", 0.06), ("2022-07-05", -0.05)]
    # The negative one specifically: an inverted curve is the signal this series carries, and
    # a parser that lost the sign would still return a believable-looking float.
    assert rows[1][1] < 0


def test_full_history_is_requested_by_sending_no_start_date(monkeypatch):
    """FRED defaults observation_start to the earliest date it holds. Sending one anyway —
    even a very old one — silently truncates any series that reaches back further."""
    sent = {}

    def _capture(url, params, timeout):
        sent.update(params)
        return _Response({"observations": []})

    monkeypatch.setattr(ingest_macro.requests, "get", _capture)
    ingest_macro.fetch_series("DGS10", "key")

    assert "observation_start" not in sent
    assert sent["series_id"] == "DGS10"
    assert sent["file_type"] == "json"


def test_apply_macro_schema_touches_no_existing_table():
    """The guardrail as an assertion: this is what makes the macro ingest safe to run
    alongside the company ingest, and it is a property of schema.sql, not of the code."""
    statements = [s for s in db.SCHEMA_PATH.read_text().split(";") if "macro_series" in s]
    # Comments carry the words "company", "quote" and "fact" as prose all over this block, so
    # the check has to be against what Postgres actually executes.
    sql = " ".join(
        line for line in ";".join(statements).lower().splitlines()
        if not line.strip().startswith("--")
    )

    assert "create table if not exists macro_series" in sql
    assert "create index if not exists macro_series_date_idx" in sql
    # Two statements, both about macro_series. Anything else that arrived in the chunk — an
    # ALTER against company or quote, a view rebuild — would ride along into the connection.
    assert len(statements) == 2
    for forbidden in ("alter table", "create or replace view", "company", "quote", "fact"):
        assert forbidden not in sql, f"apply_macro_schema would also run: {forbidden}"
