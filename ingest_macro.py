"""Ingest historical macro-economic series from FRED into Postgres.

The second data stream next to the company fundamentals: yield spreads, rates, unemployment,
CPI, VIX and money supply, stored in `macro_series` with a date as the whole key. Nothing here
interprets them — this pulls the history and stores it, and every question asked of the
numbers afterwards is asked in SQL or in the dashboard, not in this file.

Shares no table with ingest.py, deliberately: this is startable while a multi-hour company
ingest is running, and `--apply-schema` creates only macro_series for exactly that reason
(src/db.py apply_macro_schema explains what it refuses to touch and why).

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    export RK_FRED_KEY="$(op read 'op://Agent Keys/FRED API KEY/credential')"
    python ingest_macro.py --apply-schema   # first run, creates the table
    python ingest_macro.py                  # pulls all series, full history

# contra: second FRED reader in the monorepo — apps/reflex/api/src/sources/fred.source.ts is
# the first, and hits the same endpoint with the same "." sentinel rule. Not extracted into a
# shared module because there is nothing to share across the boundary: that one is a NestJS
# injectable in TypeScript writing to Reflex's own database on a daily cron, this is a Python
# batch script writing to Rechenknecht's. The upgrade path, if a third reader ever appears or
# these two databases converge, is one service owning the fetch and both apps reading from it
# — not a package, since no package can be imported by both languages.
"""

import argparse
import os
import time

import requests

from src import db

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
SOURCE = "fred"

# FRED's documented limit is 120 requests/minute and this makes nine requests in total, so the
# pause buys nothing here on its own. It is kept because every other external API in this app
# is called with one (ingest.py SEC_PAUSE_SECONDS, FINNHUB_PAUSE_SECONDS), and the series list
# below is the kind of thing that grows.
FRED_PAUSE_SECONDS = 0.5

# The series, and what each one is — agreed up front rather than discovered, because a FRED id
# is unreadable on its own and `DGS2` vs `DGS10` is a one-character difference between two very
# different numbers.
SERIES = {
    "T10Y2Y": "10yr minus 2yr treasury spread (yield curve inversion)",
    "DGS10": "10-year treasury yield",
    "DGS2": "2-year treasury yield",
    "BAA10Y": "Baa corporate spread over 10yr treasury (credit stress)",
    "UNRATE": "unemployment rate",
    "CPIAUCSL": "CPI (inflation)",
    "VIXCLS": "VIX",
    "FEDFUNDS": "fed funds rate",
    "M2SL": "M2 money supply",
    # Monthly, not daily — FRED publishes one observation per month, so a consumer asking for
    # "yesterday" gets nothing and must tolerate the last value being weeks old. Reflex's
    # nikkei-feedback-loop thesis needs it (JGB10Y).
    "IRLTLT01JPM156N": "Japan 10yr government bond yield",
    # The rest of what Reflex reads. Reflex used to fetch these itself; it now reads
    # macro_series, so a series missing here is a variable that silently resolves to nothing.
    "DCOILBRENTEU": "Brent crude spot (Reflex BRENT)",
    "DCOILWTICO": "WTI crude spot (Reflex WTI)",
    "JPNCPIALLMINMEI": "Japan CPI, all items (Reflex JP_CPI) — monthly",
    # Reflex's USDJPY is a Yahoo series whose history only reaches 1997; DEXJPUS is the
    # pre-1997 fallback the seed catalog names.
    "DEXJPUS": "JPY/USD exchange rate (Reflex USDJPY pre-1997 fallback)",
}


def fetch_series(series_id: str, api_key: str):
    """Every observation FRED holds for one series, as (date, value) pairs, oldest first.

    No observation_start is sent on purpose: FRED then starts at the earliest date it has,
    which is the whole point of the pull — DGS10 reaches back to 1962 and no hardcoded start
    date could know that per series without a second request asking.

    A row whose value will not parse as a number is dropped rather than stored. FRED's own
    no-data marker is the literal string "." — every weekend and holiday on a daily series
    carries one — and float(".") raises, so the try/except catches the documented case and
    anything else non-numeric the API might serve in the same move. Storing those as 0.0 would
    put a fake zero into every average and spread computed over the series later.
    """
    response = requests.get(
        FRED_URL,
        params={"series_id": series_id, "api_key": api_key, "file_type": "json"},
        timeout=60,
    )
    response.raise_for_status()

    rows = []
    for observation in response.json()["observations"]:
        try:
            rows.append((observation["date"], float(observation["value"])))
        except (ValueError, TypeError, KeyError):
            continue
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply-schema", action="store_true",
        help="create the macro_series table and its index first",
    )
    args = parser.parse_args()

    api_key = os.environ.get("RK_FRED_KEY")
    if not api_key:
        raise RuntimeError("RK_FRED_KEY is not set (see the module docstring for how to get it)")

    with db.connect() as connection:
        if args.apply_schema:
            db.apply_macro_schema(connection)
            print("macro_series schema applied\n")

        # One try/except per series, like ingest.py's per-ticker loop: a series FRED has
        # renamed or is briefly failing on must cost that one series, not the other eight.
        for index, (series_id, description) in enumerate(SERIES.items(), start=1):
            prefix = f"[{index}/{len(SERIES)}] {series_id:9s}"
            try:
                observations = fetch_series(series_id, api_key)
                if not observations:
                    print(f"{prefix} NO DATA  (every observation was FRED's '.' marker)")
                    continue
                written = db.write_macro_observations(connection, series_id, observations, SOURCE)
                connection.commit()
                print(
                    f"{prefix} {written:6d} rows  "
                    f"{observations[0][0]} .. {observations[-1][0]}  {description}"
                )
            except Exception as error:
                connection.rollback()
                print(f"{prefix} FAILED  {type(error).__name__}: {str(error)[:90]}")
            time.sleep(FRED_PAUSE_SECONDS)

    print("\ndone")


if __name__ == "__main__":
    main()
