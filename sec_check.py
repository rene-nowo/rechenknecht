"""Adjudicate our extraction against the SEC's own XBRL, not against Yahoo.

Agreeing with Yahoo does not make a number correct. Yahoo serves the latest restated,
vendor-normalized figures; a filing says what it said on the day it was filed. Three cases
already proved the point: Foot Locker's FY2021 revenue (8,958m as filed, 8,968m restated),
Nvidia's FY2023 share count (10x apart because of the 2024 split), and dividends declared
versus paid.

So for every disagreement between our extraction and Yahoo, this asks a third party that
CAN settle it: https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json, the SEC's own
parse of the same documents. Each fact carries its accession, so the comparison is pinned to
the filing we actually read rather than to whatever was true later.

Verdicts:
    edgar_matches_sec   our value equals the SEC's -> we are right, Yahoo differs for its
                        own reasons (restatement, normalization, split adjustment)
    edgar_differs_sec   our value does not -> our extraction bug, regardless of Yahoo
    no_sec_fact         the SEC has no such tag for that year -> a tag-mapping question

    export RK_DB_URL=...   # see src/db.py
    python sec_check.py --limit 20
"""

import argparse
import json
import pathlib
import time
from datetime import datetime

import requests

from src import db
from src.Edgar_api import EDGAR_API
from src.metrics import fiscal_year_label

CACHE = pathlib.Path(__file__).parent / ".companyfacts_cache"
INDEX_MAP = json.loads((pathlib.Path(__file__).parent / "documents" / "rechenknecht_index_map.json").read_text())

# Our concept name -> the index_map key whose tag list defines it.
CONCEPT_KEYS = {
    "revenue": "revenue", "ebit": "ebit", "net_income": "net_income",
    "interest_expense": "interest_expense", "stockholders_equity": "stockholders_equity",
    "total_equity": "total_equity", "goodwill": "goodwill",
    "intangible_assets": "intangible_assets", "current_assets": "current_assets",
    "current_liabilities": "current_liabilities", "total_liabilities": "total_liabilities",
    "operating_cash_flow": "operating_cash_flow", "capex": "capex",
    "number_of_shares_diluted": "number_of_shares_diluted",
}

def companyfacts(cik: str) -> dict:
    """Fetch and cache one company's full XBRL fact set."""
    CACHE.mkdir(exist_ok=True)
    cached = CACHE / f"{cik}.json"
    if cached.exists():
        return json.loads(cached.read_text())

    # The class attribute, not an instance: constructing EDGAR_API parses the 12k-row
    # ticker/CIK map, and this file only ever needed the user-agent header.
    response = requests.get(
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json", headers=EDGAR_API.headers, timeout=60
    )
    response.raise_for_status()
    cached.write_text(response.text)
    time.sleep(0.2)  # SEC asks for <= 10 requests/second
    return json.loads(response.text)


def sec_fact(facts: dict, tags: list, year: int, fy_end_month: int):
    """The value the SEC holds for this concept and fiscal year, as originally filed.

    Tags are a priority list, same as the extractor uses. Among several filings reporting
    the same period the EARLIEST is taken: that is the as-filed number our parser read,
    while later ones may be restatements.
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    for tag in tags:
        name = tag.split(":")[-1]
        entry = us_gaap.get(name)
        if not entry:
            continue

        candidates = []
        for values in entry["units"].values():
            for value in values:
                if value.get("form") != "10-K":
                    continue
                end = datetime.strptime(value["end"], "%Y-%m-%d")
                if fiscal_year_label(end, fy_end_month) != str(year):
                    continue
                if value.get("start"):  # a duration must be a full year, not a quarter
                    start = datetime.strptime(value["start"], "%Y-%m-%d")
                    if (end - start).days < 350:
                        continue
                candidates.append(value)

        if candidates:
            chosen = min(candidates, key=lambda v: v["filed"])
            return float(chosen["val"]), name, chosen["accn"]

    return None, None, None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--tolerance", type=float, default=0.02, help="relative match tolerance")
    args = parser.parse_args()

    with db.connect() as connection:
        connection.execute("""
            create table if not exists sec_check (
                cik           text not null,
                fiscal_year   smallint not null,
                concept       text not null,
                edgar_value   numeric,
                yahoo_value   numeric,
                sec_value     numeric,
                sec_tag       text,
                sec_accession text,
                verdict       text not null,
                checked_at    timestamptz not null default now(),
                primary key (cik, fiscal_year, concept)
            )""")
        connection.commit()

        rows = connection.execute("""
            select d.cik, c.ticker, coalesce(c.fiscal_year_end_month, 12),
                   d.fiscal_year, d.concept, d.edgar_value, d.yahoo_value
            from v_failures d join company c on c.cik = d.cik
            where d.concept = any(%s) and d.verdict in ('differs','missing_in_edgar')
            order by c.ticker, d.concept, d.fiscal_year
        """, (list(CONCEPT_KEYS),)).fetchall()

        print(f"{len(rows)} disagreements to adjudicate\n")

        tally, written = {}, 0
        for cik, ticker, fy_month, year, concept, edgar_value, yahoo_value in rows:
            try:
                facts = companyfacts(cik)
            except Exception as error:
                print(f"  {ticker}: companyfacts unavailable ({error})")
                continue

            tags = INDEX_MAP[CONCEPT_KEYS[concept]][1]
            sec_value_, sec_tag, accession = sec_fact(facts, tags, year, fy_month)

            if sec_value_ is None:
                verdict = "no_sec_fact"
            elif edgar_value is None:
                verdict = "edgar_differs_sec"
            elif abs(float(edgar_value) - sec_value_) <= args.tolerance * max(abs(sec_value_), 1e-9):
                verdict = "edgar_matches_sec"
            else:
                verdict = "edgar_differs_sec"

            tally[verdict] = tally.get(verdict, 0) + 1
            connection.execute("""
                insert into sec_check (cik, fiscal_year, concept, edgar_value, yahoo_value,
                                       sec_value, sec_tag, sec_accession, verdict)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (cik, fiscal_year, concept) do update set
                    edgar_value=excluded.edgar_value, yahoo_value=excluded.yahoo_value,
                    sec_value=excluded.sec_value, sec_tag=excluded.sec_tag,
                    sec_accession=excluded.sec_accession, verdict=excluded.verdict,
                    checked_at=now()
            """, (cik, year, concept, edgar_value, yahoo_value, sec_value_, sec_tag, accession, verdict))
            written += 1

        connection.commit()
        print(f"wrote {written} verdicts")
        for verdict, count in sorted(tally.items(), key=lambda kv: -kv[1]):
            print(f"  {verdict:20s} {count}")


if __name__ == "__main__":
    main()
