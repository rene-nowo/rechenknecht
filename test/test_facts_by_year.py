"""facts_by_year and the two setters that read around it.

facts_by_year is the piece of the extractor that decides WHICH tagged value becomes "the"
number for a fiscal year, and it had no test at all. The inputs here are built inline: a
real filing is ~4 MB of inline XBRL, while every rule under test is about contexts and tag
order, not about any company's actual figures.
"""

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from src.metrics import (
    INTEREST_EXPENSE,
    LONGTERM_LIABILITIES,
    NUMBER_OF_SHARES_DILUTED,
    SHORTTERM_LIABILITIES,
)
from src.RechenknechtBeta import RechenknechtBeta

FY_END = "--12-31"  # a calendar fiscal year, so the year label equals the end year


def document(facts, contexts=("FY2022",), fy_end=FY_END) -> str:
    """Minimal XBRL: one duration context per name, plus the facts asked for.

    A context name ending in "_<member>" carries that member as a segment, which is how a
    filing marks a value as a breakdown rather than a consolidated total.
    """
    blocks = []
    for name in contexts:
        year = name.split("_")[0].removeprefix("FY")
        segment = ""
        if "_" in name:
            member = name.split("_", 1)[1]
            segment = f"<entity><segment><explicitMember>{member}</explicitMember></segment></entity>"
        blocks.append(
            f'<context id="{name}">{segment}'
            f"<period><startDate>{year}-01-01</startDate><endDate>{year}-12-31</endDate></period>"
            f"</context>"
        )

    for tag, context_ref, value in facts:
        reference = f' contextRef="{context_ref}"' if context_ref else ""
        blocks.append(f"<{tag}{reference}>{value}</{tag}>")

    return (
        '<?xml version="1.0"?>'
        '<xbrl xmlns:us-gaap="http://fasb.org/us-gaap" xmlns:dei="http://xbrl.sec.gov/dei">'
        f"<dei:CurrentFiscalYearEndDate>{fy_end}</dei:CurrentFiscalYearEndDate>"
        + "".join(blocks)
        + "</xbrl>"
    )


def extractor(xbrl: str, frame: pd.DataFrame = None) -> RechenknechtBeta:
    """A RechenknechtBeta with nothing but bs_data (and optionally a frame) set.

    __init__ runs the whole analysis over a list of filings, which is not what any of these
    tests is about.
    """
    rechner = RechenknechtBeta.__new__(RechenknechtBeta)
    rechner.bs_data = BeautifulSoup(xbrl, "xml")
    rechner.df = pd.DataFrame() if frame is None else frame
    return rechner


# ── rule 1: a segmented value is a breakdown, not the total ──────────────────────────────


def test_segmented_value_never_overrides_the_consolidated_one():
    """The bug this replaced: find_all took the first two hits in document order, so the
    last breakdown in the document silently became the company's number."""
    rechner = extractor(
        document(
            [("us-gaap:Revenues", "FY2022", 100), ("us-gaap:Revenues", "FY2022_ClassA", 40)],
            contexts=("FY2022", "FY2022_ClassA"),
        )
    )

    assert rechner.facts_by_year("us-gaap:Revenues") == {"2022": 100.0}


def test_segmented_only_yields_nothing_without_sum_segments():
    rechner = extractor(
        document(
            [("us-gaap:Revenues", "FY2022_ClassA", 40), ("us-gaap:Revenues", "FY2022_ClassB", 60)],
            contexts=("FY2022_ClassA", "FY2022_ClassB"),
        )
    )

    assert rechner.facts_by_year("us-gaap:Revenues") == {}


# ── rule 2: share classes are additive, and deduplicated ─────────────────────────────────


def test_share_classes_are_summed_when_no_consolidated_value_exists():
    """Visa tags no consolidated diluted share count, only Class A/B1/B2/C. Taking one of
    them made Class C's 10 million the whole company and inflated EPS ~200x."""
    rechner = extractor(
        document(
            [
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassA", 10),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassB", 5),
            ],
            contexts=("FY2022_ClassA", "FY2022_ClassB"),
        )
    )

    result = rechner.facts_by_year(
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", sum_segments=True
    )
    assert result == {"2022": 15.0}


def test_the_same_share_class_appearing_twice_is_counted_once():
    """One filing tags the same class/period pair in several statements. Summing the hit
    list instead of the distinct members would double the share count."""
    rechner = extractor(
        document(
            [
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassA", 10),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassA", 10),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassB", 5),
            ],
            contexts=("FY2022_ClassA", "FY2022_ClassB"),
        )
    )

    result = rechner.facts_by_year(
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", sum_segments=True
    )
    assert result == {"2022": 15.0}


def test_a_consolidated_value_beats_the_sum_of_its_parts():
    rechner = extractor(
        document(
            [
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022", 90),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassA", 10),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022_ClassB", 5),
            ],
            contexts=("FY2022", "FY2022_ClassA", "FY2022_ClassB"),
        )
    )

    result = rechner.facts_by_year(
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", sum_segments=True
    )
    assert result == {"2022": 90.0}


# ── rule 3: a tag list is a PRIORITY order, not a set ────────────────────────────────────


def test_the_first_tag_that_yields_anything_wins():
    """3M tags both InterestExpenseDebt (448m) and InterestExpenseNonoperating (946m).
    Handing both to find_all let document order pick the winner."""
    rechner = extractor(
        document(
            [
                ("us-gaap:InterestExpenseNonoperating", "FY2022", 946),
                ("us-gaap:InterestExpenseDebt", "FY2022", 448),
            ]
        )
    )

    priority = ["us-gaap:InterestExpenseDebt", "us-gaap:InterestExpenseNonoperating"]
    assert rechner.facts_by_year(priority) == {"2022": 448.0}
    assert rechner.facts_by_year(list(reversed(priority))) == {"2022": 946.0}


def test_priority_falls_through_to_the_next_tag_when_the_first_is_absent():
    rechner = extractor(document([("us-gaap:InterestExpenseNonoperating", "FY2022", 946)]))

    result = rechner.facts_by_year(
        ["us-gaap:InterestExpenseDebt", "us-gaap:InterestExpenseNonoperating"]
    )
    assert result == {"2022": 946.0}


def test_no_tag_in_the_list_matches_gives_an_empty_result():
    rechner = extractor(document([("us-gaap:Revenues", "FY2022", 100)]))

    assert rechner.facts_by_year(["us-gaap:Missing", "us-gaap:AlsoMissing"]) == {}


# ── rule 4: a fact we cannot place in time is not a fact ─────────────────────────────────


def test_a_fact_without_a_contextref_is_skipped():
    """No contextRef means no period, so the value cannot be assigned to a fiscal year.
    Without the guard the missing attribute would raise inside get_fiscal_year_by_context."""
    rechner = extractor(
        document([("us-gaap:Revenues", None, 999), ("us-gaap:Revenues", "FY2022", 100)])
    )

    assert rechner.facts_by_year("us-gaap:Revenues") == {"2022": 100.0}


def test_a_period_shorter_than_a_year_is_skipped():
    """A quarter carries the same tag as the year. get_fiscal_year_by_context raises on it
    and facts_by_year has to swallow that rather than let one 10-Q table poison the row."""
    xbrl = (
        '<?xml version="1.0"?>'
        '<xbrl xmlns:us-gaap="http://fasb.org/us-gaap" xmlns:dei="http://xbrl.sec.gov/dei">'
        "<dei:CurrentFiscalYearEndDate>--12-31</dei:CurrentFiscalYearEndDate>"
        '<context id="Q4"><period><startDate>2022-10-01</startDate>'
        "<endDate>2022-12-31</endDate></period></context>"
        '<us-gaap:Revenues contextRef="Q4">25</us-gaap:Revenues>'
        "</xbrl>"
    )

    assert extractor(xbrl).facts_by_year("us-gaap:Revenues") == {}


# ── the two latent bugs the same input shape exposes ─────────────────────────────────────


def test_longterm_liabilities_tolerates_a_year_with_no_current_liabilities():
    """set_longterm_liabilities reads the current-liabilities COLUMN for every year the
    total-liabilities tag covers. Those two sets are equal today only by accident, and
    `.loc[row, missing_column]` raises KeyError — which kills the whole company, not one
    cell. The year we know nothing about must come out NaN.
    """
    frame = pd.DataFrame({"2022": {SHORTTERM_LIABILITIES: 400.0}})
    rechner = extractor(
        document(
            [("us-gaap:Liabilities", "FY2022", 1000), ("us-gaap:Liabilities", "FY2021", 900)],
            contexts=("FY2022", "FY2021"),
        ),
        frame=frame,
    )

    rechner.set_longterm_liabilities()

    assert rechner.df.loc[LONGTERM_LIABILITIES, "2022"] == 600.0
    assert pd.isna(rechner.df.loc[LONGTERM_LIABILITIES, "2021"])


@pytest.mark.parametrize(
    "net_value,expected",
    [
        # Foot Locker's own filings, both signs, measured from the fixtures in
        # test/resources/filings/fl: FY2022 (10-K filed 2023-01-28) tags -15,000,000 and
        # FY2019 (10-K filed 2020-02-01) tags +11,000,000 for the very same concept.
        (-15_000_000, 15_000_000.0),
        (11_000_000, 0.0),
    ],
)
def test_net_interest_fallback_reads_a_positive_net_as_no_expense(net_value, expected):
    """Documents a rule whose sign assumption is load-bearing and currently unproven.

    The fallback only runs when a filing tags NO expense concept at all. Both Foot Locker
    signs above come from filings that also tag InterestExpenseDebt (8m and 17m), so the
    priority list wins there and this branch never runs for FL — it is untested in
    production, not merely untested here. If the sign convention is ever inverted for some
    filer, a real expense silently becomes 0.0 and TIER becomes NaN for that company.
    """
    rechner = extractor(
        document([("us-gaap:InterestIncomeExpenseNonoperatingNet", "FY2022", net_value)]),
        frame=pd.DataFrame(index=[INTEREST_EXPENSE]),
    )

    rechner.set_interest_expenses()

    assert rechner.df.loc[INTEREST_EXPENSE, "2022"] == expected


# ── rule 5: a share count is only as trustworthy as the cover page it agrees with ────────


@pytest.mark.parametrize(
    "case,anchor,tagged,expected",
    [
        # Every row is a real filing on disk, anchor and tagged value both read off it.
        # mcd 10-K FY2023 (0000063908-24-000072): 732.3 means 732.3 million.
        ("mcd pre-scaled to millions", 722_051_488, 732.3, 732_300_000),
        # cop 10-K FY2019 (0001193125-20-039954): thousands. Yahoo has no data that far
        # back, so this one never showed up in v_failures at all — EPS read 6398.55.
        ("cop pre-scaled to thousands", 1_081_132_415, 1_123_536, 1_123_536_000),
        # pdd 20-F FY2024 (0001410578-25-000951): thousands, for ONE year while the filings
        # either side of it are tagged in full.
        ("pdd pre-scaled to thousands", 5_568_585_848, 5_916_592, 5_916_592_000),
        # fl 10-K FY2022 (0000850209-23-000006), the golden-fixture company: correct, and
        # the 0.98 gap to the cover page is exactly the buyback drift the band allows for.
        ("fl already correct", 93_429_371, 95_500_000, 95_500_000),
        # mcd 10-K FY2022 (0000063908-23-000012), the filing before the convention changed.
        ("mcd already correct", 731_496_951, 741_300_000, 741_300_000),
        # asti: reverse splits, so the cover page is 3.7x the weighted average. A real gap,
        # NOT a scale factor — correcting it would be a new bug in place of the old one.
        ("asti reverse split, untouched", 3_793_843, 1_025_097, 1_025_097),
        # baba: the cover page counts ADSs while the statements count ordinary shares.
        ("baba ADS anchor, untouched", 1_858_037_427, 19_235_000_000, 19_235_000_000),
    ],
)
def test_a_pre_scaled_share_count_is_corrected_against_the_cover_page(case, anchor, tagged, expected):
    frame = pd.DataFrame(index=[NUMBER_OF_SHARES_DILUTED])
    rechner = extractor(
        document(
            [
                ("dei:EntityCommonStockSharesOutstanding", "FY2022", anchor),
                ("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "FY2022", tagged),
            ]
        ),
        frame=frame,
    )

    rechner.set_number_of_shares_diluted()

    assert rechner.df.loc[NUMBER_OF_SHARES_DILUTED, "2022"] == expected


def test_the_scale_decision_is_taken_once_per_document():
    """A filer's scale convention belongs to the filing, so all of its years move together.
    Deciding year by year would let a document contradict itself — mcd's FY2023 10-K carries
    732.3 / 741.3 / 751.8, and only the newest of them is close in time to the cover page."""
    frame = pd.DataFrame(index=[NUMBER_OF_SHARES_DILUTED])
    rechner = extractor(
        document(
            [
                ("dei:EntityCommonStockSharesOutstanding", "FY2023", 722_051_488),
                ("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "FY2023", 732.3),
                ("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "FY2022", 741.3),
                ("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "FY2021", 751.8),
            ],
            contexts=("FY2023", "FY2022", "FY2021"),
        ),
        frame=frame,
    )

    rechner.set_number_of_shares_diluted()

    assert rechner.df.loc[NUMBER_OF_SHARES_DILUTED, "2023"] == 732_300_000
    assert rechner.df.loc[NUMBER_OF_SHARES_DILUTED, "2022"] == 741_300_000
    assert rechner.df.loc[NUMBER_OF_SHARES_DILUTED, "2021"] == 751_800_000


def test_a_filing_without_the_cover_page_tag_keeps_its_share_count():
    """The anchor is mandatory, but a filing that omits it must not lose its share count —
    and must not be silent about it either."""
    frame = pd.DataFrame(index=[NUMBER_OF_SHARES_DILUTED])
    rechner = extractor(
        document([("us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "FY2022", 716.4)]),
        frame=frame,
    )

    rechner.set_number_of_shares_diluted()

    assert rechner.df.loc[NUMBER_OF_SHARES_DILUTED, "2022"] == 716
