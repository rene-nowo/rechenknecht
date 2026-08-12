"""The reporting currency, and the unit filter that follows from it.

A foreign private issuer's 20-F tags the same concept twice: once in the currency it
actually reports in, and once as a USD "convenience translation" at that year's closing
rate. Both are consolidated and both are valid XBRL, so nothing but document order used to
decide which one became the company's number — and in Alibaba's FY2025 filing the USD one
comes fourth, inside the slice set_revenue() takes, so it silently overwrote the CNY figure
it had just read.

Measured on the real filings these tests are modelled on
(filings/baba/2025-03-31_0000950170-25-090161_baba-20250331.htm):
    us-gaap:Revenues       CNY 114 facts / USD 1     FY2025: CNY 996,347,000,000
                                                     then   USD 137,300,000,000
    us-gaap monetary facts CNY 1228 / USD 175 / HKD 12
and on filings/mufg/2025-03-31_0000067088-25-000011_mufg-20250331.htm:
    us-gaap monetary facts JPY 5746, plus 16 facts whose measure is `mufg:security`
"""

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from src.metrics import NET_INCOME, NUMBER_OF_SHARES, REVENUE
from src.RechenknechtBeta import RechenknechtBeta

FY_END = "--12-31"  # a calendar fiscal year, so the year label equals the end year


def document(units, facts, contexts=("FY2022",), fy_end=FY_END, root_ns="us-gaap") -> str:
    """Minimal XBRL carrying unit definitions, contexts and facts.

    `units` is {unit_id: measure}, e.g. {"cny": "iso4217:CNY"}. `facts` is
    (tag, contextRef, unitRef, value); a unitRef of None omits the attribute entirely,
    which is how a non-numeric fact appears.
    """
    blocks = [f'<unit id="{uid}"><measure>{measure}</measure></unit>' for uid, measure in units.items()]

    for name in contexts:
        year = name.removeprefix("FY")
        blocks.append(
            f'<context id="{name}">'
            f"<period><startDate>{year}-01-01</startDate><endDate>{year}-12-31</endDate></period>"
            f"</context>"
        )

    for tag, context_ref, unit_ref, value in facts:
        unit = f' unitRef="{unit_ref}"' if unit_ref else ""
        blocks.append(f'<{tag} contextRef="{context_ref}"{unit}>{value}</{tag}>')

    return (
        '<?xml version="1.0"?>'
        '<xbrl xmlns:us-gaap="http://fasb.org/us-gaap" xmlns:dei="http://xbrl.sec.gov/dei"'
        ' xmlns:ifrs-full="http://xbrl.ifrs.org/taxonomy">'
        f"<dei:CurrentFiscalYearEndDate>{fy_end}</dei:CurrentFiscalYearEndDate>"
        + "".join(blocks)
        + "</xbrl>"
    )


def extractor(xbrl: str, frame: pd.DataFrame = None) -> RechenknechtBeta:
    """A RechenknechtBeta with a document loaded through the real set_document path, so the
    unit/currency detection under test is the one that actually runs during an ingest."""
    rechner = RechenknechtBeta.__new__(RechenknechtBeta)
    rechner.df = pd.DataFrame() if frame is None else frame
    rechner.bs_data = BeautifulSoup(xbrl, "xml")
    rechner.set_reporting_currency()
    return rechner


# ── detecting the currency the filing reports in ─────────────────────────────────────────


def test_the_most_tagged_currency_is_the_reporting_one():
    rechner = extractor(
        document(
            {"cny": "iso4217:CNY", "usd": "iso4217:USD"},
            [
                ("us-gaap:Revenues", "FY2022", "cny", 100),
                ("us-gaap:NetIncomeLoss", "FY2022", "cny", 20),
                ("us-gaap:Revenues", "FY2022", "usd", 14),
            ],
        )
    )

    assert rechner.document_currency == "CNY"


def test_a_non_iso4217_measure_is_not_a_currency():
    """MUFG tags 16 facts with the measure `mufg:security`. Splitting a measure on ":"
    without checking the iso4217 prefix invents a currency called "security"."""
    rechner = extractor(
        document(
            {"jpy": "iso4217:JPY", "sec": "mufg:security", "pure": "pure"},
            [
                ("us-gaap:Revenues", "FY2022", "jpy", 100),
                ("us-gaap:NumberOfSecurities", "FY2022", "sec", 5),
                ("us-gaap:NumberOfSecurities", "FY2022", "sec", 6),
                ("us-gaap:Ratio", "FY2022", "pure", 1),
            ],
        )
    )

    assert rechner.document_currency == "JPY"
    assert "sec" not in rechner.unit_currency


def test_a_filing_with_no_monetary_facts_has_no_currency():
    rechner = extractor(
        document({"shares": "shares"}, [("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022", "shares", 50)])
    )

    assert rechner.document_currency is None


# ── the taxonomy gate ────────────────────────────────────────────────────────────────────


def test_an_ifrs_filing_reports_no_us_gaap_facts():
    """Sony and Toyota file 20-F but moved to the IFRS taxonomy at FY2022. Their filings
    parse fine and yield nothing this extractor understands, which has to be a loud failure
    rather than a company stored with empty recent years."""
    rechner = extractor(
        document(
            {"jpy": "iso4217:JPY"},
            [
                ("ifrs-full:Revenue", "FY2022", "jpy", 100),
                ("ifrs-full:ProfitLoss", "FY2022", "jpy", 20),
            ],
        )
    )

    assert rechner.document_us_gaap_facts == 0
    assert rechner.document_currency is None


def test_a_us_gaap_filing_counts_its_facts():
    rechner = extractor(
        document(
            {"jpy": "iso4217:JPY"},
            [
                ("us-gaap:Revenues", "FY2022", "jpy", 100),
                ("ifrs-full:Revenue", "FY2022", "jpy", 100),
            ],
        )
    )

    assert rechner.document_us_gaap_facts == 1


# ── the filter itself ────────────────────────────────────────────────────────────────────


def test_the_convenience_translation_never_becomes_the_revenue():
    """The exact Alibaba shape: both values consolidated, both for the same year, the USD
    one later in the document. Before the unit filter the last assignment won."""
    frame = pd.DataFrame(index=[REVENUE])
    rechner = extractor(
        document(
            {"cny": "iso4217:CNY", "usd": "iso4217:USD"},
            [
                ("us-gaap:Revenues", "FY2022", "cny", 996347000000),
                ("us-gaap:Revenues", "FY2022", "usd", 137300000000),
                # a second CNY year, so CNY is unambiguously the majority currency
                ("us-gaap:Revenues", "FY2021", "cny", 868687000000),
            ],
            contexts=("FY2022", "FY2021"),
        ),
        frame=frame,
    )

    rechner.set_revenue()

    assert rechner.df.loc[REVENUE, "2022"] == 996347000000
    assert rechner.df.loc[REVENUE, "2021"] == 868687000000


def test_net_income_ignores_the_foreign_currency_copy():
    frame = pd.DataFrame(index=[NET_INCOME])
    rechner = extractor(
        document(
            {"cny": "iso4217:CNY", "usd": "iso4217:USD"},
            [
                ("us-gaap:NetIncomeLoss", "FY2022", "cny", 130109000000),
                ("us-gaap:NetIncomeLoss", "FY2022", "usd", 17900000000),
                ("us-gaap:NetIncomeLoss", "FY2021", "cny", 80009000000),
            ],
            contexts=("FY2022", "FY2021"),
        ),
        frame=frame,
    )

    rechner.set_net_income()

    assert rechner.df.loc[NET_INCOME, "2022"] == 130109000000


def test_share_counts_survive_the_filter():
    """Share counts are monetary-adjacent but not money: their unit is `shares`, which
    resolves to no currency at all and must therefore never be filtered out."""
    frame = pd.DataFrame(index=[NUMBER_OF_SHARES])
    rechner = extractor(
        document(
            {"cny": "iso4217:CNY", "shares": "shares"},
            [
                ("us-gaap:Revenues", "FY2022", "cny", 100),
                ("us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "FY2022", "shares", 2400000000),
            ],
        ),
        frame=frame,
    )

    rechner.set_number_of_shares()

    assert rechner.df.loc[NUMBER_OF_SHARES, "2022"] == 2400000000


def test_nothing_is_filtered_when_the_currency_is_unknown():
    """A filing whose units cannot be resolved must behave exactly as it did before the
    filter existed — dropping every fact would be a worse answer than the status quo."""
    rechner = extractor(
        document({}, [("us-gaap:Revenues", "FY2022", None, 100)])
    )

    assert rechner.document_currency is None
    assert len(rechner.find_facts("us-gaap:Revenues")) == 1


@pytest.mark.parametrize("currency", ["CNY", "JPY", "INR", "GBP"])
def test_the_reporting_currency_is_read_not_assumed(currency):
    rechner = extractor(
        document(
            {"local": f"iso4217:{currency}", "usd": "iso4217:USD"},
            [
                ("us-gaap:Revenues", "FY2022", "local", 100),
                ("us-gaap:NetIncomeLoss", "FY2022", "local", 20),
                ("us-gaap:Revenues", "FY2022", "usd", 14),
            ],
        )
    )

    assert rechner.document_currency == currency
