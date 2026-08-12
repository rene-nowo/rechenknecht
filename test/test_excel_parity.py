"""Master_Vorlage is the specification; this test holds us to it.

The fixture is the Skyworks Solutions block from Rechenknecht_V9.0_Roll_Out.xlsx: the raw
input rows (19-38) AND the values Excel computed from them. We feed Excel's own inputs into
src/metrics.py and require our outputs to equal Excel's outputs.

That isolation is the point. Feeding Excel's inputs tests the ENGINE; any disagreement is a
formula difference, not an extraction difference. Comparing a full pipeline run against
Excel would conflate the two and tell us nothing about which is wrong.

Excel row -> concept mapping, from Master_Vorlage:
    r19 revenue     r21 ebit        r22 interest    r23 net income   r24 goodwill
    r25 intangibles r27 cash        r28 curr assets r29 balance sum  r30 curr liab
    r31 lt liab     r32 total liab  r33 equity      r34 op cash flow r35 capex
    r36 dividends paid              r37 diluted shares
    -> r6 div/share  r7 fcf/share   r8 EPS   r10 book value/share
       r12 RoA       r13 EBIT margin r14 equity ratio  r17 FCF  r18 working capital
       r39 TIER
"""

import json
import pathlib

import pandas as pd
import pytest

from src.metrics import (
    BALANCE_SHEET_TOTAL, BOOK_VALUE_PER_SHARE, CAPEX, CURRENT_ASSETS, DIVIDENDS_PAID,
    DIVIDENDS_PER_SHARE, EBIT, EBIT_MARGIN, EPS, EQUITY_RATIO, FCF_PER_SHARE,
    FREE_CASH_FLOW, GOODWILL, INTANGIBLE_ASSETS, INTEREST_EXPENSE, LONGTERM_LIABILITIES,
    NET_INCOME, NETTOUMLAUFVERM_GEN, NUMBER_OF_SHARES_DILUTED, OPERATING_CASH_FLOW,
    REVENUE, RO_A, SHORTTERM_LIABILITIES, STOCKHOLDERS_EQUITY, TIER, TOTAL_LIABILITIES,
    add_averages, add_sheet_ratios,
)

FIXTURE = pathlib.Path(__file__).parent / "resources" / "swks_master_vorlage_v9.json"

# This Skyworks block runs with BB39=1 ("Mit aktuellen Aktien rechnen"): every year is
# divided by the CURRENT share count, not that year's own diluted count.
#
# The number is not inferred — it is the workbook's D3 = (C10 + C11) / C38, i.e. 150,472,800
# shares outstanding over a Rechenfaktor of 1,000,000, since the whole sheet is denominated
# in millions. test_share_divisor_is_constant below re-derives it from the data as a check.
CURRENT_SHARES = 150.4728

EXCEL_INPUTS = {
    "revenue": REVENUE, "ebit": EBIT, "interest_expense": INTEREST_EXPENSE,
    "net_income": NET_INCOME, "goodwill": GOODWILL, "intangibles": INTANGIBLE_ASSETS,
    "current_assets": CURRENT_ASSETS, "balance_total": BALANCE_SHEET_TOTAL,
    "current_liab": SHORTTERM_LIABILITIES, "longterm_liab": LONGTERM_LIABILITIES,
    "total_liab": TOTAL_LIABILITIES, "equity": STOCKHOLDERS_EQUITY,
    "ocf": OPERATING_CASH_FLOW, "dividends_paid": DIVIDENDS_PAID,
    "diluted_shares": NUMBER_OF_SHARES_DILUTED,
}

EXCEL_OUTPUTS = {
    "EPS": EPS, "bvps": BOOK_VALUE_PER_SHARE, "RoA": RO_A, "EBIT_margin": EBIT_MARGIN,
    "equity_ratio": EQUITY_RATIO, "fcf_total": FREE_CASH_FLOW,
    "fcf_per_share": FCF_PER_SHARE, "div_per_share": DIVIDENDS_PER_SHARE,
    "working_capital": NETTOUMLAUFVERM_GEN, "TIER": TIER,
}


@pytest.fixture(scope="module")
def excel() -> dict:
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def ours(excel) -> pd.DataFrame:
    """Excel's raw inputs, run through our engine."""
    years = sorted((y for y in excel["net_income"]), reverse=True)
    facts = pd.DataFrame(index=list(EXCEL_INPUTS.values()), columns=years, dtype="float64")

    for excel_name, concept in EXCEL_INPUTS.items():
        for year in years:
            value = excel[excel_name].get(year)
            if value is not None:
                facts.loc[concept, year] = float(value)

    # Excel keeps capex as a negative outflow and computes FCF as ocf + capex; our
    # convention is a positive capex with FCF = ocf - capex. Same quantity, so the sign is
    # flipped on the way in rather than special-casing the formula.
    for year in years:
        value = excel["capex"].get(year)
        if value is not None:
            facts.loc[CAPEX, year] = -float(value)

    add_sheet_ratios(facts, current_shares=CURRENT_SHARES)
    return facts


@pytest.mark.parametrize("excel_name,concept", sorted(EXCEL_OUTPUTS.items()))
def test_metric_matches_master_vorlage(excel, ours, excel_name, concept):
    """Every year Excel computed a value for, we must reproduce it."""
    expected = excel[excel_name]
    assert expected, f"fixture has no {excel_name}"

    compared = 0
    for year, excel_value in expected.items():
        if year not in ours.columns:
            continue
        actual = ours.loc[concept, year]
        assert actual == pytest.approx(excel_value, rel=1e-6), (
            f"{excel_name} {year}: Excel {excel_value} != ours {actual}"
        )
        compared += 1

    # TIER only exists in the four years Skyworks actually paid interest; everything else
    # spans the full decade.
    assert compared >= 4, f"only {compared} years compared for {excel_name}"


def test_share_divisor_is_constant_across_every_year(excel):
    """Proves this block uses the current-share mode rather than per-year diluted shares.

    Three independent quotients — net income/EPS, equity/book value, dividends paid/dividend
    per share — must all land on the same divisor in every year. They do (150.4728), while
    the reported diluted count moves from 194.9m to 161.5m over the same period.
    """
    for year in excel["EPS"]:
        for numerator, per_share in (("net_income", "EPS"), ("equity", "bvps"),
                                     ("dividends_paid", "div_per_share")):
            if year in excel[numerator] and excel[per_share].get(year):
                divisor = excel[numerator][year] / excel[per_share][year]
                assert divisor == pytest.approx(CURRENT_SHARES, rel=1e-4), (
                    f"{numerator}/{per_share} in {year} implies {divisor}, not {CURRENT_SHARES}"
                )

    assert excel["diluted_shares"]["2015"] != pytest.approx(CURRENT_SHARES, rel=1e-3)


def test_tier_is_blank_exactly_where_excel_leaves_it_blank(excel, ours):
    """Skyworks had no interest expense in 2020 and Excel shows no TIER for that year.
    Our NaN has to land on the same years, or the multi-year average diverges."""
    assert excel["interest_expense"]["2020"] == 0
    assert "2020" not in excel["TIER"]
    assert pd.isna(ours.loc[TIER, "2020"])

    for year in excel["TIER"]:
        assert not pd.isna(ours.loc[TIER, year]), f"we lost TIER for {year}"


def test_valuation_ratios_match_the_summary_block(excel, ours):
    """The workbook's own summary cells for this company: price 69.50 gives a 7-year KGV of
    10.5496 (BG6) and a KBGV of 17.4107 (BH6).

    KBGV = price / LATEST book value per share x KGV — BA10 reads the last reported book
    value (its label at BB4 is "Buchwert (letzter Bericht)"), NOT a seven-year average.
    """
    price = 69.5
    add_averages(ours, price, time_span=7)

    assert ours.loc["KGV", "7_YEAR_AVG"] == pytest.approx(10.54964148088369, rel=1e-4)
    assert ours.loc["KBGV", "7_YEAR_AVG"] == pytest.approx(17.41074525185313, rel=1e-4)
    # RoI is the reciprocal view of KGV (Master_Vorlage BF6 = average EPS / price x 100).
    assert ours.loc["RoI", "7_YEAR_AVG"] == pytest.approx(100 / 10.54964148088369, rel=1e-4)
