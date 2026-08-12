"""Offline checks for the shared math and the Yahoo adapter.

No network: the Yahoo tests feed a stub in place of yf.Ticker. The point of these tests is
the two rules that silently corrupt a cross-source comparison when they drift — how a
fiscal year is labelled, and which sign/definition each source uses.
"""

from datetime import datetime

import pandas as pd
import pytest

from src import metrics
from src.metrics import (
    BALANCE_SHEET_TOTAL,
    CAPEX,
    EBIT,
    EBIT_MARGIN,
    EPS,
    FREE_CASH_FLOW,
    INTEREST_EXPENSE,
    NET_INCOME,
    NUMBER_OF_SHARES_DILUTED,
    OPERATING_CASH_FLOW,
    REVENUE,
    RO_A,
    TIER,
    add_sheet_ratios,
    fiscal_year_label,
)
from src import yahoo_source


def test_fiscal_year_label_matches_the_filing_convention():
    # Foot Locker's fiscal year ends in January, so the year ending 2023-01-28 is FY2022 —
    # this is the value the golden footlocker_expected.csv is keyed on.
    assert fiscal_year_label(datetime(2023, 1, 28), 1) == "2022"
    # Microsoft's ends in June: the year ending 2025-06-30 is FY2024 by this convention.
    assert fiscal_year_label(datetime(2025, 6, 30), 6) == "2024"
    # A calendar-year company keeps its own year.
    assert fiscal_year_label(datetime(2022, 12, 31), 12) == "2022"
    # So does one whose year ends in the second half.
    assert fiscal_year_label(datetime(2023, 7, 31), 7) == "2023"


def _facts(**rows) -> pd.DataFrame:
    """A one-year facts table: rows are concepts, the single column is fiscal year 2022."""
    return pd.DataFrame({concept: {"2022": value} for concept, value in rows.items()}).T


def test_ratios_use_one_formula():
    facts = _facts(**{
        REVENUE: 1000.0, EBIT: 200.0, NET_INCOME: 100.0, INTEREST_EXPENSE: 20.0,
        BALANCE_SHEET_TOTAL: 2000.0, NUMBER_OF_SHARES_DILUTED: 50.0,
        metrics.STOCKHOLDERS_EQUITY: 800.0, metrics.GOODWILL: 100.0,
        metrics.INTANGIBLE_ASSETS: 50.0, metrics.CURRENT_ASSETS: 600.0,
        metrics.SHORTTERM_LIABILITIES: 400.0,
        OPERATING_CASH_FLOW: 300.0, CAPEX: 120.0,
    })
    add_sheet_ratios(facts)

    assert facts.loc[EPS, "2022"] == pytest.approx(2.0)          # 100 / 50
    assert facts.loc[EBIT_MARGIN, "2022"] == pytest.approx(20.0)  # 200 / 1000
    assert facts.loc[RO_A, "2022"] == pytest.approx(5.0)          # 100 / 2000
    assert facts.loc[TIER, "2022"] == pytest.approx(10.0)         # 200 / 20  (EBIT, not revenue)
    assert facts.loc[FREE_CASH_FLOW, "2022"] == pytest.approx(180.0)  # 300 - 120


def test_tier_without_interest_expense_is_not_a_number():
    """A debt-free company has no ratio. An epsilon would make it ~1e12 and a hardcoded 100
    would invent a measurement; both then leak into the multi-year average."""
    facts = _facts(**{EBIT: 200.0, INTEREST_EXPENSE: 0.0})
    tier = metrics.times_interest_earned(facts)

    assert pd.isna(tier["2022"])
    assert pd.isna(pd.Series([tier["2022"]]).mean())  # skipped by mean(), not averaged in


class _StubTicker:
    """Stands in for yf.Ticker so the adapter is testable without the network."""

    def __init__(self, balance_sheet, income_stmt, cashflow):
        self.balance_sheet = balance_sheet
        self.income_stmt = income_stmt
        self.cashflow = cashflow
        self.dividends = pd.Series(dtype="float64", index=pd.DatetimeIndex([]))


def _stub_microsoft_shaped() -> _StubTicker:
    """Two fiscal years ending in June, plus an older balance-sheet-only column — the shape
    Yahoo actually returns, where the oldest column has no matching income statement."""
    bs_cols = [pd.Timestamp("2025-06-30"), pd.Timestamp("2024-06-30"), pd.Timestamp("2023-06-30")]
    is_cols = [pd.Timestamp("2025-06-30"), pd.Timestamp("2024-06-30")]

    balance_sheet = pd.DataFrame(
        {bs_cols[0]: [800.0, 600.0, 400.0, 900.0, 2000.0, 100.0, 50.0],
         bs_cols[1]: [700.0, 550.0, 350.0, 850.0, 1800.0, 100.0, 50.0],
         bs_cols[2]: [600.0, 500.0, 300.0, 800.0, 1600.0, 100.0, 50.0]},
        index=["Stockholders Equity", "Current Assets", "Current Liabilities",
               "Total Liabilities Net Minority Interest", "Total Assets", "Goodwill",
               "Other Intangible Assets"],
    )
    income_stmt = pd.DataFrame(
        {is_cols[0]: [1000.0, 200.0, 20.0, 100.0, 48.0, 50.0],
         is_cols[1]: [900.0, 180.0, 18.0, 90.0, 48.0, 50.0]},
        index=["Total Revenue", "Operating Income", "Interest Expense", "Net Income",
               "Basic Average Shares", "Diluted Average Shares"],
    )
    cashflow = pd.DataFrame(
        {is_cols[0]: [300.0, -120.0], is_cols[1]: [280.0, -100.0]},
        index=["Operating Cash Flow", "Capital Expenditure"],
    )
    return _StubTicker(balance_sheet, income_stmt, cashflow)


def test_yahoo_adapter_labels_years_like_the_filings_side():
    facts = yahoo_source.load_facts("STUB", stock=_stub_microsoft_shaped())

    # June fiscal year end -> the period ending 2025-06-30 is FY2024, not FY2025.
    assert list(facts.columns) == ["2024", "2023"]


def test_yahoo_adapter_flips_the_capex_sign():
    """Yahoo reports capex as a negative outflow; the filings side reads a positive
    us-gaap:PaymentsToAcquirePropertyPlantAndEquipment. The shared FCF formula is
    OCF - capex, so an unflipped sign would silently double the free cash flow."""
    facts = yahoo_source.load_facts("STUB", stock=_stub_microsoft_shaped())

    assert facts.loc[CAPEX, "2024"] == pytest.approx(120.0)
    add_sheet_ratios(facts)
    assert facts.loc[FREE_CASH_FLOW, "2024"] == pytest.approx(180.0)  # 300 - 120, not 300 + (-120)


def test_yahoo_adapter_drops_a_year_without_an_income_statement():
    """Yahoo's oldest balance sheet column has no revenue. Kept, it would become a phantom
    year of zeros that drags every multi-year average down."""
    facts = yahoo_source.load_facts("STUB", stock=_stub_microsoft_shaped())

    assert "2022" not in facts.columns
    assert facts.loc[REVENUE].notna().all()


def test_valuation_reproduces_the_workbook_summary_block():
    """The Skyworks summary cells, computed from the four scalars the dashboard reads out of
    v_valuation instead of from a DataFrame.

    test_excel_parity.py pins the same numbers through add_price_ratios. Both paths land here
    now, so this is the check that the scalar entry point and the frame entry point cannot
    drift apart: price 69.50, average EPS 6.588, latest book value/share 42.1119.
    """
    ratios = metrics.valuation(69.5, 6.58789658, 42.1119)

    assert ratios[metrics.KGV] == pytest.approx(10.54964148088369, rel=1e-4)
    assert ratios[metrics.KBGV] == pytest.approx(17.41074525185313, rel=1e-4)
    assert ratios[metrics.RO_I] == pytest.approx(100 / 10.54964148088369, rel=1e-4)


def test_graham_number_is_the_geometric_mean_of_the_two_caps():
    """sqrt(22.5 x EPS x BVPS) — 22.5 = 15x earnings x 1.5x book. Ours, not the workbook's.

    Hand-checked: 22.5 x 4 x 10 = 900, whose root is 30. The margin is measured against the
    price, positive when the Graham number sits ABOVE it.
    """
    ratios = metrics.valuation(24.0, 4.0, 10.0)

    assert ratios[metrics.GRAHAM_NUMBER] == pytest.approx(30.0)
    assert ratios[metrics.GRAHAM_MARGIN] == pytest.approx(25.0)  # 30 is 25% above 24


@pytest.mark.parametrize(
    "price,eps,bvps",
    [
        (None, None, None),        # a delisted ticker: v_valuation hands back NULLs
        (100.0, 0.0, 50.0),        # zero average earnings -> no KGV
        (100.0, 5.0, 0.0),         # no reported book value -> no KBGV
        (100.0, -2.0, 30.0),       # a loss-making year -> no Graham number
        (100.0, 5.0, -8.0),        # negative equity -> same
    ],
)
def test_valuation_answers_nan_instead_of_raising(price, eps, bvps):
    """Every one of these is a real row in the database, and the dashboard renders them as a
    dash. A ZeroDivisionError, a ValueError out of sqrt, or a silent 0.0 standing in for "no
    value" would each be worse: the first two take the page down for one bad company, the
    third invents a measurement."""
    ratios = metrics.valuation(price, eps, bvps, bvps)

    assert set(ratios) == {metrics.RO_I, metrics.KGV, metrics.KBGV, metrics.KBGV_CONSERVATIVE,
                           metrics.GRAHAM_NUMBER, metrics.GRAHAM_MARGIN}
    for value in ratios.values():
        assert isinstance(value, float)


def test_valuation_accepts_the_decimals_psycopg_returns():
    """v_valuation is read straight out of Postgres, where numeric comes back as Decimal.
    Decimal / float raises TypeError, so the conversion has to happen inside valuation()
    rather than at each of its call sites."""
    from decimal import Decimal

    ratios = metrics.valuation(Decimal("24.0"), Decimal("4.0"), Decimal("10.0"))

    assert ratios[metrics.GRAHAM_NUMBER] == pytest.approx(30.0)
    assert ratios[metrics.KGV] == pytest.approx(6.0)
