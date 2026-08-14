"""Offline checks for the shared math and the Yahoo adapter.

No network: the Yahoo tests feed a stub in place of yf.Ticker. The point of these tests is
the two rules that silently corrupt a cross-source comparison when they drift — how a
fiscal year is labelled, and which sign/definition each source uses.
"""

from datetime import date, datetime

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


# ── the display-only price fetch ─────────────────────────────────────────────────────────


class _StubHistory:
    """Stands in for yf.Ticker in price_history(). `failing` names the tickers that behave
    the way a delisted symbol, a warrant Yahoo does not know, or a throttled request does."""

    def __init__(self, failing=(), empty=(), dates=("2026-08-10", "2026-08-11")):
        self.failing, self.empty, self.dates = set(failing), set(empty), dates
        self.calls = []
        self.history_kwargs = None

    def __call__(self, ticker):
        self.calls.append(ticker)
        self.ticker = ticker
        return self

    # **kwargs rather than period=: closes() forwards whatever the caller passes straight into
    # .history(), so the stub has to accept a start=/end= range as well as a period and record
    # what it actually received.
    def history(self, **kwargs):
        self.history_kwargs = kwargs
        if self.ticker in self.failing:
            raise RuntimeError(f"$%^ no data found, symbol may be delisted ({self.ticker})")
        if self.ticker in self.empty:
            return pd.DataFrame({"Close": []}, index=pd.DatetimeIndex([]))
        return pd.DataFrame(
            {"Close": [10.0, 11.0]}, index=pd.DatetimeIndex(self.dates, tz="America/New_York")
        )


def test_price_history_returns_one_long_row_per_day_and_ticker(monkeypatch):
    monkeypatch.setattr(yahoo_source.yf, "Ticker", _StubHistory())

    prices = yahoo_source.price_history(("dac", "egle"))

    assert list(prices.columns) == yahoo_source.PRICE_COLUMNS
    assert len(prices) == 4
    assert set(prices["ticker"]) == {"dac", "egle"}
    # tz stripped: Altair plots a naive date, and two tickers on two exchanges would
    # otherwise carry two offsets into one x-axis.
    assert prices["date"].dt.tz is None


def test_one_failing_ticker_does_not_take_the_others_down(monkeypatch):
    """THE point of this function. gsl-pb is a real warrant-style row in this database, and a
    chart of five other companies must not go blank because Yahoo has nothing for it."""
    monkeypatch.setattr(yahoo_source.yf, "Ticker", _StubHistory(failing=["gsl-pb"]))

    prices = yahoo_source.price_history(("dac", "gsl-pb", "egle"), retries=1)

    assert set(prices["ticker"]) == {"dac", "egle"}
    assert "gsl-pb" not in set(prices["ticker"])


def test_a_ticker_yahoo_has_no_rows_for_is_left_out_rather_than_plotted_empty(monkeypatch):
    """Yahoo answers for an unknown symbol with an empty frame instead of an error, so the
    empty case has to be caught separately from the raising one."""
    monkeypatch.setattr(yahoo_source.yf, "Ticker", _StubHistory(empty=["ctrm"]))

    prices = yahoo_source.price_history(("dac", "ctrm"))

    assert set(prices["ticker"]) == {"dac"}


def test_every_ticker_failing_is_an_empty_frame_with_its_columns(monkeypatch):
    """Yahoo down, or no network at all. The page checks .empty and then reads
    prices["ticker"] — a frame with no columns would raise a KeyError instead."""
    monkeypatch.setattr(yahoo_source.yf, "Ticker", _StubHistory(failing=["dac", "egle"]))

    prices = yahoo_source.price_history(("dac", "egle"), retries=1)

    assert prices.empty
    assert list(prices.columns) == yahoo_source.PRICE_COLUMNS


def test_a_failing_ticker_is_retried_before_it_is_given_up_on(monkeypatch):
    """Same measured reason market_price_of() retries: this comes off Yahoo's unofficial
    endpoint, where one ordinary run turned 19 of 21 tickers priceless and every one of them
    answered on the next attempt."""
    stub = _StubHistory(failing=["dac"])
    monkeypatch.setattr(yahoo_source.yf, "Ticker", stub)

    yahoo_source.price_history(("dac",), retries=2, backoff_seconds=0)

    assert stub.calls == ["dac", "dac"]


# ── dated closes for the backtest ────────────────────────────────────────────────────────


def closes_series(dates, values, tz=None) -> pd.Series:
    return pd.Series(values, index=pd.DatetimeIndex(dates, tz=tz), dtype="float64")


def test_closes_forwards_whatever_history_arguments_the_caller_passes(monkeypatch):
    """The reason _closes() was widened instead of a second retry/backoff wrapper being
    written: the backtest needs a start=/end= RANGE (one call covering both the cutoff and
    today), while price_history() needs period=. One fetch helper, two call shapes."""
    stub = _StubHistory()
    monkeypatch.setattr(yahoo_source.yf, "Ticker", stub)

    yahoo_source.closes("dac", start="2024-08-06", end="2026-08-14")

    assert stub.history_kwargs == {"start": "2024-08-06", "end": "2026-08-14"}


def test_closes_gives_up_quietly_rather_than_raising(monkeypatch):
    """One ticker's failure is never the run's failure — 4,216 tickers are fetched in a row and
    a delisted symbol among them must not take the report down."""
    monkeypatch.setattr(yahoo_source.yf, "Ticker", _StubHistory(failing=["dac"]))

    assert yahoo_source.closes("dac", retries=2, backoff_seconds=0, period="2y") is None


def test_nearest_close_picks_the_closest_trading_day():
    """A specific calendar date is often not a trading day — 2024-08-13 is, but a cutoff on a
    weekend or a holiday simply has no row, and Yahoo does not interpolate one."""
    series = closes_series(["2024-08-09", "2024-08-12", "2024-08-15"], [9.0, 12.0, 15.0])

    price, actual = yahoo_source.nearest_close(series, date(2024, 8, 13))

    assert price == pytest.approx(12.0)
    assert actual == date(2024, 8, 12)


def test_nearest_close_breaks_a_tie_toward_the_earlier_day():
    """THE rule that keeps a cutoff price from being a look-ahead. Equidistant before and
    after, the earlier day is the one that was actually knowable on the cutoff — picking the
    later one prices the company with information from after the date being tested."""
    series = closes_series(["2024-08-12", "2024-08-14"], [12.0, 14.0])

    price, actual = yahoo_source.nearest_close(series, date(2024, 8, 13))

    assert price == pytest.approx(12.0)
    assert actual == date(2024, 8, 12)


def test_nearest_close_handles_yahoo_s_timezone_aware_index():
    """yfinance returns a tz-aware DatetimeIndex (price_history() already has to strip it).
    Comparing that against a naive date silently yields wrong or zero matches rather than
    raising, which is the worst kind of wrong."""
    series = closes_series(["2024-08-12", "2024-08-15"], [12.0, 15.0], tz="America/New_York")

    price, actual = yahoo_source.nearest_close(series, date(2024, 8, 13))

    assert price == pytest.approx(12.0)
    assert actual == date(2024, 8, 12)


@pytest.mark.parametrize("nothing", [None, closes_series([], [])])
def test_nearest_close_answers_nothing_for_nothing(nothing):
    """closes() returns None when Yahoo raised and an empty Series when it simply had no rows;
    both mean "no price", and the caller checks one thing."""
    assert yahoo_source.nearest_close(nothing, date(2024, 8, 13)) == (None, None)
