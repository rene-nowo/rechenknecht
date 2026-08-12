"""Yahoo Finance as a second source for the same facts table.

Yahoo is a CROSS-CHECK, not an oracle. It serves the latest restated figures from a data
vendor, while a filing says what it said on the day it was filed — Foot Locker's FY2021
revenue is 8,958M in the FY2021 10-K and 8,968M once restated a year later. Comparing
against Yahoo with zero tolerance therefore marks correct extraction as wrong. The exact
oracle is the SEC's own companyfacts API, which can be pinned to the same accession.

What this module IS good for: a wide-tolerance smoke check across many companies, so a
parser that silently grabs a segment total instead of a consolidated one gets noticed.

Replaces the former RechenknechtYahoo, which could not run: it read a `bs_data` attribute
that was never assigned, called a `get_context_date` method it did not define, loaded a
"Rechenmaster Overview.xlsx" that does not exist, and used DataFrame.append (removed in
pandas 2.0). Its TIER also divided revenue by interest expense despite its own comment
saying EBIT.
"""

import logging
import time

import pandas as pd
import yfinance as yf

from src.metrics import (
    BALANCE_SHEET_TOTAL,
    CAPEX,
    CURRENT_ASSETS,
    DIVIDENDS_PER_SHARE,
    EBIT,
    GOODWILL,
    INTANGIBLE_ASSETS,
    INTEREST_EXPENSE,
    LONGTERM_LIABILITIES,
    NET_INCOME,
    NUMBER_OF_SHARES,
    NUMBER_OF_SHARES_DILUTED,
    OPERATING_CASH_FLOW,
    REVENUE,
    SHORTTERM_LIABILITIES,
    STOCKHOLDERS_EQUITY,
    TOTAL_LIABILITIES,
    add_averages,
    add_sheet_ratios,
    fiscal_year_label,
    longterm_from_total,
)

logger = logging.getLogger(__name__)

# Yahoo's statement row label -> our concept name. Two deliberate choices:
#  - "Operating Income", NOT Yahoo's "EBIT": the filings side reads us-gaap:OperatingIncomeLoss,
#    and the two differ (MSFT FY2025: 128.5bn vs 126.0bn). Mapping to EBIT would invent a gap.
#  - Basic/Diluted *Average* Shares to match us-gaap:WeightedAverageNumberOf...SharesOutstanding,
#    not the point-in-time "Ordinary Shares Number" on the balance sheet.
BALANCE_SHEET_ROWS = {
    "Stockholders Equity": STOCKHOLDERS_EQUITY,
    "Current Assets": CURRENT_ASSETS,
    "Current Liabilities": SHORTTERM_LIABILITIES,
    "Total Liabilities Net Minority Interest": TOTAL_LIABILITIES,
    "Total Assets": BALANCE_SHEET_TOTAL,
    "Goodwill": GOODWILL,
    "Other Intangible Assets": INTANGIBLE_ASSETS,
}
INCOME_ROWS = {
    "Total Revenue": REVENUE,
    "Operating Income": EBIT,
    "Interest Expense": INTEREST_EXPENSE,
    "Net Income": NET_INCOME,
    "Basic Average Shares": NUMBER_OF_SHARES,
    "Diluted Average Shares": NUMBER_OF_SHARES_DILUTED,
}
CASHFLOW_ROWS = {
    "Operating Cash Flow": OPERATING_CASH_FLOW,
}

# Concepts a filing may legitimately omit. The filings side zeroes exactly these in
# nan_to_zero(); the two sources must treat absence identically or every comparison of a
# debt-free or goodwill-free company shows a false difference.
OPTIONAL_CONCEPTS = [
    DIVIDENDS_PER_SHARE,
    GOODWILL,
    INTANGIBLE_ASSETS,
    INTEREST_EXPENSE,
    CAPEX,
    OPERATING_CASH_FLOW,
]


def _copy_rows(statement: pd.DataFrame, mapping: dict, facts: pd.DataFrame, fy_end_month: int) -> None:
    for yahoo_row, concept in mapping.items():
        if yahoo_row not in statement.index:
            logger.debug("Yahoo has no row %r", yahoo_row)
            continue
        for column in statement.columns:
            value = statement.loc[yahoo_row, column]
            if pd.notna(value):
                facts.loc[concept, fiscal_year_label(pd.Timestamp(column), fy_end_month)] = float(value)


def load_facts(ticker: str, stock: yf.Ticker = None) -> pd.DataFrame:
    """Build the shared facts table for one ticker. Columns are fiscal years, newest first."""
    stock = stock or yf.Ticker(ticker)
    balance_sheet, income, cashflow = stock.balance_sheet, stock.income_stmt, stock.cashflow

    if balance_sheet.empty:
        raise ValueError(f"Yahoo returned no balance sheet for {ticker} (delisted?)")

    # The company's fiscal year end, taken from the most recent balance sheet date. The
    # filings side reads the same thing from dei:CurrentFiscalYearEndDate.
    fy_end_month = pd.Timestamp(max(balance_sheet.columns)).month

    facts = pd.DataFrame(dtype="float64")
    _copy_rows(balance_sheet, BALANCE_SHEET_ROWS, facts, fy_end_month)
    _copy_rows(income, INCOME_ROWS, facts, fy_end_month)
    _copy_rows(cashflow, CASHFLOW_ROWS, facts, fy_end_month)

    # Yahoo reports capex as a negative cash outflow; the filings side reads
    # us-gaap:PaymentsToAcquirePropertyPlantAndEquipment, which is positive. The shared
    # free-cash-flow formula is OCF - capex, so this must be flipped to match.
    if "Capital Expenditure" in cashflow.index:
        for column in cashflow.columns:
            value = cashflow.loc["Capital Expenditure", column]
            if pd.notna(value):
                facts.loc[CAPEX, fiscal_year_label(pd.Timestamp(column), fy_end_month)] = -float(value)

    # Long-term liabilities are not reported directly; derived by the shared rule.
    if TOTAL_LIABILITIES in facts.index and SHORTTERM_LIABILITIES in facts.index:
        facts.loc[LONGTERM_LIABILITIES] = longterm_from_total(
            facts.loc[TOTAL_LIABILITIES], facts.loc[SHORTTERM_LIABILITIES]
        )

    _add_dividends_per_share(stock, balance_sheet, facts, fy_end_month)

    facts = facts.reindex(sorted(facts.columns, reverse=True), axis=1)

    # Yahoo's oldest balance sheet column often has no matching income statement, which
    # would otherwise become a phantom year: no revenue, but zeros from the optional fill
    # below. Drop it BEFORE filling, the way the filings side drops empty columns in
    # clean(). Revenue is the marker — without it no ratio on the sheet is computable.
    if REVENUE in facts.index:
        usable = facts.loc[REVENUE].notna()
        for year in facts.columns[~usable]:
            logger.debug("Dropping %s: Yahoo has no revenue for that fiscal year", year)
        facts = facts.loc[:, usable]

    for concept in OPTIONAL_CONCEPTS:
        if concept in facts.index:
            facts.loc[concept] = facts.loc[concept].fillna(0.0)
    return facts.astype("float64")


def _add_dividends_per_share(
    stock: yf.Ticker, balance_sheet: pd.DataFrame, facts: pd.DataFrame, fy_end_month: int
) -> None:
    """Sum the dividends actually paid inside each fiscal year window.

    The window comes from the statement period ends, not from the year label — a fiscal
    year labelled 2022 can run to January 2023, and a calendar-year window would pick up
    the wrong quarter. Note this is dividends *paid*, whereas the filings side reads
    DividendsPerShareDeclared; the two can differ by a quarter at the year boundary.
    """
    dividends = stock.dividends
    if dividends is None or dividends.empty:
        return

    for column in balance_sheet.columns:
        end = pd.Timestamp(column)
        if dividends.index.tz is not None:
            end = end.tz_localize(dividends.index.tz) if end.tz is None else end
        start = end - pd.DateOffset(years=1)

        paid = dividends.loc[(dividends.index > start) & (dividends.index <= end)]
        facts.loc[DIVIDENDS_PER_SHARE, fiscal_year_label(pd.Timestamp(column), fy_end_month)] = float(paid.sum())


# The columns price_history() returns, also when it returns nothing. A caller that plots the
# result must get the same shape whether Yahoo answered or not — an empty frame with no
# columns raises on the first `frame["ticker"]` and takes the whole page down, which is the
# failure this function exists to avoid in the first place.
PRICE_COLUMNS = ["date", "ticker", "close"]


def price_history(tickers, period: str = "2y", retries: int = 2, backoff_seconds: float = 1.0) -> pd.DataFrame:
    """Daily closing prices for several tickers, long format (date, ticker, close).

    DISPLAY ONLY — nothing here is written to the database. The facts side of this module is
    a cross-check against filings; this is just "what has the price done lately", plotted
    next to a valuation that was computed from filings months old.

    ONE TICKER'S FAILURE IS NOT THE PAGE'S FAILURE. Every ticker is fetched inside its own
    try, and a symbol that raises, or that Yahoo simply has no rows for, is logged and left
    out — the remaining lines still plot. A delisted ticker, a warrant symbol Yahoo does not
    know (`gsl-pb` is a real row in this database), or a throttled request must not blank a
    chart of five other companies.

    Retried for the same measured reason market_price_of() in ingest.py is: fast_info and
    history() come off the same unofficial endpoint, where a single attempt was not enough —
    one ordinary ingest run turned 19 of 21 tickers priceless and every one of them worked on
    the next attempt. Two attempts here rather than three, because this one is in front of a
    waiting user rather than in a batch job.

    contra: N tickers are N sequential requests, ~1s each. Bounded by the caller's cap (the
    deep-dive fetches at most a handful of candidates). If that cap is ever raised, switch to
    the batched yf.download(tickers) — at which point per-ticker isolation has to be
    re-established over its MultiIndex columns, which is why it is not the shape used here.
    """
    frames = []
    for ticker in tickers:
        closes = _closes(ticker, period, retries, backoff_seconds)
        if closes is None or closes.empty:
            logger.warning("no price history for %s — left out of the chart", ticker)
            continue
        frames.append(pd.DataFrame({
            "date": pd.to_datetime(closes.index).tz_localize(None),
            "ticker": ticker,
            "close": closes.astype("float64").values,
        }))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=PRICE_COLUMNS)


def _closes(ticker: str, period: str, retries: int, backoff_seconds: float):
    """The Close column for one ticker, or None. Never raises — see price_history()."""
    for attempt in range(retries):
        try:
            return yf.Ticker(ticker).history(period=period)["Close"]
        except Exception as error:  # delisted, throttled, unknown symbol, or no Close column
            logger.debug("no history for %s (attempt %d/%d): %s", ticker, attempt + 1, retries, error)
            if attempt + 1 < retries:
                time.sleep(backoff_seconds * (attempt + 1))
    return None


def analyze(ticker: str, market_price: float = None) -> pd.DataFrame:
    """Yahoo facts run through the SAME ratio code the filings side uses."""
    stock = yf.Ticker(ticker)
    facts = load_facts(ticker, stock=stock)

    if market_price is None:
        market_price = stock.fast_info["lastPrice"]

    add_sheet_ratios(facts)
    add_averages(facts, market_price, time_span=7)
    return facts.round(decimals=2)
