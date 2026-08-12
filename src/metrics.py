"""The one place where a ratio is defined.

Every source (SEC filings via XBRL, Yahoo via yfinance) fills the same facts table and then
calls into here. That is the whole point: when EDGAR and Yahoo disagree about EBIT-margin,
the difference is in the *data*, never in the formula — so a cross-check actually means
something. Two copies of `ebit / revenue` would make every comparison ambiguous.

Layout of the facts table (a pandas DataFrame):
  rows    = the concept names below
  columns = fiscal years as strings ("2022"), plus the average column added at the end
"""

import math

import pandas as pd

#### FACTS — read from a source, never computed here ####
REVENUE = "revenue"
REVENUE_CUSTOMER = "revenue_customer"
EBIT = "ebit"
NET_INCOME = "net_income"
INTEREST_EXPENSE = "interest_expense"
INTEREST_INCOME_EXPENSE = "interest_income_expense"
OPERATING_CASH_FLOW = "operating_cash_flow"
CAPEX = "capex"
NUMBER_OF_SHARES = "number_of_shares"
NUMBER_OF_SHARES_DILUTED = "number_of_shares_diluted"
DIVIDENDS_PER_SHARE = "dividends_per_share"
STOCKHOLDERS_EQUITY = "stockholders_equity"
CURRENT_ASSETS = "current_assets"
SHORTTERM_LIABILITIES = "current_liabilities"
LONGTERM_LIABILITIES = "longterm_liabilities"
TOTAL_LIABILITIES = "total_liabilities"
GOODWILL = "goodwill"
INTANGIBLE_ASSETS = "intangible_assets"
CASH = "cash"
ACCOUNTS_RECEIVABLE = "accounts_receivable"
ACCRUED_EXPENSES_AND_ACCOUNTS_PAYABLE = "accrued_expenses_and_accounts_payable"

# Total cash paid out as dividends (Master_Vorlage row 36). The workbook derives dividend
# per share from THIS (row 6 = row 36 / row 37), not from the per-share figure a filing
# declares — those are different quantities and differ by roughly a quarter's payment
# around the fiscal year boundary.
DIVIDENDS_PAID = "dividends_paid"

# The balance-sheet total (German "Gesamtkapital"): us-gaap:LiabilitiesAndStockholdersEquity.
# It is the sum of the balance sheet, NOT equity — despite the row label, which is kept for
# now because it is also the key into rechenknecht_index_map.json.
BALANCE_SHEET_TOTAL = "total_equity"

#### DERIVED — computed here, by exactly one formula each ####
EPS = "EPS"
BOOK_VALUE_PER_SHARE = "book_value_per_share"
CONSERVATIVE_BOOK_VALUE_PER_SHARE = "conservative_book_value_per_share"
RO_A = "RoA"
EBIT_MARGIN = "EBIT-margin"
EQUITY_RATIO = "equity-ratio"
NETTOUMLAUFVERM_GEN = "nettoumlaufvermögen"
TIER = "TIER"
FREE_CASH_FLOW = "free_cash_flow"
FCF_PER_SHARE = "fcf_per_share"
DEBT_TO_EQUITY = "debt_to_equity"
DEBT_TO_FCF = "debt_to_fcf"
FCF_PAYOUT_RATIO = "fcf_payout_ratio"
OCF_MINUS_NET_INCOME = "ocf_minus_net_income"

#### DERIVED, price-dependent — need a market price, so they are kept separate ####
RO_I = "RoI"
KGV = "KGV"
KBGV = "KBGV"
KBGV_CONSERVATIVE = "KGBV_conservative"

# OURS, not Master_Vorlage's — the workbook has no Graham number anywhere in the summary
# block (BA-BL). Named here the same way conservative_book_value_per_share is, so that a
# reader looking for its cell in the spec does not go hunting for one that does not exist.
GRAHAM_NUMBER = "graham_number"
GRAHAM_MARGIN = "graham_margin"

# Graham's rule of thumb: a defensive investor pays at most 15x earnings and 1.5x book
# value, so the fair price is sqrt(15 * 1.5 * EPS * BVPS).
GRAHAM_FACTOR = 22.5


def longterm_from_total(total_liabilities, current_liabilities):
    """Long-term liabilities: what is left of total liabilities after the current portion.

    Neither source reports this directly, so both derived it — filings in
    RechenknechtBeta.set_longterm_liabilities(), Yahoo in yahoo_source.load_facts(). Two
    copies of a derivation is exactly what this module exists to prevent: a divergence there
    would show up as a data disagreement between the sources and be chased in the wrong
    place. Scalars or Series, whatever the caller holds.
    """
    return total_liabilities - current_liabilities


def fiscal_year_label(period_end, fiscal_year_end_month: int) -> str:
    """The year a fiscal period is filed under, from the period end and the company's
    fiscal year end month.

    A fiscal year ending in the first half of the calendar year belongs to the previous
    year: Foot Locker's year ending 2023-01-28 is FY2022, and Microsoft's ending
    2025-06-30 is FY2024 by this convention. A year ending July or later keeps its own
    calendar year.

    One rule covers both instants (balance sheet dates) and durations (income statement
    periods), because a full year ending in month <= 6 always starts in the prior calendar
    year. Both sources call this, or a cross-check silently compares adjacent years.
    """
    return str(period_end.year - 1 if fiscal_year_end_month <= 6 else period_end.year)


# The rows add_sheet_ratios reads. A source that never encountered one of these would
# otherwise raise KeyError deep inside a formula — which is how a company with no reported
# goodwill took down a whole ingest run.
SHEET_RATIO_INPUTS = [
    REVENUE, EBIT, NET_INCOME, INTEREST_EXPENSE, OPERATING_CASH_FLOW, CAPEX,
    NUMBER_OF_SHARES_DILUTED, STOCKHOLDERS_EQUITY, BALANCE_SHEET_TOTAL,
    CURRENT_ASSETS, SHORTTERM_LIABILITIES, GOODWILL, INTANGIBLE_ASSETS,
    LONGTERM_LIABILITIES, TOTAL_LIABILITIES, DIVIDENDS_PER_SHARE,
]


def ensure_inputs(df: pd.DataFrame) -> list:
    """Add any missing input row as NaN. Returns the names that were missing, so a caller
    can log real coverage gaps instead of silently treating absence as zero."""
    missing = [concept for concept in SHEET_RATIO_INPUTS if concept not in df.index]
    for concept in missing:
        df.loc[concept] = float("nan")
    return missing


def share_divisor(df: pd.DataFrame, current_shares: float = None) -> pd.Series:
    """The share count every per-share metric divides by.

    Master_Vorlage exposes this as a toggle (BB26 "Mit aktuellen Aktien rechnen", read
    through BB39): with it off each year uses its own diluted count, with it on every year
    uses today's count so a buyback does not flatter the earlier years. Both are legitimate
    and the workbook ships both, so this is a parameter rather than a decision baked in.
    """
    if current_shares is None:
        return df.loc[NUMBER_OF_SHARES_DILUTED]
    return pd.Series(float(current_shares), index=df.columns)


def add_sheet_ratios(df: pd.DataFrame, current_shares: float = None) -> None:
    """Ratios that follow from the filing alone. No market price involved."""
    ensure_inputs(df)
    shares = share_divisor(df, current_shares)

    df.loc[EPS] = df.loc[NET_INCOME] / shares

    df.loc[BOOK_VALUE_PER_SHARE] = df.loc[STOCKHOLDERS_EQUITY] / shares
    df.loc[CONSERVATIVE_BOOK_VALUE_PER_SHARE] = (
        df.loc[STOCKHOLDERS_EQUITY] - df.loc[GOODWILL] - df.loc[INTANGIBLE_ASSETS]
    ) / shares

    df.loc[RO_A] = (df.loc[NET_INCOME] / df.loc[BALANCE_SHEET_TOTAL]) * 100
    df.loc[EBIT_MARGIN] = (df.loc[EBIT] / df.loc[REVENUE]) * 100
    df.loc[EQUITY_RATIO] = (df.loc[STOCKHOLDERS_EQUITY] / df.loc[BALANCE_SHEET_TOTAL]) * 100

    df.loc[NETTOUMLAUFVERM_GEN] = df.loc[CURRENT_ASSETS] - df.loc[SHORTTERM_LIABILITIES]
    df.loc[TIER] = times_interest_earned(df)
    df.loc[FREE_CASH_FLOW] = df.loc[OPERATING_CASH_FLOW] - df.loc[CAPEX]
    df.loc[FCF_PER_SHARE] = df.loc[FREE_CASH_FLOW] / shares

    # Dividend per share is DERIVED from cash actually paid out (Master_Vorlage row 6),
    # whenever that input is available. A per-share figure declared in a filing is a
    # different quantity and is left untouched if dividends paid was never captured.
    if DIVIDENDS_PAID in df.index and df.loc[DIVIDENDS_PAID].notna().any():
        df.loc[DIVIDENDS_PER_SHARE] = df.loc[DIVIDENDS_PAID].abs() / shares

    _add_leverage_ratios(df)


def _add_leverage_ratios(df: pd.DataFrame) -> None:
    """Master_Vorlage row 15 is a selector over several debt ratios (BB30) and row 16 an
    earnings-quality flag. The workbook renders them as strings like "0.5 : 1"; the numbers
    are what matter, so they are stored numerically and formatted at display time."""
    equity = df.loc[STOCKHOLDERS_EQUITY].replace(0, float("nan"))
    df.loc[DEBT_TO_EQUITY] = df.loc[LONGTERM_LIABILITIES] / equity

    if TOTAL_LIABILITIES in df.index:
        free_cash_flow = df.loc[FREE_CASH_FLOW].replace(0, float("nan"))
        df.loc[DEBT_TO_FCF] = df.loc[TOTAL_LIABILITIES] / free_cash_flow

    fcf_per_share = df.loc[FCF_PER_SHARE].replace(0, float("nan"))
    df.loc[FCF_PAYOUT_RATIO] = df.loc[DIVIDENDS_PER_SHARE] / fcf_per_share

    # Positive means operating cash flow exceeds reported profit — the workbook shows this
    # as "Positiv"/"Negativ"; the signed amount carries strictly more information.
    df.loc[OCF_MINUS_NET_INCOME] = df.loc[OPERATING_CASH_FLOW] - df.loc[NET_INCOME]


def times_interest_earned(df: pd.DataFrame) -> pd.Series:
    """EBIT / interest expense — how often the operating result covers the cost of debt.

    A company with no interest expense has no ratio, so the result is NaN rather than a
    stand-in: an epsilon turns it into ~1e12 and a hardcoded 100 invents a measurement.
    Both then leak into the multi-year average and quietly distort it. NaN is skipped by
    `Series.mean()`, which is the honest behaviour.
    """
    interest = df.loc[INTEREST_EXPENSE].replace(0, float("nan"))
    return df.loc[EBIT] / interest


def year_columns(df: pd.DataFrame) -> list:
    """The fiscal-year columns, newest first. Sorted explicitly rather than trusting the
    order a source happened to insert them in — the averaging window and the "latest book
    value" both depend on it, and getting it wrong is silent."""
    years = [c for c in df.columns if len(str(c)) == 4 and str(c).isdigit()]
    return sorted(years, key=int, reverse=True)


def _number(value) -> float:
    """Whatever a caller holds — None, a psycopg Decimal from a query, a numpy float, a
    string out of a CSV — as a plain float, with NaN standing for "no value".

    Every formula below then propagates NaN on its own and needs no None branch. This matters
    because the dashboard reads these inputs straight out of Postgres, where a bank with no
    reported book value per share is a NULL, not a zero.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _ratio(numerator: float, denominator: float) -> float:
    """NaN when the division is not defined, never an epsilon and never a stand-in — same
    reasoning as times_interest_earned: an invented number leaks into everything downstream,
    while NaN is skipped by Series.mean() and rendered as "no value" by the dashboard.

    A zero denominator is a real case here: a company with zero average earnings has no KGV.
    """
    return numerator / denominator if denominator else float("nan")


def in_quote_currency(fx_rate: float, *amounts) -> tuple:
    """Reporting-currency figures as quote-currency figures, ready for valuation() below.

    The direction lives HERE, in one place, because both callers of valuation() need it and
    two copies of it could disagree: `fx_rate` is quote-per-base (src/fx.py rate()), so this
    MULTIPLIES, and the PRICE is never touched. A price is what the exchange says it is —
    earnings and book value adapt to it, never the reverse. It also has to be that way round
    for the Graham number, which is itself a price and has to come out comparable to the real
    one; for KGV and RoI alone, converting the price instead would give the same ratio.

    fx_rate 1.0 is the case that must be perfect rather than merely close: a US filer reports
    in the currency it trades in, and x * 1.0 is exactly x in IEEE 754, so nothing moves for
    the companies that were already stored.

    Coerced through _number for the same reason valuation() is: the dashboard passes these
    straight out of Postgres, where a numeric is a Decimal and a NULL is None — and
    Decimal * float raises TypeError while None * float raises too.
    """
    return tuple(_number(amount) * fx_rate for amount in amounts)


def valuation(
    price,
    avg_eps,
    book_value_per_share,
    conservative_book_value_per_share=None,
) -> dict:
    """The price-dependent valuation numbers, from the four inputs they need.

    THE one definition of these formulas. `add_price_ratios` below calls it for the ingest
    path and the dashboard calls it for the display path, so a stored KGV and a freshly shown
    KGV can only ever differ in the price they were given, never in the arithmetic. Three
    divergent copies of exactly these ratios is the defect this module exists to prevent, and
    v_valuation in db/schema.sql deliberately exposes the inputs so SQL never becomes a
    fourth copy.

    Master_Vorlage's summary block, pinned by test_excel_parity.py:
        RoI  = BF6 = average EPS / price x 100
        KGV  = BG6 = price / average EPS
        KBGV = BH6 = price / LATEST book value per share x KGV

    The Graham number and its margin are OURS — the workbook has none. Everything is a plain
    float, so scalars in, scalars out; nothing here knows about DataFrames.
    """
    price = _number(price)
    avg_eps = _number(avg_eps)
    book_value_per_share = _number(book_value_per_share)
    conservative_book_value_per_share = _number(conservative_book_value_per_share)

    kgv = _ratio(price, avg_eps)
    graham = _sqrt(GRAHAM_FACTOR * avg_eps * book_value_per_share)

    return {
        RO_I: _ratio(avg_eps, price) * 100,
        KGV: kgv,
        KBGV: _ratio(price, book_value_per_share) * kgv,
        KBGV_CONSERVATIVE: _ratio(price, conservative_book_value_per_share) * kgv,
        GRAHAM_NUMBER: graham,
        # Positive means the Graham number sits ABOVE the market price, i.e. the share trades
        # at a discount to it. Kept here rather than in the dashboard so that the one place
        # holding valuation arithmetic really is the only one.
        GRAHAM_MARGIN: _ratio(graham, price) * 100 - 100,
    }


def _sqrt(value: float) -> float:
    """NaN for a negative radicand instead of a ValueError. Negative average earnings and
    negative book value are both real (a company can lose money, and a buyback-funded balance
    sheet can go equity-negative) — there is simply no Graham number for them, and one
    company without one must not take down a whole ingest run."""
    return math.sqrt(value) if value >= 0 else float("nan")


def add_price_ratios(df: pd.DataFrame, market_price: float, avg_column: str, fx_rate: float = 1.0) -> None:
    """Valuation ratios. Only defined against the average column, since a price is a point
    in time while the sheet ratios are per fiscal year.

    fx_rate converts the frame's per-share figures into the currency the price is quoted in
    (see in_quote_currency). It reaches ONLY the ratios: the rows themselves stay in the
    currency the filing reported, which is what they are and what `fact` stores.
    """
    # Book value is a stock, not a flow: use the latest year rather than an average of it.
    # Master_Vorlage does the same — BA10 walks back to the last reported value and its
    # label at BB4 is "Buchwert (letzter Bericht)".
    latest = year_columns(df)[0]
    df.loc[BOOK_VALUE_PER_SHARE, avg_column] = df.loc[BOOK_VALUE_PER_SHARE, latest]
    df.loc[CONSERVATIVE_BOOK_VALUE_PER_SHARE, avg_column] = df.loc[CONSERVATIVE_BOOK_VALUE_PER_SHARE, latest]

    ratios = valuation(
        market_price,
        *in_quote_currency(
            fx_rate,
            df.loc[EPS, avg_column],
            df.loc[BOOK_VALUE_PER_SHARE, avg_column],
            df.loc[CONSERVATIVE_BOOK_VALUE_PER_SHARE, avg_column],
        ),
    )

    # The Graham figures are NOT written into the frame. They would become two extra rows in
    # every analysis frame and the Foot Locker golden CSV compares the full row index — a
    # re-baseline of a contested fixture is a far bigger decision than a dashboard, so they
    # stay a return value that the dashboard computes on demand.
    for concept in (RO_I, KGV, KBGV, KBGV_CONSERVATIVE):
        df.loc[concept, avg_column] = ratios[concept]


def add_averages(df: pd.DataFrame, market_price: float, time_span: int = 7, fx_rate: float = 1.0) -> str:
    """Average every row over the last `time_span` years, then add the valuation ratios.

    Returns the name of the column it wrote, so callers do not rebuild the string. fx_rate is
    handed straight to add_price_ratios; 1.0, the default, is a filer that reports in the
    currency it trades in and changes nothing.
    """
    avg_column = f"{time_span}_YEAR_AVG"

    # The MOST RECENT `time_span` years. Master_Vorlage's BA3 finds the last filled column
    # and averages the seven ending there (AH3:AN3 for data through 2024), so a company with
    # more history than the window must drop the oldest years, not the newest. Selecting by
    # sorted year rather than by position also means the average no longer depends on the
    # column order a source produced, and the average column itself can never leak into it.
    window = year_columns(df)[:time_span]

    for row in df.index:
        df.loc[row, avg_column] = df.loc[row, window].mean()

    add_price_ratios(df, market_price, avg_column, fx_rate)
    return avg_column
