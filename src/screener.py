"""What the screener decides: which industry a company is in, what its ratios are, and
which rows survive the filters.

Separate from pages/Screener.py because a Streamlit page runs its whole body on import and
therefore cannot be unit-tested — and every rule below is one that would silently show a
wrong company or a wrong number if it drifted. test/test_screener.py is the check.

NO RATIO IS COMPUTED HERE. Same rule as dashboard.py and for the same reason: KGV, RoI and
the Graham margin come out of src/metrics.py valuation(), the one function the ingest also
calls and test_excel_parity.py pins against the workbook. This file arranges inputs and
filters rows; the arithmetic is borrowed.
"""

import pandas as pd

from src import metrics

# SIC division H, "Finance, Insurance And Real Estate" — the standard SIC boundary, and
# exactly the ranges to exclude by default: 6000-6099 depository institutions, 6100-6199
# non-depository credit, 6200-6299 brokers and exchanges, 6300-6411 insurance carriers and
# agents, 6500-6599 real estate, 6700-6799 holding and investment offices (6798 REITs).
#
# WHY they are out by default: their accounting structure — deposit and lending flows, a
# different net-income concept, cash-flow presentation that disagrees between data vendors —
# makes KGV/RoA/equity-ratio not comparable to an industrial or tech company. That was
# confirmed on mufg's cross-source disagreement, which turned out to be a genuine methodology
# difference and not a data bug. They are hidden, never dropped: the toggle brings them back.
FINANCIAL_SIC = ((6000, 6799),)

# "Hardware/software/tech" as SIC major groups:
#   3570-3579  computer and office equipment (3571 electronic computers, 3572 storage,
#              3576 computer communications equipment, 3577 peripherals)
#   3600-3699  electronic and other electrical equipment — this is where semiconductors
#              (3674) and communications equipment (3661, 3663) live
#   7370-7379  computer programming, data processing and other computer-related services
#              (7372 prepackaged software, 7371 programming, 7373 systems design)
# Deliberately NOT a complete map of "tech companies": SIC classifies by what a filer sells,
# so amzn is 5961 (catalog retail) and baba/pdd are 7389 (business services, NEC). A quick
# filter that narrows 170 companies to the obvious hardware/software ones is what this is —
# turn it off and use the range filters when it hides something you wanted.
TECH_SIC = ((3570, 3579), (3600, 3699), (7370, 7379))

# KNOWN ISSUE, remove when the ADS-ratio brief ships. One ADS is not one ordinary share
# (baba: 1 ADS = 8 ordinary shares), so the per-share figures parsed out of the filing are
# per ORDINARY share while the price is per ADS. The currency is already handled correctly
# for these four — this is the separate, still-open ratio mismatch, measured today: baba's
# KGV comes out ~8x wrong.
#
# An explicit list rather than a heuristic on purpose: nothing in the data we store says
# "this company trades as a multi-share ADS", so any automatic detection would be a guess.
# Their KGV/RoI/Graham margin are blanked — the companies stay visible with their name,
# industry, price and the quality metrics, which are all per-company and unaffected.
# See .claude/briefs/rechenknecht-screener.md.
ADS_RATIO_MISMATCH = frozenset({"baba", "jd", "hdb", "pdd"})

# The three the screener adds per row. metrics' own names, so there is one vocabulary.
RATIO_COLUMNS = (metrics.KGV, metrics.RO_I, metrics.GRAHAM_MARGIN)

# The tool's OWN definition of "a candidate", not a new one invented for this page:
# src/RechenknechtBeta.py print_extraordinary_results has printed exactly these four in green
# for years (RoI > 8, RoA > 8, EBIT-margin > 13, equity-ratio > 35). Strictly greater, as
# there — a company sitting exactly on a cutoff has not cleared it.
#
# The keys are the frame's COLUMN names, which are two vocabularies: RoI is metrics' own
# (the screener computes it per row in add_valuation), while roa/ebit_margin/equity_ratio are
# the aliases pages/Screener.py's query gives metric_average's stored 7-year averages.
QUALITY_THRESHOLDS = {metrics.RO_I: 8, "roa": 8, "ebit_margin": 13, "equity_ratio": 35}

# How many of the four a company has to clear to count as strong, and how few make it weak.
# Named because pages/Screener.py prints the rule to the user: a bar tuned in the code but
# left standing in the caption is a page that lies about how its own colours were decided.
STRONG_AT_LEAST, WEAK_AT_MOST = 3, 1

# The four states of the quick-scan column, and the column it is written to.
#
# UNKNOWN is deliberately NOT the yellow one. "We have no Graham margin for this company" and
# "we looked at this company and it came out middling" are different statements, and painting
# the first in the second's colour would sell an absence of information as an evaluated
# score — on a page whose entire premise is that the ranking can be trusted.
#
# The glyphs are also chosen so that sorting the column descending puts them in the order a
# reader expects: their code points run ⚪ (U+26AA) < 🔴 < 🟡 < 🟢, so "descending" is
# green-first and the unknowns land at the bottom.
CHEAP_AND_STRONG, MIXED, RICH_AND_WEAK, UNKNOWN = "🟢", "🟡", "🔴", "⚪"
SIGNAL = "signal"


def in_ranges(code, ranges) -> bool:
    """True when a numeric SIC code falls inside any (low, high) range, both ends included.

    A missing code is in no range at all: an unclassified company is not a bank and not a
    tech company, it is unknown — and it is the `uncategorized` toggle that decides whether
    it is shown, never a range test that quietly answers False for both questions.
    """
    if code is None or pd.isna(code):
        return False
    return any(low <= int(code) <= high for low, high in ranges)


def industry_mask(
    frame: pd.DataFrame,
    financials: bool = False,
    tech_only: bool = False,
    uncategorized: bool = True,
) -> pd.Series:
    """Which rows the industry filter keeps.

    Three independent switches rather than one dropdown, because the three questions are
    independent: "is this a bank" is knowable, "is this tech" is knowable, and "we have no
    SIC code for it yet" is neither of those. A company ingested before company.sic_code
    existed must not vanish out of both filters silently, which is what a single dropdown
    over known industries would do to it.
    """
    code = frame["sic_code"]
    unknown = code.isna()

    # astype(bool), not the mapped dtype: a psycopg smallint column arrives as object dtype
    # and an EMPTY frame maps to an empty object Series, on which `~` raises instead of
    # inverting. Explicit here because "no company survived the previous filter" is a normal
    # state of this page, not an edge case.
    if tech_only:
        keep = code.map(lambda value: in_ranges(value, TECH_SIC)).astype(bool)
    elif financials:
        keep = ~unknown
    else:
        keep = ~unknown & ~code.map(lambda value: in_ranges(value, FINANCIAL_SIC)).astype(bool)

    return (keep | unknown) if uncategorized else keep


def _pair(reporting_currency, currency) -> tuple:
    """The key a rate is stored and looked up under.

    Missing is None here, never nan. pandas 3.0 reads a NULL text column back as the float
    nan, and nan != nan — so a (nan, "USD") key built while fetching the rates would not
    match the (nan, "USD") key built while applying them, and all 154 companies with no
    stored reporting currency would come out with empty ratios for no visible reason. Both
    sides go through this one function so they cannot disagree.
    """
    return tuple(
        value if isinstance(value, str) else None for value in (reporting_currency, currency)
    )


def currency_pairs(frame: pd.DataFrame) -> set:
    """Every distinct (reporting, quote) currency pair in the frame, so a caller fetches one
    rate per PAIR — seven across all 221 companies — instead of one per company."""
    return {_pair(row.reporting_currency, row.currency) for row in frame.itertuples()}


def add_valuation(frame: pd.DataFrame, rates: dict) -> pd.DataFrame:
    """KGV, RoI and the Graham margin per row, computed by metrics.valuation().

    `rates` maps (reporting currency, quote currency) -> exchange rate, so the caller can
    fetch one rate per PAIR instead of one per company; a pair that is missing from it
    yields NaN ratios rather than an unconverted mix of two currencies.

    The earnings side is converted into the price's currency FIRST (metrics.in_quote_currency)
    — a USD price over a CNY EPS is a number that looks like a P/E and means nothing. The
    price itself is never touched: it is what the exchange says it is.
    """
    computed = []
    for row in frame.itertuples():
        rate = rates.get(_pair(row.reporting_currency, row.currency), float("nan"))
        ratios = metrics.valuation(
            row.price,
            *metrics.in_quote_currency(
                rate,
                row.avg_eps,
                row.book_value_per_share,
                row.conservative_book_value_per_share,
            ),
        )
        blank = str(row.ticker).lower() in ADS_RATIO_MISMATCH
        computed.append(
            {column: float("nan") if blank else ratios[column] for column in RATIO_COLUMNS}
        )

    return frame.join(pd.DataFrame(computed, index=frame.index))


def in_bounds(frame: pd.DataFrame, bounds: dict) -> pd.Series:
    """Which rows survive the min/max filters. `bounds` maps a column to (min, max), either
    of which may be None for "no bound".

    A row with no value for a column that IS bounded drops out: "we do not know this
    company's RoA" is not "its RoA is above 5". A column with no bound set is not consulted
    at all, so a company with no KGV stays visible until you actually filter on KGV — which
    is what keeps the four ADS-mismatched companies and the four with no avg_eps in the
    default list instead of quietly disappearing from it.
    """
    keep = pd.Series(True, index=frame.index)
    for column, (low, high) in bounds.items():
        if low is not None:
            keep &= frame[column] >= low
        if high is not None:
            keep &= frame[column] <= high
    return keep


def quality_score(frame: pd.DataFrame) -> pd.Series:
    """How many of the four QUALITY_THRESHOLDS each row clears, 0-4.

    A missing metric counts as NOT met, never as skipped: "we do not know this company's RoA"
    is not "its RoA is above 8", the same rule in_bounds follows. That is what `nan > 8` is
    already False for — spelled out here because the alternative (score out of however many
    metrics happen to exist) would let a company with one known metric outrank one with four.
    """
    return sum((frame[column] > threshold).astype(bool)
               for column, threshold in QUALITY_THRESHOLDS.items())


def margin_quartiles(frame: pd.DataFrame) -> tuple:
    """The (bottom, top) Graham-margin quartile OF THIS FRAME — the cheapness cutoffs.

    Relative and recomputed per render, never a number written into the code. Measured
    2026-08-11 over the 992 companies that have a margin: the median is -52%, the 75th
    percentile -19%, and only the top decile is positive at all. An absolute "margin > 0 is
    cheap" would therefore call ~90% of the universe expensive, which is not a signal.

    The caller passes the FILTERED frame, so with the tech filter on "cheap" means cheap
    among the tech companies on screen — not cheap against a database of banks the user has
    just excluded. Both quantiles skip the rows with no margin; a frame with none of them
    yields NaN, against which every comparison is False and nothing is called cheap.
    """
    margin = frame[metrics.GRAHAM_MARGIN]
    return margin.quantile(0.25), margin.quantile(0.75)


def signal(frame: pd.DataFrame) -> pd.Series:
    """One glyph per row: cheap and strong, expensive and weak, mixed, or not enough data.

    The whole column in one place so the rule is one readable expression rather than six
    conditions spread over a Streamlit page — and so test_screener.py can hold it to the
    cases that matter. Order is deliberate: UNKNOWN is written last and overrides, because a
    row with no data must never keep a colour it picked up from a comparison against NaN.
    """
    margin = frame[metrics.GRAHAM_MARGIN]
    bottom, top = margin_quartiles(frame)
    score = quality_score(frame)

    out = pd.Series(MIXED, index=frame.index, dtype=object)
    out[(margin >= top) & (score >= STRONG_AT_LEAST)] = CHEAP_AND_STRONG
    out[(margin <= bottom) & (score <= WEAK_AT_MOST)] = RICH_AND_WEAK
    out[margin.isna() | frame[list(QUALITY_THRESHOLDS)].isna().all(axis=1)] = UNKNOWN
    return out
