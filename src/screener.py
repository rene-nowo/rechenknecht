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

# Which macro series the industry deep-dive overlays on an industry's own trend, and nothing
# more than that: an industry that is not in here gets NO overlay rather than a guessed one.
#
# SIC CODE RANGES, never a substring of sic_description. The SEC's own text carries DOUBLE
# spaces — measured 2026-08-12, the shipping industry is stored as 'Deep Sea Foreign
# Transportation of  Freight' — so a substring match is one invisible character away from
# matching nothing, and "matched nothing" is indistinguishable from "this industry has no
# series mapped". The code is a number and cannot be misspelled. Same shape as FINANCIAL_SIC
# and TECH_SIC above, so in_ranges() serves all three.
#
#   4400-4499  water transportation (4412 deep sea freight) — bunker fuel is the cost line
#   1520-1599  building construction (1531 operative builders) — mortgage rates track the
#              10-year yield, and mortgage rates are these companies' demand
#   6000-6799  banks, insurers, REITs — rate-sensitive by construction (hidden by default,
#              visible with the financials toggle)
MACRO_BY_SIC = {
    "DCOILBRENTEU": ((4400, 4499),),
    "DGS10": ((1520, 1599), (6000, 6799)),
}

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

# The four in the order the rollup counts them, so a column that is empty for an industry is
# still a 0 in the table rather than a missing column. Green first: the rollup exists to be
# read left to right as "how many of these are worth looking at".
SIGNAL_ORDER = (CHEAP_AND_STRONG, MIXED, RICH_AND_WEAK, UNKNOWN)

# The column the rollup groups by, and the label a row gets when it has no industry text.
#
# Both halves are needed: company.sic_description is nullable AND the SEC's submissions API
# returns an EMPTY STRING for some filers — measured 2026-08-12, 48 of the 5,046 companies in
# the default view have `''` here and NOT one has NULL, so a .fillna() alone would leave 48
# rows grouped under a nameless key that renders as a blank line in the table.
INDUSTRY = "sic_description"
UNCLASSIFIED = "(no industry)"

# The rollup's own derived columns. Named so pages and tests share one vocabulary.
#
# RATED is the sample size behind the median margin, and it is NOT decoration. A median skips
# the rows with no Graham margin, so an industry of 9 companies of which 1 has a margin
# reports that single company's number as "the industry median" — and because the rows that
# survive in such an industry are the broken ones, that is exactly how the artefacts float to
# the top of a "cheapest industries" sort. Measured 2026-08-12: every one of the top 5
# industries by median margin had 4 or fewer rated companies out of 5-21.
COMPANIES = "companies"
RATED = "rated"
QUALITY_SCORE = "quality_score"

# How far a row's Graham margin sits above the cheapness cutoff that made it green. A
# DISTANCE, not a new score: the cutoff is margin_quartiles' own top quartile, so this column
# adds no threshold that signal() does not already apply.
MARGIN_ABOVE_CUT = "margin_above_cut"

# The fact table's own column names, shared by the query in src/ui.py, the two aggregations
# below and the deep-dive charts — so the three cannot drift apart on a rename.
FISCAL_YEAR, CONCEPT, VALUE = "fiscal_year", "concept", "value"


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


def industry_label(frame: pd.DataFrame) -> pd.Series:
    """The industry each row is grouped under, with the nameless ones named.

    A missing industry is one bucket however it is spelled in the database — NULL, an empty
    string, or whitespace. Grouping on the raw column instead would split those across a
    dropped NaN group and a blank-labelled one, and the counts would then not add up to the
    number of companies on the page, which is the one property a rollup has to have.
    """
    label = frame[INDUSTRY].fillna("").astype(str).str.strip()
    return label.mask(label == "", UNCLASSIFIED)


def rollup(frame: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
    """One row per industry: how many companies, how cheap, how strong, and the signal mix.

    MEDIAN, not mean, for both middle columns. The Graham margin has a long tail in BOTH
    directions — measured 2026-08-12 over the 1,833 rows that have one, the mean is
    +293,427% against a median of -44%, because a company with a negative book value per
    share yields a Graham number out of a square root of two negatives and lands at up to
    2.4e8%. One such row would decide an industry's mean on its own; the median ignores it.

    `signals` is passed in rather than computed here so that it is the SAME series the page
    shows per company — signal()'s cheapness half is a quartile OF THE FRAME IT IS GIVEN, so
    recomputing it over a subset would silently rank an industry against itself instead of
    against the whole screen.
    """
    label = industry_label(frame)
    scored = frame.assign(**{QUALITY_SCORE: quality_score(frame)})
    grouped = scored.groupby(label, sort=False)

    counts = pd.crosstab(label, signals).reindex(columns=list(SIGNAL_ORDER), fill_value=0)
    return pd.DataFrame({
        COMPANIES: grouped.size(),
        # How many of them the median margin was actually computed over. A median SKIPS the
        # rows with no margin, so without this column an industry where one company of nine
        # has a Graham margin reports that one company as the industry's median.
        RATED: grouped[metrics.GRAHAM_MARGIN].count(),
        metrics.GRAHAM_MARGIN: grouped[metrics.GRAHAM_MARGIN].median(),
        # Over ALL companies in the group, unlike the line above: quality_score is a count of
        # cleared thresholds and a missing metric counts as not-met, so it is never absent.
        QUALITY_SCORE: grouped[QUALITY_SCORE].median(),
    }).join(counts).rename_axis(INDUSTRY).reset_index()


def candidates(frame: pd.DataFrame, signals: pd.Series) -> pd.DataFrame:
    """Only the 🟢 rows, strongest first — the outlier list behind the green glyph.

    NO NEW SCORING RULE. The bar is signal()'s own: `signals` decides membership, and the two
    sort keys are the two halves of that same rule made continuous — how many of the four
    QUALITY_THRESHOLDS the row clears, then how far its margin sits above the top quartile
    that made it cheap.

    Quality first and margin only as the tiebreak, deliberately: adding a percentage-point
    distance to a 0-4 count needs a weight nobody in this codebase has ever justified, and a
    single blended number would hide which half a company actually won on. Both columns stay
    in the output so the order can be argued with.

    `frame` must be the frame `signals` was computed over, for the same reason rollup() takes
    the series instead of recomputing it: the cutoff is a quartile of that frame, and taking
    it from the already-filtered green rows would measure them against each other.
    """
    _, cheap = margin_quartiles(frame)
    ranked = frame.assign(**{
        QUALITY_SCORE: quality_score(frame),
        MARGIN_ABOVE_CUT: frame[metrics.GRAHAM_MARGIN] - cheap,
    })
    return ranked[signals == CHEAP_AND_STRONG].sort_values(
        [QUALITY_SCORE, MARGIN_ABOVE_CUT], ascending=False
    )


def macro_series_for(sic_code):
    """The macro series_id to overlay for an industry, or None when none is mapped.

    None is a first-class answer, not a failure: the deep-dive then shows the industry's own
    trend alone. Guessing a series for an unmapped industry would put a line on the screen
    that means nothing and invite a correlation to be read off it.
    """
    for series_id, ranges in MACRO_BY_SIC.items():
        if in_ranges(sic_code, ranges):
            return series_id
    return None


def median_by_year(facts: pd.DataFrame) -> pd.DataFrame:
    """One median per (fiscal year, concept), with the sample size it stands on beside it.

    MEDIAN, for the same reason rollup() takes one: these are per-company figures with a long
    tail, and one restated or misparsed filing would decide a year's mean on its own.

    COMPANIES is not decoration here any more than RATED is in the rollup. WHICH companies
    have a fact changes from year to year — measured 2026-08-12, the deep-sea shipping
    median rests on 2 companies in 2017 and on 24 in 2019, because EDGAR coverage starts
    where the ingest's filing window starts, not where the industry does. A trend line read
    without that count is partly the sample moving rather than the industry.

    Nothing is computed on the values: EBIT-margin is a STORED fact concept, the same number
    src/metrics.py wrote at ingest time, and this only chooses which of them to show.
    """
    grouped = facts.groupby([FISCAL_YEAR, CONCEPT])[VALUE]
    return pd.DataFrame({VALUE: grouped.median(), COMPANIES: grouped.count()}).reset_index()


def macro_mean_by_year(macro: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """A daily FRED series collapsed to one point per year, in median_by_year's own shape.

    Same columns on purpose, so the overlay panel is drawn by the same chart code as the
    fundamentals panels instead of a second one that could format its axis differently.

    MEAN, not median, unlike the fundamentals above: "Brent averaged $100.93 in 2022" is how
    a price series is conventionally read for a year, and a daily oil price or Treasury yield
    has no artefact rows of the kind a per-share figure out of a misparsed filing produces.

    The year is a CALENDAR year matched against a FISCAL year label, which is an
    approximation for every company that does not close in December — a filer with a June
    year-end books half of calendar 2021 into its FY2022. That is why this is an overlay to
    eyeball and why no correlation is computed anywhere from it.
    """
    year = pd.to_datetime(macro["date"]).dt.year
    grouped = macro.groupby(year)[VALUE].mean()
    return pd.DataFrame({
        FISCAL_YEAR: grouped.index.astype(int), CONCEPT: series_id, VALUE: grouped.values,
    })
