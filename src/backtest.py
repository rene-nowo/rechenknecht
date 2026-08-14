"""Point-in-time framing: what a company had actually published as of a given date.

src/screener.py decides which industry, which numbers and which rows; src/metrics.py holds
the formulas. This module holds the THIRD axis — time. It answers "which fiscal years would
René have had on 2024-08-13", rebuilds the valuation inputs from only those, and aggregates
realized forward returns per signal bucket.

Deliberately separate from screener.py: that module's job is the LIVE frame, and a company's
publication timeline is an unrelated reason to change. Pure pandas and date arithmetic — no
database, no network, no Streamlit. backtest_report.py wires it to all three.

Nothing here recomputes a ratio. add_valuation()/signal() are called by the caller, exactly as
they exist for the live screener, over the frame this module builds.
"""

import calendar
import datetime

import pandas as pd

from src import metrics, screener

# How long after a fiscal period ends before its filing is plausibly public. A 10-K is due
# 60-90 days after year-end depending on filer status, and amendments trail further; six
# months is deliberately conservative, so a borderline company is excluded rather than
# counted early. It is an approximation of the real filing date, never a substitute for it —
# backtest_report.py exposes it as --lag-months so its sensitivity can be measured.
DEFAULT_LAG_MONTHS = 6

# The averaging window, and the same one Master_Vorlage uses (BA3 averages seven years,
# metrics.add_averages' own default).
DEFAULT_WINDOW_YEARS = 7

# The `fact.concept` -> frame-column map, and it is a RENAME, not a pass-through.
#
# The concepts are stored under metrics' own names ('EPS', 'RoA', 'EBIT-margin',
# 'equity-ratio') but the frame is consumed by screener.add_valuation(), which reads
# row.avg_eps, and by screener.signal(), whose quality columns are screener.QUALITY_THRESHOLDS'
# keys — and those are the ALIASES src/ui.py:38-40 gives metric_average, not the concept names.
# Emitting the concept names here raises KeyError: 'roa' inside screener.quality_score().
# It fails loudly, which is a mercy, but only if the names are right in the first place.
AVERAGED_CONCEPTS = {
    metrics.EPS: "avg_eps",
    metrics.RO_A: "roa",
    metrics.EBIT_MARGIN: "ebit_margin",
    metrics.EQUITY_RATIO: "equity_ratio",
}

# Book value is a stock, not a flow: the latest known year, never an average of it — the same
# rule metrics.add_price_ratios() follows (src/metrics.py:319-324) and v_valuation encodes in
# SQL. These two already carry the names the screener reads, so they are not renamed.
LATEST_CONCEPTS = (metrics.BOOK_VALUE_PER_SHARE, metrics.CONSERVATIVE_BOOK_VALUE_PER_SHARE)

# Every column point_in_time_frame() returns, also when it returns nothing — the same reason
# yahoo_source.PRICE_COLUMNS exists: a caller that indexes the result by name must get the
# same shape whether or not any company qualified.
PIT_COLUMNS = tuple(AVERAGED_CONCEPTS.values()) + LATEST_CONCEPTS


def known_by(fiscal_year: int, fiscal_year_end_month, lag_months: int = DEFAULT_LAG_MONTHS):
    """The date a fiscal year's numbers were plausibly public, as a datetime.date.

    This INVERTS metrics.fiscal_year_label(), which reads "a fiscal year ending in the first
    half of the calendar year belongs to the previous year" — Foot Locker's year ending
    2023-01-28 is FY2022 (pinned by test_metrics.py:37). Run backwards, FY2023 for that same
    January filer is the year ending 2024-01-31, a full year later than the label suggests.
    Getting that direction wrong is silent and makes every downstream number optimistic, so
    both sides of the month <= 6 boundary are pinned in test_backtest.py.

    A missing fiscal_year_end_month is December, the coalesce(fiscal_year_end_month, 12)
    convention sec_check.py:128 already uses. NULL arrives as the float nan through pandas,
    not as None, so both are handled the way screener.in_ranges() handles a missing SIC code.

    Stdlib only, deliberately: calendar.monthrange for the month length and integer month
    arithmetic for the lag. python-dateutil would do the same job, but it is a TRANSITIVE and
    UNDECLARED dependency here, and pyproject.toml's own comments record that exact mistake
    having already broken a fresh install of this repo three times (psycopg, requests, lxml).
    """
    month = 12 if fiscal_year_end_month is None or pd.isna(fiscal_year_end_month) else int(fiscal_year_end_month)
    year = int(fiscal_year) if month > 6 else int(fiscal_year) + 1

    # Adding calendar months, not days: a flat +183 disagrees with month arithmetic at every
    # month-length boundary. The day clamps to the target month's length, so the last day of
    # June plus six months is 30 December, not an invalid 31st.
    shifted = (month - 1) + int(lag_months)
    end_year, end_month = year + shifted // 12, shifted % 12 + 1
    day = min(calendar.monthrange(year, month)[1], calendar.monthrange(end_year, end_month)[1])
    return datetime.date(end_year, end_month, day)


def is_known(
    fiscal_year: int,
    fiscal_year_end_month,
    cutoff: datetime.date,
    lag_months: int = DEFAULT_LAG_MONTHS,
) -> bool:
    """Whether a fiscal year had plausibly been published by `cutoff`.

    Inclusive of the boundary date: a filing available ON the cutoff was available on the
    cutoff. The exclusive alternative would drop one day's worth of filings for no reason.
    """
    return known_by(fiscal_year, fiscal_year_end_month, lag_months) <= cutoff


def known_facts(
    facts: pd.DataFrame,
    fiscal_year_end_month,
    cutoff: datetime.date,
    lag_months: int = DEFAULT_LAG_MONTHS,
) -> pd.DataFrame:
    """The rows of `facts` that had plausibly been published by `cutoff`.

    `facts` is (cik, fiscal_year, concept, value) — screener's own column names, so this and
    the query that fills it cannot drift apart. `fiscal_year_end_month` maps cik -> month, as a
    dict or a Series, because Foot Locker (January) and Microsoft (June) reach "published" on
    different calendar dates for the identically-labelled fiscal year. A cik that is not in the
    mapping is December, the same coalesce sec_check.py:128 uses — fail soft, the way
    metrics.ensure_inputs() does, rather than take the whole pass down for one company.

    known_by() depends only on (fiscal_year, month), which is a few hundred distinct pairs
    across ~200,000 rows, so it is evaluated once per pair and mapped back rather than called
    per row.
    """
    if facts.empty:
        return facts

    month = facts["cik"].map(fiscal_year_end_month).fillna(12).astype(int)
    key = pd.MultiIndex.from_arrays([facts[screener.FISCAL_YEAR].astype(int), month])
    distinct = key.unique()
    verdict = pd.Series(
        [is_known(year, end_month, cutoff, lag_months) for year, end_month in distinct],
        index=distinct,
    )
    return facts[verdict.reindex(key).to_numpy()]


def point_in_time_frame(
    facts: pd.DataFrame,
    fiscal_year_end_month,
    cutoff: datetime.date,
    lag_months: int = DEFAULT_LAG_MONTHS,
    window_years: int = DEFAULT_WINDOW_YEARS,
) -> pd.DataFrame:
    """One row per cik holding the valuation inputs as they stood at `cutoff`.

    The averaged metrics are means over the up-to-`window_years` most recent KNOWN fiscal
    years; the two book values are the latest known year's. Indexed by cik, with NO price and
    NO currency column — the caller joins a price to this frame, which is the only reason it
    can be built once and scored once.

    A company with fewer known years than the window averages over what it has. A company with
    nothing known at the cutoff is ABSENT, not a row of NaNs: it was not a candidate that day,
    and a NaN row would land in the ⚪ bucket and inflate the report's denominator.

    The windowing rule is metrics.year_columns()' — "the most recent N fiscal years, chosen by
    sorted year rather than by the order a source inserted them" (src/metrics.py:206-211). The
    rule is reused; the call is not. year_columns() filters the COLUMN LABELS of a wide frame
    and `fact` is long/narrow, so routing through it would mean materialising one pivot per
    company to consult a two-line helper.
    """
    known = known_facts(facts, fiscal_year_end_month, cutoff, lag_months)
    empty = pd.DataFrame(columns=list(PIT_COLUMNS), index=pd.Index([], name="cik"), dtype="float64")
    if known.empty:
        return empty

    # A non-finite stored fact is not a measurement, so it is skipped exactly like a NULL.
    #
    # MEASURED 2026-08-13: `fact` really does contain Postgres Infinity/-Infinity — an EPS of
    # net_income / 0 shares at ingest time — and 249 companies in the eligible pool carried at
    # least one. src/db.py write_metric_averages already refuses to store one (math.isfinite,
    # src/db.py:269); write_facts guards only pd.isna(), which is False for inf, so they reach
    # the table. Left in, metrics.valuation() turns an infinite EPS into an INFINITE Graham
    # margin — the largest value in the frame — so the company is ranked cheapest of all and
    # reported as 🟢. Dropping the value rather than the company keeps its good years.
    known = known.assign(**{
        screener.VALUE: pd.to_numeric(known[screener.VALUE], errors="coerce").replace(
            [float("inf"), float("-inf")], pd.NA
        )
    })

    years = known[["cik", screener.FISCAL_YEAR]].drop_duplicates()
    window = years.sort_values(screener.FISCAL_YEAR, ascending=False).groupby("cik").head(window_years)
    windowed = known.merge(window, on=["cik", screener.FISCAL_YEAR])

    averaged = _by_concept(windowed, list(AVERAGED_CONCEPTS), "mean").rename(columns=AVERAGED_CONCEPTS)

    # "latest" here means latest WITH A VALUE, exactly as v_valuation's own lateral spells out
    # in SQL with `value is not null` (db/schema.sql:335-342): a company can report a year with
    # no equity figure at all, and ordering by year alone would hand back that NULL as "the
    # latest book value". No filter is needed for it — agg("last") skips NaN — so the guarantee
    # is pinned by a test rather than by a redundant line here.
    latest = _by_concept(windowed, list(LATEST_CONCEPTS), "last")

    frame = averaged.join(latest, how="outer")
    return frame.reindex(columns=list(PIT_COLUMNS)) if len(frame) else empty


def _by_concept(windowed: pd.DataFrame, concepts: list, how: str) -> pd.DataFrame:
    """One column per concept, one row per cik. `how` is 'mean' over the window or 'last' —
    the newest known year, which is why the rows are sorted by fiscal year first."""
    rows = windowed[windowed[screener.CONCEPT].isin(concepts)].sort_values(screener.FISCAL_YEAR)
    if rows.empty:
        return pd.DataFrame(index=pd.Index([], name="cik"))
    return rows.groupby(["cik", screener.CONCEPT])[screener.VALUE].agg(how).unstack()


# The fixed seed behind --limit. A sanity run has to hit the same companies twice or two runs
# of it are not comparable; there is deliberately no --seed flag, because the subset needs to
# be deterministic, not variable.
DEFAULT_SEED = 42


def sample_universe(frame: pd.DataFrame, size: int = None, seed: int = DEFAULT_SEED) -> pd.DataFrame:
    """`size` companies drawn uniformly from the eligible pool, or the whole pool.

    The default really is "everything": every fundamentals-complete, industry-masked company
    gets a real price fetch, because selecting on anything that depends on TODAY's price would
    drop the companies that have since been delisted — the exact population whose returns a
    backtest must not lose. `size` exists for backtest_report.py's --limit sanity run.

    UNIFORM, never head(n). The pool arrives ordered by cik, which is roughly SEC registration
    age, so head() would score old, large filers only — and screener.signal() takes its
    cheapness quartile from whatever frame it is handed (src/screener.py:294), which would make
    a --limit run's buckets mean something different from the full run's.

    A `size` above the population returns the whole pool rather than raising, which is what
    pandas' own .sample(n=) does with the default replace=False — and a first sanity run is
    exactly where someone passes a --limit larger than the pool.
    """
    if size is None or size >= len(frame):
        return frame
    return frame.sample(n=size, random_state=seed)


# The rollup's own two derived columns. The other two are screener.COMPANIES and
# screener.RATED, reused verbatim — they already mean exactly "how many are in this bucket"
# and "how many of them the middle column was actually computed over".
FORWARD_RETURN = "forward_return"
HIT_RATE = "hit_rate"

ROLLUP_COLUMNS = (screener.SIGNAL, screener.COMPANIES, screener.RATED, FORWARD_RETURN, HIT_RATE)


def forward_return(cutoff_price, today_price) -> float:
    """What one share actually returned between the cutoff and today, as a fraction.

    Scalar in, scalar out — called per row, like metrics.valuation(). NaN when either price is
    missing or the cutoff price is zero, never a stand-in and never inf: an invented number
    leaks into the median and the hit-rate, while NaN is skipped by both.

    metrics' own _number/_ratio do exactly this coercion and this zero-denominator rule, so
    they are CALLED rather than copied — a second definition of "how a missing input becomes
    NaN" is precisely the drift src/metrics.py exists to prevent. A NaN cutoff price also comes
    out NaN, because NaN divides to NaN.
    """
    cutoff = metrics._number(cutoff_price)
    return metrics._ratio(metrics._number(today_price) - cutoff, cutoff)


def backtest_rollup(frame: pd.DataFrame, signal: pd.Series, returns: pd.Series) -> pd.DataFrame:
    """One row per signal bucket: how many companies, how many we could price, what the median
    one returned, and how often the bucket beat the field.

    `signal` and `returns` are the series computed OVER `frame` — passed in rather than
    recomputed here, the same rule screener.rollup() and screener.candidates() follow, and for
    a sharper reason: signal()'s cheapness half is a quartile of the frame it was given, so a
    second computation over a subset would rank the sample against itself.

    BOTH counts, always. COMPANIES is everyone in the bucket; RATED is how many of them have a
    forward return at all. The gap between them is how many companies the price fetch failed
    for, and a median printed without it is the failure screener.rollup()'s own RATED column
    exists to prevent.

    The comparison median is taken ONCE over the whole frame. Per bucket it would be scored
    against itself, and roughly half of every bucket would beat its own median by construction.
    """
    signal, returns = signal.reindex(frame.index), returns.reindex(frame.index)
    field = returns.median()
    grouped = returns.groupby(signal)

    rollup = pd.DataFrame({
        screener.COMPANIES: signal.value_counts(),
        screener.RATED: grouped.count(),
        FORWARD_RETURN: grouped.median(),
        # An empty bucket yields NaN rather than 0.0 — "no company here beat the field" and
        # "there was no company here" are different statements, the same distinction
        # screener.signal() keeps between ⚪ UNKNOWN and 🟡 MIXED.
        HIT_RATE: grouped.apply(lambda bucket: (bucket.dropna() > field).mean()),
    })

    counted = rollup.reindex(list(screener.SIGNAL_ORDER))
    for column in (screener.COMPANIES, screener.RATED):
        counted[column] = counted[column].fillna(0).astype(int)
    return counted.rename_axis(screener.SIGNAL).reset_index()[list(ROLLUP_COLUMNS)]
