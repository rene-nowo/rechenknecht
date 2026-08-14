"""Point-in-time framing: which fiscal years a company had actually published by a cutoff.

Every case here is one that would silently turn the backtest into a look-ahead-biased report
that agrees with itself — a fiscal year counted as "known" before it existed makes the
screener's own signal look prescient for free. The first draft of the plan shipped exactly
that error in its acceptance criterion (January fiscal-year-ends read one year early), which
is why the January case below carries the full derivation.

Same style as test_screener.py: plain pandas frames built by tiny helpers, no DB, no network,
one behaviour per test, a docstring stating the measured reason for the case.
"""

import datetime

import pandas as pd
import pytest

from src import backtest, metrics, screener

CUTOFF = datetime.date(2024, 8, 13)

# The four columns `fact` is read into, using screener's own constants so a rename cannot make
# the test and the code disagree about what the frame is called.
FACT_COLUMNS = ["cik", screener.FISCAL_YEAR, screener.CONCEPT, screener.VALUE]


def facts(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=FACT_COLUMNS)


def series(cik, concept, values: dict) -> list:
    """One (cik, year, concept, value) row per year — `fact` is long/narrow, so a company's
    ten years of EPS are ten rows, not a column each."""
    return [(cik, year, concept, value) for year, value in values.items()]


# ── known_by / is_known ──────────────────────────────────────────────────────────────────


def test_a_january_fiscal_year_end_is_read_a_year_forward():
    """THE off-by-one the first plan draft shipped in its own success criterion.

    metrics.fiscal_year_label(2023-01-28, 1) == "2022" (test_metrics.py:37) — a January filer's
    year ENDING in January 2023 is labelled FY2022. Run backwards, FY2023 for that filer is the
    year ending 2024-01-31, not 2023-01-31. Plus the 6-month lag: 2024-07-31.

    Reading it as 2023-07-31 would have admitted a filing that did not exist at a 2024-08-13
    cutoff — a full year of look-ahead for every Jan-Jun fiscal-year-end company.
    """
    assert backtest.known_by(2023, 1) == datetime.date(2024, 7, 31)


def test_the_june_boundary_belongs_to_the_next_calendar_year():
    """month <= 6 is "first half" per fiscal_year_label's own operator (src/metrics.py:109),
    and June is ON that boundary — Microsoft's year ending 2025-06-30 is FY2024. Backwards,
    FY2023 ends 2024-06-30; the lag lands on 2024-12-30, because adding six calendar months to
    a 30th gives a 30th, not a month end."""
    assert backtest.known_by(2023, 6) == datetime.date(2024, 12, 30)


def test_a_july_fiscal_year_end_keeps_its_own_calendar_year():
    """The other side of the same boundary: month > 6 keeps the fiscal year's own calendar
    year (fiscal_year_label(2023-07-31, 7) == "2023"). One month apart from the case above,
    a full year apart in the answer — which is why both sides are pinned."""
    assert backtest.known_by(2023, 7) == datetime.date(2024, 1, 31)


def test_the_december_common_case_lands_six_months_after_year_end():
    """The overwhelming majority of filers. FY2023 ends 2023-12-31; six calendar months later
    is 2024-06-30 — June has 30 days, so the day clamps down rather than overflowing."""
    assert backtest.known_by(2023, 12) == datetime.date(2024, 6, 30)


@pytest.mark.parametrize("missing", [None, float("nan")])
def test_a_missing_fiscal_year_end_month_defaults_to_december(missing):
    """coalesce(fiscal_year_end_month, 12) is the convention sec_check.py:128 already uses.

    Both spellings of "missing" have to work: company.fiscal_year_end_month is a nullable
    smallint, and a NULL read back through pandas arrives as the float nan, not as None — the
    same trap screener._pair() documents for NULL text columns.
    """
    assert backtest.known_by(2023, missing) == backtest.known_by(2023, 12)


def test_the_lag_is_a_real_parameter_and_not_a_baked_in_constant():
    """The 6-month lag is THE approximation this whole report rests on, so it has to be
    steerable — backtest_report.py exposes it as --lag-months precisely so its sensitivity can
    be checked without editing code. A hardcoded lag would make that flag silently inert."""
    no_lag = backtest.known_by(2023, 12, lag_months=0)
    long_lag = backtest.known_by(2023, 12, lag_months=12)

    assert no_lag == datetime.date(2023, 12, 31)
    assert long_lag == datetime.date(2024, 12, 31)
    assert no_lag != long_lag


def test_is_known_includes_the_boundary_date_and_excludes_the_day_after():
    """<= , not < : a filing published exactly on the cutoff date was available on the cutoff
    date. The day after is the first day it must not count, and that one-day step is the whole
    difference between an honest point-in-time frame and a look-ahead one."""
    exactly = backtest.known_by(2023, 12)

    assert backtest.is_known(2023, 12, exactly) is True
    assert backtest.is_known(2023, 12, exactly - datetime.timedelta(days=1)) is False
    assert backtest.is_known(2023, 12, exactly + datetime.timedelta(days=1)) is True


def test_is_known_answers_no_for_a_fiscal_year_that_is_still_in_the_future():
    """The case the cutoff filter exists for: at 2024-08-13, a December filer's FY2024 has not
    even ended yet, let alone been filed. It is in `fact` today because the ingest runs in 2026
    — which is exactly why "what is in the table" can never be the point-in-time answer."""
    cutoff = datetime.date(2024, 8, 13)

    assert backtest.is_known(2023, 12, cutoff) is True
    assert backtest.is_known(2024, 12, cutoff) is False


# ── known_facts / point_in_time_frame ────────────────────────────────────────────────────


def test_two_companies_with_different_year_ends_see_different_cutoffs_for_one_label():
    """The reason the fiscal-year-end month is a PER-CIK mapping and not one global number.

    Both rows are labelled FY2023. A December filer published it by 2024-06-30; a January
    filer's FY2023 only ends 2024-01-31 and is published by 2024-07-31. At a 2024-07-01 cutoff
    exactly one of them existed — a single global month would silently include or exclude both.
    """
    frame = facts(("dec", 2023, metrics.EPS, 1.0), ("jan", 2023, metrics.EPS, 2.0))

    kept = backtest.known_facts(frame, {"dec": 12, "jan": 1}, datetime.date(2024, 7, 1))

    assert list(kept["cik"]) == ["dec"]


def test_the_columns_are_the_names_add_valuation_and_signal_actually_read():
    """F1, the trap that fails loudest: `fact.concept` holds 'RoA'/'EBIT-margin'/'equity-ratio',
    but screener.QUALITY_THRESHOLDS keys on the ALIASES ui.ROWS gives metric_average
    ('roa'/'ebit_margin'/'equity_ratio'), and add_valuation reads row.avg_eps. Emitting the
    concept names instead makes screener.quality_score() raise KeyError: 'roa'.

    The expectation is derived from screener's OWN constants rather than hardcoded, so a rename
    there shows up here instead of at runtime. RoI is excluded because add_valuation adds it.
    """
    frame = backtest.point_in_time_frame(
        facts(*series("a", metrics.EPS, {2022: 1.0}), *series("a", metrics.RO_A, {2022: 9.0})),
        {"a": 12},
        CUTOFF,
    )

    assert list(frame.columns) == list(backtest.PIT_COLUMNS)
    assert {"avg_eps", "book_value_per_share", "conservative_book_value_per_share"} <= set(frame.columns)
    assert set(screener.QUALITY_THRESHOLDS) - {metrics.RO_I} <= set(frame.columns)


def test_a_fiscal_year_filed_after_the_cutoff_never_enters_the_average():
    """The whole point of the module. The company has 2014-2025 in `fact` because the ingest
    runs in 2026, but at a 2024-08-13 cutoff a December filer's FY2024 and FY2025 did not
    exist. The window is then the 7 most recent KNOWN years, 2017-2023, mean 20.0 — not the
    7 most recent stored years, 2019-2025, which would be 22.0."""
    frame = backtest.point_in_time_frame(
        facts(*series("a", metrics.EPS, {year: year - 2000 for year in range(2014, 2026)})),
        {"a": 12},
        CUTOFF,
    )

    assert frame.loc["a", "avg_eps"] == pytest.approx(20.0)


def test_book_value_comes_from_the_latest_known_year_not_the_latest_filed_one():
    """Book value is a stock, not a flow — metrics.add_price_ratios() takes the latest year
    rather than an average (src/metrics.py:319-324). Point-in-time, "latest" has to mean latest
    AS OF THE CUTOFF: the 2025 row is in the database today and must not be the answer."""
    frame = backtest.point_in_time_frame(
        facts(
            *series("a", metrics.EPS, {2022: 1.0}),
            *series("a", metrics.BOOK_VALUE_PER_SHARE, {2021: 21.0, 2023: 23.0, 2025: 25.0}),
        ),
        {"a": 12},
        CUTOFF,
    )

    assert frame.loc["a", "book_value_per_share"] == pytest.approx(23.0)


def test_a_short_history_averages_over_what_it_has_without_padding_the_window():
    """A company with 3 known years averages over 3, not over 7 with four NaNs — the mean of
    10/20/30 is 20.0, while dividing by the full window would give 8.57. Newly-listed companies
    are a normal case here, not an edge one."""
    frame = backtest.point_in_time_frame(
        facts(*series("a", metrics.EPS, {2021: 10.0, 2022: 20.0, 2023: 30.0})),
        {"a": 12},
        CUTOFF,
    )

    assert frame.loc["a", "avg_eps"] == pytest.approx(20.0)


def test_a_company_with_nothing_known_yet_is_absent_rather_than_a_row_of_nans():
    """A company whose first filing lands after the cutoff did not exist to screen on. A NaN
    row would be counted as a company with missing data and land in the ⚪ bucket, inflating
    the report's denominator with companies that were never candidates."""
    frame = backtest.point_in_time_frame(
        facts(
            *series("old", metrics.EPS, {2022: 1.0}),
            *series("new", metrics.EPS, {2025: 5.0}),
        ),
        {"old": 12, "new": 12},
        CUTOFF,
    )

    assert list(frame.index) == ["old"]


def test_a_cik_missing_from_the_year_end_mapping_defaults_to_december():
    """Fail soft, the way metrics.ensure_inputs() does: one company absent from the mapping
    must not take the whole point-in-time pass down with a KeyError. December is the
    coalesce(fiscal_year_end_month, 12) convention sec_check.py:128 already uses."""
    frame = backtest.point_in_time_frame(
        facts(*series("ghost", metrics.EPS, {2022: 7.0})), {}, CUTOFF
    )

    assert frame.loc["ghost", "avg_eps"] == pytest.approx(7.0)


def test_an_empty_facts_frame_yields_the_full_shape_and_not_a_crash():
    """"No company has facts yet" is a normal state of a fresh database, and the caller
    indexes the result by column name — the same reason yahoo_source.PRICE_COLUMNS exists."""
    frame = backtest.point_in_time_frame(facts(), {}, CUTOFF)

    assert frame.empty
    assert list(frame.columns) == list(backtest.PIT_COLUMNS)


def test_a_null_in_the_latest_year_falls_back_to_the_last_year_that_had_a_value():
    """v_valuation's own lateral says `value is not null` before ordering by year
    (db/schema.sql:335-342) for exactly this case: a company can report a year with no equity
    figure, and taking the newest ROW rather than the newest VALUE hands back a NULL as "the
    latest book value", which empties the Graham margin for a company that has one."""
    frame = backtest.point_in_time_frame(
        facts(
            *series("a", metrics.EPS, {2022: 1.0}),
            *series("a", metrics.BOOK_VALUE_PER_SHARE, {2021: 21.0, 2022: 22.0, 2023: float("nan")}),
        ),
        {"a": 12},
        CUTOFF,
    )

    assert frame.loc["a", "book_value_per_share"] == pytest.approx(22.0)


# ── sample_universe ──────────────────────────────────────────────────────────────────────


def pool(n: int) -> pd.DataFrame:
    """A stand-in eligible pool, indexed by cik the way point_in_time_frame() returns one."""
    return pd.DataFrame({"avg_eps": range(n)}, index=pd.Index([f"c{i}" for i in range(n)], name="cik"))


def test_no_size_prices_the_whole_pool():
    """The default path and the one that actually runs: René's decision is that every
    fundamentals-complete, industry-masked company gets a real price (4,216 measured), so
    "no --limit" must mean "everything", not a silently capped default."""
    frame = pool(50)

    assert len(backtest.sample_universe(frame)) == 50
    assert list(backtest.sample_universe(frame).index) == list(frame.index)


def test_a_size_larger_than_the_pool_is_not_an_error():
    """pandas .sample(n=) RAISES when n exceeds the population and replace=False (the default),
    so an unguarded call would crash the whole run on any --limit above the pool size — which
    is exactly what a first sanity run is likely to pass."""
    frame = pool(10)

    sampled = backtest.sample_universe(frame, size=99)

    assert len(sampled) == 10
    assert not sampled.index.duplicated().any()


def test_a_smaller_size_returns_exactly_that_many():
    assert len(backtest.sample_universe(pool(100), size=7)) == 7


def test_the_same_seed_reproduces_the_same_companies():
    """A --limit run has to be re-runnable against the same companies, or two sanity runs are
    not comparable and neither is comparable to itself after a code change."""
    frame = pool(100)

    first = backtest.sample_universe(frame, size=10, seed=42)
    second = backtest.sample_universe(frame, size=10, seed=42)

    assert list(first.index) == list(second.index)


def test_the_seed_is_load_bearing_and_not_ignored():
    """The kill-proof on the test above: identical output for two DIFFERENT seeds would also
    satisfy it, and would mean the seed is being dropped on the floor."""
    frame = pool(100)

    assert list(backtest.sample_universe(frame, size=10, seed=42).index) != list(
        backtest.sample_universe(frame, size=10, seed=7).index
    )


def test_the_sample_is_uniform_rather_than_the_first_n():
    """NOT head(n). The pool arrives ordered by cik, which is roughly SEC registration age, so
    head() would score old, large filers only — and screener.signal() takes its cheapness
    quartile from whatever frame it is handed (src/screener.py:294), so a biased subset makes a
    --limit run's buckets mean something different from the full run's."""
    frame = pool(100)

    sampled = backtest.sample_universe(frame, size=10, seed=42)

    assert list(sampled.index) != list(frame.index[:10])


def test_an_empty_pool_samples_to_an_empty_pool():
    """"Nothing survived the filters" is a normal state, not an edge case — the same reason
    screener.industry_mask() spells out its empty-frame behaviour."""
    assert backtest.sample_universe(pool(0)).empty
    assert backtest.sample_universe(pool(0), size=5).empty


# ── forward_return / backtest_rollup ─────────────────────────────────────────────────────


def scored(*rows) -> tuple:
    """(signal glyph, forward return) per company — the three aligned objects
    backtest_rollup() takes, in the shape backtest_report.py builds them."""
    index = pd.Index([f"c{i}" for i in range(len(rows))], name="cik")
    frame = pd.DataFrame({"ticker": list(index)}, index=index)
    return (
        frame,
        pd.Series([glyph for glyph, _ in rows], index=index),
        pd.Series([value for _, value in rows], index=index, dtype="float64"),
    )


def bucket(rollup: pd.DataFrame, glyph: str) -> pd.Series:
    return rollup.set_index(screener.SIGNAL).loc[glyph]


def test_the_forward_return_is_the_realized_move_over_the_cutoff_price():
    assert backtest.forward_return(100.0, 150.0) == pytest.approx(0.5)
    assert backtest.forward_return(100.0, 50.0) == pytest.approx(-0.5)
    assert backtest.forward_return(100.0, 100.0) == pytest.approx(0.0)


@pytest.mark.parametrize(
    "cutoff_price, today_price",
    [(None, 150.0), (100.0, None), (float("nan"), 150.0), (100.0, float("nan"))],
)
def test_a_missing_price_on_either_side_is_no_return_rather_than_a_stand_in(cutoff_price, today_price):
    """The same rule metrics._ratio() and times_interest_earned() follow: an invented number
    leaks into the median and the hit-rate, while NaN is skipped by both. A company we could
    not price is not a company that returned 0%."""
    assert pd.isna(backtest.forward_return(cutoff_price, today_price))


def test_a_zero_cutoff_price_is_nan_and_never_infinity():
    """A Series division by zero yields inf, not NaN — and one inf renders as a real
    measurement in the CSV and drags any mean computed off it. Scalar in, scalar out, through
    metrics' own zero-denominator rule."""
    result = backtest.forward_return(0.0, 150.0)

    assert pd.isna(result)
    assert result != float("inf")


def test_the_rollup_reports_the_four_states_in_the_screener_s_own_order():
    """Reusing screener.SIGNAL_ORDER rather than a second ordering, so the report's rows read
    in the same order as every other place in the app that shows these four states."""
    rollup = backtest.backtest_rollup(*scored((screener.CHEAP_AND_STRONG, 0.5)))

    assert list(rollup[screener.SIGNAL]) == list(screener.SIGNAL_ORDER)
    assert list(rollup.columns) == list(backtest.ROLLUP_COLUMNS)


def test_companies_counts_everyone_while_rated_counts_only_the_priced_ones():
    """F7, and the report's own error bar. screener.rollup() already carries this exact split
    (COMPANIES vs RATED, src/screener.py:123-131) because a median SKIPS the rows it has no
    value for — here the gap is how many companies the fetch failed for, which is the number
    that says whether to believe the median beside it."""
    rollup = backtest.backtest_rollup(
        *scored(
            (screener.CHEAP_AND_STRONG, 0.5),
            (screener.CHEAP_AND_STRONG, 0.7),
            (screener.CHEAP_AND_STRONG, float("nan")),
        )
    )
    green = bucket(rollup, screener.CHEAP_AND_STRONG)

    assert green[screener.COMPANIES] == 3
    assert green[screener.RATED] == 2
    assert green[backtest.FORWARD_RETURN] == pytest.approx(0.6)


def test_the_hit_rate_is_scored_against_the_whole_frame_not_against_each_bucket():
    """The same bug class screener.rollup()'s docstring warns about for the cheapness quartile:
    scored against itself, roughly half of every bucket beats its own median by construction
    and the report says nothing.

    Whole-frame median of [0.5, 0.6, -0.5, -0.6] is 0.0, so green's hit-rate is 2/2 = 1.0 and
    red's is 0/2 = 0.0. Per-bucket it would be 0.5 for BOTH — the numbers that would appear if
    the median were recomputed inside each group.
    """
    rollup = backtest.backtest_rollup(
        *scored(
            (screener.CHEAP_AND_STRONG, 0.5),
            (screener.CHEAP_AND_STRONG, 0.6),
            (screener.RICH_AND_WEAK, -0.5),
            (screener.RICH_AND_WEAK, -0.6),
        )
    )

    assert bucket(rollup, screener.CHEAP_AND_STRONG)[backtest.HIT_RATE] == pytest.approx(1.0)
    assert bucket(rollup, screener.RICH_AND_WEAK)[backtest.HIT_RATE] == pytest.approx(0.0)


def test_a_state_no_company_is_in_is_a_zero_row_and_not_a_missing_one():
    """Mirrors test_screener.py:385 for the industry rollup, and for the same reason: a caller
    reading the table by bucket must not hit a KeyError on the day nothing is green, and "no
    green companies" is a finding worth printing rather than a row to omit."""
    rollup = backtest.backtest_rollup(*scored((screener.MIXED, 0.1)))
    green = bucket(rollup, screener.CHEAP_AND_STRONG)

    assert green[screener.COMPANIES] == 0
    assert green[screener.RATED] == 0
    assert pd.isna(green[backtest.FORWARD_RETURN])
    assert pd.isna(green[backtest.HIT_RATE])


def test_the_company_counts_add_up_to_the_frame():
    """The one property a rollup has to have — the same check
    test_the_signal_counts_add_up_to_the_company_count makes for the industry rollup."""
    frame, signal, returns = scored(
        (screener.CHEAP_AND_STRONG, 0.5),
        (screener.MIXED, 0.1),
        (screener.RICH_AND_WEAK, -0.3),
        (screener.UNKNOWN, float("nan")),
        (screener.UNKNOWN, float("nan")),
    )

    rollup = backtest.backtest_rollup(frame, signal, returns)

    assert rollup[screener.COMPANIES].sum() == len(frame)
    assert rollup[screener.RATED].sum() == 3


def test_an_infinite_stored_fact_is_treated_as_missing_rather_than_scored():
    """MEASURED 2026-08-13 against the real DB: `fact` contains Postgres Infinity/-Infinity
    values — EPS = net_income / 0 shares at ingest time — and 249 companies in the eligible
    pool carried at least one. src/db.py write_metric_averages already refuses to store a
    non-finite value (math.isfinite, src/db.py:269); write_facts only guards pd.isna(), which
    is False for inf, so they reach the table.

    Left alone they are not merely noise: metrics.valuation() turns an infinite EPS into an
    INFINITE Graham margin, which is the largest value in the frame, so the company lands in
    the top cheapness quartile and is reported as 🟢 — the report's headline bucket, filled
    with companies whose fundamentals are a division by zero.

    A non-finite value is not a measurement, so it is skipped exactly like a NULL, and the
    remaining good years still count.
    """
    frame = backtest.point_in_time_frame(
        facts(
            *series("a", metrics.EPS, {2021: 2.0, 2022: 4.0, 2023: float("inf")}),
            *series("a", metrics.BOOK_VALUE_PER_SHARE, {2022: 50.0, 2023: float("inf")}),
        ),
        {"a": 12},
        CUTOFF,
    )

    # the mean of the two REAL years, not inf and not a mean poisoned by it
    assert frame.loc["a", "avg_eps"] == pytest.approx(3.0)
    # the latest year that carried a real number, the same fallback a NULL gets
    assert frame.loc["a", "book_value_per_share"] == pytest.approx(50.0)


def test_a_company_whose_every_year_is_infinite_drops_out_entirely():
    """The other half: nothing real is left, so there is no measurement to report. It must not
    survive as an inf — that is the row that would be ranked cheapest of all."""
    frame = backtest.point_in_time_frame(
        facts(
            *series("a", metrics.EPS, {2022: float("inf"), 2023: float("-inf")}),
            *series("a", metrics.BOOK_VALUE_PER_SHARE, {2022: float("inf")}),
        ),
        {"a": 12},
        CUTOFF,
    )

    assert pd.isna(frame.loc["a", "avg_eps"])
    assert pd.isna(frame.loc["a", "book_value_per_share"])
