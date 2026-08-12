"""The screener's three decisions: which industry, which numbers, which rows.

Every case here is one that would silently show a wrong company or a wrong number on a page
whose whole purpose is a trustworthy ranking. The numbers are the real stored ones (baba,
msft), so a regression reads as "this is what the page would print".
"""

import pandas as pd
import pytest

from src import metrics, screener

# The columns v_valuation hands the page, with the four the ratios need.
COLUMNS = [
    "ticker", "sic_code", "price", "currency", "reporting_currency",
    "avg_eps", "book_value_per_share", "conservative_book_value_per_share",
]


def frame(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=COLUMNS)


def company(ticker, sic_code=None, price=100.0, currency="USD", reporting_currency=None,
            avg_eps=5.0, book_value=50.0, conservative_book_value=50.0) -> tuple:
    return (ticker, sic_code, price, currency, reporting_currency,
            avg_eps, book_value, conservative_book_value)


# ── industry ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("code", [6021, 6199, 6311, 6411, 6798])
def test_banks_insurers_and_reits_are_out_by_default(code):
    """jpm is 6021, hdb 6022, the insurers sit in 63xx and REITs at 6798 — one division,
    one range. Their ratios are not comparable to an industrial company's."""
    assert not screener.industry_mask(frame(company("x", code))).iloc[0]


def test_the_toggle_brings_the_financials_back():
    assert screener.industry_mask(frame(company("jpm", 6021)), financials=True).iloc[0]


@pytest.mark.parametrize("code", [3571, 3674, 3663, 7372, 7370])
def test_computers_electronics_and_software_are_the_tech_filter(code):
    """Electronic computers, semiconductors, communications equipment, prepackaged software,
    computer services — the ranges René asked for."""
    assert screener.industry_mask(frame(company("x", code)), tech_only=True).iloc[0]


@pytest.mark.parametrize("code", [2834, 6021, 5961])
def test_the_tech_filter_excludes_everything_else(code):
    """Pharma, a bank, and catalog retail — amzn really is 5961, which is why this filter is
    a quick filter and not a definition of "tech company"."""
    assert not screener.industry_mask(frame(company("x", code)), tech_only=True).iloc[0]


def test_an_unclassified_company_is_visible_by_default():
    """Every company ingested before company.sic_code existed has NULL here. Vanishing out
    of both the default view and the tech view without a word is the failure mode."""
    assert screener.industry_mask(frame(company("msft", None))).iloc[0]
    assert screener.industry_mask(frame(company("msft", None)), tech_only=True).iloc[0]


def test_the_uncategorized_bucket_can_be_turned_off():
    assert not screener.industry_mask(frame(company("msft", None)), uncategorized=False).iloc[0]


def test_an_empty_frame_filters_to_an_empty_frame():
    """A filter combination that matches nothing is a normal state of this page."""
    empty = frame()
    assert screener.industry_mask(empty, tech_only=True).empty
    assert empty[screener.industry_mask(empty)].empty


# ── the ratios ───────────────────────────────────────────────────────────────────────────


def test_the_ratios_are_the_ones_metrics_computes():
    """No second formula on the page: the same inputs through metrics.valuation() have to
    give the same numbers."""
    row = company("msft", 7372, price=500.0, avg_eps=10.0, book_value=25.0)
    expected = metrics.valuation(500.0, 10.0, 25.0, 25.0)

    got = screener.add_valuation(frame(row), {(None, "USD"): 1.0}).iloc[0]

    assert got[metrics.KGV] == expected[metrics.KGV]
    assert got[metrics.RO_I] == expected[metrics.RO_I]
    assert got[metrics.GRAHAM_MARGIN] == expected[metrics.GRAHAM_MARGIN]


def test_a_foreign_filer_is_converted_before_the_ratio():
    """A USD price over a CNY EPS is not a P/E. jd: price 33.47 USD, avg EPS 7.11 CNY —
    ~4.7 unconverted, ~34 converted at 0.14 USD/CNY."""
    row = company("x", 5961, price=33.47, reporting_currency="CNY", avg_eps=7.11, book_value=75.57)

    got = screener.add_valuation(frame(row), {("CNY", "USD"): 0.14}).iloc[0]

    assert got[metrics.KGV] == pytest.approx(33.47 / (7.11 * 0.14))


def test_a_company_with_no_stored_currency_still_finds_its_rate():
    """pandas 3.0 reads a NULL text column back as the float nan, and nan != nan. Keyed on
    the raw value, the rate fetched for (nan, "USD") would not be found again when it is
    applied — and 154 of 170 companies would show empty ratios for no visible reason."""
    rows = frame(company("msft", 7372, reporting_currency=float("nan")))

    pairs = screener.currency_pairs(rows)
    assert pairs == {(None, "USD")}

    got = screener.add_valuation(rows, {pair: 1.0 for pair in pairs}).iloc[0]
    assert got[metrics.KGV] == pytest.approx(20.0)


def test_a_missing_rate_empties_the_ratios_rather_than_mixing_currencies():
    row = company("x", 5961, reporting_currency="JPY")

    got = screener.add_valuation(frame(row), {}).iloc[0]

    assert pd.isna(got[metrics.KGV])


def test_a_company_without_a_price_has_no_valuation_ratios():
    """fl was acquired and no longer trades; v_valuation hands back a NULL price."""
    got = screener.add_valuation(frame(company("fl", 5661, price=None)), {(None, "USD"): 1.0}).iloc[0]

    assert pd.isna(got[metrics.KGV])
    assert pd.isna(got[metrics.GRAHAM_MARGIN])


@pytest.mark.parametrize("ticker", sorted(screener.ADS_RATIO_MISMATCH))
def test_the_ads_mismatched_companies_show_no_ratio_at_all(ticker):
    """1 ADS is not 1 ordinary share, so their KGV is off by the ADS ratio (baba ~8x). Until
    that is fixed the cells stay empty — a company that is visibly missing a number is
    recoverable, a plausible wrong number is not."""
    row = company(ticker, 7389, price=132.32, reporting_currency="CNY",
                  avg_eps=5.17, book_value=55.15)

    got = screener.add_valuation(frame(row), {("CNY", "USD"): 0.14}).iloc[0]

    assert pd.isna(got[metrics.KGV])
    assert pd.isna(got[metrics.RO_I])
    assert pd.isna(got[metrics.GRAHAM_MARGIN])


def test_the_ads_companies_keep_everything_that_is_not_a_ratio():
    """Blanked, not filtered out: the row is still there with its price and its industry."""
    got = screener.add_valuation(frame(company("baba", 7389)), {(None, "USD"): 1.0})

    assert list(got["ticker"]) == ["baba"]
    assert got["price"].iloc[0] == 100.0


# ── the range filters ────────────────────────────────────────────────────────────────────


def test_a_bound_keeps_only_what_clears_it():
    rows = frame(company("a"), company("b"), company("c"))
    rows["RoA"] = [2.0, 8.0, 15.0]

    assert list(rows[screener.in_bounds(rows, {"RoA": (5.0, None)})]["ticker"]) == ["b", "c"]
    assert list(rows[screener.in_bounds(rows, {"RoA": (5.0, 10.0)})]["ticker"]) == ["b"]


def test_a_company_with_no_value_fails_a_bound_that_is_set():
    """"We do not know its RoA" is not "its RoA is above 5"."""
    rows = frame(company("a"), company("b"))
    rows["RoA"] = [float("nan"), 8.0]

    assert list(rows[screener.in_bounds(rows, {"RoA": (5.0, None)})]["ticker"]) == ["b"]


def test_a_company_with_no_value_survives_when_nothing_is_bounded():
    """The default view: no bound set, so a company with no KGV is still on the page."""
    rows = frame(company("a"))
    rows["KGV"] = [float("nan")]

    assert screener.in_bounds(rows, {"KGV": (None, None)}).iloc[0]


# ── the quick-scan signal ────────────────────────────────────────────────────────────────

NAN = float("nan")


def scored(*rows) -> pd.DataFrame:
    """(Graham margin, RoI, RoA, EBIT margin, equity ratio) per company — the five columns
    the signal reads, in the names the page's frame carries them under."""
    return pd.DataFrame(
        list(rows),
        columns=[metrics.GRAHAM_MARGIN, metrics.RO_I, "roa", "ebit_margin", "equity_ratio"],
    )


def test_the_quality_score_counts_the_thresholds_the_workbook_already_prints():
    """RoI > 8, RoA > 8, EBIT margin > 13, equity ratio > 35 — src/RechenknechtBeta.py
    print_extraordinary_results, the tool's own definition of "a candidate". Strictly
    greater there, so a company sitting exactly on all four cutoffs scores 0."""
    rows = scored(
        (0.0, 9.0, 9.0, 14.0, 36.0),
        (0.0, 8.0, 8.0, 13.0, 35.0),
        (0.0, 9.0, 9.0, 12.0, 30.0),
    )

    assert list(screener.quality_score(rows)) == [4, 0, 2]


def test_a_missing_quality_metric_is_not_a_met_one():
    """"We do not know this company's RoA" is not "its RoA is above 8" — the same rule the
    range filters follow. Scoring out of the metrics that happen to exist would let a
    company with one known number outrank one with four."""
    rows = scored((0.0, NAN, NAN, 20.0, 40.0))

    assert screener.quality_score(rows).iloc[0] == 2


def test_cheap_and_strong_is_green_expensive_and_weak_is_red_the_rest_is_yellow():
    """Margins -90/-70/-30/-10 put the quartile cutoffs at -75 and -25, so exactly one row
    is in each tail. Yellow is what a row EARNS by clearing neither bar, not a fallback."""
    rows = scored(
        (-10.0, 9.0, 9.0, 14.0, 36.0),   # cheapest quarter, 4 of 4
        (-90.0, 2.0, 3.0, 5.0, 20.0),    # dearest quarter, 0 of 4
        (-30.0, 9.0, 9.0, 14.0, 36.0),   # strong, but mid-priced
        (-70.0, 2.0, 3.0, 5.0, 20.0),    # weak, but mid-priced
    )

    assert list(screener.signal(rows)) == ["🟢", "🔴", "🟡", "🟡"]


def test_a_cheap_but_weak_company_is_not_green():
    """Both halves have to hold. The tail alone is a price, not a verdict — this is the row
    a "sort by Graham margin" list puts on top and this column has to qualify."""
    rows = scored(
        (-10.0, 2.0, 9.0, 5.0, 20.0),    # cheapest quarter, 1 of 4
        (-90.0, 9.0, 9.0, 14.0, 36.0),   # dearest quarter, 4 of 4
        (-30.0, 5.0, 5.0, 5.0, 5.0),
        (-70.0, 5.0, 5.0, 5.0, 5.0),
    )

    assert list(screener.signal(rows))[:2] == ["🟡", "🟡"]


def test_the_quartiles_come_from_the_frame_and_not_from_a_number_in_the_code():
    """"Cheap" is cheap RELATIVE TO WHAT IS ON SCREEN. The same company, scored against two
    different peer groups: dearest-quarter-and-strong among expensive peers, mid-field among
    cheap ones. A cutoff hardcoded today would go stale the moment the universe grows."""
    row = (-30.0, 9.0, 9.0, 14.0, 36.0)
    dear = scored(row, (-80.0, 0.0, 0.0, 0.0, 0.0), (-85.0, 0.0, 0.0, 0.0, 0.0),
                  (-90.0, 0.0, 0.0, 0.0, 0.0))
    cheap = scored(row, (10.0, 0.0, 0.0, 0.0, 0.0), (20.0, 0.0, 0.0, 0.0, 0.0),
                   (30.0, 0.0, 0.0, 0.0, 0.0))

    assert screener.signal(dear).iloc[0] == "🟢"
    assert screener.signal(cheap).iloc[0] == "🟡"


def test_no_graham_margin_is_its_own_state_and_never_the_mixed_one():
    """baba/jd/hdb/pdd have their ratios blanked for the ADS mismatch, and brk-b/xom/ma/itw
    have no average EPS to build a Graham number from — 435 of 1427 rows have no margin at
    all, measured 2026-08-11. Painting them yellow would sell an absence of information as
    an evaluated middling score, which is the one thing this column must not do. Real stored
    quality metrics here, so the row is missing only the margin."""
    rows = scored(
        (NAN, NAN, 6.47, 11.61, 56.12),   # baba: ratios blanked, quality intact
        (NAN, NAN, 6.08, 0.86, 53.29),    # xom: no avg_eps
        (-50.0, 2.0, 3.0, 5.0, 20.0),
    )

    assert list(screener.signal(rows))[:2] == ["⚪", "⚪"]


def test_a_company_with_no_quality_metric_at_all_is_also_unknown():
    """A margin without a single quality number is half a judgement. Calling it red would
    assert a weakness nobody measured — the score is 0 only because there is nothing there."""
    rows = scored(
        (-90.0, NAN, NAN, NAN, NAN),
        (-10.0, 9.0, 9.0, 14.0, 36.0),
        (-30.0, 5.0, 5.0, 5.0, 5.0),
        (-70.0, 5.0, 5.0, 5.0, 5.0),
    )

    assert screener.signal(rows).iloc[0] == "⚪"


def test_the_four_states_are_four_distinct_glyphs():
    """The point of the fourth state is that it is VISIBLY not the third one."""
    states = [screener.CHEAP_AND_STRONG, screener.MIXED, screener.RICH_AND_WEAK,
              screener.UNKNOWN]

    assert len(set(states)) == 4


# ── the industry rollup ──────────────────────────────────────────────────────────────────


def grouped(*rows) -> pd.DataFrame:
    """(industry, Graham margin, RoI, RoA, EBIT margin, equity ratio) — what the rollup reads:
    the signal's five columns plus the column it groups by."""
    return pd.DataFrame(
        list(rows),
        columns=[screener.INDUSTRY, metrics.GRAHAM_MARGIN, metrics.RO_I,
                 "roa", "ebit_margin", "equity_ratio"],
    )


STRONG = (9.0, 9.0, 14.0, 36.0)     # clears all four thresholds
WEAK = (2.0, 3.0, 5.0, 20.0)        # clears none


def test_the_rollup_counts_every_company_in_its_industry():
    rows = grouped(
        ("Software", -10.0, *STRONG),
        ("Software", -30.0, *WEAK),
        ("Mining", -50.0, *WEAK),
    )

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Software", screener.COMPANIES] == 2
    assert out.loc["Mining", screener.COMPANIES] == 1


def test_the_rollup_reports_how_many_companies_the_median_stands_on():
    """A median skips the rows with no Graham margin, so an industry of four where one has a
    margin reports that ONE company as its median. Without this column the reader sees
    "4 companies" beside it — and measured 2026-08-12 that is exactly how the artefacts came
    to occupy the whole top of a cheapest-industries sort."""
    rows = grouped(
        ("Software", 500.0, *WEAK), ("Software", NAN, *WEAK),
        ("Software", NAN, *WEAK), ("Software", NAN, *WEAK),
    )

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Software", screener.COMPANIES] == 4
    assert out.loc["Software", screener.RATED] == 1
    assert out.loc["Software", metrics.GRAHAM_MARGIN] == 500.0


def test_an_industry_where_nothing_has_a_margin_is_rated_zero():
    """Not an error and not a 0% margin — an empty median is NaN, which renders as an empty
    cell. "We cannot price this industry" is not "this industry is priced at zero"."""
    rows = grouped(("Mining", NAN, *WEAK), ("Mining", NAN, *WEAK))

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Mining", screener.RATED] == 0
    assert pd.isna(out.loc["Mining", metrics.GRAHAM_MARGIN])


def test_the_rollup_takes_the_median_margin_not_the_mean():
    """A negative book value per share yields a Graham number out of the square root of two
    negatives, and margins up to 2.4e8% are really in the data (measured 2026-08-12). One
    such row decides an industry's MEAN on its own; the median does not move."""
    rows = grouped(
        ("Software", -60.0, *WEAK),
        ("Software", -40.0, *WEAK),
        ("Software", 240_000_000.0, *WEAK),
    )

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Software", metrics.GRAHAM_MARGIN] == -40.0
    assert out.loc["Software", screener.QUALITY_SCORE] == 0


def test_the_rollup_counts_the_four_signal_states_per_industry():
    """The distribution is the point of the view: "which industry has the most green ones"
    has to be answerable without opening the industry."""
    rows = grouped(
        ("Software", -10.0, *STRONG),    # cheapest quarter, 4 of 4 -> green
        ("Software", -90.0, *WEAK),      # dearest quarter, 0 of 4  -> red
        ("Software", -30.0, *STRONG),    # mid-priced               -> yellow
        ("Software", NAN, *WEAK),        # no margin                -> white
    )

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Software", screener.CHEAP_AND_STRONG] == 1
    assert out.loc["Software", screener.RICH_AND_WEAK] == 1
    assert out.loc["Software", screener.MIXED] == 1
    assert out.loc["Software", screener.UNKNOWN] == 1


def test_a_state_no_company_is_in_is_a_zero_and_not_a_missing_column():
    """An industry with no green company has 0 green, and the page still has the column to
    sort on. A missing column would be a KeyError on the sort, not an empty cell."""
    rows = grouped(("Mining", -50.0, *WEAK), ("Mining", -55.0, *WEAK))

    out = screener.rollup(rows, screener.signal(rows))

    assert list(out.columns[-4:]) == list(screener.SIGNAL_ORDER)
    assert out[screener.CHEAP_AND_STRONG].iloc[0] == 0


def test_the_signal_counts_add_up_to_the_company_count():
    """The one property a rollup must have: no company falls out of its own group. This is
    what a dropped NaN industry key would break silently."""
    rows = grouped(
        ("Software", -10.0, *STRONG), ("Software", NAN, *WEAK), (None, -50.0, *WEAK),
        ("", -90.0, *WEAK), ("   ", -20.0, *STRONG),
    )

    out = screener.rollup(rows, screener.signal(rows))

    assert out[screener.COMPANIES].sum() == len(rows)
    assert out[list(screener.SIGNAL_ORDER)].to_numpy().sum() == len(rows)


def test_a_company_with_no_industry_is_named_rather_than_dropped():
    """company.sic_description is nullable AND the SEC returns an empty string for some
    filers — 48 of 5,046 in the default view, measured 2026-08-12, all of them '' and not one
    NULL. Both spellings are one bucket with a visible name, never a blank row."""
    rows = grouped((None, -10.0, *WEAK), ("", -20.0, *WEAK), ("  ", -30.0, *WEAK))

    out = screener.rollup(rows, screener.signal(rows))

    assert list(out[screener.INDUSTRY]) == [screener.UNCLASSIFIED]
    assert out[screener.COMPANIES].iloc[0] == 3


def test_the_rollup_is_scored_against_the_whole_screen_not_against_each_industry():
    """The cheapness half of the signal is a quartile of the frame it is given. Recomputing
    it per industry would make every industry look equally cheap — a quarter of its own rows
    would always be in its own top quarter — and the view exists to compare industries."""
    rows = grouped(
        ("Cheap", 50.0, *STRONG), ("Cheap", 40.0, *STRONG),
        ("Dear", -90.0, *STRONG), ("Dear", -95.0, *STRONG),
    )

    out = screener.rollup(rows, screener.signal(rows)).set_index(screener.INDUSTRY)

    assert out.loc["Cheap", screener.CHEAP_AND_STRONG] == 1
    assert out.loc["Dear", screener.CHEAP_AND_STRONG] == 0


# ── the candidate list ───────────────────────────────────────────────────────────────────


def test_the_candidates_are_exactly_the_green_rows():
    """Not "cheap", not "strong" — the same bucket the glyph already names. A second
    definition of a candidate is the drift this whole module exists to prevent."""
    rows = grouped(
        ("Software", -10.0, *STRONG),   # green
        ("Software", -90.0, *WEAK),     # red
        ("Mining", -30.0, *STRONG),     # yellow
        ("Mining", -70.0, *WEAK),       # yellow
    )
    signals = screener.signal(rows)

    out = screener.candidates(rows, signals)

    assert len(out) == 1
    assert list(out[screener.INDUSTRY]) == ["Software"]
    assert (signals[out.index] == screener.CHEAP_AND_STRONG).all()


def test_the_candidates_are_ranked_by_quality_first():
    """Strength is the signal's own two halves made continuous, quality ahead of price:
    blending a 0-4 count with a percentage-point distance needs a weight nobody justified."""
    # Six rows so the top quartile holds two of them: with four the cutoff lands at 32.5 and
    # only the single cheapest row is ever green, which cannot show an ordering at all.
    rows = grouped(
        ("A", 100.0, 9.0, 9.0, 14.0, 20.0),    # 3 of 4, far above the cut
        ("B", 60.0, 9.0, 9.0, 14.0, 36.0),     # 4 of 4, less far above it
        ("C", -90.0, *WEAK), ("D", -95.0, *WEAK),
        ("E", -97.0, *WEAK), ("F", -99.0, *WEAK),
    )

    out = screener.candidates(rows, screener.signal(rows))

    assert list(out[screener.INDUSTRY]) == ["B", "A"]
    assert list(out[screener.QUALITY_SCORE]) == [4, 3]
    # ...and the one that sorted second really is the one further above the cheapness bar,
    # so this pins the priority and not merely the order.
    assert out[screener.MARGIN_ABOVE_CUT].iloc[1] > out[screener.MARGIN_ABOVE_CUT].iloc[0]


def test_margin_above_cut_is_the_distance_from_the_quartile_that_made_it_cheap():
    """A distance, not a new threshold: the cutoff is margin_quartiles' own top quartile, so
    a green row's value here is never negative."""
    rows = grouped(
        ("A", 100.0, *STRONG), ("B", 60.0, *STRONG),
        ("C", -90.0, *WEAK), ("D", -95.0, *WEAK),
        ("E", -97.0, *WEAK), ("F", -99.0, *WEAK),
    )
    _, cheap = screener.margin_quartiles(rows)

    out = screener.candidates(rows, screener.signal(rows))

    assert len(out) == 2
    assert list(out[screener.MARGIN_ABOVE_CUT]) == pytest.approx([100.0 - cheap, 60.0 - cheap])
    assert (out[screener.MARGIN_ABOVE_CUT] >= 0).all()


def test_the_cheapness_cutoff_comes_from_the_whole_frame_not_from_the_survivors():
    """Taking the quartile from the already-filtered green rows would measure them against
    each other, and the top row's distance from the cut would collapse toward zero."""
    rows = grouped(
        ("A", 100.0, *STRONG), ("B", 90.0, *STRONG), ("C", 80.0, *STRONG),
        ("D", -90.0, *WEAK), ("E", -95.0, *WEAK), ("F", -99.0, *WEAK),
    )
    signals = screener.signal(rows)

    out = screener.candidates(rows, signals)
    green_only = rows[signals == screener.CHEAP_AND_STRONG]

    _, cut_of_all = screener.margin_quartiles(rows)
    _, cut_of_green = screener.margin_quartiles(green_only)
    assert cut_of_all != cut_of_green
    assert out[screener.MARGIN_ABOVE_CUT].max() == pytest.approx(100.0 - cut_of_all)


def test_a_filter_that_matches_nothing_still_yields_the_full_shape():
    """Reachable from the page: the tech filter over a database with no tech company. Both
    views have to come back empty WITH their columns — a rollup missing its glyph columns
    would be a KeyError on the sort, not an empty table."""
    empty = grouped()

    table = screener.rollup(empty, screener.signal(empty))
    picks = screener.candidates(empty, screener.signal(empty))

    assert table.empty and picks.empty
    assert list(table.columns[-4:]) == list(screener.SIGNAL_ORDER)
    assert screener.RATED in table.columns


def test_no_green_company_is_an_empty_list_and_not_an_error():
    """A filter combination that leaves no candidate is a normal state of this page."""
    rows = grouped(("A", -50.0, *WEAK), ("B", -60.0, *WEAK))

    out = screener.candidates(rows, screener.signal(rows))

    assert out.empty
    assert screener.MARGIN_ABOVE_CUT in out.columns


# ── the industry deep-dive ───────────────────────────────────────────────────────────────


def facts(*rows) -> pd.DataFrame:
    """(ticker, fiscal_year, concept, value) — the shape ui.fact_history() returns."""
    return pd.DataFrame(
        list(rows), columns=["ticker", screener.FISCAL_YEAR, screener.CONCEPT, screener.VALUE]
    )


@pytest.mark.parametrize("code, expected", [
    (4412, "DCOILBRENTEU"),   # Deep Sea Foreign Transportation of  Freight — 30 companies
    (4400, "DCOILBRENTEU"),   # Water Transportation, the top of the same range
    (1531, "DGS10"),          # Operative Builders — 21 companies
    (6021, "DGS10"),          # a bank, visible only with the financials toggle
])
def test_the_mapped_industries_get_their_macro_series(code, expected):
    assert screener.macro_series_for(code) == expected


@pytest.mark.parametrize("code", [3571, 2834, 5961, 4512, None, float("nan")])
def test_an_unmapped_industry_gets_no_macro_series(code):
    """Semiconductors, pharma, catalog retail, airlines — and the companies with no SIC code
    at all. A line nobody chose invites a correlation to be read off it, so the honest
    answer is no line."""
    assert screener.macro_series_for(code) is None


def test_the_mapping_is_on_the_code_because_the_sec_text_has_a_double_space():
    """The reason this maps SIC CODES and not description substrings. The SEC really stores
    'Deep Sea Foreign Transportation of  Freight' with two spaces (measured 2026-08-12, 39
    companies), so `'of Freight' in description` is False and a substring mapping would
    silently show no overlay for the one industry it was built for."""
    stored = "Deep Sea Foreign Transportation of  Freight"

    assert "Transportation of Freight" not in stored
    assert screener.macro_series_for(4412) == "DCOILBRENTEU"


def test_the_trend_is_a_median_per_year_and_concept():
    rows = facts(
        ("a", 2024, metrics.EBIT_MARGIN, 10.0),
        ("b", 2024, metrics.EBIT_MARGIN, 20.0),
        ("c", 2024, metrics.EBIT_MARGIN, 30.0),
        ("a", 2023, metrics.EBIT_MARGIN, 5.0),
        ("a", 2024, metrics.REVENUE, 1_000.0),
    )

    out = screener.median_by_year(rows).set_index([screener.FISCAL_YEAR, screener.CONCEPT])

    assert out.loc[(2024, metrics.EBIT_MARGIN), screener.VALUE] == 20.0
    assert out.loc[(2023, metrics.EBIT_MARGIN), screener.VALUE] == 5.0
    assert out.loc[(2024, metrics.REVENUE), screener.VALUE] == 1_000.0


def test_the_trend_takes_the_median_not_the_mean():
    """Same reason the rollup does. One misparsed filing — a share count stored in thousands,
    a segment total read as a consolidated one — would otherwise decide the industry's whole
    year on its own."""
    rows = facts(*[("x", 2024, metrics.EBIT_MARGIN, 10.0)] * 3,
                 ("artefact", 2024, metrics.EBIT_MARGIN, 900_000.0))

    out = screener.median_by_year(rows)

    assert out[screener.VALUE].iloc[0] == 10.0


def test_the_trend_carries_the_sample_size_of_every_year():
    """The column that says whether a rising line is the industry or the sample. Measured
    2026-08-12: the deep-sea shipping EBIT-margin median stands on 2 companies in 2017 and
    on 24 in 2019, purely because EDGAR coverage starts where the ingest's window does."""
    rows = facts(
        ("a", 2017, metrics.EBIT_MARGIN, 8.0),
        ("b", 2017, metrics.EBIT_MARGIN, 9.0),
        *[(f"c{n}", 2019, metrics.EBIT_MARGIN, 9.0) for n in range(24)],
    )

    out = screener.median_by_year(rows).set_index(screener.FISCAL_YEAR)

    assert out.loc[2017, screener.COMPANIES] == 2
    assert out.loc[2019, screener.COMPANIES] == 24


def test_a_company_without_the_fact_is_not_counted_as_a_zero():
    """A missing EBIT-margin is a company that is not in the median, never a company with a
    margin of 0 — the same rule quality_score() follows for a missing metric."""
    rows = facts(
        ("a", 2024, metrics.EBIT_MARGIN, 30.0),
        ("b", 2024, metrics.EBIT_MARGIN, float("nan")),
    )

    out = screener.median_by_year(rows)

    assert out[screener.VALUE].iloc[0] == 30.0
    assert out[screener.COMPANIES].iloc[0] == 1


def test_an_industry_with_no_stored_facts_is_an_empty_frame_and_not_an_error():
    """Reachable: an industry whose companies were all ingested before EBIT-margin was
    written. The page checks .empty — it must not get a KeyError first."""
    out = screener.median_by_year(facts())

    assert out.empty


def test_the_macro_series_is_one_mean_per_calendar_year():
    macro = pd.DataFrame({
        "date": ["2022-01-03", "2022-07-01", "2023-01-02"],
        screener.VALUE: [100.0, 102.0, 80.0],
    })

    out = screener.macro_mean_by_year(macro, "DCOILBRENTEU").set_index(screener.FISCAL_YEAR)

    assert out.loc[2022, screener.VALUE] == 101.0
    assert out.loc[2023, screener.VALUE] == 80.0


def test_the_macro_frame_has_the_same_shape_the_trend_panels_draw():
    """Same columns as median_by_year, so the overlay is drawn by the same chart code as the
    fundamentals panels instead of a second one that could format its axis differently."""
    macro = pd.DataFrame({"date": ["2024-01-02"], screener.VALUE: [4.21]})

    out = screener.macro_mean_by_year(macro, "DGS10")

    assert list(out.columns) == [screener.FISCAL_YEAR, screener.CONCEPT, screener.VALUE]
    assert out[screener.CONCEPT].iloc[0] == "DGS10"
    assert out[screener.FISCAL_YEAR].iloc[0] == 2024
