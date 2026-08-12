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
