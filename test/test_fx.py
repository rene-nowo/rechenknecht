"""The currency conversion that sits in front of the valuation ratios.

A foreign private issuer reports in CNY/JPY/INR while its ADR trades in USD, so
`price / avg_eps` divides two different currencies into each other and produces a number that
looks like a P/E. Two things can silently go wrong when fixing that, and both are worse than
the original bug because they look right:

  * the DIRECTION — dividing instead of multiplying makes Alibaba's KGV 45x too small
    instead of 7x too large, and nothing about the number says which,
  * the NO-OP — every company ingested before any of this existed reports in the currency it
    trades in, and their stored ratios must not move by so much as a rounding error.

No network: the fetch is replaced where it is exercised, and every rate below is the real one
Frankfurter served on 2026-08-10 (CNY 0.14827, JPY 0.0063 per USD).
"""

import datetime
import math

import pandas as pd
import pytest

from src import fx, metrics

# ECB reference rates as served by api.frankfurter.dev on 2026-08-10, base first:
# 1 CNY = 0.14827 USD, 1 JPY = 0.0063 USD.
CNY_USD = 0.14827
JPY_USD = 0.0063


def _explode(*args, **kwargs):
    raise AssertionError("no rate may be fetched here")


# ── the no-op: everything ingested before this existed ───────────────────────────────────


@pytest.mark.parametrize(
    "base,quote",
    [
        (None, "USD"),   # every company ingested before company.reporting_currency existed
        ("USD", "USD"),  # a US 10-K filer: reports in the currency it trades in
        ("USD", None),   # a delisted ticker has no quote and therefore no quote currency
        (None, None),
        # The SAME two cases as they arrive from a DataFrame: pandas 3.0 reads a NULL text
        # column back as the float nan, which is neither falsy nor equal to itself. Both
        # pages read their currencies out of a frame, so this is how the guard is really
        # called — and before it checked for a string, nan reached `where base = %s` and
        # took the page down with "operator does not exist: text = double precision".
        (float("nan"), "USD"),
        (float("nan"), float("nan")),
        ("USD", float("nan")),
    ],
)
def test_nothing_to_convert_is_answered_without_touching_anything(base, quote):
    """None as the connection is the assertion: the guard has to run before any database
    access, because this is the path taken for 214 of the 220 stored companies and by every
    single company the planned screener will render."""
    assert fx.rate(None, base, quote) == 1.0


def test_a_rate_of_one_leaves_the_inputs_bit_identical():
    """Not approx — identical. The brief's hard requirement is that no already-stored ratio
    moves, and x * 1.0 is exactly x in IEEE 754."""
    values = (6.65, 4.91, 3.21)
    assert metrics.in_quote_currency(1.0, *values) == values


def test_a_us_filers_ratios_do_not_move(monkeypatch):
    """The full path, end to end: aapl's stored inputs through valuation() with and without
    the conversion in the way. Same numbers, exactly."""
    monkeypatch.setattr(fx, "fetch_rate", _explode)
    rate = fx.rate(None, "USD", "USD")

    before = metrics.valuation(308.26, 6.65, 4.91, 4.91)
    after = metrics.valuation(308.26, *metrics.in_quote_currency(rate, 6.65, 4.91, 4.91))

    assert after == before


# ── the direction ────────────────────────────────────────────────────────────────────────


def test_the_conversion_moves_earnings_into_the_price_currency():
    """Alibaba's 7-year average EPS is CNY 5.17. At 0.14827 USD/CNY that is USD 0.7666 —
    multiplied, because the rate is quote-per-base. Divided it would read 34.87, which is
    still a plausible-looking EPS and a completely wrong one."""
    (eps,) = metrics.in_quote_currency(CNY_USD, 5.17)

    assert eps == pytest.approx(0.76656, rel=1e-5)
    assert eps < 5.17  # CNY is worth less than USD, so the figure must SHRINK


def test_the_price_is_the_number_that_does_not_move():
    """MUFG: a JPY 74.86 average EPS against a USD 22.24 ADR price. The unconverted KGV is
    0.297, which is not a P/E any company has ever had; converted it is a bank-shaped 47."""
    unconverted = metrics.valuation(22.24, 74.86, 1717.24, 1717.24)
    converted = metrics.valuation(22.24, *metrics.in_quote_currency(JPY_USD, 74.86, 1717.24, 1717.24))

    assert unconverted[metrics.KGV] == pytest.approx(0.297, rel=1e-2)
    assert converted[metrics.KGV] == pytest.approx(0.297 / JPY_USD, rel=1e-2)
    # The Graham number is itself a price and has to come out comparable to the real one.
    assert converted[metrics.GRAHAM_NUMBER] == pytest.approx(1700.7154947256756 * JPY_USD, rel=1e-6)


def test_a_null_out_of_postgres_stays_missing_rather_than_becoming_zero():
    """v_valuation LEFT JOINs everything, so a bank with no reported book value hands back a
    None and a numeric arrives as Decimal. Decimal * float and None * float both raise."""
    from decimal import Decimal

    eps, book_value = metrics.in_quote_currency(CNY_USD, Decimal("5.17"), None)

    assert eps == pytest.approx(0.76656, rel=1e-5)
    assert math.isnan(book_value)


# ── the two paths must agree ─────────────────────────────────────────────────────────────


def _frame(eps_per_year: dict, book_value: float) -> pd.DataFrame:
    """The two rows add_price_ratios reads, as a source would leave them."""
    frame = pd.DataFrame(
        {year: {metrics.EPS: eps, metrics.BOOK_VALUE_PER_SHARE: book_value,
                metrics.CONSERVATIVE_BOOK_VALUE_PER_SHARE: book_value}
         for year, eps in eps_per_year.items()}
    )
    return frame


def test_the_stored_ratio_and_the_displayed_one_cannot_diverge():
    """add_averages is the ingest path (writes metric_average), valuation() the dashboard
    path (recomputes from storage). A conversion applied in only one of them would show a
    different KGV than the one on file, which is the exact failure src/metrics.py exists to
    prevent — so it is checked, not assumed."""
    frame = _frame({"2025": 5.0, "2024": 5.34}, book_value=55.15)
    metrics.add_averages(frame, 132.32, time_span=7, fx_rate=CNY_USD)

    scalar = metrics.valuation(132.32, *metrics.in_quote_currency(CNY_USD, 5.17, 55.15, 55.15))

    assert frame.loc[metrics.KGV, "7_YEAR_AVG"] == pytest.approx(scalar[metrics.KGV])
    assert frame.loc[metrics.RO_I, "7_YEAR_AVG"] == pytest.approx(scalar[metrics.RO_I])
    assert frame.loc[metrics.KBGV, "7_YEAR_AVG"] == pytest.approx(scalar[metrics.KBGV])


def test_the_frames_own_rows_stay_in_the_currency_the_filing_reported():
    """Only the RATIOS are converted. The per-share rows are filing facts and get stored in
    `fact` as they are — converting them would leave the database holding CNY figures
    labelled as nothing in particular, and re-converting them on read would double-apply."""
    frame = _frame({"2025": 5.0}, book_value=55.15)
    metrics.add_averages(frame, 132.32, time_span=7, fx_rate=CNY_USD)

    assert frame.loc[metrics.EPS, "2025"] == 5.0
    assert frame.loc[metrics.BOOK_VALUE_PER_SHARE, "7_YEAR_AVG"] == 55.15


# ── where the rate comes from ────────────────────────────────────────────────────────────


def test_todays_stored_rate_is_reused_instead_of_refetched(monkeypatch):
    """Same convention as the quote cache: dated data, keyed on the date, not re-fetched the
    same day. The planned screener renders every company on one page, so a fetch per company
    per page load is the difference between one request a day and hundreds."""
    monkeypatch.setattr(
        fx.db, "latest_fx_rate",
        lambda connection, base, quote: {"rate": CNY_USD, "as_of": datetime.date.today(), "source": "frankfurter"},
    )
    monkeypatch.setattr(fx, "fetch_rate", _explode)

    assert fx.rate(None, "CNY", "USD") == CNY_USD


def test_a_failed_fetch_falls_back_to_the_last_known_rate(monkeypatch):
    """A rate from three days ago is off by a fraction of a percent. Falling back to 1.0
    instead would be off by a factor of seven and look completely normal."""
    stale = datetime.date.today() - datetime.timedelta(days=3)
    monkeypatch.setattr(
        fx.db, "latest_fx_rate",
        lambda connection, base, quote: {"rate": 0.149, "as_of": stale, "source": "frankfurter"},
    )
    monkeypatch.setattr(fx, "fetch_rate", lambda base, quote: None)

    assert fx.rate(None, "CNY", "USD") == 0.149


def test_with_no_rate_at_all_the_ratios_go_empty_rather_than_wrong(monkeypatch):
    """The last line of defence. NaN renders as "--" in the dashboard and is dropped by
    write_metric_averages' isfinite check, so an unreachable currency API costs the ratios
    for that company and nothing else."""
    monkeypatch.setattr(fx.db, "latest_fx_rate", lambda connection, base, quote: None)
    monkeypatch.setattr(fx, "fetch_rate", lambda base, quote: None)

    rate = fx.rate(None, "CNY", "USD")
    assert math.isnan(rate)

    ratios = metrics.valuation(132.32, *metrics.in_quote_currency(rate, 5.17, 55.15, 55.15))
    assert all(math.isnan(value) for value in ratios.values())


def test_a_fetched_rate_is_written_back_before_it_is_returned(monkeypatch):
    """Otherwise every page load and every ticker in a batch pays for its own request."""
    written = {}
    monkeypatch.setattr(fx.db, "latest_fx_rate", lambda connection, base, quote: None)
    monkeypatch.setattr(fx, "fetch_rate", lambda base, quote: CNY_USD)
    monkeypatch.setattr(
        fx.db, "write_fx_rate",
        lambda connection, base, quote, rate, source, as_of: written.update(
            base=base, quote=quote, rate=rate, source=source, as_of=as_of
        ),
    )

    assert fx.rate(None, "CNY", "USD") == CNY_USD
    assert written == {
        "base": "CNY", "quote": "USD", "rate": CNY_USD,
        "source": "frankfurter", "as_of": datetime.date.today(),
    }
