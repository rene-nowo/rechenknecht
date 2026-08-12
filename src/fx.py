"""Where an exchange rate comes from. What is DONE with it lives in src/metrics.py
(in_quote_currency), so this module has no arithmetic in it beyond returning a number.

A foreign private issuer reports in its functional currency (company.reporting_currency:
Alibaba CNY, MUFG JPY, HDFC INR) while its ADR trades in USD (quote.currency). Dividing the
USD price by a CNY earnings figure produces a number that looks like a P/E and means
nothing, so the earnings side is converted first — at ingest time (ingest.py, via
RechenknechtBeta) and again when the dashboard recomputes live from storage.

Source: the Frankfurter API — ECB reference rates, free, no key, no account. Finnhub's free
tier was checked first and does NOT include forex: /forex/rates answers 403 with the very
same key that works for /quote.
"""

import datetime
import logging

import requests

from src import db

logger = logging.getLogger(__name__)

FRANKFURTER_URL = "https://api.frankfurter.dev/v1/latest"
SOURCE = "frankfurter"


def rate(connection, base: str, quote: str, as_of: datetime.date = None) -> float:
    """How many `quote` units one `base` unit buys. 1.0 when there is nothing to convert.

    That 1.0 is the case to get exactly right, not the fetch: every company ingested before
    company.reporting_currency existed has it NULL, and a US filer reports in the currency it
    trades in. Both must come out as a hard 1.0 — no request, no rounding, no 0.9999 — so
    that not one of the 214 already-stored ratios moves. The guard runs before the connection
    is touched at all, which is why it is safe to call this for every company.

    Cached per day in the fx_rate table, the same shape quote uses for a price: a dashboard
    page load and a re-ingest on the same day share one fetch.

    Failure is answered in this order: today's stored rate, then a live fetch, then the newest
    stored rate whatever its age, then NaN. NaN propagates through metrics.valuation() and
    renders as "--" — no number is a worse answer than a stale rate and a far better one than
    a silently mixed-currency ratio, which is the exact defect this whole path exists to fix.
    """
    # isinstance(str), not `not base`: pandas hands a NULL text column back as the FLOAT nan
    # (pandas 3.0 infers `str` dtype and its missing value is nan, not None), and nan is
    # neither falsy nor equal to itself — so both halves of the old guard let it straight
    # through into `where base = %s` and the query died with "operator does not exist:
    # text = double precision". That is the path 209 of 221 companies take, every one of them
    # a US filer whose answer is a hard 1.0. A currency is a string or it is not known.
    if not isinstance(base, str) or not isinstance(quote, str) or base == quote:
        return 1.0

    as_of = as_of or datetime.date.today()
    stored = db.latest_fx_rate(connection, base, quote)
    if stored is not None and stored["as_of"] == as_of:
        return stored["rate"]

    fetched = fetch_rate(base, quote)
    if fetched is None:
        if stored is not None:
            logger.warning(
                "fx: %s->%s could not be fetched, using the rate stored on %s",
                base, quote, stored["as_of"],
            )
            return stored["rate"]
        logger.warning("fx: no %s->%s rate available — its valuation ratios stay empty", base, quote)
        return float("nan")

    db.write_fx_rate(connection, base, quote, fetched, SOURCE, as_of)
    return fetched


def fetch_rate(base: str, quote: str):
    """One live rate, or None — never an exception and never a stand-in, so a currency API
    being down can only empty the ratios, not take an ingest run with it.

    The ECB publishes on business days, so `latest` over a weekend answers with Friday's rate
    and dates it Friday. That is the correct rate for a Sunday, not a stale one.
    """
    try:
        response = requests.get(
            FRANKFURTER_URL, params={"base": base, "symbols": quote}, timeout=10
        )
        response.raise_for_status()
        value = response.json().get("rates", {}).get(quote)
    except (requests.RequestException, ValueError) as error:
        logger.debug("frankfurter failed for %s->%s: %s", base, quote, error)
        return None
    return float(value) if value else None
