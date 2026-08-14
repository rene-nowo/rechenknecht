# Rechenknecht

A Graham-style value-investing screener: ingests fundamentals from SEC EDGAR (XBRL) and
Yahoo Finance for ~9,000+ US-listed companies, stores them in Supabase Postgres, computes
KGV/RoI/Graham-margin/quality-score ratios from the SAME formula every source and every page
goes through (`src/metrics.py valuation()`), and ranks companies in a Streamlit dashboard.

Built for one user (René) as a personal research tool, not a customer-facing product.

## What it does

- **Ingest** (`ingest.py`, `ingest_macro.py`, `ingest_parallel.py`) — pulls annual-report XBRL
  from EDGAR (10-K / 20-F) and cross-checks against Yahoo Finance, storing both under their
  own `source` so a disagreement between them is a query, not a guess. `ingest_macro.py`
  separately pulls FRED macro series (yield curve, credit spreads, unemployment, CPI, VIX,
  Fed funds, M2) for the industry deep-dive overlay.
- **Screen** (`pages/Screener.py`, `pages/Opportunities.py`) — every company at once, ranked
  by KGV/RoI/Graham margin, with a quick-scan 🟢/🟡/🔴/⚪ signal column
  (`src/screener.py signal()`) combining cheapness (Graham margin, relative to the current
  frame's own quartiles) and quality (RoI/RoA/EBIT-margin/equity-ratio thresholds).
  `Opportunities.py` rolls the same signal up by industry and lists the 🟢 candidates.
- **Cross-check** (`sec_check.py`) — adjudicates EDGAR-vs-Yahoo disagreements against the
  SEC's own `companyfacts` API, pinned to the exact filing accession.
- **Single-company view** (`dashboard.py`) — one company the way the original Excel workbook
  (`Master_Vorlage`) shows it, pinned against it by `test/test_excel_parity.py`.
- **Backtest** (`backtest_report.py`) — point-in-time report: takes the screener's own signal,
  filtered to only what a company's fiscal-year filings would plausibly have shown by a given
  cutoff date, and checks the realized forward return per signal bucket against live market
  prices. See "Backtest report" below.

## Architecture

```
ingest.py, ingest_macro.py, ingest_parallel.py   ingest scripts (argparse, DB writes)
sec_check.py                                     EDGAR-vs-Yahoo-vs-SEC adjudication
backtest_report.py                               point-in-time signal backtest report

src/metrics.py       THE ONE formula module — KGV, RoI, Graham number/margin, sheet ratios.
                      Every source and every page calls into here; no ratio is ever
                      computed a second time anywhere else.
src/screener.py       Industry filters, the 🟢/🟡/🔴/⚪ signal, the industry rollup and
                      candidate list. No ratio is computed here either — only decisions
                      about which rows/industries survive and how they're bucketed.
src/backtest.py       Point-in-time logic: which fiscal years were "known" as of a cutoff
                      date (fiscal-year-end + ~6 month lag approximation), the point-in-time
                      valuation-input frame, uniform subset sampling, forward-return
                      aggregation. Pure pandas/date logic, no DB or network I/O.
src/db.py             Supabase Postgres access — upserts, reads, the Delta-Law-respecting
                      "check storage before a live fetch" caching used everywhere.
src/fx.py             Exchange rates (Frankfurter/ECB) for foreign-currency filers.
src/yahoo_source.py   Yahoo Finance as a second data source AND live price fetches
                      (today's quote, historical closes for the backtest). Display/fetch
                      only — nothing here writes to the database; callers do that.
src/ui.py             Shared Streamlit page helpers (query caching, valuation_frame()).
src/Edgar_api.py, app_edgar.py, src/RechenknechtBeta.py   EDGAR filing search + XBRL parsing.

db/schema.sql          Postgres schema: company, fact (long/narrow, source='edgar'|'yahoo'),
                        quote, fx_rate, metric_average, ingest_run, macro_series, and the
                        v_valuation / v_source_diff / v_company_year views.

test/                   pytest suite — pure-logic modules (metrics, screener, backtest, fx)
                        are unit tested with plain pandas fixtures; live-API code
                        (ingest.py, yahoo_source.py's fetch functions) is verified manually.
```

## Setup

```bash
poetry install

export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
export RK_FINNHUB_KEY="$(op read 'op://Server/Finnhub/credential')"   # optional — see
                                                                       # ingest.py market_price_of()
```

`RK_DB_URL` is a direct Supabase connection string (`db.<ref>.supabase.co`); `src/db.py`
rewrites it to the IPv4 session pooler automatically, since the direct host resolves IPv6-only
without the paid add-on.

This app has its **own nested git repo** — it is not part of the monorepo's git history.
Commits here happen separately.

## Usage

```bash
# Ingest a handful of tickers (both EDGAR and Yahoo)
python ingest.py --limit 20

# Ingest specific tickers only
python ingest.py --tickers aapl,msft,jpm

# Cross-check EDGAR extraction against the SEC's own companyfacts API
python sec_check.py --limit 20

# Pull macro series (yield curve, credit spreads, CPI, VIX, ...) from FRED
python ingest_macro.py --apply-schema   # first run only
python ingest_macro.py

# Run the dashboard (screener, opportunities, single-company view)
streamlit run dashboard.py

# Point-in-time backtest report: was the signal predictive?
python backtest_report.py --cutoff 2024-08-13
```

### Backtest report

`backtest_report.py` answers one question: **has the screener's own 🟢/🟡/🔴 signal
historically been predictive of forward returns?** It is a report script, not a dashboard
page, and reuses `src/screener.py add_valuation()`/`signal()` exactly as they exist for the
live screener — never a second implementation.

It works in one pass, over a pool chosen without looking at any price:

1. **Select** (zero network calls) — rebuild each company's valuation inputs from only the
   fiscal years that had plausibly been published by the cutoff (`src/backtest.py`'s
   fiscal-year-end + ~6-month lag approximation, `--lag-months` to vary it), then apply
   `screener.industry_mask()` with the live page's own defaults. A company is eligible if it
   has an average EPS and a book value at the cutoff and survives that mask. Measured at
   cutoff 2024-08-13: 6,773 companies with EDGAR data → 5,575 point-in-time complete → 4,215
   after the mask.
2. **Price** — one yfinance call per company for a date RANGE covering both the cutoff and
   today, so both closes come out of a single request. The actual trading day is recorded
   alongside each price.
3. **Score** — attach the real cutoff price and call `screener.add_valuation()` /
   `screener.signal()` once, then report the realized ~2-year forward return per bucket:
   companies, rated (how many we could actually price), median return, and hit-rate against
   the whole sampled frame's median.

Selection deliberately does **not** depend on the price already stored in `quote`. That is
information from *after* the cutoff, and using it silently drops the companies that have since
been delisted — 730 of them, measured — which are exactly the returns a backtest must not
lose. `--limit N` takes a uniform random subset for a sanity run; the seed is fixed so two
sanity runs are comparable.

Output: a table printed to stdout, and a CSV at `reports/backtest_<cutoff>.csv` (gitignored —
generated, not source).

The cutoff-date price is written to the `quote` table only for companies that **already have**
a quote row, where a newer row always shadows it and the write is provably inert. For the
1,144 eligible companies with no quote at all it is skipped — it would otherwise become their
*latest* quote and show a two-year-old price on the live Screener as if it were current.
Today's price is fetched live but never written.

## Testing

```bash
poetry run pytest
```

Pure-logic modules (`src/metrics.py`, `src/screener.py`, `src/backtest.py`, `src/fx.py`) are
unit tested with hand-built pandas fixtures, no database or network calls — see
`test/test_screener.py` for the house style: small row-builder helpers, one behavior per test,
a docstring stating the *measured* reason for the case. `test/test_excel_parity.py` pins the
ratio formulas against the original Excel workbook (`Master_Vorlage`). Live-API code
(`ingest.py`'s yfinance/Finnhub calls, `src/yahoo_source.py`'s fetch functions) is verified
manually against real tickers rather than mocked — the same convention `backtest_report.py`
and its new `src/yahoo_source.py` additions follow.

## Resources

- [SEC EDGAR — developer FAQ](https://www.sec.gov/os/webmaster-faq#developers) (filing
  business hours: 6am–10pm ET, indexes updated nightly from ~10pm ET; some late submissions
  roll to the next business day's index)
- [SEC EDGAR company search](https://www.sec.gov/edgar/searchedgar/companysearch)
- [XBRL US — GAAP tag guide](https://xbrl.us/data-rule/guid-tag/)
