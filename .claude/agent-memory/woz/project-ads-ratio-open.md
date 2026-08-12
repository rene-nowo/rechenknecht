---
name: project-ads-ratio-open
description: The ADS-ratio mismatch (1 ADS != 1 ordinary share) is still unfixed; four tickers have their valuation ratios suppressed as a stopgap
metadata:
  type: project
---

`baba`, `jd`, `hdb`, `pdd` are **currency-correct but ADS-ratio-wrong**: the filing's
per-share figures are per ORDINARY share while the price is per ADS (baba: 1 ADS = 8 shares),
so their KGV/RoI/Graham margin are off by the ADS ratio — ~8x for baba, measured 2026-08-11.

As of 2026-08-11 the fix does not exist. The screener suppresses their ratio cells via an
explicit ticker list (`src/screener.py ADS_RATIO_MISMATCH`); `dashboard.py` still shows the
wrong numbers for them.

**Why:** nothing in the stored data says "this company trades as a multi-share ADS", so no
heuristic can detect it — the ratio has to be sourced (the 20-F cover page states it) before
any general fix is possible. A wrong-but-plausible KGV in a ranking destroys the ranking.

**How to apply:** when the ADS-ratio brief ships, **delete** the ticker list rather than
generalize it, and re-check `dashboard.py` at the same time — the suppression only ever
existed on the screener. Until then, treat any per-share ratio for these four as unusable,
and expect more tickers to belong in the list than the four we happened to measure.

Related: [[project-sec-taxonomy-check-per-accession]] — same lesson shape, a company-level
property that only the filing itself states.
