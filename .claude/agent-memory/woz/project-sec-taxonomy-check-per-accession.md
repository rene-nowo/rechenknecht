---
name: sec-taxonomy-check-per-accession
description: A companyfacts us-gaap section does NOT mean the current filing is us-gaap — filers migrate to IFRS and keep historical facts forever; pin the check to the latest accession
metadata:
  type: project
---

To decide whether an EDGAR filer is usable by this parser, check which taxonomy carries the
facts of **the latest inline-XBRL annual report accession** — never "does
`data.sec.gov/api/xbrl/companyfacts` contain a `us-gaap` section at all".

**Why:** the aggregated check false-positives badly. A company that migrated to IFRS keeps
its historical us-gaap facts in companyfacts forever, so the section is still there years
later. Measured 2026-08-11 while widening ingest to Form 20-F: a brief claimed 20 tickers
were us-gaap 20-F filers based on the aggregated check; pinning taxonomy to the latest
accession showed only **7** were (`baba`, `jd`, `pdd`, `ntes`, `mufg`, `hdb`, `abb`). Sony
and Toyota both migrated to `ifrs-full` at FY2022 and their pre-2022 20-Fs are still tagged
us-gaap — so they look eligible and are not. Nine of the eighteen named tickers were IFRS,
and two (`cni`, `bns`) have no inline-XBRL 20-F at all.

**How to apply:** get the newest `form in ('10-K','20-F') and isInlineXBRL == 1` accession
from the submissions endpoint, then count facts per taxonomy filtered on `accn == that
accession`. Also expect a lag — the very newest filing often has no companyfacts entries yet
(all four 2026 filings checked returned none), so walk back one accession before concluding
"not us-gaap". Related: the parser now enforces this itself, raising `LookupError` when the
newest filing carries no us-gaap facts.

Two more SEC-side gotchas from the same session:
- A 20-F carries the same concept twice — functional currency **and** a USD convenience
  translation, both consolidated. Filter facts on `unitRef` → `<unit>` → `iso4217:*`.
- Only `iso4217:` measures are currencies. Filers define units like `mufg:security` and
  `baba:Segment`; splitting a measure on `:` without that guard invents a currency.

See [[golden-test-is-contested]] for the fixture that must never be re-baselined when
verifying parser changes.
