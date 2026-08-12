---
name: project-golden-test-is-contested
description: rechenknecht's footlocker_expected.csv has been re-baselined three times and is treated as an adversarial artefact — never regenerate it to make a test pass
metadata:
  type: project
---

`test/resources/footlocker_expected.csv` is the golden expectation for the EDGAR extractor. It has already been **re-baselined three times**, and an adversarial review called it a self-ratifying snapshot: of 35 rows, 4 assert nothing once `7_YEAR_AVG` is dropped (`RoI`, `KBGV`, `KGV`, `KGBV_conservative`), and 3 asserted cells are `0.0` fabricated by `nan_to_zero()` rather than read from a filing.

**Why:** the test writes `footlocker_actual.csv` into the *same* resources folder, so re-baselining costs one `cp` — the artefact can ratify itself. René's standing instruction (2026-08-10) is: **never `cp actual expected` to make a test pass**; if a cell moves, diff cell-by-cell and justify each change.

**How to apply:** on any rechenknecht extractor change, run `.venv/bin/python -m pytest test/ -q` and then `diff test/resources/footlocker_actual.csv test/resources/footlocker_expected.csv`. An empty diff is the goal. If it is non-empty, that is a finding to report, not a file to update. `test/test_excel_parity.py` is the stronger oracle — its numbers come from the Excel workbook, so if it breaks, the change is wrong, not the test.

Related: [[project-secret-rotation-pending]]
