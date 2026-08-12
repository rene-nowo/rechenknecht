# Master_Vorlage as specification

Extracted from `Rechenknecht_V9.0_Roll_Out.xlsx`, sheet `Master_Vorlage`, rows 1–40 and the
summary block in columns BA–BL. `Master_Vorlage` is the empty template that carries the
methodology; computed values for real companies live in the `Rechenknecht` sheet in
repeating 38-row blocks (block 1 is Skyworks Solutions, used as the test oracle).

Excel is **not** part of the production architecture. It is the reference implementation and
the test oracle — see `test/test_excel_parity.py`.

## Units and the Rechenfaktor

The whole sheet is denominated in millions. `C38` ("Rechenfaktor") holds the divisor:
`D3 = (C10 + C11) / C38`, so Skyworks' 150,472,800 outstanding shares become `150.4728`.
Any value fed to or compared against the workbook must be scaled the same way.

## Raw input rows (19–38)

These are entered, never computed. Several change meaning by sector — the label cell itself
is a formula keyed on `B30` (the company's industry).

| Row | Concept | Sector override |
|-----|---------|-----------------|
| 19 | revenue | Bank → "Operative Erträge" (operating income, not sales) |
| 21 | EBIT / operating result | |
| 22 | interest expense | Bank → blank |
| 23 | net income (shareholders' share) | |
| 24 | goodwill | Versicherung → "Net premiums written" |
| 25 | intangible assets | Versicherung → "Total Operating Expenses & Benefits" |
| 26 | accounts receivable | |
| 27 | cash and equivalents | |
| 28 | current assets | Leasing → "Revenue earning assets" |
| 29 | balance sheet total | |
| 30 | current liabilities | Leasing → "Total liabilities" |
| 31 | long-term liabilities | Bank → blank |
| 32 | total liabilities | |
| 33 | shareholders' equity | |
| 34 | operating cash flow | |
| 35 | capital expenditure (**negative**, an outflow) | |
| 36 | dividends paid (total cash) | |
| 37 | diluted shares | |
| 38 | — | Immobilien → "Total FFO"; Versicherung → "Aktienquote" |

## Derived rows

| Row | Metric | Excel formula | Ours |
|-----|--------|---------------|------|
| 3 | EPS at fixed share count | `AD23/D3` | `add_sheet_ratios(current_shares=…)` |
| 4 | book value/share at fixed count | `AD33/D3` | same |
| 6 | dividend per share | `AD36/AD37` | `DIVIDENDS_PER_SHARE` |
| 7 | FCF per share | `(AD34+AD35)/AD37` | `FCF_PER_SHARE` |
| 8 | EPS | `AD23/AD37` | `EPS` |
| 9 | FFO/share (Immobilien), Combined Ratio (Versicherung) | sector-branched | **not implemented** |
| 10 | book value per share | `AD33/AD37` | `BOOK_VALUE_PER_SHARE` |
| 12 | RoA | `AD23/AD29*100` | `RO_A` |
| 13 | EBIT margin | `AD21/AD19*100` | `EBIT_MARGIN` |
| 14 | equity ratio | `AD33/AD29*100` | `EQUITY_RATIO` |
| 15 | leverage selector (see below) | `BB30`-branched | `DEBT_TO_EQUITY`, `DEBT_TO_FCF`, `FCF_PAYOUT_RATIO` |
| 16 | operating cash flow vs net income | `AD34-AD23 > 0` → "Positiv" | `OCF_MINUS_NET_INCOME` (signed) |
| 17 | free cash flow | `AD34+AD35` | `FREE_CASH_FLOW` |
| 18 | working capital | `AD28-AD30` | `NETTOUMLAUFVERM_GEN` |
| 39 | TIER | `AD21/AD22` | `TIER` |

Row 15 is one row with six selectable meanings driven by `BB30`, rendered as strings like
`"0.5 : 1"`. The presentation is dropped; the numbers are kept:
`Dept to Equity` = `AD31/AD33`, `Schulden zu Freie Mittel` and `FCF to Total Dept` =
`AD32/AD17`, `Schulden zu Bank Cash` = `AD32/AD27`, `Verhältnis Dividende zu FC` =
`AD6/AD7`, `Assets zu Luftgeld` = `AD29/AD24`.

## The share-count toggle

`BB26` "Mit aktuellen Aktien rechnen" drives `BB39`, which every per-share row branches on:

- `BB39 = 0` → divide by `AD37`, **that year's diluted shares**
- `BB39 = 1` → divide by `H3` → `D3`, **the current share count for every year**

Both modes ship with the workbook, so this is a parameter, not a decision. The Skyworks
block runs mode 1: every year divides by 150.4728 while the reported diluted count moves
from 194.9m (2015) to 161.5m (2024).

## Summary block (columns BA–BL)

| Cell | Metric | Formula |
|------|--------|---------|
| `BA3` | 7-year average EPS | last filled column, averaged over the seven ending there |
| `BA9` | 7-year average FFO | same, real estate |
| `BA10` | book value per share, **last report** | walks back to the newest non-empty value |
| `BC6` | current price | |
| `BF6` | RoI % | `BA3/BC6*100`, Immobilien → `BA9/BC6*100` |
| `BG6` | 7-year KGV | `BC6/BA3` |
| `BH6` | 7-year KBGV | `BC6/BA10*BG6` |
| `BI6` | purchase price in EUR | nested currency conversion over 18 currencies |
| `BJ6`–`BL6` | the same three ratios at the purchase price | |

**KBGV divides by the LATEST book value, not a seven-year average.** `BA10`'s label at `BB4`
is "Buchwert (letzter Bericht)", and the arithmetic confirms it: Skyworks' KBGV 17.4107 over
KGV 10.5496 gives price/BVPS = 1.6504, i.e. BVPS = 42.1119 — exactly the 2024 book value.

**The averaging window is the most recent seven years.** `BA3` resolves to `AVERAGE(AH3:AN3)`
for data ending 2024, i.e. 2018–2024. A company with more history drops its oldest years.

## Known gaps against the specification

- Row 9: FFO per share (Immobilien) and Combined Ratio (Versicherung) are not implemented.
- Sector branches for Bank, Versicherung and Leasinggesellschaft are not implemented; this
  is why `jpm` yields far fewer facts than an industrial company.
- `BI6`–`BL6`: purchase price, its ratios, and the EUR conversion are not implemented.
- Dividend per share is specified as dividends **paid** / shares (row 6). The EDGAR
  extractor currently reads `CommonStockDividendsPerShareDeclared`, a different quantity,
  which is the source of the systematic ~2.3% gap against Yahoo.
- `conservative_book_value_per_share` is ours, not the workbook's. Row 9's non-real-estate
  branch computes `(AD33-AD24-AD25)/AD38`, which divides by the FFO/Aktienquote row and
  looks dormant rather than intentional.
