"""All companies at once, so the interesting ones can be found instead of visited.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    .venv/bin/streamlit run dashboard.py     # this page is in the sidebar nav

dashboard.py is one company the way the workbook shows it; this is every company the way a
screener shows it. A separate page rather than a mode of that one: nothing is shared but the
database and the helpers in src/ui.py, and the single-company page is already 260 lines.

The frame itself comes from ui.valuation_frame(), which pages/Opportunities.py loads too —
this page ranks those companies one by one, that one groups the very same rows by industry,
and two copies of the query is how the two pages would come to disagree about which
companies exist at all.

NO RATIO IS COMPUTED HERE, exactly as in dashboard.py — KGV, RoI and the Graham margin come
out of src/metrics.py valuation() per row, from the inputs v_valuation exposes. The quality
metrics (RoA, EBIT margin, equity ratio) are read from metric_average, where the ingest
stored them; they are NOT re-derived from the raw rows there, whose EPS/book-value are
unconverted while the KGV/RoI rows beside them are already FX-converted (see the schema
comment on metric_average).

What this page must never do is show a number it knows to be wrong — the whole point of a
ranking is that the order can be trusted. Two guards, both visible in the UI: the four
ADS-ratio-mismatched companies have their ratio cells blanked (src/screener.py
ADS_RATIO_MISMATCH), and a company whose currency pair has no exchange rate gets empty ratios
rather than a mixed-currency one.
"""

import pandas as pd
import streamlit as st

from src import metrics, screener, ui

st.set_page_config(page_title="Screener", layout="wide")

# The metrics you can filter and sort on, with the label they carry in the table.
METRICS = {
    metrics.GRAHAM_MARGIN: "Graham margin %",
    metrics.KGV: "KGV",
    metrics.RO_I: "RoI %",
    "roa": "RoA %",
    "ebit_margin": "EBIT margin %",
    "equity_ratio": "Equity ratio %",
}

# Everything the table shows, in order. The signal first — it is the column you scan, the
# rest is what you read once it has caught your eye — then the identity columns, then the
# numbers, which are unchanged and still the sortable, filterable truth behind it.
COLUMNS = {
    screener.SIGNAL: "Signal",
    "ticker": "Ticker",
    "name": "Name",
    "sic_description": "Industry",
    "sic_code": "SIC",
    "price": "Price",
    "currency": "Cur",
    "price_as_of": "Price as of",
    **METRICS,
}

frame = ui.valuation_frame()
if frame.empty:
    st.error("No company has EDGAR facts yet. Run `python ingest.py --limit 20` first.")
    st.stop()

st.title("Screener")
st.caption(
    f"{len(frame)} companies with EDGAR data. KGV, RoI and the Graham margin are computed per "
    "row by src/metrics.py valuation(); RoA, EBIT margin and the equity ratio are the stored "
    "7-year averages."
)

unclassified = int(frame["sic_code"].isna().sum())

st.sidebar.subheader("Industry")
tech_only = st.sidebar.toggle(
    "Tech only (computers, electronics, software)",
    value=False,
    help="SIC 3570-3579, 3600-3699 and 7370-7379. SIC classifies by what a filer sells, so "
         "this misses companies filed as retail (amzn, 5961) or business services (baba, 7389).",
)
financials = st.sidebar.toggle(
    "Include banks, insurance & real estate",
    value=False,
    disabled=tech_only,
    help="SIC 6000-6799 is hidden by default: deposit and lending flows, a different "
         "net-income concept and inconsistent cash-flow presentation make these ratios not "
         "comparable to an industrial or tech company's.",
)
uncategorized = st.sidebar.toggle(
    f"Include uncategorized ({unclassified} without a SIC code)",
    value=True,
    help="A company ingested before company.sic_code existed. `python ingest.py --sic-only` "
         "backfills it from the SEC submissions API without re-downloading a filing.",
)

st.sidebar.subheader("Ranges")
st.sidebar.caption(
    "Empty means no bound. A company with no value for a metric you bound drops out — "
    "\"we do not know\" is not \"it qualifies\"."
)
bounds = {}
for column, label in METRICS.items():
    low, high = st.sidebar.columns(2)
    bounds[column] = (
        low.number_input(f"min {label}", value=None, placeholder="any", key=f"min_{column}"),
        high.number_input(f"max {label}", value=None, placeholder="any", key=f"max_{column}"),
    )

st.sidebar.subheader("Sort")
# Graham margin descending: the tool's own framing is the Graham number, and a positive
# margin means the share trades below it. Nulls go last in every direction — a company with
# no value has not earned the top of a "find the good ones" list.
sort_label = st.sidebar.selectbox("By", list(METRICS.values()), index=0)
descending = st.sidebar.toggle("Descending", value=True)
sort_column = {label: column for column, label in METRICS.items()}[sort_label]

visible = frame[
    screener.industry_mask(frame, financials, tech_only, uncategorized)
    & screener.in_bounds(frame, bounds)
].sort_values(sort_column, ascending=not descending, na_position="last")

# AFTER the filters, on purpose: the cheapness half of the signal is a quartile of the rows
# on screen, so with the tech filter on "cheap" means cheap among tech companies and not
# cheap against a database of banks that were just excluded. That also makes it the one
# number on this page that changes when you change a filter, which is why the cutoffs are
# printed below rather than left implicit.
visible = visible.assign(**{screener.SIGNAL: screener.signal(visible)})
dear, cheap = screener.margin_quartiles(visible)

st.caption(
    f"**{len(visible)}** of {len(frame)} companies match. "
    f"Sorted by {sort_label}, {'highest' if descending else 'lowest'} first, no value last."
)

# The rule, in the UI rather than only in the brief that asked for it: a score whose
# derivation is invisible is a black box, and this one is meant to be argued with.
signal_rule = (
    f"**{screener.CHEAP_AND_STRONG} cheap and strong · {screener.MIXED} mixed · "
    f"{screener.RICH_AND_WEAK} expensive and weak · {screener.UNKNOWN} not enough data.**  \n"
    f"*Cheap* is the top quarter of the Graham margin **of these {len(visible)} rows** "
    f"(≥ {ui.fmt(cheap, '%')} right now), *expensive* the bottom quarter "
    f"(≤ {ui.fmt(dear, '%')}) — recomputed on every filter change, never a fixed cutoff. "
    f"*Strong* counts how many of RoI > {screener.QUALITY_THRESHOLDS[metrics.RO_I]}, "
    f"RoA > {screener.QUALITY_THRESHOLDS['roa']}, "
    f"EBIT margin > {screener.QUALITY_THRESHOLDS['ebit_margin']} and equity ratio > "
    f"{screener.QUALITY_THRESHOLDS['equity_ratio']} hold (the same four "
    "src/RechenknechtBeta.py has always called a candidate); a metric we do not have counts "
    f"as not met. {screener.CHEAP_AND_STRONG} needs the top quarter **and** "
    f"{screener.STRONG_AT_LEAST} of {len(screener.QUALITY_THRESHOLDS)}, "
    f"{screener.RICH_AND_WEAK} the bottom quarter **and** at most "
    f"{screener.WEAK_AT_MOST} of {len(screener.QUALITY_THRESHOLDS)}, "
    f"{screener.MIXED} is everything in between. {screener.UNKNOWN} means no Graham margin "
    "at all (the ADS companies below, or no average EPS) or no quality metric at all — an "
    f"absence of data, which is not the same statement as {screener.MIXED}."
)
st.caption(signal_rule)

blanked = sorted(screener.ADS_RATIO_MISMATCH & set(visible["ticker"]))
if blanked:
    st.info(
        f"KGV, RoI and the Graham margin are deliberately empty for **{', '.join(blanked)}**: "
        "one ADS is not one ordinary share (baba: 8), so the filing's per-share figures and "
        "the ADS price are per different things. The currency is already handled — this ratio "
        "is not, yet, so no number is shown instead of one that is ~8x wrong. The quality "
        "metrics beside it are per-company and unaffected."
    )

table = visible[list(COLUMNS)].rename(columns=COLUMNS)
st.dataframe(
    table,
    # column_config rather than a pandas Styler: the Styler's display values are strings, and
    # a string column cannot be sorted by clicking its header — which is the one interaction a
    # screener needs. The cells stay real numbers, and a missing one renders as an EMPTY cell
    # (Streamlit's own "no value"), not as the "None" the Styler path leaked into the grid.
    column_config={
        # A TextColumn, so the glyph is a value in a sortable cell like any other. The four
        # code points run ⚪ < 🔴 < 🟡 < 🟢, so clicking this header descending groups the
        # candidates at the top and the rows we cannot judge at the bottom.
        "Signal": st.column_config.TextColumn(width="small", help=signal_rule),
        "SIC": st.column_config.NumberColumn(format="%d", help="Standard Industrial Classification"),
        "Price": st.column_config.NumberColumn(format="%.2f"),
        **{label: st.column_config.NumberColumn(format="%.2f") for label in METRICS.values()},
    },
    width="stretch",
    # st.dataframe defaults to ~10 visible rows, which for a 138-row ranked list means
    # scrolling a viewport inside a viewport to see the tenth-best company.
    height=680,
    hide_index=True,
)

if pd.isna(frame[list(METRICS)]).all(axis=None):
    st.warning("Every metric is empty — that usually means metric_average is not populated.")
