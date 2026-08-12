"""One layer above the screener: which INDUSTRIES are cheap and strong, and which individual
companies the signal has already picked out.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    .venv/bin/streamlit run dashboard.py     # this page is in the sidebar nav

pages/Screener.py ranks companies one at a time; this groups the very same rows. Both load
ui.valuation_frame(), so the two pages cannot come to disagree about which companies exist or
what their ratios are.

NO RATIO IS COMPUTED HERE and none is aggregated in SQL. KGV, RoI and the Graham margin come
out of src/metrics.py valuation() per row (via screener.add_valuation), and the grouping is
pandas over that result — a `select avg(price/eps) ... group by sic_description` would be a
second, unpinned definition of the same arithmetic, which is the drift src/metrics.py exists
to prevent. The read is one query and touches nothing.

Two tabs rather than two pages, because both must be scored against the SAME peer group: the
cheap half of the signal is a quartile of the rows on screen, so the industry table's green
counts and the candidate list below it are only consistent while one frame feeds both.
"""

import altair as alt
import pandas as pd
import streamlit as st

from src import metrics, screener, ui

st.set_page_config(page_title="Opportunities", layout="wide")

# What the industry table shows, in reading order: how big the group is, how cheap, how
# strong, then the four-state mix. Every one of them is a column you can click to sort.
ROLLUP_COLUMNS = {
    screener.INDUSTRY: "Industry",
    screener.COMPANIES: "Companies",
    screener.RATED: "…with a margin",
    metrics.GRAHAM_MARGIN: "Median Graham margin %",
    screener.QUALITY_SCORE: "Median quality 0-4",
    screener.CHEAP_AND_STRONG: "🟢",
    screener.MIXED: "🟡",
    screener.RICH_AND_WEAK: "🔴",
    screener.UNKNOWN: "⚪",
}

# The candidate table. The two ranking columns come first after the identity ones — they are
# the answer to "why is this one above that one" — then the ratios that back them up.
CANDIDATE_COLUMNS = {
    "ticker": "Ticker",
    "name": "Name",
    screener.INDUSTRY: "Industry",
    screener.QUALITY_SCORE: "Quality 0-4",
    screener.MARGIN_ABOVE_CUT: "% above cheap cutoff",
    metrics.GRAHAM_MARGIN: "Graham margin %",
    metrics.KGV: "KGV",
    metrics.RO_I: "RoI %",
    "roa": "RoA %",
    "ebit_margin": "EBIT margin %",
    "equity_ratio": "Equity ratio %",
    "price": "Price",
    "currency": "Cur",
}

# The deep-dive's charts, in the order they are drawn. Nothing here decides anything: the
# glyph colours are the ones the four states already read as, so a point that is green in the
# scatter is the same 🟢 the table above it counted.
SIGNAL_COLORS = {
    screener.CHEAP_AND_STRONG: "#2ca02c",
    screener.MIXED: "#e0a800",
    screener.RICH_AND_WEAK: "#d62728",
    screener.UNKNOWN: "#9e9e9e",
}

# What the fundamentals trend is drawn from — both are STORED fact concepts, written by the
# ingest through src/metrics.py. Named from metrics so the page cannot spell a concept the
# database does not have.
TREND_CONCEPTS = (metrics.EBIT_MARGIN, metrics.REVENUE)

# How many companies the live price fetch is allowed to ask Yahoo for. The candidates are
# already ranked (quality, then distance above the cheap cutoff), so a cap takes the
# strongest ones rather than an arbitrary slice. Today's data has at most 6 green companies
# in an industry — this is the guard for the day that changes, not a limit anyone hits now.
PRICE_TICKER_CAP = 8

# The Graham margin runs from -100% to 2.4e8% in this data, so a linear axis would put every
# company of an industry on top of each other next to one artefact. symlog keeps the sign and
# the ordering — the only two things the quadrant reading needs — and stays readable.
MARGIN_SCALE = alt.Scale(type="symlog", zero=False)

def quadrant(rows: pd.DataFrame, cheap_cutoff: float) -> alt.LayerChart:
    """Every company of one industry as a point: how cheap across, how strong up.

    The two dashed rules are not new thresholds — they are signal()'s own two halves drawn:
    the vertical one is the cheapness quartile of the whole screen, the horizontal one is
    STRONG_AT_LEAST. A point up and to the right of both is exactly a 🟢, which is what makes
    this a quadrant rather than a scatter.
    """
    points = alt.Chart(rows).mark_circle(size=150, opacity=0.8).encode(
        x=alt.X(f"{metrics.GRAHAM_MARGIN}:Q", title="Graham margin % (symlog)", scale=MARGIN_SCALE),
        y=alt.Y(f"{screener.QUALITY_SCORE}:Q", title="Quality 0-4",
                scale=alt.Scale(domain=[-0.3, 4.3]), axis=alt.Axis(values=[0, 1, 2, 3, 4])),
        color=alt.Color(f"{screener.SIGNAL}:N", title="Signal", scale=alt.Scale(
            domain=list(SIGNAL_COLORS), range=list(SIGNAL_COLORS.values()))),
        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("name:N", title="Name"),
            alt.Tooltip(f"{screener.QUALITY_SCORE}:Q", title="Quality 0-4", format=".0f"),
            alt.Tooltip(f"{metrics.GRAHAM_MARGIN}:Q", title="Graham margin %", format=",.1f"),
            # The four the quality score is counted from, so a point's height can be argued
            # with on the spot instead of being taken on trust.
            alt.Tooltip(f"{metrics.RO_I}:Q", title=f"RoI % (>{screener.QUALITY_THRESHOLDS[metrics.RO_I]})", format=",.2f"),
            alt.Tooltip("roa:Q", title=f"RoA % (>{screener.QUALITY_THRESHOLDS['roa']})", format=",.2f"),
            alt.Tooltip("ebit_margin:Q", title=f"EBIT margin % (>{screener.QUALITY_THRESHOLDS['ebit_margin']})", format=",.2f"),
            alt.Tooltip("equity_ratio:Q", title=f"Equity ratio % (>{screener.QUALITY_THRESHOLDS['equity_ratio']})", format=",.2f"),
            alt.Tooltip(f"{metrics.KGV}:Q", title="KGV", format=",.2f"),
        ],
    )
    cheap_rule = alt.Chart(pd.DataFrame({"cut": [cheap_cutoff]})).mark_rule(
        strokeDash=[6, 4], color="#2ca02c").encode(x=alt.X("cut:Q", scale=MARGIN_SCALE))
    strong_rule = alt.Chart(pd.DataFrame({"cut": [screener.STRONG_AT_LEAST - 0.5]})).mark_rule(
        strokeDash=[6, 4], color="#2ca02c").encode(y="cut:Q")
    return (points + cheap_rule + strong_rule).properties(height=420)


def trend_panel(data: pd.DataFrame, title: str, y_title: str, value_format: str) -> alt.Chart:
    """One line of the small-multiples stack: a value per fiscal year, with its sample size.

    Small multiples rather than one dual-axis chart, deliberately. A percentage and a revenue
    in billions share no axis, and two y-axes on one frame is the standard way to make two
    unrelated series look like they move together — the exact reading this page must not
    put in front of anybody.

    The x is ordinal so the panels line up column by column, which is the only thing that
    makes a stack of them comparable at a glance.
    """
    tooltip = [
        alt.Tooltip(f"{screener.FISCAL_YEAR}:O", title="Fiscal year"),
        alt.Tooltip(f"{screener.VALUE}:Q", title=y_title, format=value_format),
    ]
    # Never hidden where it exists: the median moves when the SAMPLE moves, and this is the
    # only thing on the chart that says which of the two just happened. The macro panel has
    # no such count — it is one published series, not an aggregate over companies.
    if screener.COMPANIES in data.columns:
        tooltip.append(
            alt.Tooltip(f"{screener.COMPANIES}:Q", title="Companies in the median", format=".0f")
        )

    return alt.Chart(data).mark_line(point=True).encode(
        x=alt.X(f"{screener.FISCAL_YEAR}:O", title="Fiscal year"),
        y=alt.Y(f"{screener.VALUE}:Q", title=y_title, scale=alt.Scale(zero=False)),
        tooltip=tooltip,
    ).properties(title=title, height=200)


def price_lines(prices: pd.DataFrame) -> alt.Chart:
    """One line per ticker of recent closes.

    LOG y, because the point is the SHAPE of each line next to the others and the candidates
    of one industry sit two orders of magnitude apart (ctrm at $2.06 against dac at $135.02,
    measured 2026-08-12). On a linear axis the cheap tickers are a flat line on the floor.
    Still the closing price itself — nothing is rebased to 100, which would be a computed
    number on a page that computes none.
    """
    return alt.Chart(prices[prices["close"] > 0]).mark_line().encode(
        x=alt.X("date:T", title=None),
        y=alt.Y("close:Q", title="Close (log scale)", scale=alt.Scale(type="log", zero=False)),
        color=alt.Color("ticker:N", title="Ticker"),
        tooltip=[
            alt.Tooltip("ticker:N", title="Ticker"),
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("close:Q", title="Close", format=",.2f"),
        ],
    ).properties(height=360)


frame = ui.valuation_frame()
if frame.empty:
    st.error("No company has EDGAR facts yet. Run `python ingest.py --limit 20` first.")
    st.stop()

st.title("Opportunities")

# The same three switches pages/Screener.py has, and for the same reasons — a rollup that
# silently included banks would be comparing deposit-and-lending accounting against
# industrial accounting in one median.
st.sidebar.subheader("Industry")
tech_only = st.sidebar.toggle("Tech only (computers, electronics, software)", value=False)
financials = st.sidebar.toggle(
    "Include banks, insurance & real estate", value=False, disabled=tech_only,
    help="SIC 6000-6799 is hidden by default: a different net-income concept and "
         "inconsistent cash-flow presentation make these ratios not comparable.",
)
uncategorized = st.sidebar.toggle(
    f"Include uncategorized ({int(frame['sic_code'].isna().sum())} without a SIC code)",
    value=True,
)

visible = frame[screener.industry_mask(frame, financials, tech_only, uncategorized)]

# ONCE, over the whole filtered frame, and handed to both tabs. Computing it per tab — or
# worse, per industry — would score each group against itself, and every industry would come
# out with the same quarter of its rows called cheap.
signals = screener.signal(visible)
dear, cheap = screener.margin_quartiles(visible)

# ONCE, for the same reason: the candidates tab lists these and the deep-dive picks its price
# fetch out of them. Two calls would be two rankings that could disagree after a filter change.
picks = screener.candidates(visible, signals)

st.caption(
    f"**{len(visible)}** of {len(frame)} companies with EDGAR data, grouped into "
    f"**{screener.industry_label(visible).nunique()}** "
    f"industries. *Cheap* is the top quarter of the Graham margin of these rows "
    f"(≥ {ui.fmt(cheap, '%')}), *strong* is at least {screener.STRONG_AT_LEAST} of the four "
    f"quality thresholds — the same rule the Screener page paints its glyphs with, "
    "recomputed on every filter change."
)

industries, outliers = st.tabs(["By industry", f"{screener.CHEAP_AND_STRONG} Candidates"])

with industries:
    # On the RATED count, not the group size — that is the whole point. A median stands only
    # on the companies that have a Graham margin, and only 1,833 of 5,046 do; filtering on
    # the group size would still let an industry of nine with one rated company sort to the
    # top of "cheapest". Measured 2026-08-12: every one of the top five industries by median
    # margin had 4 or fewer rated companies until this slider was raised.
    minimum = st.slider("Minimum companies WITH a Graham margin", 1, 25, 5)

    table = screener.rollup(visible, signals)
    table = table[table[screener.RATED] >= minimum]

    st.caption(
        f"**{len(table)}** industries where at least {minimum} companies have a Graham margin "
        f"({int(table[screener.COMPANIES].sum())} companies in them, "
        f"{int(table[screener.RATED].sum())} of them rated). Click a column header to sort — "
        "median Graham margin for the cheapest, 🟢 for the most candidates. "
        "**Click a row to open the deep-dive charts underneath.**"
    )
    # on_select makes the table the deep-dive's own control, rather than a second dropdown
    # listing the same industries next to it — which could then be set to an industry the
    # slider above has already filtered out of the table.
    event = st.dataframe(
        table[list(ROLLUP_COLUMNS)].rename(columns=ROLLUP_COLUMNS),
        column_config={
            "Median Graham margin %": st.column_config.NumberColumn(format="%.1f"),
            "Median quality 0-4": st.column_config.NumberColumn(format="%.1f"),
        },
        width="stretch", height=680, hide_index=True,
        on_select="rerun", selection_mode="single-row",
        # An explicit key, because without one Streamlit derives the widget's identity from
        # its own arguments — and the arguments here change whenever the slider above moves
        # the row count, which would drop the user's selection on a filter change.
        key="industry_table",
    )
    st.caption(
        "Median, never mean: a company with a NEGATIVE book value per share yields a Graham "
        "number out of the square root of two negatives, and margins up to 2.4e8% are really "
        "in this data — one such row would decide an industry's mean on its own. "
        "**\"…with a margin\" is the sample size the median stands on**, and it is usually far "
        "below the company count: 1,833 of 5,046 companies have a Graham margin at all, so an "
        "industry can show nine companies and a median computed over one of them."
    )

    # ── one industry, one level deeper ───────────────────────────────────────────────────
    #
    # Four views over the SAME rows the table above counted — no second query for the
    # companies, no second ratio, no verdict. The two things that do leave this frame are a
    # narrow read of already-stored EDGAR facts and a live Yahoo price fetch, and neither of
    # them is written anywhere.
    if event.selection.rows:
        selected = table.iloc[event.selection.rows[0]][screener.INDUSTRY]
        rows = visible[screener.industry_label(visible) == selected]
        # quality_score is per row, so the subset is safe; the SIGNAL is not — it is taken
        # from the series computed over the whole screen, because recomputing it here would
        # score this industry against itself and paint a quarter of any industry green.
        rows = rows.assign(**{
            screener.QUALITY_SCORE: screener.quality_score(rows),
            screener.SIGNAL: signals[rows.index],
        })

        st.divider()
        st.subheader(selected)
        st.caption(
            f"**{len(rows)}** companies, "
            f"{int((rows[screener.SIGNAL] == screener.CHEAP_AND_STRONG).sum())} of them "
            f"{screener.CHEAP_AND_STRONG}. Everything below is this industry's slice of the "
            "frame above — the charts show what the data says, the reading is yours."
        )

        st.markdown("**Value against quality** — cheap to the right, strong at the top")
        st.altair_chart(quadrant(rows, cheap), width="stretch")
        st.caption(
            f"The dashed lines are the signal's own cutoffs: cheapest quarter of the whole "
            f"screen (≥ {ui.fmt(cheap, '%')}) and at least {screener.STRONG_AT_LEAST} of the "
            "four quality thresholds. Top right of both is a 🟢. Hover a point for the four "
            "metrics its height is counted from. The x-axis is symlog — the Graham margin "
            "reaches 2.4e8% in this data, and one such company would flatten every other "
            "point in its industry onto a single line."
            # The same sample-size honesty the rollup's RATED column enforces: a company
            # with no Graham margin has no x position, so it is silently absent from the
            # scatter while still being counted in the line above it.
            + (f" **{int(rows[metrics.GRAHAM_MARGIN].isna().sum())} of the {len(rows)} "
               "companies have no Graham margin** and cannot be placed on this chart at all "
               f"— they are the {screener.UNKNOWN} rows."
               if rows[metrics.GRAHAM_MARGIN].isna().any() else "")
        )

        history = ui.fact_history(tuple(rows["ticker"].dropna()), TREND_CONCEPTS)
        trend = screener.median_by_year(history)
        if trend.empty:
            st.info("No stored EDGAR EBIT-margin or revenue facts for these companies.")
        else:
            years = sorted(trend[screener.FISCAL_YEAR].unique())
            revenue = trend[trend[screener.CONCEPT] == metrics.REVENUE].assign(
                # A display unit, not a computed figure: revenue is stored in units and an
                # axis labelled 2,304,455,000 is unreadable.
                **{screener.VALUE: lambda f: f[screener.VALUE] / 1e9}
            )
            panels = [
                trend_panel(trend[trend[screener.CONCEPT] == metrics.EBIT_MARGIN],
                            "Median EBIT margin %", "EBIT margin %", ",.2f"),
                trend_panel(revenue, "Median revenue, bn as filed", "Revenue bn", ",.2f"),
            ]

            # The overlay, and ONLY where a series is actually mapped to this industry's SIC
            # code. An unmapped industry gets the two panels above and nothing invented.
            codes = rows["sic_code"].dropna()
            series_id = screener.macro_series_for(codes.iloc[0]) if not codes.empty else None
            if series_id:
                macro = screener.macro_mean_by_year(
                    ui.macro_history(series_id, int(min(years))), series_id
                )
                # Clipped to the fiscal years the fundamentals actually have, so the panels
                # share one x-axis and the macro line cannot run past the last filing.
                macro = macro[macro[screener.FISCAL_YEAR].isin(years)]
                if not macro.empty:
                    panels.append(trend_panel(macro, f"{series_id}, yearly mean", series_id, ",.2f"))

            st.markdown("**How the industry itself has been doing**")
            st.altair_chart(alt.vconcat(*panels).resolve_scale(x="shared"), width="stretch")
            st.caption(
                "Median across the companies of this industry per fiscal year, out of the "
                "**stored** EDGAR facts — EBIT-margin is a concept the ingest wrote, not "
                "something derived here. **Hover for the number of companies behind each "
                "point**: EDGAR coverage starts where the ingest's filing window starts, so "
                "an early year can be a median over two companies and a rising line can be "
                "the sample changing rather than the industry. Revenue is the median company's "
                "revenue **as filed**, in that company's own reporting currency and not "
                "FX-converted — an industry whose filers report in different currencies mixes "
                "units on that panel."
                + (f" {series_id} is overlaid because this industry's SIC code is mapped to "
                   "it — a yearly mean of the daily series, aligned on the calendar year "
                   "against a fiscal-year label. Two panels that move together are two "
                   "panels that move together; nothing here computes a correlation."
                   if series_id else
                   " No macro series is mapped to this industry's SIC code, so none is shown.")
            )

        st.markdown(f"**Recent price action** — the {screener.CHEAP_AND_STRONG} companies only")
        green = picks[screener.industry_label(picks) == selected]
        if green.empty:
            st.info(f"No {screener.CHEAP_AND_STRONG} company in this industry — no prices to fetch.")
        else:
            wanted = tuple(green["ticker"].dropna().head(PRICE_TICKER_CAP))
            prices = ui.price_frame(wanted)
            if prices.empty:
                st.warning(f"Yahoo returned no price history for any of {', '.join(wanted)}.")
            else:
                st.altair_chart(price_lines(prices), width="stretch")
                missing = [ticker for ticker in wanted if ticker not in set(prices["ticker"])]
                st.caption(
                    f"Two years of daily closes for {prices['ticker'].nunique()} of "
                    f"{len(green)} {screener.CHEAP_AND_STRONG} companies, fetched live from "
                    "Yahoo when this industry was opened and **stored nowhere** — the "
                    "valuation above it is built on filings that are months old, this is what "
                    "the market has done since."
                    + (f" No history for: {', '.join(missing)} — left out rather than "
                       "blanking the chart." if missing else "")
                    + (f" Capped at the {PRICE_TICKER_CAP} strongest candidates."
                       if len(green) > PRICE_TICKER_CAP else "")
                )

with outliers:
    st.caption(
        f"**{len(picks)}** of {len(visible)} companies are "
        f"{screener.CHEAP_AND_STRONG} — in the top quarter by Graham margin "
        f"(≥ {ui.fmt(cheap, '%')}) **and** clearing at least {screener.STRONG_AT_LEAST} of "
        f"RoI > {screener.QUALITY_THRESHOLDS[metrics.RO_I]}, "
        f"RoA > {screener.QUALITY_THRESHOLDS['roa']}, "
        f"EBIT margin > {screener.QUALITY_THRESHOLDS['ebit_margin']}, equity ratio > "
        f"{screener.QUALITY_THRESHOLDS['equity_ratio']}. Ranked by how many of the four it "
        "clears, then by how far above the cheapness cutoff it sits — no new scoring rule, "
        "just the signal's own two halves made continuous."
    )
    st.dataframe(
        picks[list(CANDIDATE_COLUMNS)].rename(columns=CANDIDATE_COLUMNS),
        column_config={
            "Quality 0-4": st.column_config.NumberColumn(format="%d"),
            **{label: st.column_config.NumberColumn(format="%.2f")
               for label in ("% above cheap cutoff", "Graham margin %", "KGV", "RoI %",
                             "RoA %", "EBIT margin %", "Equity ratio %", "Price")},
        },
        width="stretch", height=680, hide_index=True,
    )
    st.warning(
        "**Read the KGV column before trusting the order.** This list is ranked on a 7-year "
        "average EPS, and a handful of rows reach the top on a broken one: an EDGAR filing "
        "that reports share counts in THOUSANDS is stored unscaled, which inflates EPS ~1000x "
        "and collapses the KGV toward zero (nvmi: stored avg EPS 1,131.93 against a $395.95 "
        "price, KGV 0.35). A KGV far below 1 is that bug, not a bargain. Warrant tickers "
        "(a `w` suffix) divide a warrant price by the common share's earnings, the same "
        "mismatch the ADS companies are blanked for."
    )
