"""One company at a time, the way the workbook shows it.

    export OP_SERVICE_ACCOUNT_TOKEN="$(grep -E '^OP_SERVICE_ACCOUNT_TOKEN=' ../../.env | cut -d= -f2-)"
    export RK_DB_URL="$(op read 'op://Server/Supabase Rechenknecht DB/connection_string')"
    .venv/bin/streamlit run dashboard.py

Master_Vorlage's summary block (BC6, BF6, BG6, BH6) is current price, RoI, 7-year KGV and
7-year KBGV — so that is the page, plus the ratio sheet those numbers came from.

NO RATIO IS COMPUTED HERE. Every valuation number comes out of src/metrics.py valuation(),
the same function the ingest calls, and the inputs come out of v_valuation, which contains no
formula at all. A `price / eps` in this file would be the third copy of a formula that has
already drifted once, and the one copy furthest from the Excel parity test that would catch
it.

Sparse data is the normal case, not an error: a bank reports a quarter of the concepts an
industrial does, a delisted ticker has no quote, and a non-filer has no filing-derived sheet
at all. Every block below renders what it has and says so when it has nothing.
"""

import calendar
import pathlib

import pandas as pd
import streamlit as st

from src import db, metrics
# query/fx_rate/numeric/fmt moved to src/ui.py when pages/Screener.py appeared and needed
# exactly the same four. Imported as names rather than as `ui.` so the call sites below read
# the way they always did.
from src.ui import fmt, fx_rate, numeric, query

st.set_page_config(page_title="Rechenknecht", layout="wide")

TICKER_MAP = pathlib.Path(__file__).parent / "maps" / "ticker-cik_map.txt"


@st.cache_data
def ticker_universe() -> pd.DataFrame:
    """Every ticker SEC EDGAR knows about, not just the ones we ingested — the same file
    ingest.py reads, so a ticker found here is guaranteed ingestable by `--tickers`. No
    network call: it is a 12k-row file already on disk."""
    return pd.read_csv(TICKER_MAP, sep="\t", header=None, names=["ticker", "cik"])


def save_watchlist(cik: str, watch_key: str, target_key: str, note_key: str) -> None:
    with db.connect() as connection:
        if st.session_state[watch_key]:
            connection.execute(
                """
                insert into watchlist (cik, target_price, note)
                values (%s, %s, %s)
                on conflict (cik) do update
                    set target_price = excluded.target_price,
                        note         = excluded.note
                """,
                (cik, st.session_state.get(target_key), st.session_state.get(note_key) or None),
            )
        else:
            connection.execute("delete from watchlist where cik = %s", (cik,))
        connection.commit()
    # The watchlist read is cached like every other query, so it has to be invalidated here
    # or the toggle would snap back on the rerun that follows this callback.
    query.clear()


companies = query("select ticker from company order by ticker")
if companies.empty:
    st.error("No companies in the database. Run `python ingest.py --apply-schema` first.")
    st.stop()

tickers = companies["ticker"].tolist()
watched = query(
    "select c.ticker from watchlist w join company c on c.cik = w.cik order by w.added_at limit 1"
)
# The watchlist is the reason to open this page at all, so it wins over the msft default.
preferred = watched["ticker"].iloc[0] if not watched.empty else "msft"
if preferred not in tickers:
    preferred = tickers[0]

# A selectbox rather than a free text field: only ingested companies have anything to show,
# and typing "MSFT " into a text input would render an empty page that looks like a bug.
ticker = st.sidebar.selectbox("Ticker", tickers, index=tickers.index(preferred))

with st.sidebar.expander("Find more companies"):
    st.caption(f"Searches all {len(ticker_universe())} SEC-listed tickers, not just the {len(tickers)} ingested here.")
    needle = st.text_input("Ticker contains", placeholder="e.g. amzn").strip().lower()
    if needle:
        universe = ticker_universe()
        hits = universe[universe["ticker"].str.contains(needle, regex=False)].head(25)
        if hits.empty:
            st.caption("No match.")
        else:
            for _, hit in hits.iterrows():
                status = "already ingested" if hit["ticker"] in tickers else "not ingested yet"
                st.caption(f"**{hit['ticker']}** (CIK {hit['cik']}) — {status}")
            st.caption("Add one: `python ingest.py --tickers TICKER`")

valuation_inputs = query("select * from v_valuation where ticker = %s", (ticker,))
company = valuation_inputs.iloc[0]

st.title(company["name"] or ticker.upper())
st.caption(company["sic_description"] or "sector unknown")

header = st.columns(4)
header[0].metric("Price", fmt(company["price"]))
header[1].metric("Currency", company["currency"] or "--")
header[2].metric("Price as of", str(company["price_as_of"] or "--"))
header[3].metric(
    "Fiscal year ends",
    calendar.month_name[int(company["fiscal_year_end_month"])]
    if pd.notna(company["fiscal_year_end_month"])
    else "--",
)

if pd.isna(company["price"]):
    st.warning(
        "No quote stored for this ticker — the valuation ratios below need a price and stay "
        "empty. A delisted company (fl was acquired by DKS) never gets one again."
    )

st.subheader("Valuation")

# The filing reports in one currency and the share trades in another for every foreign
# private issuer (Alibaba CNY, MUFG JPY, HDFC INR), so the earnings and book values are moved
# into the price's currency BEFORE any ratio is computed — a USD price over a CNY EPS is a
# number that looks like a P/E and is meaningless. Equal or unknown currencies give a hard
# 1.0 and nothing changes.
rate = fx_rate(company["reporting_currency"], company["currency"])
avg_eps, book_value_per_share, conservative_book_value_per_share = metrics.in_quote_currency(
    rate,
    company["avg_eps"],
    company["book_value_per_share"],
    company["conservative_book_value_per_share"],
)

ratios = metrics.valuation(
    company["price"], avg_eps, book_value_per_share, conservative_book_value_per_share
)

if pd.isna(rate):
    # Not a caption: everything below is empty and the reason is not the company.
    st.warning(
        f"No {company['reporting_currency']}->{company['currency']} exchange rate could be "
        "fetched or found on file, so the ratios below stay empty rather than dividing two "
        "different currencies into each other."
    )
elif rate != 1.0:
    st.caption(
        f"Reported in **{company['reporting_currency']}**, traded in "
        f"**{company['currency']}**: average EPS and book value are converted at "
        f"{fmt(rate, decimals=5)} {company['currency']}/{company['reporting_currency']} "
        "(ECB reference rate via Frankfurter) before any ratio below is computed."
    )

window = company["avg_window_years"]
book_year = company["book_value_fiscal_year"]

# The CONVERTED inputs, not the stored ones: these are the two numbers the ratios beside them
# were divided from, so showing the filing's own figures here would make a hand-check of
# price / EPS disagree with the KGV shown right underneath it.
inputs = st.columns(2)
inputs[0].metric(
    f"Avg EPS ({int(window)}y)" if pd.notna(window) else "Avg EPS", fmt(avg_eps)
)
inputs[1].metric(
    f"Book value/share ({int(book_year)})" if pd.notna(book_year) else "Book value/share",
    fmt(book_value_per_share),
)

figures = st.columns(4)
figures[0].metric("KGV", fmt(ratios[metrics.KGV]))
figures[1].metric("KBGV", fmt(ratios[metrics.KBGV]))
figures[2].metric("RoI", fmt(ratios[metrics.RO_I], " %"))
figures[3].metric("Graham number", fmt(ratios[metrics.GRAHAM_NUMBER]))

margin = ratios[metrics.GRAHAM_MARGIN]
if pd.isna(margin):
    st.caption("Graham number vs. price: not computable without a price and positive inputs.")
else:
    # Positive margin = the Graham number sits above the price, i.e. a discount to it.
    st.caption(
        f"Graham number vs. price: **{fmt(margin, ' %')}** — "
        f"{'discount' if margin > 0 else 'premium'} to the Graham number."
    )

st.caption(
    "KGV, KBGV, RoI and the Graham number are computed by src/metrics.py valuation(); "
    "v_valuation supplies the inputs only. The Graham number is ours, not the workbook's."
)

st.subheader("Ratio sheet")

sheet = query(
    "select * from v_company_year where ticker = %s order by fiscal_year desc", (ticker,)
)
if sheet.empty:
    st.info(
        "No filing-derived ratios for this ticker. v_company_year reads EDGAR facts only, so "
        "a company we could not parse a 10-K for (a non-filer, or an ADR) has no sheet."
    )
else:
    sheet = sheet.drop(columns=["ticker", "name", "sic_description"]).set_index("fiscal_year")
    # Years as columns, newest first: the `order by` above put the rows in that order and the
    # transpose carries it over.
    sheet = numeric(sheet).T
    sheet.columns = [str(year) for year in sheet.columns]
    st.dataframe(sheet.style.format("{:,.2f}", na_rep="--"), width="stretch")

st.sidebar.subheader("Watchlist")

entry = query("select target_price, note from watchlist where cik = %s", (company["cik"],))
watch_key, target_key, note_key = (f"{key}_{ticker}" for key in ("watch", "target", "note"))
callback = dict(on_change=save_watchlist, args=(company["cik"], watch_key, target_key, note_key))

st.sidebar.toggle("Watch this company", value=not entry.empty, key=watch_key, **callback)
st.sidebar.number_input(
    "Target price",
    # None, not 0.0: an empty field means "no target", and a target of zero is a real
    # statement that happens to be a bad one.
    value=None if entry.empty or pd.isna(entry["target_price"].iloc[0])
    else float(entry["target_price"].iloc[0]),
    min_value=0.0,
    placeholder="optional",
    key=target_key,
    **callback,
)
st.sidebar.text_input(
    "Note",
    value="" if entry.empty else (entry["note"].iloc[0] or ""),
    placeholder="optional",
    key=note_key,
    **callback,
)
