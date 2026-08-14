-- Rechenknecht storage.
--
-- Design note: `fact` is long/narrow rather than one column per concept, because the set
-- of concepts changes as tag coverage improves and a wide table needs a migration each
-- time. Long also makes the cross-source comparison a single join instead of 30 hand
-- written column diffs.
--
-- Neither source is privileged in the schema. "Yahoo is ground truth" lives in a view, so
-- it can be changed without re-ingesting: Yahoo serves the latest restated figures while a
-- filing says what it said on the day it was filed, and those legitimately differ.

create table if not exists company (
    cik                   text primary key,
    ticker                text not null unique,
    name                  text,
    sic_description       text,
    fiscal_year_end_month smallint,
    updated_at            timestamptz not null default now()
);

-- The currency the FILINGS report in — which is NOT the currency the share trades in, and
-- the two must never be conflated. A foreign private issuer reports in its functional
-- currency (Alibaba CNY, MUFG JPY, HDFC INR) while its ADR trades on a US exchange in USD.
-- quote.currency is the trading currency and stays that way; putting a reporting currency
-- there would mislabel a genuinely-USD price.
--
-- Same idempotent shape as quote.updated_at below: apply_schema re-runs this whole file, so
-- `create table if not exists` above is a no-op against the table that already exists and
-- only an ALTER reaches it.
alter table company add column if not exists reporting_currency text;

-- The NUMERIC SIC code the SEC submissions API returns alongside sicDescription, e.g. 3571
-- for "Electronic Computers". The free text was already stored; the code was read from the
-- same response and thrown away, which left industry filtering to string matching over 84
-- distinct descriptions ("National Commercial Banks", "Commercial Banks, NEC", "Security
-- Brokers, Dealers & Flotation Companies" are all one thing to a screener and share no
-- substring).
--
-- The codes group into the standard SIC major groups and divisions — 6000-6799 is the whole
-- Finance/Insurance/Real-Estate division, 3570-3579 computers, 3600-3699 electronics,
-- 7370-7379 software — which is what pages/Screener.py filters on. smallint: the range is
-- 0100-9999 and it has to compare as a number, not as text ('700' > '3571' as text).
--
-- NULL until a company is re-ingested, exactly like reporting_currency after it was added:
-- `python ingest.py --sic-only` backfills it from the submissions API alone, without
-- re-downloading a single filing.
alter table company add column if not exists sic_code smallint;

create table if not exists fact (
    cik         text not null references company (cik) on delete cascade,
    source      text not null check (source in ('edgar', 'yahoo')),
    fiscal_year smallint not null,
    concept     text not null,
    value       numeric,
    -- EDGAR only: which filing this number came from, so a disagreement can be traced
    -- back to an exact accession and later pinned against the companyfacts API.
    accession   text,
    ingested_at timestamptz not null default now(),
    primary key (cik, source, fiscal_year, concept)
);

create index if not exists fact_concept_year_idx on fact (concept, fiscal_year);
create index if not exists fact_source_idx on fact (source);

-- A ticker that never loaded is a different failure from one that loaded and disagrees.
-- Without this table the two are indistinguishable in the diff views.
create table if not exists ingest_run (
    id            bigserial primary key,
    ticker        text not null,
    source        text not null,
    status        text not null check (status in ('ok', 'failed')),
    error         text,
    facts_written integer not null default 0,
    started_at    timestamptz not null,
    finished_at   timestamptz not null default now()
);

create index if not exists ingest_run_ticker_idx on ingest_run (ticker, source, finished_at desc);

-- A price is a point in time, not a fiscal-year fact, so it cannot live in `fact` — that
-- table is keyed on a fiscal year and a quote has none. Without this table the valuation
-- ratios were uncomputable from storage: they were derived at ingest time from a price that
-- was then discarded (see src/db.py write_facts).
--
-- `as_of` is a DATE, not a timestamp: one recorded price per company per day makes a second
-- ingest run on the same day an update instead of a second row, which is what makes the
-- table idempotent under re-runs.
create table if not exists quote (
    cik        text not null references company (cik) on delete cascade,
    as_of      date not null,
    price      numeric not null,
    currency   text not null,
    source     text not null,
    updated_at timestamptz not null default now(),
    primary key (cik, as_of)
);

-- create table if not exists is a no-op against the table Supabase already has, so the
-- column above only reaches a fresh install. This reaches the existing one too — apply_schema
-- re-runs this whole file, so it has to be idempotent, not a one-shot migration.
alter table quote add column if not exists updated_at timestamptz not null default now();

-- The exchange rates the valuation ratios need. A foreign private issuer reports in its
-- functional currency (company.reporting_currency) while its ADR trades in USD
-- (quote.currency), so price / avg_eps mixes two currencies unless the earnings side is
-- converted first — src/fx.py fetches the rate, src/metrics.py in_quote_currency applies it.
--
-- Cached per DAY, exactly like a quote and for the same reason: a dashboard page load and a
-- re-ingest on the same day must reuse one fetch instead of re-asking the API per company.
--
-- `base` is the currency converted FROM and `quote` the one converted TO — the standard FX
-- pair naming, which for our single use lines up exactly: base = company.reporting_currency,
-- quote = quote.currency. `rate` is how many `quote` units one `base` unit buys.
create table if not exists fx_rate (
    as_of      date not null,
    base       text not null,
    quote      text not null,
    rate       numeric not null,
    source     text not null,
    fetched_at timestamptz not null default now(),
    primary key (as_of, base, quote)
);

-- The multi-year averages src/metrics.py add_averages computes. They were thrown away on
-- every ingest, because write_facts only persists 4-digit-year columns and an average is not
-- a year.
--
-- These are STORED, never recomputed in SQL. "The most recent N years" is an Excel-derived
-- rule (Master_Vorlage BA3 -> AVERAGE(AH3:AN3)) that lives in metrics.year_columns and is
-- covered by test/test_excel_parity.py; a second copy of that window rule in SQL would drift
-- with no test watching it.
--
-- No `source` column on purpose: the whole valuation path is filing-derived (v_company_year
-- is `where source = 'edgar'`), so only the EDGAR frame is stored here. Mixing a Yahoo
-- average EPS with an EDGAR book value in v_valuation below would produce a ratio from two
-- different sets of books.
--
-- MIXED UNITS ON PURPOSE, READ CAREFULLY: for a foreign filer, the concept = 'EPS' /
-- 'book_value_per_share' rows here are in company.reporting_currency (raw, unconverted),
-- while concept = 'KGV' / 'RoI' / 'KBGV' / 'KGBV_conservative' are already FX-converted into
-- quote.currency (src/metrics.py in_quote_currency, applied at ingest time before
-- add_price_ratios runs). Computing `price / EPS` straight from this table reproduces the
-- exact currency-mixing bug src/fx.py exists to fix. Anything that needs a ratio should read
-- the KGV/RoI/etc. rows directly, or go through v_valuation + metrics.valuation() like the
-- dashboard does — never re-derive a ratio from this table's raw EPS/book-value rows.
create table if not exists metric_average (
    cik          text not null references company (cik) on delete cascade,
    concept      text not null,
    window_years smallint not null,
    value        numeric,
    computed_at  timestamptz not null default now(),
    primary key (cik, concept, window_years)
);

create table if not exists watchlist (
    cik          text primary key references company (cik) on delete cascade,
    added_at     timestamptz not null default now(),
    target_price numeric,
    note         text
);

-- The per-company detail behind a backtest run (backtest_report.py). The aggregate rollup
-- lands in a 4-row CSV under reports/, which answers "did the signal work" but not "why did
-- THIS company score 🟢 on that date" — and re-deriving that means re-running a two-hour
-- fetch. So the row keeps everything the score was computed FROM: the point-in-time
-- valuation inputs (src/backtest.py point_in_time_frame), the three ratios the screener
-- derived, both price endpoints with the trading day each actually resolved to, and the
-- realized forward return.
--
-- `signal` is the screener constant's NAME ('CHEAP_AND_STRONG', 'MIXED', 'RICH_AND_WEAK',
-- 'UNKNOWN'), not the glyph — src/screener.py's constants ARE the emoji, and a WHERE clause
-- against an emoji is the kind of query nobody writes twice.
--
-- Keyed (cik, cutoff) so a re-run of the same cutoff updates in place, exactly like quote's
-- (cik, as_of): a second run carries fresher today-prices, not a duplicate row.
create table if not exists backtest_result (
    cik                                text not null references company (cik) on delete cascade,
    cutoff                             date not null,
    signal                             text not null,
    avg_eps                            numeric,
    book_value_per_share               numeric,
    conservative_book_value_per_share  numeric,
    roa                                numeric,
    ebit_margin                        numeric,
    equity_ratio                       numeric,
    graham_margin                      numeric,
    kgv                                numeric,
    roi                                numeric,
    cutoff_price                       numeric,
    cutoff_price_as_of                 date,
    today_price                        numeric,
    today_price_as_of                  date,
    forward_return                     numeric,
    computed_at                        timestamptz not null default now(),
    primary key (cik, cutoff)
);


-- Every (company, year, concept) with both sources side by side. FULL OUTER JOIN on
-- purpose: a concept the filing parser missed entirely is the most interesting failure of
-- all, and an inner join would hide exactly those rows.
create or replace view v_source_diff as
with e as (select cik, fiscal_year, concept, value, accession from fact where source = 'edgar'),
     y as (select cik, fiscal_year, concept, value from fact where source = 'yahoo')
select c.ticker,
       c.name,
       coalesce(e.cik, y.cik)                 as cik,
       coalesce(e.fiscal_year, y.fiscal_year) as fiscal_year,
       coalesce(e.concept, y.concept)         as concept,
       e.value                                as edgar_value,
       y.value                                as yahoo_value,
       e.value - y.value                      as abs_diff,
       case
           when y.value is not null and y.value <> 0
               then round(abs(e.value - y.value) / abs(y.value) * 100, 2)
           end                                as pct_diff,
       case
           when e.value is null then 'missing_in_edgar'
           when y.value is null then 'missing_in_yahoo'
           when e.value = 0 and y.value = 0 then 'both_zero'
           when y.value = 0 then 'yahoo_zero'
           when abs(e.value - y.value) / abs(y.value) <= 0.02 then 'match'
           else 'differs'
           end                                as verdict,
       e.accession
from e
         full outer join y
                         on y.cik = e.cik
                             and y.fiscal_year = e.fiscal_year
                             and y.concept = e.concept
         join company c on c.cik = coalesce(e.cik, y.cik);


-- The failure list. Ranked worst first, and deliberately including the "one side has no
-- value at all" rows, which a percentage filter alone would drop.
--
-- Restricted to fiscal years BOTH sources cover. Yahoo serves ~4 years of annual
-- statements while the filings go back further, so without this the list is dominated by
-- years Yahoo simply never had — coverage noise that buries the actual disagreements.
create or replace view v_failures as
with overlapping_years as (select cik, fiscal_year
                           from fact
                           group by cik, fiscal_year
                           having count(distinct source) = 2)
select d.*
from v_source_diff d
         join overlapping_years o on o.cik = d.cik and o.fiscal_year = d.fiscal_year
where d.verdict in ('differs', 'missing_in_edgar', 'missing_in_yahoo')
order by case d.verdict when 'differs' then 0 else 1 end,
         d.pct_diff desc nulls last,
         d.ticker,
         d.fiscal_year desc;


-- Which concept is broken, across how many companies. This is the "what do I fix first"
-- view: a tag mapping that fails for one company is a special case, one that fails for
-- thirty is a missing tag in rechenknecht_index_map.json.
create or replace view v_failure_pattern as
select concept,
       verdict,
       count(*)                  as occurrences,
       count(distinct ticker)    as companies,
       round(avg(pct_diff), 2)   as avg_pct_diff,
       min(fiscal_year)          as first_year,
       max(fiscal_year)          as last_year
from v_failures
group by concept, verdict
order by count(distinct ticker) desc, count(*) desc;


-- Wide pivot of the filing-derived numbers, for browsing and for the dashboard later.
create or replace view v_company_year as
select c.ticker,
       c.name,
       c.sic_description,
       f.fiscal_year,
       max(f.value) filter (where f.concept = 'revenue')                  as revenue,
       max(f.value) filter (where f.concept = 'ebit')                     as ebit,
       max(f.value) filter (where f.concept = 'net_income')               as net_income,
       max(f.value) filter (where f.concept = 'stockholders_equity')      as stockholders_equity,
       max(f.value) filter (where f.concept = 'total_equity')             as balance_sheet_total,
       max(f.value) filter (where f.concept = 'free_cash_flow')           as free_cash_flow,
       max(f.value) filter (where f.concept = 'EPS')                      as eps,
       max(f.value) filter (where f.concept = 'RoA')                      as roa,
       max(f.value) filter (where f.concept = 'EBIT-margin')              as ebit_margin,
       max(f.value) filter (where f.concept = 'equity-ratio')             as equity_ratio,
       max(f.value) filter (where f.concept = 'TIER')                     as tier,
       max(f.value) filter (where f.concept = 'book_value_per_share')     as book_value_per_share,
       max(f.value) filter (where f.concept = 'dividends_per_share')      as dividends_per_share
from fact f
         join company c on c.cik = f.cik
where f.source = 'edgar'
group by c.ticker, c.name, c.sic_description, f.fiscal_year;


-- Which tickers actually made it in, per source, latest attempt only.
create or replace view v_ingest_health as
with latest as (select distinct on (ticker, source) ticker, source, status, error, facts_written, finished_at
                from ingest_run
                order by ticker, source, finished_at desc)
select ticker,
       max(status) filter (where source = 'edgar')        as edgar_status,
       max(status) filter (where source = 'yahoo')        as yahoo_status,
       sum(facts_written)                                 as facts_written,
       max(finished_at)                                   as last_run,
       max(error) filter (where status = 'failed')        as last_error
from latest
group by ticker
order by (max(status) filter (where source = 'edgar') = 'failed') desc,
         (max(status) filter (where source = 'yahoo') = 'failed') desc,
         ticker;


-- The valuation INPUTS for one company, one row per company. Deliberately contains NOT ONE
-- ratio: no division, no sqrt, nothing that could be called a formula.
--
-- KGV, KBGV, RoI and the Graham number are computed by src/metrics.py valuation(), which is
-- the same function add_price_ratios calls during ingest and which test_excel_parity.py pins
-- against the workbook's own summary cells. A copy of `price / eps` here would be a second
-- definition with no oracle behind it — and three divergent copies of exactly these formulas
-- is the reason this codebase exists.
--
-- Everything is LEFT JOINed: a delisted ticker has no quote (fl), a bank yields no book
-- value per share (jpm), and a non-filer has no EDGAR facts at all. Each of those must come
-- back as NULL in one row, not as a missing row.
create or replace view v_valuation as
select c.cik,
       c.ticker,
       c.name,
       c.sic_description,
       c.fiscal_year_end_month,
       q.price,
       q.as_of                                as price_as_of,
       q.currency,
       a.value                                as avg_eps,
       a.window_years                         as avg_window_years,
       b.value                                as book_value_per_share,
       b.fiscal_year                          as book_value_fiscal_year,
       cb.value                               as conservative_book_value_per_share,
       -- Last in the list rather than next to q.currency where it belongs: `create or
       -- replace view` may only ADD columns at the END of an existing view's list, and this
       -- file is re-run as a whole rather than applied as a one-shot migration.
       --
       -- Both currencies are exposed because avg_eps and the two book values are in the
       -- currency the FILING reports, while price is in the currency the share TRADES in —
       -- dividing one by the other without converting is a number that looks like a P/E and
       -- means nothing. NULL means "no difference known": every US filer, and every company
       -- ingested before reporting_currency existed.
       c.reporting_currency,
       -- Appended for the same reason reporting_currency was, and under the same constraint:
       -- `create or replace view` may only ADD columns at the END. The screener filters on
       -- ranges of this, so it has to travel with the valuation inputs rather than force a
       -- second query against company.
       c.sic_code
from company c
         -- Latest quote. Cheaper and clearer than a window function over the whole table,
         -- and it stays one row even when a company has years of price history.
         left join lateral (select price, as_of, currency
                            from quote
                            where quote.cik = c.cik
                            order by as_of desc
                            limit 1) q on true
    -- 7 is the workbook's window (Master_Vorlage BA3 averages seven years) and the only one
    -- ingest.py writes. This SELECTS a stored row; it does not recompute the average.
         left join metric_average a
                   on a.cik = c.cik and a.concept = 'EPS' and a.window_years = 7
    -- Book value is a stock, not a flow: the LATEST reported one, never an average of it.
    -- Master_Vorlage does the same -- BA10's label at BB4 is "Buchwert (letzter Bericht)".
    -- `value is not null` matters: a company can report a year with no equity figure at all,
    -- and ordering by year alone would then hand back a NULL as "the latest book value".
         left join lateral (select value, fiscal_year
                            from fact
                            where fact.cik = c.cik
                              and fact.source = 'edgar'
                              and fact.concept = 'book_value_per_share'
                              and fact.value is not null
                            order by fiscal_year desc
                            limit 1) b on true
         left join lateral (select value
                            from fact
                            where fact.cik = c.cik
                              and fact.source = 'edgar'
                              and fact.concept = 'conservative_book_value_per_share'
                              and fact.value is not null
                            order by fiscal_year desc
                            limit 1) cb on true;


-- Macro-economic history — the second data stream, and the only table in this file with no
-- company in it. A yield spread, an unemployment rate or a CPI print is a property of the
-- economy rather than of a filer, so there is no cik to reference and no fiscal year to hang
-- it on: a calendar date is the whole key alongside the series name.
--
-- Long/narrow for exactly the reason `fact` is, and the same note at the top of this file
-- applies: the set of series grows as questions get asked, and one column per series would
-- need a migration every time one is added.
--
-- `value` is NOT NULL on purpose. FRED marks a date it holds no number for with the literal
-- string "." (weekends and holidays on a daily series, or a month not yet published), and
-- ingest_macro.py drops those rows at the boundary. Neither alternative survives contact
-- with the data: a stored 0.0 silently poisons every spread and average computed over the
-- series, and a stored NULL makes "FRED had no number" and "the number was genuinely zero"
-- indistinguishable — and for T10Y2Y, whose whole point is the sign of the spread, zero is a
-- real and meaningful value.
--
-- Keyed (series_id, date) so a re-run updates in place rather than duplicating. FRED revises
-- published series — CPIAUCSL picks up seasonal-adjustment revisions months after first
-- publication — so a changed value on a later run is the correct outcome, not a bug.
--
-- NOTE for whoever appends the next block below this one: src/db.py apply_macro_schema pulls
-- these statements back out of this file by splitting on the semicolon and keeping the chunks
-- that mention macro_series, so this comment block must stay semicolon-free.
create table if not exists macro_series (
    series_id  text not null,
    date       date not null,
    value      numeric not null,
    source     text not null default 'fred',
    updated_at timestamptz not null default now(),
    primary key (series_id, date)
);

-- A scan across every series at once ("what did the whole macro picture look like in 2008")
-- filters on date without a series_id, which the primary key's leading column cannot serve.
create index if not exists macro_series_date_idx on macro_series (date);
