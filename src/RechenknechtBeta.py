import json
import logging
import pathlib
from collections import Counter
from datetime import datetime

import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from colorama import Fore, Style

# in dem dokument sind alle tags und indexes für die analyse zu finden

# No basicConfig here. This module is a library: configuring the root logger at import made
# `import src.RechenknechtBeta` raise FileNotFoundError from any working directory without a
# ./logs folder (it is gitignored, so from a fresh clone always), and it silently won over
# whatever level the entry point set afterwards. Entry points configure logging.
logger = logging.getLogger(__name__)

# Concept names and every ratio formula live in src/metrics.py, shared with the Yahoo source
# so that a disagreement between the two is a data difference and not a formula difference.
from src.metrics import (
    ACCOUNTS_RECEIVABLE,
    ACCRUED_EXPENSES_AND_ACCOUNTS_PAYABLE,
    BALANCE_SHEET_TOTAL,
    BOOK_VALUE_PER_SHARE,
    CAPEX,
    CASH,
    CONSERVATIVE_BOOK_VALUE_PER_SHARE,
    CURRENT_ASSETS,
    DIVIDENDS_PER_SHARE,
    EBIT,
    EBIT_MARGIN,
    EPS,
    EQUITY_RATIO,
    GOODWILL,
    INTANGIBLE_ASSETS,
    INTEREST_EXPENSE,
    INTEREST_INCOME_EXPENSE,
    KBGV,
    LONGTERM_LIABILITIES,
    NET_INCOME,
    NETTOUMLAUFVERM_GEN,
    NUMBER_OF_SHARES,
    NUMBER_OF_SHARES_DILUTED,
    OPERATING_CASH_FLOW,
    REVENUE,
    REVENUE_CUSTOMER,
    RO_A,
    RO_I,
    SHORTTERM_LIABILITIES,
    STOCKHOLDERS_EQUITY,
    TIER,
    TOTAL_LIABILITIES,
    add_averages,
    add_sheet_ratios,
    fiscal_year_label,
    longterm_from_total,
)


# Some filers tag the weighted-average share count PRE-SCALED and say so nowhere a machine
# can read: McDonald's FY2023-FY2025 10-Ks carry `716.4` meaning 716.4 million, ConocoPhillips'
# FY2017-FY2021 and PDD's FY2024 20-F carry thousands. `decimals` is precision, not scale
# (MCD's pre-scaled facts and Foot Locker's correct ones are both plausible values of it), so
# the element on its own cannot be read for this. The only usable signal is a second count of
# the same thing in the SAME document: dei:EntityCommonStockSharesOutstanding, the cover-page
# disclosure every 10-K/20-F must carry, tagged in whole shares (decimals="INF" or "0").
SHARE_ANCHOR_TAG = "dei:EntityCommonStockSharesOutstanding"
SHARE_SCALES = (1_000, 1_000_000)
# A factor of two either side of a candidate scale, never an exact match: the anchor is a
# point-in-time count at the cover date while the value is a weighted average over the fiscal
# year, so buybacks and issuance put a real gap between the two even for a filer that scales
# correctly. Two is far more than any of that and still three orders of magnitude short of the
# next candidate. Measured over all 993 filings on disk (2026-08-11): every ratio lands either
# within 2x of 1 or inside one of these bands, except Alibaba's ADS-vs-ordinary factor of 10.3
# and Ascent Solar's reverse splits at 3.7-7.9 — both left alone, and both nearly two orders of
# magnitude clear of the nearest band edge.
SHARE_SCALE_TOLERANCE = 2

try:
    index_map_path = (pathlib.Path(__file__).parent.parent / "documents" / "rechenknecht_index_map.json").absolute()
    with open(index_map_path, "r") as f:
        index_map = json.load(f)
except FileNotFoundError:
    logger.error(f"Path {index_map_path} doesn't exist.")


class RechenknechtBeta:
    # Class-level defaults, not instance state: these describe the document currently loaded
    # in bs_data, and every reader of them has to work on an instance that never ran
    # __init__ (set_document is callable on its own, and the tests build exactly such an
    # instance). The defaults are the neutral case — no units known, so no fact is filtered.
    unit_currency: dict = {}
    document_currency: str = None
    document_us_gaap_facts: int = 0
    us_gaap_facts: int = 0
    # Same reason: unscale_shares() names the company in its warnings, and it has to be
    # callable on an instance that never ran __init__.
    name: str = None

    def __init__(self, name: str, isin: str, currency: str, ticker: str, sector: str, file_list,
                 log_level=logging.DEBUG, market_price: float = None, fx_rate=None):
        logger.setLevel(log_level)

        print(f"RECHENKNECHTBETA CALLED at {Fore.BLUE}{name}{Style.RESET_ALL}")
        # initial field values to work with.
        self.current_year = None
        self.market_price = None
        self.name = name
        self.isin = isin
        # A FALLBACK, not the answer: the filing itself says which currency it reports in,
        # and calculate() overwrites this with what the newest one actually tagged. It stays
        # only for a filing that tags no monetary fact at all.
        self.currency = currency
        self.ticker = ticker
        self.branche = sector
        # The price is an input, not something the analysis fetches: passing it keeps the
        # filing math testable offline and works for tickers that no longer trade (FL).
        if market_price is None:
            self.get_stock_price()
        else:
            self.market_price = market_price

        # A CALLABLE reporting_currency -> rate, not a finished number: which currency this
        # company reports in is only known once calculate() below has parsed the newest
        # filing, so the caller cannot resolve the rate beforehand. None means no conversion,
        # which keeps this class free of any database or network dependency — the tests build
        # it that way and every filer that reports in the currency it trades in needs nothing
        # else.
        self.fx_rate = fx_rate

        self.bs_data: BeautifulSoup = None

        rows: list[str] = \
            [DIVIDENDS_PER_SHARE,
             EPS,
             BOOK_VALUE_PER_SHARE,
             RO_A,
             EBIT_MARGIN,
             EQUITY_RATIO,
             CONSERVATIVE_BOOK_VALUE_PER_SHARE,
             NETTOUMLAUFVERM_GEN,
             STOCKHOLDERS_EQUITY,
             LONGTERM_LIABILITIES,
             SHORTTERM_LIABILITIES,
             CURRENT_ASSETS,
             GOODWILL,
             INTANGIBLE_ASSETS,
             BALANCE_SHEET_TOTAL,
             REVENUE,
             EBIT,
             INTEREST_EXPENSE,
             NET_INCOME,
             NUMBER_OF_SHARES,
             NUMBER_OF_SHARES_DILUTED,
             TIER,
             RO_I,
             KBGV,
             # Declared up front because nan_to_zero() reads them unconditionally. They
             # used to be created only as a side effect of finding their tag, so any
             # company without a capex or cash-flow tag crashed the whole analysis.
             # Order matches set_cash_flow_statement_data() so the row order is unchanged.
             OPERATING_CASH_FLOW,
             CAPEX]

        self.file_list = file_list
        self.df = pd.DataFrame(index=rows)
        self.calculate()

        self.print_extraordinary_results()

    def print_extraordinary_results(self):
        if self.df.loc[RO_I, "7_YEAR_AVG"] > 8:
            print(f"{Fore.GREEN}{self.name} is a candidate with RoI of {self.df.loc[RO_I, '7_YEAR_AVG']}%")
        if self.df.loc[RO_A, "7_YEAR_AVG"] > 8:
            print(f"{Fore.GREEN}{self.name} is a candidate with RoA of {self.df.loc[RO_A, '7_YEAR_AVG']}%")
        if self.df.loc[EBIT_MARGIN, "7_YEAR_AVG"] > 13:
            print(
                f"{Fore.GREEN}{self.name} is a candidate with EBIT-margin of {self.df.loc[EBIT_MARGIN, '7_YEAR_AVG']}%")
        if self.df.loc[EQUITY_RATIO, "7_YEAR_AVG"] > 35:
            print(
                f"{Fore.GREEN}{self.name} is a candidate with equity-ratio of {self.df.loc[EQUITY_RATIO, '7_YEAR_AVG']}%")
        if self.df.loc[KBGV, "7_YEAR_AVG"] < 10:
            print(f"{Fore.GREEN}{self.name} is a candidate with KBGV of {self.df.loc[KBGV, '7_YEAR_AVG']}")
            print(Style.RESET_ALL)

    def to_csv(self, path: pathlib.Path):
        self.df.to_csv(f"{path}/{self.ticker}.csv")

    def get_stock_price(self):
        stock_info = yf.Ticker(self.ticker)
        self.market_price = stock_info.fast_info["lastPrice"]

    def set_document(self, file_path: str):
        # die datei einlesen und in beautifulsoup format bringen zur analyse
        with open(
                file_path,
                "r",
        ) as filing:
            data = filing.read()
        document: BeautifulSoup = BeautifulSoup(data, "xml")
        self.bs_data = document
        self.set_reporting_currency()

    def set_reporting_currency(self):
        """Which currency THIS filing reports in, and how many us-gaap facts it carries.

        A foreign private issuer's 20-F tags the same concept twice: once in its functional
        currency and once as a USD convenience translation at a single closing rate. Both
        are consolidated, both are valid XBRL, and only document order decided which one
        won — Alibaba's FY2025 revenue is tagged CNY 996,347,000,000 and then again as USD
        137,300,000,000 four elements later, inside the slice set_revenue() takes, so the
        translation silently overwrote the real figure. The convenience translation is also
        not comparable year over year (each filing uses its own closing rate), so the
        filing's own currency is the one to keep.
        """
        # Only iso4217:* is money. A measure like `mufg:security` or `baba:Segment` is a
        # unit too, and splitting on ":" without this check invents a currency called
        # "security" out of it.
        self.unit_currency = {
            unit.get("id"): unit.find("measure").text.split(":")[-1]
            for unit in self.bs_data.find_all("unit")
            if unit.find("measure") and unit.find("measure").text.startswith("iso4217:")
        }

        monetary = Counter()
        self.document_us_gaap_facts = 0
        for fact in self.bs_data.find_all(attrs={"unitRef": True}):
            if fact.prefix != "us-gaap":
                continue
            self.document_us_gaap_facts += 1
            currency = self.unit_currency.get(fact.get("unitRef"))
            if currency:
                monetary[currency] += 1

        # contra: the functional currency is taken to be the most frequently tagged one.
        # No filing in the sample carries dei:EntityReportingCurrencyISOCode to read it off
        # directly (checked on baba and mufg 20-Fs, both return None). The count holds
        # because the convenience translation covers only the primary statements while the
        # functional currency also carries every note — baba 1228 CNY vs 175 USD, mufg 5746
        # JPY vs 0. Upgrade path: resolve it from the income-statement presentation link if
        # a filing ever ties.
        self.document_currency = monetary.most_common(1)[0][0] if monetary else None

    def find_facts(self, tags):
        """Every hit for `tags`, minus the ones tagged in a currency this filing does not
        report in.

        The one place the unit filter lives. Every setter reads its facts through here, so
        a concept tagged in both the functional currency and the USD convenience
        translation cannot resolve to whichever came first in the document.
        """
        return [fact for fact in self.bs_data.find_all(name=tags) if self.in_reporting_currency(fact)]

    def in_reporting_currency(self, fact) -> bool:
        """True for anything that is not money in a foreign currency.

        Non-monetary facts (share counts, ratios, percentages) carry a unitRef that is not
        an iso4217 measure and must pass untouched; so must every fact when the filing's
        currency could not be determined at all, because dropping everything would be a
        worse answer than the status quo.
        """
        currency = self.unit_currency.get(fact.get("unitRef"))
        return currency is None or self.document_currency is None or currency == self.document_currency

    def cover_page_shares(self):
        """Shares outstanding as THIS document's cover page states them, or None.

        Summed over share classes, because the setters below read the weighted-average count
        the same way — holding a per-class anchor against a consolidated value would invent a
        scale factor out of the class split alone.
        """
        anchors = self.facts_by_year(SHARE_ANCHOR_TAG, sum_segments=True)
        return anchors[max(anchors)] if anchors else None

    def unscale_shares(self, by_year: dict) -> dict:
        """`by_year` with a filer-side scale factor undone. Unchanged is the normal case.

        The decision is taken ONCE per document, from the year closest to the cover date, and
        then applied to every year in it: a filer's scale convention belongs to the filing, so
        correcting year by year could only ever produce a document whose own years disagree.
        """
        anchor = self.cover_page_shares()
        newest = by_year.get(max(by_year)) if by_year else None
        if not anchor or not newest:
            if by_year and not anchor:
                logger.warning("%s: no %s in this filing to check the share count against — "
                               "leaving %s as tagged", self.name, SHARE_ANCHOR_TAG, newest)
            return by_year

        ratio = anchor / newest
        for scale in SHARE_SCALES:
            if scale / SHARE_SCALE_TOLERANCE <= ratio <= scale * SHARE_SCALE_TOLERANCE:
                logger.warning("%s: share count is tagged pre-scaled (%s against a cover page "
                               "count of %s) — correcting by %d", self.name, newest, anchor, scale)
                return {year: round(value * scale) for year, value in by_year.items()}

        if not 1 / SHARE_SCALE_TOLERANCE <= ratio <= SHARE_SCALE_TOLERANCE:
            logger.warning("%s: share count %s is %.3g x the cover page count %s, which is no "
                           "power of ten this corrects — leaving it as tagged",
                           self.name, newest, ratio, anchor)
        return by_year

    def set_number_of_shares(self):
        # Share classes are disjoint and additive, so a multi-class company that never
        # reports a consolidated count gets the sum rather than one arbitrary class.
        shares = self.facts_by_year(index_map[NUMBER_OF_SHARES][1], sum_segments=True)
        for year, value in self.unscale_shares(shares).items():
            self.df.loc[NUMBER_OF_SHARES, year] = int(value)

    def set_number_of_shares_diluted(self):
        shares = self.facts_by_year(index_map[NUMBER_OF_SHARES_DILUTED][1], sum_segments=True)
        for year, value in self.unscale_shares(shares).items():
            self.df.loc[NUMBER_OF_SHARES_DILUTED, year] = int(value)

    def get_fiscal_year(self):
        fy = self.bs_data.find("dei:DocumentFiscalYearFocus")
        fiscal_year = fy.text
        return fiscal_year

    def calculate(self):
        """
        calculate all values for the dataframe for every year
        """

        # First of all, go over all documents and set the data
        for index, (path, year) in enumerate(self.file_list):
            year_parsed = year.split("-")[0]
            self.current_year = year_parsed
            self.set_document(path)

            # file_list is newest first (EDGAR lists recent filings that way, and ingest.py
            # reads file_list[0] as the newest accession), so the NEWEST filing decides both
            # what currency the company reports in today and whether it is a us-gaap filer
            # at all. Deliberately not "any filing": Sony and Toyota migrated to IFRS at
            # FY2022 and still carry us-gaap facts in their older 20-Fs, so accepting them
            # on those would store a company whose recent years are empty.
            if index == 0:
                self.currency = self.document_currency or self.currency
                self.us_gaap_facts = self.document_us_gaap_facts
                if not self.us_gaap_facts:
                    raise LookupError(
                        f"{self.ticker}: newest annual report carries no us-gaap facts "
                        f"(IFRS filer?) — {path}"
                    )

            self.set_data()

        # Afterwards, we can calculate the ratios and averages — shared with the Yahoo source.
        # The price is in the currency the share TRADES in, the frame is in the currency the
        # filing REPORTS in, so the valuation ratios need the rate between them; asked for
        # here rather than passed in because self.currency was only just determined above.
        self.nan_to_zero()
        add_sheet_ratios(self.df)
        add_averages(self.df, self.market_price, time_span=7,
                     fx_rate=self.fx_rate(self.currency) if self.fx_rate else 1.0)

        # We cant 2 comma precision only
        self.df = self.df.round(decimals=2)

    def set_data(self):
        self.set_number_of_shares()
        self.set_number_of_shares_diluted()
        self.set_dividends()

        self.set_balance_sheet_data()
        self.set_income_statement_data()
        self.set_cash_flow_statement_data()

    def set_dividends(self):
        key = "dividends"
        tags = index_map[key][1]
        dividends = self.find_facts(tags)

        for dividend in dividends:
            try:
                year = self.get_fiscal_year_by_context(dividend["contextRef"])
                self.df.loc[DIVIDENDS_PER_SHARE, year] = float(dividend.text)
            except ValueError:
                logger.debug(f"ValueError for {self.name} for {key}")

    def set_balance_sheet_data(self):
        # BALANCE SHEET
        self.set_total_equity()
        self.set_current_assets()
        self.set_current_liabilities()
        self.set_longterm_liabilities()
        self.set_stockholders_equity()
        self.set_intangible_assets()
        self.set_goodwill()
        self.set_total_liabilities()

    def set_income_statement_data(self):
        # INCOME STATEMENT
        self.set_revenue()
        self.set_operating_income()
        self.set_interest_expenses()
        self.set_net_income()


    def set_total_equity(self):
        # total equity
        key = BALANCE_SHEET_TOTAL
        tags = index_map[key][1]
        total_equities = self.find_facts(tags)[:2]

        if not total_equities:
            logger.warning("No total_equities found")

        for total_equity in total_equities:
            year = self.get_fiscal_year_by_context(total_equity["contextRef"])
            self.df.loc[BALANCE_SHEET_TOTAL, year] = float(total_equity.text)

    def set_stockholders_equity(self, limit=2, retry=True):
        # Total stockholders’ equity
        key = STOCKHOLDERS_EQUITY
        tags = index_map[key][1]
        total_equities = self.find_facts(tags)[:limit]

        if not total_equities:
            logger.warning("No total_equities found in set_stockholders_equity")

        for total_equity in total_equities:
            year = self.get_fiscal_year_by_context(total_equity["contextRef"])
            total_equity = total_equity.text

            # So sometimes the key "Stockholders equity doesn't work, which means the first 2 values are empty strings ("")"
            # This means, we have to somehow adjust the function to get the correct value. The solution is:
            # Retry set_stockholders_equity with a higher limit. If it still doesn't work, then just skip it.
            if total_equity == "" and retry is True:
                retry = False
                self.set_stockholders_equity(limit=limit + 2, retry=retry)

            # If the total_equity is still "", then just continue the loop
            if total_equity == "":
                continue

            self.df.loc[STOCKHOLDERS_EQUITY, year] = float(total_equity)

    def set_current_liabilities(self):
        key = SHORTTERM_LIABILITIES
        tags = index_map[key][1]
        current_liabilities = self.find_facts(tags)[:2]
        for current_liability in current_liabilities:
            year = self.get_fiscal_year_by_context(current_liability["contextRef"])
            self.df.loc[SHORTTERM_LIABILITIES, year] = float(current_liability.text)

        # No current liabilities found? Calculate them.
        if not current_liabilities:
            self.calculate_current_liabilities()

    def calculate_current_liabilities(self):
        # Joined on the fiscal year. This used to zip() two hit lists positionally, which
        # silently paired one year's liabilities with another year's accrued expenses
        # whenever the two tags appeared a different number of times.
        total_liabilities = self.facts_by_year(index_map[TOTAL_LIABILITIES][1])
        accrued_expenses = self.facts_by_year(index_map[ACCRUED_EXPENSES_AND_ACCOUNTS_PAYABLE][1])

        for year, total_liability in total_liabilities.items():
            if year in accrued_expenses:
                self.df.loc[SHORTTERM_LIABILITIES, year] = total_liability - accrued_expenses[year]

    def set_current_assets(self):
        current_assets = self.facts_by_year(index_map[CURRENT_ASSETS][1])
        for year, value in current_assets.items():
            self.df.loc[CURRENT_ASSETS, year] = value

        if not current_assets:
            self.calculate_current_assets()

    def calculate_current_assets(self):
        key = ACCOUNTS_RECEIVABLE
        tags = index_map[key][1]
        accounts_receivable = self.find_facts(tags)[:2]

        key = CASH
        tags = index_map[key][1]
        cashs = self.find_facts(tags)[:2]

        for account_receivable, cash in zip(accounts_receivable, cashs):
            year = self.get_fiscal_year_by_context(account_receivable["contextRef"])
            account_receivable = float(account_receivable.text)
            cash = float(cash.text)

            current_asset = account_receivable + cash
            self.df.loc[CURRENT_ASSETS, year] = current_asset

    def set_longterm_liabilities(self):
        total_liabilities = self.facts_by_year(index_map[TOTAL_LIABILITIES][1])

        if not total_liabilities:
            # TODO calculate by other means
            logger.warning("No total liabilities found. Please implement a way to calculate them.")

        # .get, not .loc[…, year]: this reads a COLUMN that only exists once some earlier
        # setter has written that year. It survives today because set_current_assets happens
        # to cover a superset of the years total liabilities are tagged for — a tag list
        # change on either side turns that into a KeyError that kills the whole company.
        # A year we have no current liabilities for yields NaN, which is the honest answer.
        current = self.df.loc[SHORTTERM_LIABILITIES]
        for year, total_liability in total_liabilities.items():
            self.df.loc[LONGTERM_LIABILITIES, year] = longterm_from_total(
                total_liability, current.get(year, float("nan"))
            )

    def set_goodwill(self):
        # Not summed across segments: goodwill per reporting unit is a hierarchy, and
        # adding the parts risks double counting when a total is also tagged.
        goodwills = self.facts_by_year(index_map[GOODWILL][1])

        if not goodwills:
            logger.warning("No goodwill found. Please implement a way to calculate them.")

        for year, value in goodwills.items():
            self.df.loc[GOODWILL, year] = value

    def set_intangible_assets(self):
        intangibles = self.facts_by_year(index_map[INTANGIBLE_ASSETS][1])

        if not intangibles:
            logger.warning("No intangible assets found. Please implement a way to calculate them.")

        for year, value in intangibles.items():
            self.df.loc[INTANGIBLE_ASSETS, year] = value

    def set_total_liabilities(self):
        """Master_Vorlage row 32. Previously never populated at all, which left the
        workbook's debt ratios uncomputable for every company."""
        for year, value in self.facts_by_year(index_map[TOTAL_LIABILITIES][1]).items():
            self.df.loc[TOTAL_LIABILITIES, year] = value

    def facts_by_year(self, tags, sum_segments=False):
        """Resolve an XBRL tag (or list of tags) to {fiscal_year: value}.

        Replaces the `find_all(tags)[:2]` pattern, which took whatever the first two hits
        happened to be. Two rules it got wrong:

        1. A value carrying a segment is a BREAKDOWN — one share class, one business unit,
           one debt instrument — not the consolidated total. Letting it through means the
           last breakdown in document order silently overwrites the real number.
        2. Some companies report a concept ONLY per segment. Visa never tags a consolidated
           diluted share count, only Class A/B1/B2/C, which is how Class C's 10 million
           became Visa's share count and inflated its EPS by a factor of ~200. Where that
           is genuinely additive (share classes) the members are summed, deduplicated,
           because the same class/period pair appears several times in one filing.
        """
        # A list of tags is a PRIORITY order, not a set. Handing them all to find_all at
        # once lets document order decide which concept wins: 3M tags both
        # InterestExpenseDebt (448m) and InterestExpenseNonoperating (946m), and whichever
        # appeared first silently became "the" interest expense. Each tag is tried in turn
        # and the first one that yields anything is used.
        if isinstance(tags, (list, tuple)):
            for tag in tags:
                found = self.facts_by_year(tag, sum_segments=sum_segments)
                if found:
                    return found
            return {}

        plain, segmented = {}, {}

        for hit in self.find_facts(tags):
            context_id = hit.get("contextRef")
            if not context_id:
                continue
            try:
                year = self.get_fiscal_year_by_context(context_id)
                value = float(hit.text)
            except (ValueError, TypeError, AttributeError):
                continue

            context = self.bs_data.find(id=context_id)
            segments = context.find_all("segment") if context else []

            if not segments:
                plain.setdefault(year, value)
            elif sum_segments:
                member = segments[0].get_text(strip=True)
                segmented.setdefault(year, {})[member] = value

        for year, members in segmented.items():
            if year not in plain:
                plain[year] = sum(members.values())

        return plain

    def get_fiscal_year_by_context(self, contextid):
        """
        Calculates the REAL fiscal year from a given contextid. Assume a fiscal year from 01.01.2022 to 31.12.2022. \n
        => The FY to be returned will be 2022 \n
        Assume a fiscal year from 01.09.2022 to 31.08.2023. \n
        => The FY to be returned will be 2023, because the start-date is in the 2nd half of the year. \n
        Assume a fiscal year from 01.02.2022 to 31.01.2023. \n
        => The FY to be returned will be 2022, because the start-date is in the 1st half of the year. \n

        :param contextid: The context_id of the element to get the fiscal year from
        :return: the REAL fiscal year of the element
        """
        context = self.bs_data.find(id=contextid)
        current_fy_end_date = self.bs_data.find("dei:CurrentFiscalYearEndDate").text[2:]
        fiscal_year_end_date = datetime.strptime(current_fy_end_date, "%m-%d")

        if context.find("instant") is not None:
            period_end = datetime.strptime(context.find("instant").text, "%Y-%m-%d")
        else:
            start = datetime.strptime(context.find("startDate").text, "%Y-%m-%d")
            end = datetime.strptime(context.find("endDate").text, "%Y-%m-%d")

            delta = (end - start).days
            if int(delta) < 350:
                logger.debug(f"No whole Year: {start} - {end}. Delta: {delta}. ContextID: {contextid}")
                raise ValueError(f"No whole Year: {start} - {end}")

            period_end = end

        # Instants and full-year durations collapse to the same rule: a year ending in the
        # first half of the calendar year started in the previous one, so the label is the
        # same whether it is derived from the start date or from the end date.
        return fiscal_year_label(period_end, fiscal_year_end_date.month)

    def set_revenue(self):
        """
        Sets the revenue for the current year. \n
        The revenue is saved in the index_map with the key "Revenue". \n
        The revenue is saved in the dataframe with the key "REVENUE". \n
        The revenue is saved in the dataframe with the year as column name. \n
        """

        key = REVENUE
        tags = index_map[key][1]
        revenues = self.find_facts(tags)[:9]

        # Try other key
        if not revenues:
            key = REVENUE_CUSTOMER
            tags = index_map[key][1]
            revenues = self.find_facts(tags)[:9]

        if not revenues:
            logger.warning("No revenues found. Please implement a way to calculate them.")

        for revenue in revenues:

            # We need to filter out the segments, because they represent special items in the revenues, like
            # revenues of sales, revenues of services, etc. We only want the total revenue.
            ctx = self.bs_data.find(id=revenue["contextRef"])
            segments = ctx.find_all("segment")

            if not segments:
                try:
                    year = self.get_fiscal_year_by_context(revenue["contextRef"])
                    self.df.loc[REVENUE, year] = float(revenue.text)
                except ValueError:
                    logger.debug(msg=f"ValueError for {revenue}")
                    pass

    def set_operating_income(self):
        key = EBIT
        tags = index_map[key][1]
        operating_incomes = self.find_facts(tags)[:3]

        if not operating_incomes:
            logger.warning("No operating incomes found. Please implement a way to calculate them.")

        for operating_income in operating_incomes:
            try:
                year = self.get_fiscal_year_by_context(operating_income["contextRef"])
                self.df.loc[EBIT, year] = float(operating_income.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {operating_income}")
                pass

    def set_interest_expenses(self):
        expenses = self.facts_by_year(index_map[INTEREST_EXPENSE][1])

        # If no expenses were found, search for Income/Expense and filter for the expenses
        if not expenses:
            net = self.facts_by_year(index_map[INTEREST_INCOME_EXPENSE][1])
            # A positive net figure means interest INCOME exceeded expense, so there is no
            # expense to report rather than a negative one.
            expenses = {year: (0.0 if value > 0 else abs(value)) for year, value in net.items()}

        for year, value in expenses.items():
            self.df.loc[INTEREST_EXPENSE, year] = abs(value)

    def set_net_income(self):
        key = NET_INCOME
        tags = index_map[key][1]
        net_incomes = self.find_facts(tags)[:3]

        if not net_incomes:
            logger.warning("No net incomes found. Please implement a way to calculate them.")

        for net_income in net_incomes:
            try:
                year = self.get_fiscal_year_by_context(net_income["contextRef"])
                self.df.loc[NET_INCOME, year] = float(net_income.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {net_income}")
                pass

    def set_operating_cash_flow(self):
        key = OPERATING_CASH_FLOW
        tags = index_map[key][1]
        operating_cash_flows = self.find_facts(tags)[:3]

        if not operating_cash_flows:
            logger.warning("No operating cash flows found. Please implement a way to calculate them.")

        for operating_cash_flow in operating_cash_flows:
            try:
                year = self.get_fiscal_year_by_context(operating_cash_flow["contextRef"])
                self.df.loc[OPERATING_CASH_FLOW, year] = float(operating_cash_flow.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {operating_cash_flow}")
                pass

    def set_capex(self):
        key = CAPEX
        tags = index_map[key][1]
        capexs = self.find_facts(tags)[:3]

        if not capexs:
            logger.warning("No capexs found. Please implement a way to calculate them.")

        for capex in capexs:
            try:
                year = self.get_fiscal_year_by_context(capex["contextRef"])
                self.df.loc[CAPEX, year] = float(capex.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {capex}")
                pass

    def nan_to_zero(self):
        # fill empty values with 0
        def set_to_zero(key, year_col):
            if self.df.loc[key, year_col] == "" or pd.isna(self.df.loc[key, year_col]):
                logger.debug(msg=f"{key} is empty for year {year_col}")
                self.df.loc[key, year_col] = 0

        for year in self.df.columns:
            set_to_zero(DIVIDENDS_PER_SHARE, year)
            set_to_zero(GOODWILL, year)
            set_to_zero(INTANGIBLE_ASSETS, year)
            set_to_zero(INTEREST_EXPENSE, year)
            set_to_zero(CAPEX, year)
            set_to_zero(OPERATING_CASH_FLOW, year)

    def set_cash_flow_statement_data(self):
        self.set_operating_cash_flow()
        self.set_capex()
