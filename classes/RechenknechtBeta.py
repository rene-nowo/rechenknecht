import logging
import pathlib
from xmlrpc.client import Boolean
import pandas as pd
import xml
from bs4 import BeautifulSoup
import re
import yfinance as yf
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import math
import cProfile

# in dem dokument sind alle tags und indexes für die analyse zu finden

logger = logging.getLogger(__name__)

KBGV = "KBGV"
RO_I = "RoI"
TIER = "TIER"
NUMBER_OF_SHARES_DILUTED = "number_of_shares_diluted"
NUMBER_OF_SHARES = "number_of_shares"
NET_INCOME = "net_income"
INTEREST_EXPENSE = "interest_expense"
EBIT = "ebit"
REVENUE = "revenue"
TOTAL_ASSETS = "total_equity"
INTANGIBLE_ASSETS = "intangible_assets"
GOODWILL = "goodwill"
CURRENT_ASSETS = "current_assets"
SHORTTERM_LIABILITIES = "current_liabilities"
LONGTERM_LIABILITIES = "longterm_liabilities"
STOCKHOLDERS_EQUITY = "stockholders_equity"
NETTOUMLAUFVERM_GEN = "nettoumlaufvermögen"
CONSERVATIVE_BOOK_VALUE_PER_SHARE = "conservative_book_value_per_share"
EQUITY_RATIO = "equity-ratio"
EBIT_MARGIN = "EBIT-margin"
RO_A = "RoA"
BOOK_VALUE_PER_SHARE = "book_value_per_share"
EPS = "EPS"
DIVIDENDS_PER_SHARE = "dividends_per_share"

index_map_path = pathlib.Path("./documents/rechenknecht_index_map.json").absolute()
with open(index_map_path, "r") as f:
    index_map = json.load(f)


class RechenknechtBeta:
    def __init__(self, name: str, isin: str, currency: str, ticker: str, sector: str, file_list,
                 log_legel=logging.DEBUG):
        logger.setLevel(log_legel)

        # initial field values to work with.
        self.current_year = None
        self.market_price = None
        self.name = name
        self.isin = isin
        self.currency = currency
        self.ticker = ticker
        self.branche = sector
        self.get_stock_price()

        self.bs_data: BeautifulSoup = None

        values_to_calculate: list[str] = \
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
             TOTAL_ASSETS,
             REVENUE,
             EBIT,
             INTEREST_EXPENSE,
             NET_INCOME,
             NUMBER_OF_SHARES,
             NUMBER_OF_SHARES_DILUTED,
             TIER,
             RO_I,
             KBGV]

        self.file_list = file_list

        columns: list[str] = []
        for path, year in self.file_list:
            year_parsed = year.split("-")[0]
            columns.append(year_parsed)

        self.df = pd.DataFrame(index=values_to_calculate, columns=columns)

        self.calculate()

        self.df.to_csv(f"./documents/csv/{self.name}.csv")

    def get_stock_price(self):
        stock_info = yf.Ticker(self.ticker)
        self.market_price = stock_info.fast_info["lastPrice"]

    def set_document(self, file_path: str):
        # die datei einlesen und in beautifulsoup format bringen zur analyse
        with open(
                file_path,
                "r",
        ) as f:
            data = f.read()
        document: BeautifulSoup = BeautifulSoup(data, "xml")
        self.bs_data = document

    def set_number_of_shares(self):
        # TAG: dei:EntityCommonStockSharesOutstanding
        tag = "dei:EntityCommonStockSharesOutstanding"
        shares = self.bs_data.find(tag)

        if shares is None:
            tag = "us-gaap:CommonStockSharesOutstanding"
            shares = self.bs_data.find(tag).text
        else:
            shares = shares.text

        fiscal_year = self.get_fiscal_year()
        self.df.loc[NUMBER_OF_SHARES, fiscal_year] = shares

    def set_number_of_shares_diluted(self):
        # TAG: dei:EntityCommonStockSharesOutstanding
        tag = "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding"
        shares = self.bs_data.find(tag).text

        fiscal_year = self.get_fiscal_year()
        self.df.loc[NUMBER_OF_SHARES_DILUTED, fiscal_year] = shares

    def get_fiscal_year(self):
        fy = self.bs_data.find("dei:DocumentFiscalYearFocus")
        fiscal_year = fy.text
        return fiscal_year

    def calculate(self):
        """
        calculate all values for the dataframe for every year
        """

        for path, year in self.file_list:
            year_parsed = year.split("-")[0]
            self.current_year = year_parsed
            self.set_document(path)
            self.set_data()

    def set_data(self):
        self.set_number_of_shares()
        self.set_number_of_shares_diluted()
        self.set_dividends()

        self.set_balance_sheet_data()

    def set_dividends(self):
        key = "dividends"
        tags = index_map[key][1]
        dividends = self.bs_data.find_all(tags)
        for dividend in dividends:
            year, is_full_year = self.get_context_date(dividend["contextRef"])
            self.df.loc[DIVIDENDS_PER_SHARE, year] = float(dividend.text)

    def get_context_date(self, context_id):
        context = self.bs_data.find(id=context_id)

        segments = context.find_all("segment")
        # segments = 0, weil man nur werte haben will die zu keinem segment gehören
        if len(segments) == 0:
            try:
                start_date = context.find("startDate").text
                end_date = context.find("endDate").text

                is_full_year = self.check_for_full_year(context_id)

            except AttributeError:
                end_date = context.find("instant").text
                start_date = end_date
                is_full_year = True

            fiscal_year = end_date.split("-")[0]
            start_year = start_date.split("-")[0]

            # Assume the fiscal year is from June 2022 to May 2023, then fiscal_year would be 2023, but start_year would be 2022.
            # This means, the data refers to the fiscal_year 2022, NOT 2023.
            # TODO. Potential bug. What if fiscal year is from September 2022 to August 2023? Should FY then be 2023?
            if fiscal_year != start_year:
                fiscal_year = start_year

            return fiscal_year, is_full_year

        return None, None

    def check_for_full_year(self, context_id):
        """ Wenn das Geschäftsjahr nicht wie das normale Jahr ist, dann muss das Auszahlungsdatum der Dividende auch dem richtigen Jahr zugeordnet werden"""

        # ziehe das anfangsdatum und das end-datum, um zu schauen ob es sich auf ein jahr bezieht
        context = self.bs_data.find(id=context_id)
        # print(context_id)

        start_date = context.find("startDate").text
        start = datetime.strptime(start_date, "%Y-%m-%d")

        end_date = context.find("endDate").text
        end = datetime.strptime(end_date, "%Y-%m-%d")

        diff = end - start

        if diff.days > 350:  # and (int(end_date.split("-")[0]) == int(self.current_year))
            return True

        return False

    def set_balance_sheet_data(self):
        # BALANCE SHEET
        self.set_current_assets()
        self.set_current_liabilities()
        self.set_longterm_liabilities()
        self.set_stockholders_equity()
        self.set_total_equity()
        self.set_intangible_assets()
        self.set_goodwill()

        # INCOME STATEMENT
        self.set_revenue()
        self.set_operating_income()
        self.set_interest_expenses()
        self.set_net_income()

    def set_total_equity(self):
        # total equity
        key = TOTAL_ASSETS
        tags = index_map[key][1]
        total_equities = self.bs_data.find_all(tags)
        for total_equity in total_equities:
            year = self.get_fiscal_year_by_context(total_equity["contextRef"])
            self.df.loc[TOTAL_ASSETS, year] = float(total_equity.text)

    def set_stockholders_equity(self):
        # Total stockholders’ equity
        key = STOCKHOLDERS_EQUITY
        tags = index_map[key][1]
        total_equities = self.bs_data.find_all(tags)
        for total_equity in total_equities:
            year = self.get_fiscal_year_by_context(total_equity["contextRef"])
            self.df.loc[STOCKHOLDERS_EQUITY, year] = float(total_equity.text)

    def set_current_liabilities(self):
        key = SHORTTERM_LIABILITIES
        tags = index_map[key][1]
        current_liabilities = self.bs_data.find_all(tags)
        for current_liability in current_liabilities:
            year = self.get_fiscal_year_by_context(current_liability["contextRef"])
            self.df.loc[SHORTTERM_LIABILITIES, year] = float(current_liability.text)

    def set_current_assets(self):
        key = CURRENT_ASSETS
        tags = index_map[key][1]
        current_assets = self.bs_data.find_all(tags)
        for current_asset in current_assets:
            year = self.get_fiscal_year_by_context(current_asset["contextRef"])
            self.df.loc[CURRENT_ASSETS, year] = float(current_asset.text)

    def set_longterm_liabilities(self):
        key = LONGTERM_LIABILITIES
        tags = index_map[key][1]
        longterm_liabilities = self.bs_data.find_all(tags)
        for longterm_liability in longterm_liabilities:
            year = self.get_fiscal_year_by_context(longterm_liability["contextRef"])
            self.df.loc[LONGTERM_LIABILITIES, year] = float(longterm_liability.text)

    def set_goodwill(self):
        key = GOODWILL
        tags = index_map[key][1]
        goodwills = self.bs_data.find_all(tags)
        for goodwill in goodwills:
            year = self.get_fiscal_year_by_context(goodwill["contextRef"])
            self.df.loc[GOODWILL, year] = float(goodwill.text)

    def set_intangible_assets(self):
        key = INTANGIBLE_ASSETS
        tags = index_map[key][1]
        goodwills = self.bs_data.find_all(tags)
        for goodwill in goodwills:
            year = self.get_fiscal_year_by_context(goodwill["contextRef"])
            self.df.loc[INTANGIBLE_ASSETS, year] = float(goodwill.text)

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
            instant_date = datetime.strptime(context.find("instant").text, "%Y-%m-%d")

            # Der Gedanke ist, wenn das Fiscal year zum Monat >6 endet, dann gehört das instant datum zum jetzigen Jahr.
            # Beispiel. FY endet am 01.September und das Jahr, das wir anschauen ist 2023. Dann ist das Geschäftsjahr, was
            # wir betrachten 2023.
            # Wenn das FY am 01. Januar endet, dann muss das instant datum dem vorherigen Jahr zugeordnet werden.
            if fiscal_year_end_date.month <= 6:
                instant_date = instant_date.replace(year=instant_date.year - 1)
        else:
            start = datetime.strptime(context.find("startDate").text, "%Y-%m-%d")
            end = datetime.strptime(context.find("endDate").text, "%Y-%m-%d")

            delta = (end - start).days
            if int(delta) < 350:
                logger.debug(f"No whole Year: {start} - {end}. Delta: {delta}")
                raise ValueError(f"No whole Year: {start} - {end}")

            # instant date which should be checked to be the fiscal year set to start date bc. start date is within
            # the fiscal year of the company usually
            instant_date = start

        return str(instant_date.year)

    def set_revenue(self):
        key = REVENUE
        tags = index_map[key][1]
        revenues = self.bs_data.find_all(tags)
        for revenue in revenues:
            try:
                year = self.get_fiscal_year_by_context(revenue["contextRef"])
                self.df.loc[REVENUE, year] = float(revenue.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {revenue}")
                pass

    def set_operating_income(self):
        key = EBIT
        tags = index_map[key][1]
        operating_incomes = self.bs_data.find_all(tags)
        for operating_income in operating_incomes:
            try:
                year = self.get_fiscal_year_by_context(operating_income["contextRef"])
                self.df.loc[EBIT, year] = float(operating_income.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {operating_income}")
                pass

    def set_interest_expenses(self):
        key = INTEREST_EXPENSE
        tags = index_map[key][1]
        interest_expenses = self.bs_data.find_all(tags)
        for interest_expense in interest_expenses:
            # Important. If interest expense is positive, set to zero, because nothing was paid, otherwise absolute value
            if float(interest_expense.text) > 0:
                expense = 0.0
            else:
                expense = abs(float(interest_expense.text))

            try:
                year = self.get_fiscal_year_by_context(interest_expense["contextRef"])
                self.df.loc[INTEREST_EXPENSE, year] = expense
            except ValueError:
                logger.debug(msg=f"ValueError for {interest_expense}")
                pass

    def set_net_income(self):
        key = NET_INCOME
        tags = index_map[key][1]
        net_incomes = self.bs_data.find_all(tags)
        for net_income in net_incomes:
            try:
                year = self.get_fiscal_year_by_context(net_income["contextRef"])
                self.df.loc[NET_INCOME, year] = float(net_income.text)
            except ValueError:
                logger.debug(msg=f"ValueError for {net_income}")
                pass
