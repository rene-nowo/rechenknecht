import logging
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

class Rechenknecht:
    df = pd.DataFrame(
        columns=[
            "Company",
            "Quarterly Reports",
            "Years",
            "2015",
            "2016",
            "2017",
            "2018",
            "2019",
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
            "2025",
            "Averages",
            "Current Price",
            "ROI in %",
            "Kaufkurs Wunschrendite",
            "7 J. KGV",
            "KBGV",
        ],
        index=range(0, 200),
    )

    calculated_years = []

    is_first_file = True
    is_last_file = False
    latest_year = "2023"

    # in dem dokument sind alle tags und indexes für die analyse zu finden
    with open("./documents/rechenknecht_index_map.json", "r") as f:
        index_map = json.load(f)


    def __init__(self, name, isin, currency, ticker: str, sector):
        self.name = name
        self.isin = isin
        self.currency = currency
        self.ticker = ticker
        self.branche = sector
        self.get_stock_price()

    def set_report_period(self):
        pass

    def set_document(self, file_path):
        # die datei einlesen und in beautifulsoup format bringen zur analyse
        with open(
            file_path,
            "r",
        ) as f:
            data = f.read()
        document = BeautifulSoup(data, "xml")
        self.bs_data = document

    def get_stock_price(self):
        stock_info = yf.Ticker(self.ticker)        
        self.market_price = stock_info.fast_info["lastPrice"]

        #self.isin = stock_info.isin

    def set_report_date(self, report_date):
        self.report_date = report_date
        self.year = report_date.split("-")[0]

    def set_current_year(self, current_year):
        self.current_year = current_year
        self.calculated_years.append(current_year)

    def set_paid_dividends(self, dividend: float):
        pass

    def set_balance_sheet_data(self):
        # BALANCE SHEET
        # total current assets
        key = "current_assets"
        tags = self.index_map[key][1]
        current_assets = self.set_key_value(key, tags)
        # print(current_assets)

        # total current liabilties
        key = "current_liabilities"
        tags = self.index_map[key][1]
        current_liabilities = self.set_key_value(key, tags)
        # print(current_liabilities)

        # longterm_liabilities
        key = "longterm_liabilities"
        tags = self.index_map[key][1]
        long_term_debts = self.set_key_value(key, tags)
        # print(long_term_debts)

        if len(long_term_debts) == 0:
            long_term_debts = [(key, self.current_year, 0)]
            print("LONG TERM DEBTS CHECK")
            print(long_term_debts)
            exit()

        # Total stockholders’ equity
        key = "stockholders_equity"
        tags = self.index_map[key][1]
        stockholder_equities = self.set_key_value(key, tags)
        # total equity
        key = "total_equity"
        tags = self.index_map[key][1]
        total_equities = self.set_key_value(key, tags)

        self.save_balance_sheet_data(
            stockholder_equities,
            current_assets,
            current_liabilities,
            long_term_debts,
            total_equities,
        )

    def save_balance_sheet_data(
        self,
        total_stockholders_equities: list,
        current_assets: list,
        current_liabilities: list,
        longterm_liabilities: list,
        total_equities: list,
    ):

        # wenn es die letzte Datei ist, dann nimm alle Werte um auch die vorherigen Jahre zu speichern
        if self.is_last_file == True:
            keys_data = [
                total_stockholders_equities,
                current_assets,
                current_liabilities,
                longterm_liabilities,
                total_equities,
            ]
            for key_data in keys_data:
                for tupel in key_data:
                    key = tupel[0]
                    year = tupel[1]
                    result = tupel[2]

                    self.save_value_in_dataframe(key, year, result)

        else:
            total_stockholders_equity = total_stockholders_equities[0]
            total_equity = total_equities[0]
            current_liability = current_liabilities[0]
            longterm_liability = longterm_liabilities[0]
            current_asset = current_assets[0]

            # print("Total Stockholder Equity: ", total_stockholders_equity)
            # print("Total Equity: ", total_equity)
            # print("Current Liability: ", current_liability)
            # print("Current Long Term Debt: ", longterm_liability)
            # print("Current Assets: ", current_asset)

            # print("Current Liquidity: ", (current_asset - current_liabilities))
            # current_liquidity = current_asset - current_liabilities
            # self.df.at[9, str(self.current_year)] = current_liquidity
            # self.total_stockholders_equity = total_stockholders_equity
            # self.total_equity = total_equity

            # self.df.at[6, str(self.current_year)] = total_stockholders_equity
            self.save_value_in_dataframe(
                total_stockholders_equity[0],
                total_stockholders_equity[1],
                total_stockholders_equity[2],
            )

            # self.df.at[7, str(self.current_year)] = current_asset
            self.save_value_in_dataframe(
                current_asset[0],
                current_asset[1],
                current_asset[2],
            )
            # self.df.at[8, str(self.current_year)] = current_liabilities
            self.save_value_in_dataframe(
                current_liability[0],
                current_liability[1],
                current_liability[2],
            )
            self.df.at[10, str(self.current_year)] = longterm_liabilities
            self.save_value_in_dataframe(
                longterm_liability[0],
                longterm_liability[1],
                longterm_liability[2],
            )
            self.df.at[11, str(self.current_year)] = total_equity
            self.save_value_in_dataframe(
                total_equity[0],
                total_equity[1],
                total_equity[2],
            )

    def get_balance_sheet_data_html(self, df):
        try:
            self.assets = df["total assets"][0]
        except (KeyError, IndexError):
            pass

        try:
            self.current_assets = df["total current assets"][0]
        except (KeyError, IndexError):
            pass

        try:
            self.liabilities = df["total liabilities"][0]
        except (KeyError, IndexError):
            pass

        try:
            self.total_stockholders_equity = df["total stockholders equity"][0]
        except (KeyError, IndexError):
            try:
                self.total_stockholders_equity = df["total stockholders’ equity"][0]
            except (KeyError, IndexError):
                pass

        try:
            self.total_liabilities_and_equity = df[
                "total liabilities and stockholders’ equity"
            ][0]
        except (KeyError, IndexError):
            pass

    def set_key_value(self, key, possible_tags):
        results = []
        #print(key)
        for tag in possible_tags:
            tag_data = self.bs_data.find_all(tag)
            # wenn es keine ergebnisse gibt gar nicht checken
            if len(tag_data) != 0:
                # solange es noch keine ergebnisse gibt in der schleife bleiben
                while len(results) == 0:

                    for data in tag_data:

                        tag_value = data.text
                        unit_ref = data["unitRef"]

                        if not str(unit_ref).lower().__contains__("usd"):
                            print(
                                "Unit not known. Please Check: ",
                                unit_ref,
                                " for tag: ",
                                tag,
                            )
                        context_id = data["contextRef"]

                        #print(context_id)
                        # hier wird geprüft zu welchem Zeitraum diese Zahl gehört, damit man die Zahl auch zeitlich zuordnen kann
                        year, is_full_year = self.get_context_date(context_id)
                        
                        
                        if is_full_year:
                            if ((key, year, tag_value) in results) == False:  
                                results.append((key, year, tag_value))

        if len(results) == 0:
            print("NICHTS GEFUNDEN! Key: ", key)
            results.append((key, self.current_year, 0))

        return results

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
        
        if diff.days > 350 and (int(end_date.split("-")[0]) == int(self.current_year)):
            return True
        
        return False

    def get_context_date(self, context_id):
        context = self.bs_data.find(id=context_id)

        segments = context.find_all("segment")
        start_date = None
        # segments = 0, weil man nur werte haben will die zu keinem segement gehören
        if len(segments) == 0:
            try:
                start_date = context.find("startDate").text
                end_date = context.find("endDate").text

                is_full_year = self.check_for_full_year(context_id)

            except AttributeError:
                end_date = context.find("instant").text
                is_full_year = True

            year = end_date.split("-")[0]

            

            return year, is_full_year
        
        return None, None

    def save_value_in_dataframe(self, key, year, value):
        index = self.index_map[key][0]

        if key == "interest_expense":
            interest_expense_value = float(value)
            if interest_expense_value > 0:
                value = 0
            else:
                value = abs(interest_expense_value)

        self.df.at[index, year] = float(value)

    def set_operations_data(self):
        # OPERATIONS
        # net revenue
        key = "revenue"
        tags = self.index_map[key][1]
        revenues = self.set_key_value(key, tags)

        # print("Revenue: ", revenues)

        # ebit
        key = "ebit"
        tags = self.index_map[key][1]
        ebits = self.set_key_value(key, tags)
        # print(ebits)

        # interest expense
        key = "interest_expense"
        tags = self.index_map[key][1]
        interest_expenses = self.set_key_value(key, tags)

        if len(interest_expenses) == 0:
            interest_expenses = [(key, self.current_year, 0)]
        # print(interest_expenses)

        # net income
        key = "net_income"
        tags = self.index_map[key][1]
        net_incomes = self.set_key_value(key, tags)
        # print(net_incomes)

        self.save_statemens_of_operations(
            revenues, ebits, interest_expenses, net_incomes
        )

    def save_statemens_of_operations(
        self,
        revenues: list,
        ebits: list,
        interest_expenses: list,
        net_incomes: list,
    ):
        # wenn es die letzte Datei ist, dann nimm alle Werte um auch die vorherigen Jahre zu speichern
        #print("Aktuelles Jahr: ", self.current_year)
        #print("Letzte Datei: ", self.is_last_file)
        if self.is_last_file == True:
            keys_data = [revenues, ebits, interest_expenses, net_incomes]
            for key_data in keys_data:
                for tupel in key_data:
                    key = tupel[0]
                    year = tupel[1]
                    result = tupel[2]

                    self.save_value_in_dataframe(key, year, result)
        else:
            # 3-tuple (key, year, value)
            net_revenue = revenues[0]
            self.save_value_in_dataframe(net_revenue[0], net_revenue[1], net_revenue[2])

            ebit = ebits[0]
            self.save_value_in_dataframe(ebit[0], ebit[1], ebit[2])

            interest_expense = interest_expenses[0]
            self.save_value_in_dataframe(interest_expense[0], interest_expense[1], interest_expense[2])

            net_income = net_incomes[0]
            self.save_value_in_dataframe(net_income[0], net_income[1], net_income[2])

    def set_number_of_shares(self):

        # TAG: dei:EntityCommonStockSharesOutstanding
        key = "shares"
        tag = "dei:EntityCommonStockSharesOutstanding"
        shares = self.bs_data.find(tag)
        if shares == None:
            tag = "us-gaap:CommonStockSharesOutstanding"
            shares = self.bs_data.find(tag).text
        else:
            shares = shares.text
        self.save_number_of_shares(float(shares))

    def save_number_of_shares(self, shares):
        #print("Number of Shares: ", shares)
        self.shares = shares

        self.df.at[17, str(self.current_year)] = float(shares)

    def set_dividends(self):
        key = "dividends"
        tags = self.index_map[key][1]
        dividends = self.set_key_value(key, tags)

        # wenn es die letzte Datei ist, dann nimm alle Werte um auch die vorherigen Jahre zu speichern
        # print("Aktuelles Jahr: ", self.current_year)
        # print("Letzte Datei: ", self.is_last_file)
        if self.is_last_file == True:
            keys_data = [dividends]
            for key_data in keys_data:
                for tupel in key_data:
                    key = tupel[0]
                    year = tupel[1]
                    result = tupel[2]

                    self.save_value_in_dataframe(key, year, result)
        else:
            # 3-tuple (key, year, value)
            dividend = dividends[0]
            self.save_value_in_dataframe(dividend[0], dividend[1], dividend[2])

    def is_year_complete(self, year) -> Boolean:
        i = 6
        while i < 16:
            value = self.df[year][i]

            if value == None:
                return False

            i += 1

        return True

    def calculate_returns(self, year):
        # print("Year Complete? ", self.is_year_complete(year), year)

        # wir nehmen die anzahl der shares vom aktuellsten jahr um hier das verhältnis des buchwertes und des eps auf das heutige niveau zu ziehen
        number_of_shares = self.df[self.latest_year][17]
        # hier einbauen if year_is_complete() == True:
        # net_income / shares
        earnings_per_share = self.df[year][15] / number_of_shares
        self.save_value_in_dataframe("eps", year, earnings_per_share)

        # stockholders equity / shares
        book_value_per_share = self.df[year][6] / number_of_shares
        self.save_value_in_dataframe("book_value", year, book_value_per_share)

        # net income / total equity *100 -> für % anzeige
        return_on_assets = (self.df[year][15] / self.df[year][11]) * 100
        self.save_value_in_dataframe("roa", year, return_on_assets)

        # ebit / revenue * 100 -> für % anzeige
        ebit_margin = (self.df[year][13] / self.df[year][12]) * 100
        self.save_value_in_dataframe("ebit_margin", year, ebit_margin)

        # stockholder equity / total_equiity * 100 -> für %
        own_capital_quote = (self.df[year][6] / self.df[year][11]) * 100
        self.save_value_in_dataframe("own_capital_quote", year, own_capital_quote)

        # current assets - current liabilities
        current_liquidity = self.df[year][7] - self.df[year][8]
        self.save_value_in_dataframe("current_liquidities", year, current_liquidity)

        # interest expense
        if self.df[year][14] == 0:
            self.df[year][16] = 100
        else:
            # ebit / interest_expense -> wie lange kann das unternehmen die aktuellen Schulden von den Einnahmen zurückzahlen?
            self.df[year][16] = self.df[year][12] / self.df[year][14]

        # self.df.at[0, str(self.current_year)] = self.earnings_per_share
        # self.df.at[1, str(self.current_year)] = self.book_value_per_share
        # self.df.at[3, str(self.current_year)] = self.return_on_assets
        # self.df.at[4, str(self.current_year)] = self.ebit_margin
        # self.df.at[5, str(self.current_year)] = self.own_capital_quote
        # self.df.at[9, str(self.current_year)] = self.own_capital_quote
        # self.df.at[16, str(self.current_year)] = self.tier

    def build_company_report_main_data(self):
        self.df.at[0, "Company"] = self.name
        self.df.at[1, "Company"] = f"ISIN {self.isin}"
        self.df.at[2, "Company"] = f"Währung: {self.currency}"
        self.df.at[4, "Company"] = f"CEO since: "
        self.df.at[5, "Company"] = f"Business year ends"
        self.df.at[6, "Company"] = str(self.report_date)
        self.df.at[8, "Company"] = "B2C/B2B/D2C"
        # self.df.at[10, "Company"] = str(self.report_date)
        # self.df.at[11, "Company"] = str(self.report_date)
        # self.df.at[12, "Company"] = str(self.report_date)

        # self.df.at[0, "Quarterly Reports"] = self.name
        # self.df.at[1, "Quarterly Reports"] = f"ISIN {self.isin}"
        # self.df.at[2, "Quarterly Reports"] = f"ISIN {self.currency}"
        # self.df.at[4, "Quarterly Reports"] = f"CEO since: "
        # self.df.at[5, "Quarterly Reports"] = f"Business year ends"
        # self.df.at[6, "Quarterly Reports"] = str(self.report_date)
        self.df.at[8, "Quarterly Reports"] = "Branche: " + self.branche

        self.df.at[0, "Years"] = "EPS"
        self.df.at[1, "Years"] = "Buchwert/Aktie"
        self.df.at[2, "Years"] = "Dividende/Aktie"
        self.df.at[3, "Years"] = "Return On Assets"
        self.df.at[4, "Years"] = "EBIT-Marge"
        self.df.at[5, "Years"] = "Eigenkapitalquote in %"

        self.df.at[6, "Years"] = "Total Stockholder Equity"
        self.df.at[7, "Years"] = "Total Current Asset"
        self.df.at[8, "Years"] = "Total Current Liabilities"
        self.df.at[9, "Years"] = "Nettoumlaufvermögen"
        self.df.at[10, "Years"] = "Longterm Liabilities"

        self.df.at[11, "Years"] = "Bilanzsumme/Total Equity"
        self.df.at[12, "Years"] = "Umsatz/Net Revenue"
        self.df.at[13, "Years"] = "EBIT/operatives Ergebnis"
        self.df.at[14, "Years"] = "Zinszahlungen/Interest Expense"
        self.df.at[15, "Years"] = "KONZERN Ergebnis der Anteilseigner/Net Income"
        self.df.at[16, "Years"] = "TIER"

        self.df.at[17, "Years"] = "Aktienanzahl"

    def calculate_averages(self):
        years_total = len(self.calculated_years)
        eps_average = 0
        roa_average = 0
        dividends_average = 0
        ebit_average = 0
        own_capital_average = 0

        for year in self.calculated_years:
            eps_average += self.df[str(year)][0]
            dividends_average += self.df[str(year)][2]
            roa_average += self.df[str(year)][3]
            ebit_average += self.df[str(year)][4]
            own_capital_average += self.df[str(year)][5]

        
        eps_average = eps_average / years_total
        dividends_average = dividends_average / years_total
        roa_average = roa_average / years_total
        ebit_average = ebit_average / years_total
        own_capital_average = own_capital_average / years_total

        logging.debug("EPS Average: " + str(eps_average))
        logging.debug("Dividends Average: " + str(dividends_average))
        logging.debug("ROA Average: " + str(roa_average))
        logging.debug("EBIT Average: " + str(ebit_average))
        logging.debug("Own capital Average: " + str(own_capital_average))

        self.df.at[0, "Averages"] = eps_average
        self.df.at[2, "Averages"] = dividends_average
        self.df.at[3, "Averages"] = roa_average
        self.df.at[4, "Averages"] = ebit_average
        self.df.at[5, "Averages"] = own_capital_average

        self.df.at[0, "Current Price"] = self.market_price
        self.df.at[0, "ROI in %"] = (eps_average / self.market_price) * 100
        self.df.at[0, "7 J. KGV"] = self.market_price / eps_average
        try:
            self.df.at[0, "KBGV"] = (self.market_price / self.df["2022"][1]) * (
                self.market_price / eps_average
            )
        except ZeroDivisionError as e:
            print(e)
            logging.error(str(e))

        print("storing result at ... "+ "./documents/analyzed_files/Rechenknecht " + str(self.name) + ".xlsx")
        self.df.to_excel(
            "./documents/analyzed_files/Rechenknecht " + str(self.name) + ".xlsx"
        )

