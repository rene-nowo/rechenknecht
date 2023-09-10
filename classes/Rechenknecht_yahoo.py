
class RechenknechtYahoo:

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

    # in dem dokument sind alle tags und indexes für die analyse zu finden
    with open("./documents/rechenknecht_index_map.json", "r") as f:
        index_map = json.load(f)
        # print(index_map)

    def __init__(self, ticker: str):
        self.ticker = ticker

        self.stock = yf.Ticker(ticker)
        self.rechen_master = pd.read_excel("Rechenmaster Overview.xlsx")

        self.set_general_data()

    def set_general_data(self):
        stock_info = self.stock.info

        # set name, isin, branche and currency
        self.name = stock_info["shortName"]
        print("Analyzing: " + str(self.name))
        try:
            self.sector = stock_info["sector"]
        except KeyError:
            self.sector = ""
        self.market_price = stock_info["regularMarketPrice"]
        self.isin = str(self.stock.isin)
        self.country = stock_info["country"]
        self.currency = stock_info["financialCurrency"]
        # print(stock_info["currency"])

    def set_paid_dividends(self, end_date):
        div_df = pd.DataFrame(self.stock.dividends)

        end = datetime.strptime(end_date, "%Y-%m-%d")
        start = end - relativedelta(years=1)

        div = div_df.loc[start:end_date]
        dividend = sum(div["Dividends"])
        # print(dividend)

        self.df.at[2, str(self.current_year)] = dividend

    def set_dividends_rate(self):
        div_rate = self.stock.info["dividendRate"]
        # print(div_rate)

    def set_balance_sheet_data(self):
        # BALANCE SHEET
        balance_df = pd.DataFrame(self.stock.balance_sheet)
        # balance_df.to_csv(self.name + "_balance_sheet.csv", sep=";")
        columns = balance_df.columns
        # print(columns)
        self.report_date = str(columns[0]).split(" ")[0]
        self.latest_year = str(columns[0]).split("-")[0]
        for year in balance_df.columns:
            self.current_year = str(year).split("-")[0]
            stockholder_equities = balance_df[year]["Stockholders Equity"]
            # print(stockholder_equities)
            current_assets = balance_df[year]["Current Assets"]
            current_liabilities = balance_df[year]["Current Liabilities"]
            total_liabilities = balance_df[year][
                "Total Liabilities Net Minority Interest"
            ]
            long_term_liabilities = total_liabilities - current_liabilities
            total_equities = balance_df[year]["Total Assets"]
            number_of_shares = balance_df[year]["Ordinary Shares Number"]

            self.save_balance_sheet_data(
                stockholder_equities,
                current_assets,
                current_liabilities,
                long_term_liabilities,
                total_equities,
                number_of_shares,
            )

            # start calculatiion of dividends
            self.set_paid_dividends(str(year).split(" ")[0])

    def save_balance_sheet_data(
        self,
        total_stockholders_equities: float,
        current_assets: float,
        current_liabilities: float,
        longterm_liabilities: float,
        total_equities: float,
        number_of_shares: int,
    ):

        self.df.at[6, str(self.current_year)] = total_stockholders_equities

        self.df.at[7, str(self.current_year)] = current_assets

        self.df.at[8, str(self.current_year)] = current_liabilities

        self.df.at[10, str(self.current_year)] = longterm_liabilities

        self.df.at[11, str(self.current_year)] = total_equities

        self.df.at[17, str(self.current_year)] = number_of_shares

    def set_key_value(self, key, possible_tags):
        results = []
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

                        # print(context_id)
                        # hier wird geprüft zu welchem Zeitraum diese Zahl gehört, damit man die Zahl auch zeitlich zuordnen kann
                        date = self.get_context_date(context_id)

                        if key == "dividends":
                            # bei den divididenden muss man den wert mit dem größten zeitraum wählen
                            # weil dividenden auch quartalsweise ausgezahlt werden und man direkt die summe finden möchte
                            # wenn die zahl sich nicht auf das ganze Jahr bezieht soll None zurückgegeben werden
                            date = self.check_if_correct_dividend(context_id)

                        if (
                            date != None
                            and ((key, date[0], tag_value) in results) == False
                        ):
                            results.append((key, date[0], tag_value))

        if len(results) == 0:
            print("NICHTS GEFUNDEN! Key: ", key)
            results.append((key, self.current_year, 0))

        return results

    def save_value_in_dataframe(self, key, year, value):
        index = self.index_map[key][0]

        self.df.at[index, year] = float(value)

    def set_operations_data(self):
        # OPERATIONS
        operations_df = pd.DataFrame(self.stock.income_stmt)
        # operations_df.to_csv(self.name + "_operations.csv", sep=";")
        columns = operations_df.columns
        # print(columns)
        # print(columns[0])
        for year in operations_df.columns:
            self.current_year = str(year).split("-")[0]
            self.calculated_years.append(str(year).split("-")[0])
            revenues = operations_df[year]["Total Revenue"]
            print(revenues)
            ebits = operations_df[year]["Operating Income"]
            try:
                interest_expenses = operations_df[year]["Interest Expense"]
            except KeyError:
                interest_expenses = 0

            net_incomes = operations_df[year]["Net Income"]

            self.save_statemens_of_operations(
                revenues, ebits, interest_expenses, net_incomes
            )

            self.calculate_returns()

    def save_statemens_of_operations(
        self,
        revenues: list,
        ebits: list,
        interest_expenses: list,
        net_incomes: list,
    ):

        self.df.at[12, str(self.current_year)] = revenues
        self.df.at[13, str(self.current_year)] = ebits
        self.df.at[14, str(self.current_year)] = interest_expenses
        self.df.at[15, str(self.current_year)] = net_incomes

    def is_year_complete(self, year) -> Boolean:
        i = 6
        while i < 16:
            value = self.df[year][i]

            if value == None:
                return False

            i += 1

        return True

    def calculate_returns(self):
        # print("Year Complete? ", self.is_year_complete(year), year)
        year = self.current_year
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
        self.df.at[8, "Quarterly Reports"] = "Branche: " + self.sector

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
        # print("Total Years: ", years_total)
        # print(self.calculated_years)

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

        self.df.at[0, "Averages"] = eps_average
        self.df.at[2, "Averages"] = dividends_average
        self.df.at[3, "Averages"] = roa_average
        self.df.at[4, "Averages"] = ebit_average
        self.df.at[5, "Averages"] = own_capital_average

        self.df.at[0, "Current Price"] = self.market_price
        self.df.at[0, "ROI in %"] = (eps_average / self.market_price) * 100
        self.df.at[0, "7 J. KGV"] = self.market_price / eps_average
        self.df.at[0, "KBGV"] = (self.market_price / self.df["2022"][1]) * (
            self.market_price / eps_average
        )

        self.df.to_excel(
            "./documents/analyzed_files/Rechenknecht "
            + str(self.name).replace("/", " ")
            + ".xlsx"
        )

        self.save_results_in_master(
            eps_average,
            dividends_average,
            roa_average,
            ebit_average,
            own_capital_average,
            self.market_price,
        )

    def save_results_in_master(self, eps, dividend, roa, ebit, own_cap, market_price):
        pass
        wunschrendite = 15
        # print(market_price)
        # print(self.latest_year)
        # print(float(self.df[self.latest_year][1]))
        # print(eps)

        self.rechen_master = self.rechen_master.append(
            {
                "Unnamed: 0": self.name,
                "Unnamed: 1": self.ticker,
                "Unnamed: 2": self.sector,
                "Unnamed: 3": self.currency,
                "Unnamed: 4": eps,
                "Unnamed: 5": dividend,
                "Unnamed: 6": roa,
                "Unnamed: 7": ebit,
                "Unnamed: 8": own_cap,
                "Unnamed: 9": market_price,
                "Unnamed: 10": self.df[self.latest_year][1],  # book value
                "Unnamed: 11": (eps / market_price) * 100,  # ROI
                "Unnamed: 12": (eps / wunschrendite * 100),  # Kaufkurs Wunsch
                "Unnamed: 13": market_price / eps,  # 7 J. KGV
                "Unnamed: 14": (
                    float(market_price)
                    / float(self.df[self.latest_year][1])
                    * (market_price / eps)
                ),  # Kurs/Buchwert * 7 J. KGV
            },
            ignore_index=True,
        )

        # self.rechen_master.reset_index(drop=True)

        self.rechen_master.to_excel("Rechenmaster Overview.xlsx", index=False)
