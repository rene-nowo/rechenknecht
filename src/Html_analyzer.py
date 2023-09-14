from operator import index
import pandas as pd
from bs4 import BeautifulSoup
import xml
import re


class HTML_analyzer:
    def __init__(self, file_path) -> None:

        self.file_path = file_path

    def convert_html_to_dataframe(self):
        df = pd.DataFrame(
            columns=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M"]
        )

        with open(
            self.file_path,
            "r",
        ) as f:
            data = f.read()

        # regex = re.compile(
        #    r"(>Item(\s| | )(1A|1B|7A|7|8)\.{0,1})|(ITEM\s(1A|1B|7A|7|8))"
        # )

        data = data[data.find("<html>") :]

        document = BeautifulSoup(data, "html")
        """ find all table values and ignore rest """
        tables = document.find_all("tr")
        """ ignore these characters since not needed for now"""
        unnecessary_words = ["", "\xa0", "$", "þ", "%"]

        """ build the dataframe"""
        for table in tables:
            data_list = [
                str(data.text).lower()
                for data in table
                if data.text not in unnecessary_words
            ]

            if len(data_list) != 0:
                print(data_list)
                # df.loc[len(df)] = data_list
                df = df.append(
                    pd.Series(data_list, index=df.columns[: len(data_list)]),
                    ignore_index=True,
                )

                """
                for table_data in table:
                    print(table_data.text)

                    if str(table_data.text).lower().__contains__("net revenue"):
                        print(data_list)
                        print(df)
                        exit()"""

        return df

    def get_operations_data(self, df):
        # df.set_index("A", inplace=True)
        try:
            revenue = df[df["A"] == "net revenue"]
        except (IndexError):
            print("Revenue not found!")

        try:
            gross_profit = df[df["A"] == "gross profit"]
        except (KeyError, IndexError):
            pass

        try:
            operating_income = df[df["A"] == "operating income"]
        except (KeyError, IndexError):
            pass

        try:
            ebit = df[df["A"] == "income before income taxes"]
        except (KeyError, IndexError):
            pass

        try:
            net_income = df[df["A"] == "net income"]
        except (KeyError, IndexError):
            pass

        try:
            comp_income = df[df["A"] == "comprehensive income"]
        except (KeyError, IndexError):
            pass

        print(revenue)
        print(gross_profit)
        print(operating_income)
        print(ebit)
        print(net_income)
        print(comp_income)

        all_lengths = [
            len(revenue),
            len(gross_profit),
            len(operating_income),
            len(ebit),
            len(net_income),
            len(comp_income),
        ]

        index_min = min(range(len(all_lengths)), key=all_lengths.__getitem__)
        # self.set_statemens_of_operations(revenue, operating_income, 0, net_income)
        if index_min == 5:
            base_index = comp_income.index[0]

            """ get which value in revenues is closest to base index"""
            revenue_index = self.find_lowest_diff(base_index, list(revenue.index))
            revenue = revenue["B"][revenue_index]
            print(revenue)

            revenue_index = self.find_lowest_diff(base_index, list(revenue.index))
            revenue = revenue["B"][revenue_index]
            print(revenue)

            revenue_index = self.find_lowest_diff(base_index, list(revenue.index))
            revenue = revenue["B"][revenue_index]
            print(revenue)

            revenue_index = self.find_lowest_diff(base_index, list(revenue.index))
            revenue = revenue["B"][revenue_index]
            print(revenue)

    def find_lowest_diff(self, base, index_list):
        diff_list = []

        for v in index_list:
            diff_list.append(abs(base - v))

        index_min = min(range(len(diff_list)), key=diff_list.__getitem__)

        print(diff_list)
        print(index_min)

        return index_list[index_min]


file_path = "/Users/renenowotny/Documents/Programmierung/python/sec/fillings/swks/2016-09-30_0000004127-16-000068_fy1610k93016.htm"

analyzer = HTML_analyzer(file_path)

# df = analyzer.convert_html_to_dataframe()
# df.to_csv("skwork_2016.csv", sep=";")

df = pd.read_csv("skwork_2016.csv", sep=";")

analyzer.get_operations_data(df)
