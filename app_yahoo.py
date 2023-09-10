import re
import time
from Edgar_API import EDGAR_API
from rechenknecht import Rechenknecht, RechenknechtYahoo
import pandas as pd
from bs4 import BeautifulSoup


def get_fiscal_year(ticker: str):
    edgar = EDGAR_API()
    data = edgar.get_all_data(ticker)

    # only filter the information about all filters
    sec_data = edgar.fillings_data_to_dataframe(data["filings"]["recent"])
    sec_data.to_csv(f"sec_data_{ticker}.csv", sep=";")
    yearly_reports = sec_data[
        (sec_data["form"] == "10-K") & (sec_data["isInlineXBRL"] == 1)
    ].reset_index()

    # start downloading all form 10-K files available
    files = []

    access_number = yearly_reports["accessionNumber"][0]
    document = yearly_reports["primaryDocument"][0]
    report_date = yearly_reports["reportDate"][0]
    file_path = edgar.get_file(access_number, document, report_date)

    year = report_date.split("-")[0]

    rechenknecht = Rechenknecht()
    rechenknecht.set_document(file_path)

    return files


if __name__ == "__main__":

    df = pd.read_csv(
        "./ticker-cik_map.txt",
        header=None,
        sep="\t",
    )
    df.columns = ["Ticker", "CIK", "ANALYSED_RESULT"]
    not_analyzed = []
    # tickers = ["msft", "aapl", "tsla", "googl"]
    for i in range(len(df)):
        # last interrupt eggf
        i += 4218
        # print(i)
        # if i == 4000:
        #    exit()
        ticker = df["Ticker"][i]
        # ticker = "hznp"
        print(ticker)
        # single run
        ticker = "swks"
        single_analysation = True

        try:
            # set general data
            rechner = RechenknechtYahoo(ticker)
            print(rechner)
            # request balance sheet data and put into dataframe
            # also calculates dividends -> rechner.set_paid_dividends
            rechner.set_balance_sheet_data()
            # request income statements and put into dataframe
            # also calculates returns -> rechner.calculate_returns
            rechner.set_operations_data()
            # build dataframe
            rechner.build_company_report_main_data()

            # calculate averages
            rechner.calculate_averages()

            df.at[i, "ANALYSED_RESULT"] = "Success"
            print("SUCCESS")
        except (KeyError, IndexError, TypeError, ZeroDivisionError):
            print("ERROR")
            not_analyzed.append(ticker)
            df.at[i, "ANALYSED_RESULT"] = "Error"
        except Exception as e:
            print("Timeout")
            print(e)
            df.at[i, "ANALYSED_RESULT"] = "Error"
            time.sleep(30)
        

        if single_analysation == True:
            exit()
        df.to_csv("ticker-cik_map_after.txt", sep="\t", index=False)
    print("--------------")
    print(not_analyzed)
