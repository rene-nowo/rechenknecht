import pathlib
import re
from classes.Edgar_api import EDGAR_API
import pandas as pd
from multiprocessing import Pool
import argparse
import logging
import cProfile

# Configure logging
from classes.RechenknechtBeta import RechenknechtBeta

logging.basicConfig(filename='./logs/' + __name__ + '.log', level=logging.DEBUG)

edgar = EDGAR_API()


def search_edgar_data(ticker: str):
    # first get all documents (form 10-K) from last years and safe them in a proper folder
    data = edgar.get_all_data(ticker)
    if data is not None:
        branche = data["sicDescription"]
        name = data["name"]

        # only filter the information about all filters
        sec_data = edgar.fillings_data_to_dataframe(data["filings"]["recent"])
        # sec_data.to_csv(f"sec_data_{ticker}.csv", sep=";")
        yearly_reports = sec_data[
            (sec_data["form"] == "10-K") & (sec_data["isInlineXBRL"] == 1)
            ].reset_index()
        quarter_reports = sec_data[
            (sec_data["form"] == "10-Q") & (sec_data["isInlineXBRL"] == 1)
            ].reset_index()

        # start downloading all form 10-K files available
        files = []
        for i in range(len(yearly_reports)):
            access_number = yearly_reports["accessionNumber"][i]
            document = yearly_reports["primaryDocument"][i]
            report_date = yearly_reports["reportDate"][i]
            file_path = edgar.get_file(access_number, document, report_date)
            files.append((file_path, report_date))

        return files, name, branche

    return None, None, None


def analyze_company(ticker):
    file_list, name, industry = search_edgar_data(ticker)

    if file_list is not None:
        rechner_beta = RechenknechtBeta(name, "", "USD", ticker, industry, file_list)
        rechner_beta.to_csv(pathlib.Path("documents/csv"))

        """
        # rechner = Rechenknecht(ticker)
        rechner.set_report_date(file_list[0][1])

        # es wird immer bei der neusten Datei angefangen bzw. der letzten Meldung
        for file_path, report_date in file_list:
            # setze das jahr welches gerade bearbeitet wird
            year = report_date.split("-")[0]

            # wenn das geschäftsjahr ungerade ist, kann es sein dass das report date jahr und das current_year ungleich sind
            # dann muss das jahr so angepasst werden, dass die Werte auch der richtigen Spalte bzw. dem richtigen jahr zugeordnet werden
            # beispiel bericht für 2022 aber geschäftsjahr bis  Feb 2022 -> dann muss die Spalte 2022 genommen werden und nicht 2023
            if file_path != file_list[0][0]:
                rechner.is_first_file = False
            else:
                rechner.latest_year = year

            # wird benutzt, damit man bei der letzten Datei alle Jahre speichert die vorhanden sind, da in 2019 auch daten aus 2018 vorhanden sind
            if file_path == file_list[-1][0]:
                rechner.is_last_file = True

            rechner.set_current_year(year)

            # übergib dem rechner das aktuelle dokument
            rechner.set_document(file_path)
            # baue das gerüst für die analyse auf
            rechner.build_company_report_main_data()
            # finde die anzahl der shares zu dem aktuellen jahr
            rechner.set_number_of_shares()
            # finde ausgezahlte dividenden
            rechner.set_dividends()
            # finde die hauptinformationen
            rechner.set_operations_data()
            rechner.set_balance_sheet_data()
            rechner.calculate_returns(year)

            if rechner.is_last_file:
                year = int(year) - 1
                # you can not append this year, because the number of shares is not
                rechner.calculated_years.append(year)
                rechner.calculate_returns(str(year))

        rechner.calculate_averages()"""


def worker(ticker):
    try:
        print(f"Processing {ticker}")
        analyze_company(ticker)
    except Exception as e:
        print(e)
    finally:
        print(f"Finished processing {ticker}")


def analyze_all(ticker_list):
    # Number of worker processes
    num_workers = 10

    # Create a pool of worker processes
    with Pool(num_workers) as p:
        p.map(worker, ticker_list)


if __name__ == "__main__":
    # Read the CSV file into a DataFrame
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Ticker of the company to analyze", required=False)
    parser.add_argument("--all", help="Analyze all companies", required=False, action="store_true")
    args = parser.parse_args()

    ticker_map: str = "./ticker-cik_map.txt"

    if args.ticker:
        # analyze_company(args.ticker)
        cProfile.run('analyze_company(args.ticker)')
    elif args.all:
        df = pd.read_csv(
            ticker_map,
            names=["Ticker", "CIK", "ANALYSED_RESULT"],
            header=None,
            sep="\t",
        )
        ticker_list = list(df["Ticker"][:100])
        analyze_all(ticker_list)
    else:
        # If no args are given, analyze Foot Locker
        ticker_symbol = "pfe"
        # ticker_symbol = "pfe"
        # cProfile.run('analyze_company(ticker_symbol)')
        analyze_company(ticker_symbol)
