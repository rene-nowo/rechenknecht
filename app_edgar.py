import pathlib
import time

from src.Edgar_api import EDGAR_API, discard_filings
import pandas as pd
from multiprocessing import Pool
import argparse
import logging

from src.RechenknechtBeta import RechenknechtBeta

edgar = EDGAR_API()

# The annual report, whichever form the filer uses for it. A foreign private issuer files a
# 20-F instead of a 10-K, not in addition to one, so treating only 10-K as "the annual
# report" silently excluded every one of them.
#
# Deliberately NOT 40-F (Canadian MJDS) or 6-K: 40-F filers largely report under IFRS or
# Canadian GAAP, and a 6-K is an interim report, not an annual one. A 20-F that turns out to
# be IFRS-tagged is rejected downstream by RechenknechtBeta rather than filtered here —
# the taxonomy is a property of the document, not of the form type (Sony and Toyota file
# 20-F and moved from us-gaap to IFRS at FY2022 without changing form).
ANNUAL_FORMS = ("10-K", "20-F")


def sic_code(data) -> int:
    """The numeric SIC code out of a submissions payload, or None.

    The API hands it back as a STRING ("3571", verified live for msft/baba/jpm), next to the
    sicDescription this file already read — so the industry was only ever stored as free
    text while the code that groups into standard industry ranges was thrown away one line
    later. Parsed in one place because two callers need it (search_edgar_data below and
    ingest.py's --sic-only backfill).

    Not every filer has one: a trust or a shell can carry "" here, and int("") raises. None
    then means "not classified", which is a different statement from any code.
    """
    code = str((data or {}).get("sic") or "").strip()
    return int(code) if code.isdigit() else None


def search_edgar_data(ticker: str):
    # first get all annual reports from the last years and safe them in a proper folder
    data = edgar.get_all_data(ticker)
    if data is not None:
        branche = data["sicDescription"]
        name = data["name"]
        sic = sic_code(data)

        # only filter the information about all filters
        sec_data = edgar.fillings_data_to_dataframe(data["filings"]["recent"])
        # sec_data.to_csv(f"sec_data_{ticker}.csv", sep=";")
        yearly_reports = sec_data[
            (sec_data["form"].isin(ANNUAL_FORMS)) & (sec_data["isInlineXBRL"] == 1)
            ].reset_index()
        quarter_reports = sec_data[
            (sec_data["form"] == "10-Q") & (sec_data["isInlineXBRL"] == 1)
            ].reset_index()

        # start downloading all annual report files available
        files = []
        for i in range(len(yearly_reports)):
            access_number = yearly_reports["accessionNumber"][i]
            document = yearly_reports["primaryDocument"][i]
            report_date = yearly_reports["reportDate"][i]
            file_path = edgar.get_file(access_number, document, report_date)
            files.append((file_path, report_date))

        return files, name, branche, sic

    return None, None, None, None


def analyze_company(ticker, output_path: pathlib.Path, keep_filings: bool = False):
    # Same contract as ingest.py: the downloads are a means, not an output. Without the
    # finally this path wrote into filings/ and never cleaned up, so one entry point
    # deleted and the other silently filled the same directory.
    try:
        # The SIC code is not unpacked here: this path writes a CSV, and only the database
        # has a column for it. ingest.py is where it goes somewhere.
        file_list, name, industry, _ = search_edgar_data(ticker)

        # not file_list, not "is not None": search_edgar_data returns [], not None, when no
        # matching filing exists (e.g. a ticker whose only 20-F predates inline XBRL). The
        # old `is not None` check let an empty list through to RechenknechtBeta, whose
        # calculate() loop then does nothing and writes an empty CSV instead of raising —
        # the same silent-near-empty-frame failure ingest.py's `if not file_list:` guard
        # exists to prevent for this exact function's sibling entry point.
        if not file_list:
            raise ConnectionError("No data found for ticker " + ticker)

        # None, not "USD": the filing says which currency it reports in, and passing a
        # literal here is how every foreign filer's figures would get labelled USD.
        rechner_beta = RechenknechtBeta(name, "", None, ticker, industry, file_list)
        rechner_beta.to_csv(path=output_path)
    finally:
        if not keep_filings:
            discard_filings(ticker)


def worker(args):
    ticker, output_path, keep_filings = args
    try:
        print(f"Processing {ticker}")
        analyze_company(ticker, output_path, keep_filings)
    except Exception as e:
        print(e)
    finally:
        print(f"Finished processing {ticker}")


def analyze_all(ticker_list, output_path: pathlib.Path, keep_filings: bool = False):
    # Number of worker processes
    num_workers = 10

    # Create a pool of worker processes
    with Pool(num_workers) as p:
        p.map(worker, [(ticker, output_path, keep_filings) for ticker in ticker_list])


if __name__ == "__main__":
    # Logging is configured HERE, not at import: this module is also imported by ingest.py,
    # and a basicConfig(filename=...) at module scope both crashes when the folder is missing
    # and silently wins over the entry point's own level. The folder is derived from the file
    # rather than the CWD, and created, because /logs is gitignored.
    log_dir = pathlib.Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(filename=log_dir / f"{__name__}.log", level=logging.DEBUG)

    # Read the CSV file into a DataFrame
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", help="Ticker of the company to analyze", required=False)
    parser.add_argument("--tickers", help="Several comma-separated tickers to analyze", required=False)
    parser.add_argument("--all", help="Analyze all companies", required=False, action="store_true")
    parser.add_argument("--keep-filings", action="store_true", help="do not delete filings after parsing")

    standard_output_path = (pathlib.Path(__file__).parent / "documents" / "csv").absolute()
    parser.add_argument("--output_path", help="Output file", required=False, default=standard_output_path)

    args = parser.parse_args()

    ticker_map: str = "./ticker-cik_map.txt"

    output_path = pathlib.Path(args.output_path)
    if args.ticker:
        analyze_company(ticker=args.ticker, output_path=output_path, keep_filings=args.keep_filings)
    elif args.tickers:
        ticker_list = args.tickers.split(",")
        for ticker in ticker_list:
            analyze_company(ticker=ticker, output_path=output_path, keep_filings=args.keep_filings)

            waiting_time: int = 10
            for i in range(waiting_time):
                print(f"next company in {waiting_time-i}")
                time.sleep(1)
    elif args.all:
        df = pd.read_csv(
            ticker_map,
            names=["Ticker", "CIK", "ANALYSED_RESULT"],
            header=None,
            sep="\t",
        )
        ticker_list = list(df["Ticker"][:100])
        analyze_all(ticker_list, output_path, keep_filings=args.keep_filings)
    else:
        # If no args are given, analyze Foot Locker
        ticker_symbol = "fl"
        # ticker_symbol = "pfe"
        # cProfile.run('analyze_company(ticker_symbol)')
        analyze_company(ticker_symbol, output_path, keep_filings=args.keep_filings)
