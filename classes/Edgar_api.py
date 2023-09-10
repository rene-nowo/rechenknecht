import requests
import json
import pandas as pd
import os


class EDGAR_API:

    headers = {"user-agent": "Rene Nowotny renenowo98@gmail.com"}

    """
    first choose ticker i.e SWKS for Skywork Solutions
    then use get_all_data to create a directory and create a dataframe with all submissions
    use get_file to download one specific file at a time
    
    """

    def __init__(self) -> None:
        cik_map = pd.read_csv(
            "../helpful_files/ticker-cik_map.txt", sep="\t", header=None
        )
        cik_map.set_index(0, inplace=True)
        self.cik_map = cik_map

    def get_all_data(self, ticker: str, retries=3):

        if retries > 0:
            self.ticker = ticker
            dir_path = "../fillings/" + str(ticker)
            # create directory for all following files
            if os.path.exists(dir_path) == False:
                os.makedirs(dir_path.lower())

            self.cik = self.cik_map[1][ticker.lower()]
            cik = self.generate_cik_format(str(self.cik))
            req = requests.get(
                "https://data.sec.gov/submissions/CIK{cik}.json".format(cik=cik),
                headers=self.headers,
            )

            if req.status_code == 200:
                return json.loads(req.text)
            else:
                self.get_all_data(ticker, retries-1)
        
        return None
        

    def generate_cik_format(self, cik: str):
        while len(cik) < 10:
            cik = "0" + cik

        return cik

    def fillings_data_to_dataframe(self, fillings: json):
        self.df = pd.DataFrame(fillings)

        return self.df

    def get_file(self, accession_number: str, primary_document: str, report_date: str):
        file_path = (
            "../fillings/"
            + self.ticker.lower()
            + "/"
            + report_date
            + "_"
            + accession_number
            + "_"
            + primary_document
        )

        if os.path.exists(file_path) == False:
            # https://www.sec.gov/ix?doc=/Archives/edgar/data/4127/0000004127-19-000049/fy1910k92719.htm
            url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{accession_number.replace('-','')}/{primary_document}"
            print(url)
            #  BEISPIEL für XML url = "https://www.sec.gov/Archives/edgar/data/4127/000000412721000058/swks-20211001_htm.xml"
            url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{accession_number.replace('-','')}/{str(primary_document).replace('.htm','_htm.xml')}"

            req = requests.get(url, headers=self.headers)

            if req.status_code != 200:
                print(req.status_code)
                print(req.text)
                exit()

            with open(file_path, "w") as f:
                f.writelines(req.text)
                f.close()

        return file_path
