import pathlib

import requests
import json
import pandas as pd
import os

import DBConnector

file_path = pathlib.Path(__file__).parent.parent / "filings"

with open("../db.json", "r") as f:
    data = json.load(f)
    print(data)


def generate_cik_format(cik: str):
    while len(cik) < 10:
        cik = "0" + cik

    return cik


class EDGAR_API:
    user_agents_path = pathlib.Path(__file__).parent.parent.absolute() / "user_agent.csv"
    user_agent = pd.read_csv(user_agents_path, sep=",", header=0)
    user_agent_name = user_agent.at[0, "name"]
    user_agent_mail = user_agent.at[0, "mail"]
    headers = {f"user-agent": f"{user_agent_name} {user_agent_mail}"}

    """
    first choose ticker i.e SWKS for Skywork Solutions
    then use get_all_data to create a directory and create a dataframe with all submissions
    use get_file to download one specific file at a time
    
    """

    """
    database connection
    """
    db_con = DBConnector.DatabaseXML(data["host"], data["user"], data["pw"], data["user"])

    def __init__(self) -> None:
        self.df = None
        self.cik = None
        self.ticker = None

        cik_map_path = pathlib.Path(__file__).parent.parent.absolute() / "maps/ticker-cik_map.txt"
        cik_map = pd.read_csv(
            cik_map_path, sep="\t", header=None
        )
        cik_map.set_index(0, inplace=True)
        self.cik_map = cik_map

    def get_all_data(self, ticker: str, retries=3):

        if retries > 0:
            self.ticker = ticker
            dir_path = pathlib.Path(file_path / str(ticker)).absolute()
            # create directory for all following files
            if not os.path.exists(dir_path):
                os.makedirs(str(dir_path).lower())

            self.cik = self.cik_map[1][ticker.lower()]
            cik = generate_cik_format(str(self.cik))
            req = requests.get(
                "https://data.sec.gov/submissions/CIK{cik}.json".format(cik=cik),
                headers=self.headers,
            )

            if req.status_code == 200:
                return json.loads(req.text)
            else:
                self.get_all_data(ticker, retries - 1)

        return None

    def fillings_data_to_dataframe(self, fillings: json):
        self.df = pd.DataFrame(fillings)

        return self.df

    def get_file(self, accession_number: str, primary_document: str, report_date: str):
        file_path_to_use = (
                file_path / self.ticker.lower() / (report_date + "_" + accession_number + "_" + primary_document)
        )

        if not os.path.exists(file_path_to_use):
            # https://www.sec.gov/ix?doc=/Archives/edgar/data/4127/0000004127-19-000049/fy1910k92719.htm
            url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{accession_number.replace('-', '')}/{primary_document}"
            #  BEISPIEL für XML url = "https://www.sec.gov/Archives/edgar/data/4127/000000412721000058/swks-20211001_htm.xml"
            url = f"https://www.sec.gov/Archives/edgar/data/{self.cik}/{accession_number.replace('-', '')}/{str(primary_document).replace('.htm', '_htm.xml')}"

            req = requests.get(url, headers=self.headers)

            if req.status_code != 200:
                print(req.status_code)
                print(req.text)
                exit()

            with open(file_path_to_use, "w") as f:
                f.writelines(req.text)
                f.close()

        return file_path_to_use
