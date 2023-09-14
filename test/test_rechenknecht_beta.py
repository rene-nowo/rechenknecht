import pathlib
import pytest

from app_edgar import search_edgar_data
from src.RechenknechtBeta import RechenknechtBeta
import pandas as pd
import pandas.testing as pd_testing


def test_foot_locker():
    file_list = [
        (pathlib.Path(__file__).parent / "resources" / "filings" / "fl" / "2023-01-28_0000850209-23-000006_fl-20230128x10k.htm", "2023-01-28"),
        (pathlib.Path(__file__).parent / "resources" / "filings" / "fl" / "2022-01-29_0000850209-22-000003_fl-20220129x10k.htm", "2022-01-29"),
        (pathlib.Path(__file__).parent / "resources" / "filings" / "fl" / "2021-01-30_0000850209-21-000003_fl-20210130x10k.htm", "2021-01-30"),
        (pathlib.Path(__file__).parent / "resources" / "filings" / "fl" / "2020-02-01_0000850209-20-000007_fl-20200201x10k.htm", "2020-02-01")
    ]
    name = "FOOT LOCKER, INC."
    industry = "Retail-Shoe Stores"
    if file_list:
        expected: pd.DataFrame = pd.read_csv(pathlib.Path(__file__).parent / "resources" / "footlocker_expected.csv",
                                             index_col=0)
        expected.drop(columns=["7_YEAR_AVG"], inplace=True)

        rechner = RechenknechtBeta(name, "", "USD", "fl", industry, file_list)
        actual = rechner.df

        actual.to_csv(pathlib.Path(__file__).parent / "resources" / "footlocker_actual.csv")
        actual = pd.read_csv(pathlib.Path(__file__).parent / "resources" / "footlocker_actual.csv", index_col=0)
        actual.drop(columns=["7_YEAR_AVG"], inplace=True)

        pd_testing.assert_frame_equal(expected, actual, check_exact=False)
    else:
        pytest.fail("file_list is empty")
