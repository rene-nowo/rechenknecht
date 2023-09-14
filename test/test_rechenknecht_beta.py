import pathlib
import pytest

from app_edgar import search_edgar_data
from classes.RechenknechtBeta import RechenknechtBeta
import pandas as pd
import pandas.testing as pd_testing


def test_foot_locker():
    file_list, name, industry = search_edgar_data("fl")
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
