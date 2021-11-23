from gts import LGTS
import pytest
import pandas as pd


def test_dataframe():
    df = pd.DataFrame(
        {
            "timestamps": range(5),
            "values": range(5),
            "label1": ["1", "1", "1", "2", "2"],
            "label2": ["2", "2", "1", "1", "1"],
        }
    )
    res = LGTS.from_dataframe(df)
    res["timestamps"] = [int(x) for x in pd.to_numeric(res["timestamps"]) / 1000]
    pd.testing.assert_frame_equal(
        df, res.sort_values("timestamps").reset_index(drop=True)
    )
