from pywarp10.gts import LGTS, GTS, is_lgts, is_gts
import pandas as pd
import pytest


gts = {
    "timestamps": range(10),
    "values": range(10),
    "classname": "metric",
    "labels": {"foo": "bar"},
}


def test_gts():
    empty_gts = gts.copy()
    empty_gts["timestamps"] = []
    empty_gts["values"] = []
    res = GTS(empty_gts)
    pd.testing.assert_frame_equal(
        res.data, pd.DataFrame({"classname": "metric", "foo": "bar"}, index=[0])
    )
    missing_classname_gts = gts.copy()
    del missing_classname_gts["classname"]
    assert not is_gts(missing_classname_gts)
    toomuch_columns_gts = gts.copy()
    toomuch_columns_gts["foo"] = "bar"
    assert not is_gts(toomuch_columns_gts)


def test_lgts():
    lgts = [gts]
    assert is_lgts(lgts)
    assert len(LGTS(lgts)) == 10
    assert not is_lgts([])
    assert not is_lgts([1])


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
