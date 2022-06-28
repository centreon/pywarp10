import pandas as pd
import pytest

from pywarp10.gts import GTS, LGTS, is_gts, is_lgts

gts_pickle = {
    "timestamps": range(10),
    "values": range(10),
    "classname": "metric",
    "labels": {"foo": "bar"},
}

gts = {
    "c": "metric",
    "l": {"foo": "bar"},
    "a": {"foo": "bar"},
    "la": {"foo": "bar"},
    "v": [[1e13, 2]],
}


def test_gts():
    empty_gts = gts_pickle.copy()
    empty_gts["timestamps"] = []
    empty_gts["values"] = []
    res = GTS(empty_gts)
    pd.testing.assert_frame_equal(
        res.to_pandas(), pd.DataFrame({"classname": "metric", "foo": "bar"}, index=[0])
    )
    missing_classname_gts = gts_pickle.copy()
    del missing_classname_gts["classname"]
    assert not is_gts(missing_classname_gts)
    with pytest.raises(TypeError):
        GTS(missing_classname_gts)
    toomuch_columns_gts = gts_pickle.copy()
    toomuch_columns_gts["foo"] = "bar"
    assert not is_gts(toomuch_columns_gts)
    assert is_gts(gts)
    pd.testing.assert_frame_equal(
        GTS(gts).to_pandas(),
        pd.DataFrame(
            {
                "timestamps": pd.to_datetime(1e13, unit="us"),
                "values": 2,
                "classname": "metric",
                "foo": "bar",
            },
            index=[0],
        ),
    )


def test_lgts():
    lgts = [gts_pickle]
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
    pd.testing.assert_frame_equal(
        df, res.sort_values("timestamps").reset_index(drop=True)
    )
