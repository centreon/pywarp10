import pandas as pd
import pytest

from pywarp10.gts import GTS, is_gts, is_lgts

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

df = pd.DataFrame({"metric": range(10), "foo": "bar"})


def test_gts():
    with pytest.raises(TypeError):
        GTS(1)
    empty_gts = gts_pickle.copy()
    empty_gts["timestamps"] = []
    empty_gts["values"] = []
    res = GTS(empty_gts)
    pd.testing.assert_frame_equal(
        res, pd.DataFrame({"classname": ["metric"], "foo": ["bar"]}, index=[0])
    )
    pd.testing.assert_frame_equal(
        GTS({"c": "", "l": {}, "a": {}, "la": 0, "v": []}), pd.DataFrame()
    )
    pd.testing.assert_frame_equal(df, pd.DataFrame(GTS(df)))
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
        GTS(gts),
        pd.DataFrame(
            {
                "metric": 2,
                "foo": "bar",
            },
            index=[pd.to_datetime(1e13, unit="us")],
        ),
    )


def test_lgts():
    lgts = [gts]
    assert is_lgts(lgts)
    assert len(GTS(lgts)) == 1
    assert not is_lgts([])
    assert not is_lgts([1])
    with pytest.raises(TypeError):
        GTS([1])
