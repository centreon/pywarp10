from pywarp10 import SanitizeError, Warpscript
import pytest
import pandas as pd

ws = Warpscript()


def test_sanitize():
    ws = Warpscript(host)

    object = {
        "string": "foo",
        "numeric": 1,
        "boolean": True,
        "list": [],
        "dict": {},
        "date": "2020-01-01",
        "string_number": "1871",
    }
    result = "{\n 'string' 'foo'\n 'numeric' 1\n 'boolean' TRUE\n 'list' []\n 'dict' {}\n 'date' '2020-01-01T00:00:00.000000Z'\n 'string_number' '1871'\n}"
    assert ws.sanitize(object) == result

    # Test error
    with pytest.raises(SanitizeError):
        ws.sanitize(("foo", "bar"))


def test_script_convert():
    ws = Warpscript(host)
    object = {
        "token": "token",
        "class": "~.*",
        "labels": "{}",
        "start": "2020-01-01T00:00:00.000000Z",
        "end": "2021-01-01T00:00:00.000000Z",
    }
    result = "{\n 'token' 'token'\n 'class' '~.*'\n 'labels' '{}'\n 'start' '2020-01-01T00:00:00.000000Z'\n 'end' '2021-01-01T00:00:00.000000Z'\n} FETCH\n"
    assert ws.script(object, fun="FETCH").warpscript == result


def test_dataframe():
    ws = Warpscript(host)
    df = pd.DataFrame(
        {
            "timestamps": range(5),
            "values": range(5),
            "label1": ["1", "1", "1", "2", "2"],
            "label2": ["2", "2", "1", "1", "1"],
        }
    )
    res = ws.dataframe_to_gts(df).exec()
    res["timestamps"] = [int(x) for x in pd.to_numeric(res["timestamps"]) / 1000]
    pd.testing.assert_frame_equal(df, res)
