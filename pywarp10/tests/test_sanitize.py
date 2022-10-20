import datetime

import pandas as pd
import pytest

from pywarp10.gts import GTS
from pywarp10.sanitize import SanitizeError, desanitize, sanitize


def test_sanitize():
    object = {
        "string": "foo",
        "numeric": 1,
        "boolean": True,
        "list": [1, 2, 3],
        "dict": {},
        "date_string": "2020-01-01",
        "date_datetime": pd.Timestamp("2020-01-01"),
        "date_timedelta": pd.Timedelta("1d"),
        "date_date": datetime.date(2020, 1, 1),
        "duration": "1h",
        "string_number": "1871",
        "warpscript": "ws:foo",
    }
    result = """{
 'string' 'foo'
 'numeric' 1
 'boolean' TRUE
 'list' [ 1 2 3 ]
 'dict' {}
 'date_string' '2020-01-01T00:00:00.000000Z'
 'date_datetime' 1577854800000000
 'date_timedelta' 86400000000
 'date_date' 1577854800000000
 'duration' 3600000000
 'string_number' '1871'
 'warpscript' foo
}"""
    assert sanitize(object) == result

    # Test error
    with pytest.raises(SanitizeError):
        sanitize(("foo", "bar"))


def test_desanitize():

    gts = {
        "c": "metric",
        "l": {"foo": "bar"},
        "a": {"foo": "bar"},
        "la": {"foo": "bar"},
        "v": [[1, 2]],
    }
    pd.testing.assert_frame_equal(desanitize(gts), GTS(gts))
    pd.testing.assert_frame_equal(desanitize([gts]), GTS([gts]))
    result = desanitize([1, 2, gts])
    assert result[0:-1] == (1, 2)
    pd.testing.assert_frame_equal(result[-1], GTS(gts))
