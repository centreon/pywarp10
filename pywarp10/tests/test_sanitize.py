import pandas as pd
import pytest

from pywarp10.gts import GTS, LGTS
from pywarp10.sanitize import SanitizeError, desanitize, sanitize


def test_sanitize():
    object = {
        "string": "foo",
        "numeric": 1,
        "boolean": True,
        "list": [1, 2, 3],
        "dict": {},
        "date": "2020-01-01",
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
 'date' '2020-01-01T00:00:00.000000Z'
 'duration' 3600
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
    assert desanitize(gts) == GTS(gts)
    assert desanitize([gts]) == LGTS([gts])
    assert desanitize([1, 2, gts]) == (1, 2, GTS(gts))
