import pytest

from pywarp10.pywarp10 import SanitizeError, Warpscript


def test_sanitize(mocker):
    ws = Warpscript()

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
    ws = Warpscript()
    object = {
        "token": "token",
        "class": "~.*",
        "labels": "{}",
        "start": "2020-01-01T00:00:00.000000Z",
        "end": "2021-01-01T00:00:00.000000Z",
    }
    result = "{\n 'token' 'token'\n 'class' '~.*'\n 'labels' '{}'\n 'start' '2020-01-01T00:00:00.000000Z'\n 'end' '2021-01-01T00:00:00.000000Z'\n} FETCH\n"
    assert ws.script(object, fun="FETCH").warpscript == result
