import tempfile

import pytest

from pywarp10.pywarp10 import SanitizeError, Warpscript


def test_sanitize():
    ws = Warpscript()

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


def test_warpscript():
    with pytest.raises(ValueError):
        Warpscript(connection="wrong_type")
    ws = Warpscript(host="https://sandbox.senx.io", connection="http")
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(b"$foo")
        fp.seek(0)
        res = ws.load(fp.name, foo="bar").exec()
        assert res == "bar"
    assert ws.script(3).exec() == 3

    ws = Warpscript(host="metrics.nlb.qual.internal-mycentreon.net")
    assert ws.script("foo").exec() == "foo"


def test_repr():
    host = "https://sandbox.senx.io"
    ws = Warpscript(host, connection="http")
    assert repr(ws) == f"Warp10 server connected on {host}\nscript: \n"

    ws = Warpscript(host)
    ws.script("foo")
    assert repr(ws) == f"Warp10 server connected on {host}:25333\nscript: \n'foo' \n"
