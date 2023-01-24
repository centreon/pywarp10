import tempfile
import warnings
from ast import Assert
from socket import gaierror

import pandas as pd
import pytest
from py4j.protocol import Py4JJavaError
from requests.exceptions import HTTPError

from pywarp10.pywarp10 import Warpscript


def test_script_convert():
    ws = Warpscript()
    object = {
        "token": "token",
        "class": "~.*",
        "labels": "{}",
        "start": "2020-01-01T00:00:00.000000Z",
        "end": "2021-01-01T00:00:00.000000Z",
    }
    result = "{\n 'token' 'token'\n 'class' '~.*'\n 'labels' '{}'\n 'start' 1577836800000000\n 'end' 1609459200000000\n} FETCH\n"  # noqa: E501
    assert ws.script(object, fun="FETCH").warpscript == result


def test_warpscript():
    with pytest.raises(ValueError):
        Warpscript(connection="wrong_type")
    ws = Warpscript(host="https://sandbox.senx.io", connection="http")
    with tempfile.NamedTemporaryFile(delete=False) as fp:
        fp.write(b"$foo")
        fp.seek(0)
        res = ws.load(fp.name, foo="bar").exec()
        assert res == "bar"
    assert ws.script(3).exec() == 3
    script = """ws:
    [ 
        NEWGTS 
        'foo' RENAME 
        0 NaN NaN NaN 0 ADDVALUE 
        NEWGTS 
        'bar' RENAME 
        1 NaN NaN NaN 1 ADDVALUE 
    ]
    """
    request = ws.script(script)
    pd.testing.assert_frame_equal(
        request.exec(reset=False),
        pd.DataFrame([{"foo": 0, "index": 0}, {"bar": 1, "index": 1}])
        .set_index("index")
        .reset_index(drop=True),
    )
    assert request.exec(reset=False, raw=True) == [
        [
            {"c": "foo", "l": {}, "a": {}, "la": 0, "v": [[0, 0]]},
            {"c": "bar", "l": {}, "a": {}, "la": 0, "v": [[1, 1]]},
        ]
    ]
    res = request.exec(bind_lgts=False)
    pd.testing.assert_frame_equal(res[0], pd.DataFrame({"foo": 0}, index=[0]))
    pd.testing.assert_frame_equal(res[1], pd.DataFrame({"bar": 1}, index=[1]))

    object = pd.DataFrame({"foo": [1]}, index=[1])
    try:
        ws = Warpscript(host="metrics.nlb.qual.internal-mycentreon.net")
        result = ws.script("ws:NEWGTS 'foo' RENAME 1 NaN NaN NaN 1 ADDVALUE").exec()
        with pytest.raises(Py4JJavaError):
            ws.script("ws:foo").exec()
    except gaierror:
        warnings.warn(
            "Cannot connect to metrics.nlb.qual.internal-mycentreon.net, some tests will be skipped."  # noqa: E501
        )
        result = object
    pd.testing.assert_frame_equal(object, pd.DataFrame(result))

    with pytest.raises(HTTPError):
        Warpscript(host="http://sandbox.senx.io/dummy", connection="http").script(
            "foo"
        ).exec()


def test_repr():
    host = "https://sandbox.senx.io"
    ws = Warpscript(host, connection="http")
    assert repr(ws) == f"Warp10 requests sent to {host}:443/api/v0/exec\nscript: \n"

    ws = Warpscript(host)
    ws.script("foo")
    assert repr(ws) == f"Warp10 requests sent to {host}:25333\nscript: \n'foo' \n"

    ws = Warpscript("http://dummy.com", connection="http")
    assert (
        repr(ws)
        == "Warp10 requests sent to http://dummy.com:8080/api/v0/exec\nscript: \n"
    )
