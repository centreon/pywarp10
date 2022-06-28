import tempfile
import warnings
from ast import Assert
from socket import gaierror

import pandas as pd
import pytest

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

    try:
        ws = Warpscript(host="metrics.nlb.qual.internal-mycentreon.net")
        df = ws.script("ws:NEWGTS 'foo' RENAME 1 NaN NaN NaN 1 ADDVALUE").exec()
    except gaierror:
        warnings.warn(
            "Cannot connect to metrics.nlb.qual.internal-mycentreon.net, some tests will be skipped."
        )
    pd.testing.assert_frame_equal(
        df,
        pd.DataFrame(
            {"timestamps": [1], "values": [1], "classname": ["foo"]}, index=[0]
        ),
    )


def test_repr():
    host = "https://sandbox.senx.io"
    ws = Warpscript(host, connection="http")
    assert repr(ws) == f"Warp10 server connected on {host}\nscript: \n"

    ws = Warpscript(host)
    ws.script("foo")
    assert repr(ws) == f"Warp10 server connected on {host}:25333\nscript: \n'foo' \n"
