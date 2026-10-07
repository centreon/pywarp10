import tempfile
import warnings
from ast import Assert
from socket import gaierror

import pandas as pd
import pytest
from py4j.protocol import Py4JJavaError  # type: ignore
from requests import Response
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
    assert len(res) == 2
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
    assert (
        repr(ws)
        == f"Warp10 requests sent to {host}:443/api/v0/exec (http)\nscript: 0 lines, 0 characters"
    )

    ws = Warpscript(host)
    ws.script("foo")
    assert (
        repr(ws)
        == f"Warp10 requests sent to {host}:25333 (py4j)\nscript: 1 lines, 7 characters"
    )

    ws = Warpscript("http://dummy.com", connection="http")
    assert (
        repr(ws)
        == "Warp10 requests sent to http://dummy.com:8080/api/v0/exec (http)\nscript: 0 lines, 0 characters"
    )


TOKEN = "s3cr3t-Read-T0ken.with_chars"


def fetch_script(ws: Warpscript) -> Warpscript:
    with tempfile.NamedTemporaryFile("w", suffix=".mc2", delete=False) as fp:
        fp.write("$token 'class' 'foo' 'labels' {} 'count' 1 FETCH\n")
    return (
        ws.script({"token": TOKEN, "class": "~.*", "labels": {}}, fun="FETCH")
        .script(f"ws:'{TOKEN}' 'write' STORE")
        .load(fp.name, token=TOKEN)
    )


def test_repr_hides_tokens():
    for connection in ("py4j", "http"):
        ws = fetch_script(Warpscript("127.0.0.1", connection=connection))
        assert TOKEN in ws.warpscript
        assert TOKEN not in repr(ws)
        assert TOKEN not in str(ws)


def test_exec_note_hides_tokens_py4j(mocker):
    gateway = mocker.patch("pywarp10.pywarp10.java_gateway.JavaGateway").return_value
    gateway.entry_point.newStack.return_value.execMulti.side_effect = RuntimeError(
        "boom"
    )
    ws = fetch_script(Warpscript("127.0.0.1"))

    with pytest.raises(RuntimeError) as excinfo:
        ws.exec()

    notes = "\n".join(excinfo.value.__notes__)
    assert "127.0.0.1:25333 (py4j)" in notes
    assert TOKEN not in notes
    gateway.close.assert_called_once()


def test_exec_note_hides_tokens_http(mocker):
    response = Response()
    response.status_code = 500
    response.url = "http://127.0.0.1:8080/api/v0/exec"
    post = mocker.patch("pywarp10.pywarp10.requests.post", return_value=response)
    ws = fetch_script(Warpscript("http://127.0.0.1", connection="http"))

    with pytest.raises(HTTPError) as excinfo:
        ws.exec()

    assert TOKEN in post.call_args.kwargs["data"].decode()
    notes = "\n".join(excinfo.value.__notes__)
    assert "http://127.0.0.1:8080/api/v0/exec (http)" in notes
    assert TOKEN not in notes
    assert TOKEN not in str(excinfo.value)
