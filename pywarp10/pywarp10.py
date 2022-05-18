"""Make it easy to work with warpscripts.

Easily transform python objects into valid warpscripts ones, automatically retrieve GTS 
or list of GTS as pandas dataframe.

    Typical usage example:

    ws = Warpscript()
    ws.script({"foo": "bar"})
    ws.exec()
"""


import os
import pickle as pkl  # nosec
from typing import Any, Dict, Iterable, List, Literal, Optional, Union

import dateparser
import durations
import pandas as pd
import requests
from py4j import java_gateway

from pywarp10.gts import GTS, LGTS, is_gts_pickle, is_lgts


class SanitizeError(Exception):
    """Exception for sanitize error.

    Attributes:
        type: object that could not be sanitize.
        message: explanation of the error.
    """

    def __init__(self, object: Any, message: Optional[str] = None) -> None:
        self.type = str(type(object))
        if not message:
            message = f"Could not sanitize object type `{self.type}`"
        self.message = message
        super().__init__(self.message)

    pass


class Warpscript:
    """Handle warp10 connections and wrapping.

    The main class used throughout this module to handle warpscripts.

    Attributes:
        host:
            The adress at which the warp10 server is running.
        port:
            The port to reach the warp10 server.
        warpscript:
            A string with warpscript that will be sent to warp10.
        connection:
            Define how request are made, either py4j or through an http request.
        **kwargs:
            Others arguments passed to requests if connection is http.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        connection: Literal["py4j", "html"] = "py4j",
        **kwargs,
    ) -> None:
        """Inits Warpscript with default host and port"""
        if connection not in ["py4j", "http"]:
            raise ValueError("connection must be either py4j or http.")
        self.host = host or os.getenv("WARP10_HOST", "127.0.0.1")
        self.port = port or int(os.getenv("WARP10_PORT", 25333))
        self.request_kwargs = kwargs
        self.connection = connection
        self.warpscript = ""

    def __repr__(self):
        repr = (
            f"Warp10 server connected on {self.host}:{self.port}\n"
            f"script: \n"
            f"{self.warpscript}"
        )
        return repr

    def script(self, *parameters: Any, fun: str = ""):
        """Write warpscripts.

        Args:
            *parameters:
                One or more python objects that need to be translated.
            fun:
                Optional; A warpscript function that will automatically be escaped and
                put in upper case.

        Returns:
            Self object with updated warpscript.
        """
        for param in parameters:
            self.warpscript += f"{Warpscript.sanitize(param)} "
        self.warpscript += f"{fun}\n"
        return self

    def load(self, file: str, **kwargs):
        """Load WarpScript file.

        Args:
            file:
                A path to be used as warpscript.
            **kwargs:
                Sometimes the script needs some parameters to be passed to it. This
                argument is used to put known objects at the beginning of the script.
                **kwargs must be arguments in the form key=value, where key will be the
                name of the variable in the script and value will be sanitized and
                assigned to it.

        Returns:
            Self object with updated Warpscript as read.
        """
        header = ""
        for key, value in kwargs.items():
            header += f"{Warpscript.sanitize(value)} '{key}' STORE\n"
        with open(file) as f:
            self.warpscript += header + f.read()
        self.warpscript += "\n"
        return self

    def exec(self, reset=True):
        """Execute warpscript.

        The script is slightly altered before the execution to automatically put all the
        scripts into a list and transform it into binary format. Putting everything into
        a list makes sure that poping the last element of the stack will pop all objects
        created in warpscript. Also using the binary format is the way to go to make
        sure that all warp10 objects will be correctly parse in python (including GTS
        and LGTS).

        Note that The alteration is only done if the connection is set to py4j.

        Args:
            reset: Optional; If True, the script will be reset once it has been executed.

        Returns:
            If more than one element are created in warp10 stack, then a tuple is
            generated, otherwise the element is returned as is. Note that GTS and LGTS
            are automatically parsed to pandas dataframe.
        """
        if self.connection == "py4j":
            altered_script = f"[ {self.warpscript} ] ->PICKLE"
            params = java_gateway.GatewayParameters(
                self.host, self.port, auto_convert=True
            )
            gateway = java_gateway.JavaGateway(gateway_parameters=params)
            stack = gateway.entry_point.newStack()
            try:
                stack.execMulti(altered_script)
                res = pkl.loads(stack.pop())  # nosec
            finally:
                gateway.close()
        elif self.connection == "http":
            res = requests.post(
                f"{self.host}/api/v0/exec",
                data=self.warpscript.encode(),
                headers={"Content-Type": "application/json"},
                **self.request_kwargs,
            )
            res.raise_for_status()
            res = res.json()
        if reset:
            self.warpscript = ""
        if is_lgts(res):
            return LGTS(res)
        objects = []
        for object in res:
            if isinstance(object, list) and is_lgts(object):
                objects.append(LGTS(object))
            elif isinstance(object, list) and is_gts_pickle(object):
                objects.append(GTS(object))
            else:
                objects.append(object)
        self.warpscript = ""
        if len(objects) == 1:
            return objects[0]
        return tuple(objects)

    @staticmethod
    def sanitize(x: Any) -> str:
        """Transforms python object into warpscript.

        Transforms python object into strings that warpscript will comprehend (list,
        dictionaries, strings, ...). By default, strings are wrap around single quote.
        This can be escaped by starting the string with `ws:`.

        Args:
            x: the object to transform

        Returns:
            A valid warpscript string.
        """
        if type(x) == str:
            if x.startswith("ws:"):
                return x[3:]
            try:
                duration = durations.Duration(x).to_seconds()
            except:
                duration = 0
            if duration > 0:
                return duration
            date = dateparser.parse(
                x, settings={"REQUIRE_PARTS": ["day", "month", "year"]}
            )
            if date is not None:
                date = date.replace(tzinfo=None)
                x = date.isoformat(timespec="microseconds") + "Z"
            return f"'{x}'"
        elif isinstance(x, bool):
            return str(x).upper()
        elif isinstance(x, GTS):
            return Warpscript.script(pkl.dumps(x).hex(), fun="HEX-> PICKLE->")
        elif isinstance(x, Iterable):
            if isinstance(x, Dict):
                symbol_start = "{"
                symbol_end = "}"
            elif isinstance(x, List):
                symbol_start = "["
                symbol_end = "]"
            else:
                raise SanitizeError(x)
            if len(x) == 0:
                return symbol_start + symbol_end
            separator = " "
            indentation = ""
            if len(str(x)) > 80:
                separator = "\n"
                indentation = " "
            res = f"{symbol_start}{separator}"
            if isinstance(x, Dict):
                for key, value in x.items():
                    res += (
                        f"{indentation}'{key}' {Warpscript.sanitize(value)}{separator}"
                    )
            elif isinstance(x, List):
                for value in x:
                    res += f"{indentation}{Warpscript.sanitize(value)}{separator}"
            res += symbol_end
            return res
        return x
