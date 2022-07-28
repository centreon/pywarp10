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
import ssl
from typing import Any, Literal, Optional

import requests
import urllib3
from py4j import java_gateway
from urllib3.exceptions import InsecureRequestWarning

from pywarp10.gts import GTS
from pywarp10.sanitize import desanitize, sanitize

urllib3.disable_warnings(InsecureRequestWarning)

client_ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

# Set this to True if the client loads a certification chain and a hostname is specified
#  in GatewayParameters.
client_ssl_context.check_hostname = False

# The client won't check the certification chain as we trust the server self-certificate
# since is is generated.
client_ssl_context.verify_mode = ssl.CERT_NONE


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
        connection: Literal["py4j", "http"] = "py4j",
        **kwargs,
    ) -> None:
        """Inits Warpscript with default host and port"""
        if connection not in ["py4j", "http"]:
            raise ValueError("connection must be either py4j or http.")
        self.host = host or os.getenv("WARP10_HOST", "127.0.0.1")
        if connection == "http":
            self.host = f"{self.host}/api/v0/exec"
        self.port = port or int(os.getenv("WARP10_PORT", 25333))
        self.request_kwargs = kwargs
        self.connection = connection
        self.warpscript = ""

    def __repr__(self) -> str:
        repr_port = f":{self.port}" if self.connection == "py4j" else ""
        repr = (
            f"Warp10 requests sent to {self.host}{repr_port}\n"
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
            self.warpscript += f"{sanitize(param)} "
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
            header += f"{sanitize(value)} '{key}' STORE\n"
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
                self.host,
                self.port,
                auto_convert=True,
                ssl_context=client_ssl_context,
                auth_token=None,
            )
            gateway = java_gateway.JavaGateway(gateway_parameters=params)
            stack = gateway.entry_point.newStack()
            try:
                stack.execMulti(altered_script)
                res = pkl.loads(stack.pop())  # nosec
            except Exception as e:
                print(self)
                raise e
            finally:
                gateway.close()
        elif self.connection == "http":
            res = requests.post(
                self.host,
                data=self.warpscript.encode(),
                headers={"Content-Type": "application/json"},
                **self.request_kwargs,
            )
            try:
                res.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print(self)
                raise e
            res = res.json()
        if reset:
            self.warpscript = ""
        return desanitize(res)
