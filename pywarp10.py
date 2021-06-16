"""Make it easy to work with warpscripts.

Easily transform python objects into valid warpscripts ones, automatically retrieve GTS 
or list of GTS as pandas dataframe.

    Typical usage example:

    ws = Warpscript()
    ws.script({"foo": "bar"})
    ws.exec()
"""


from typing import Any, Dict, Iterable, List, Optional, TypedDict, Union
from numpy import isin
from py4j import java_gateway
import pickle as pkl
import pandas as pd
import os
import dateparser
import durations


class SanitizeError(Exception):
    """Exception for sanitize error.

    Attributes:
        type: object that could not be sanitize.
        message: explanation of the error.
    """

    def __init__(self, object: Any, message: Union[None, str] = None) -> None:
        self.type = str(type(object))
        if not message:
            message = f"Could not sanitize object type `{self.type}`"
        self.message = message
        super().__init__(self.message)

    pass


class GTS(TypedDict):
    """GTS type hint.

    Define how a GTS is define in python.

    Attributes:
        classname:
            The name of the class.
        timestamps:
            A list of timestamps.
        values:
            A list of values associated with timestamps.
        attributes:
            Attributes of the GTS.
        labels:
            Labels of the GTS.
    """

    classname: str
    timestamps: List[int]
    values: List
    attributes: Dict[str, str]
    labels: Dict[str, str]


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
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """Inits Warpscript with default host and port"""
        self.host = host if host is not None else os.getenv("WARP10_HOST", "127.0.0.1")
        self.port = port if port is not None else int(os.getenv("WARP10_PORT", 25333))
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

    def exec(self, close: bool = True):
        """Execute warpscript.

        The script is slightly altered before the execution to automatically put all the
        scripts into a list and transform it into binary format. Putting everything into
        a list makes sure that poping the last element of the stack will pop all objects
        created in warpscript. Also using the binary format is the way to go to make
        sure that all warp10 objects will be correctly parse in python (including GTS
        and LGTS).

        Args:
            close:
                Optional; close the gateway connection after the execution.

        Returns:
            If more than one element are created in warp10 stack, then a tuple is
            generated, otherwise the element is returned as is. Note that GTS and LGTS
            are automatically parsed to pandas dataframe.
        """
        altered_script = f"[ {self.warpscript} ] ->PICKLE"
        params = java_gateway.GatewayParameters(self.host, self.port, auto_convert=True)
        gateway = java_gateway.JavaGateway(gateway_parameters=params)
        stack = gateway.entry_point.newStack()
        try:
            stack.execMulti(altered_script)
            res = pkl.loads(stack.pop())
        finally:
            gateway.close()
        objects = []
        for object in res:
            if Warpscript.is_gts(object):
                objects.append(Warpscript.gts_to_dataframe(object))
            elif isinstance(object, List):
                objects.append(Warpscript.list_to_dataframe(object))
            else:
                objects.append(object)
        self.warpscript = ""
        if len(objects) == 1:
            return objects[0]
        return tuple(objects)

    def dataframe_to_gts(self, x: pd.DataFrame, value_col: str = "values") -> str:
        """Transform a dataframe to warpscript.

        Transform a dataframe to binary format that will be understood by warp10. The
        dataframe will be pickled. If the dataframe contains more than timestamps and
        values, then the other columns will be considered labels of a GTS.

        Args:
            x:
                A panda dataframe that will be transformed.
            value_col:
                The column which define values in the GTS.

        Returns:
            A warpscript with dataframe represented as a pickle object.
        """
        label_col = [
            col for col in x.columns.tolist() if col not in ["timestamps", value_col]
        ]
        grouped_df = x.groupby(label_col)
        res = []
        for group in grouped_df.groups.keys():
            df = grouped_df.get_group(group)
            labels = {
                str(k): str(l[0])
                for k, l in df[label_col].drop_duplicates().to_dict("list").items()
            }
            gts = df.drop(label_col, axis="columns").to_dict("list")
            if value_col == "values":
                classname = ""
            else:
                classname = value_col
                gts["values"] = gts.pop(classname)
            gts["classname"] = classname
            gts["labels"] = labels
            gts["attributes"] = []
            res.append(gts)
        return self.script(pkl.dumps(res).hex(), fun="HEX-> PICKLE->")

    @staticmethod
    def gts_to_dataframe(x: GTS) -> pd.DataFrame:
        """Converts GTS to panda dataframe

        By default, a pickled GTS is returned as a dictionary with special keys.
        This static method converts the dictionary into a comprehensible pandas
        dataframe.

        Args:
            x:
                A GTS object which is a dictionary with special keys.

        Returns:
            A panda dataframe with 2 columns: timestamps and classname values.
        """
        values = x["values"]
        timestamps = pd.to_datetime(x["timestamps"], unit="us")
        col_values = x["classname"] if x["classname"] != "" else "values"
        df = pd.DataFrame.from_dict({"timestamps": timestamps, col_values: values})
        return df

    @staticmethod
    def is_gts(x: Any) -> bool:
        """Is the object a GTS?

        Args:
            x: An object

        Returns: True if the object is a GTS.
        """
        if not isinstance(x, Dict):
            return False
        for key in ["classname", "timestamps", "values", "labels"]:
            if key not in x.keys():
                return False
        for key in x.keys():
            if key not in ["classname", "timestamps", "values", "attributes", "labels"]:
                return False
        return True

    @staticmethod
    def is_lgts(l: List) -> bool:
        """Is the list a list of GTS?

        Args:
            x: An object

        Returns: True if all the element of the list are GTS.
        """
        for element in l:
            if not Warpscript.is_gts(element):
                return False
        return True

    @staticmethod
    def list_to_dataframe(l: List) -> Union[List, pd.DataFrame]:
        """Converts a list of GTS to pandas dataframe.

        Args:
            l: The list to convert.

        Returns:
            A panda dataframe if all the element of the list are GTS, otherwise returns
            the list as is. If a panda dataframe is returned, all GTS are bind together.
            Labels that differ from one GTS to another are added to the columns of the
            dataframe.
        """
        if not Warpscript.is_lgts(l):
            return l
        res = []
        for x in l:
            gts = Warpscript.gts_to_dataframe(x)
            for label, value in x["labels"].items():
                gts[label] = value
            res.append(gts)
        df = pd.concat(res).sort_values("timestamps").reset_index(drop=True)
        return df[[c for c in list(df) if len(df[c].unique()) > 1]]

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
            date = dateparser.parse(x)
            if date is not None:
                date = date.replace(tzinfo=None)
                x = date.isoformat(timespec="microseconds") + "Z"
            return f"'{x}'"
        elif isinstance(x, bool):
            return str(x).upper()
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
