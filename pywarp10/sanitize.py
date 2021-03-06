from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateparser
import durations

import pywarp10.gts as gts


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
            return int(duration)
        date = dateparser.parse(x, settings={"REQUIRE_PARTS": ["day", "month", "year"]})
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
                res += f"{indentation}'{key}' {sanitize(value)}{separator}"
        elif isinstance(x, List):
            for value in x:
                res += f"{indentation}{sanitize(value)}{separator}"
        res += symbol_end
        return res
    return x


def desanitize(l: List[Any]) -> Tuple[Any]:
    """Transforms a warpscript output into python object.

    Args:
        l: a list to be desanitized.

    Returns:
        A valid python object.
    """
    if gts.is_lgts(l):
        return gts.LGTS(l)
    if isinstance(l, List):
        for i, x in enumerate(l):
            l[i] = desanitize(x)
        if len(l) == 1:
            return l[0]
        return tuple(l)
    if gts.is_gts(l):
        return gts.GTS(l)
    return l
