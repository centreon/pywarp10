import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dateparser
import durations
from durations.exceptions import ScaleFormatError

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
    if isinstance(x, str):
        if x.startswith("ws:"):
            return x[3:]
        try:
            duration = durations.Duration(x).to_seconds()
        except ScaleFormatError:
            duration = 0
        if duration > 0:
            return int(duration * 1000000)
        date = dateparser.parse(x, settings={"REQUIRE_PARTS": ["day", "month", "year"]})
        if date is not None:
            return int(date.timestamp() * 1000000)
        return f"'{x}'"
    if isinstance(x, bool):
        return str(x).upper()
    if isinstance(x, datetime.datetime):
        # Someone may ask why I don't use isoformat() here like in the dateparser above.
        # It's because dateparser can be ambiguous for some dates, so it's easier to
        # check that the date was parsed correctly in logs if something went wrong.
        # However, with datetime object, there is no ambiguity, and timestamp can be
        # used directly, so warp10 won't have to transform it back itself.
        return int(x.timestamp() * 1e6)
    if isinstance(x, datetime.date):
        # Date cannot be converted easily to a timestamp without making it a datetime.
        x = datetime.datetime(x.year, x.month, x.day, tzinfo=datetime.timezone.utc)
        return int(x.timestamp() * 1e6)
    if isinstance(x, datetime.timedelta):
        return int(x.total_seconds() * 1e6)
    if isinstance(x, Iterable):
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


def desanitize(x: List[Any], bind_lgts=True) -> Tuple[Any]:
    """Transforms a warpscript output into python object.

    Args:
        l: a list to be desanitized.

    Returns:
        A valid python object.
    """
    if gts.is_gts(x):
        return gts.GTS(x)

    if gts.is_lgts(x):
        if not bind_lgts:
            return [desanitize(g) for g in x]
        else:
            return gts.GTS(x)

    if isinstance(x, List):
        for i, y in enumerate(x):
            x[i] = desanitize(y, bind_lgts=bind_lgts)
        if len(x) == 1:
            return x[0]
        return tuple(x)
    return x
