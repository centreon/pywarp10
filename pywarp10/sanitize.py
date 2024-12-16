import datetime
from functools import singledispatch
from typing import Any, Optional

import dateparser
import durations  # type: ignore
from durations.exceptions import InvalidTokenError, ScaleFormatError  # type: ignore

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


@singledispatch
def sanitize(x: Any) -> str | int:
    """Transforms python object into warpscript.

    Transforms python object into strings that warpscript will comprehend (list,
    dictionaries, strings, ...). By default, strings are wrap around single quote.
    This can be escaped by starting the string with `ws:`.

    Args:
        x: the object to transform

    Returns:
        A valid warpscript string.
    """
    raise SanitizeError(x)


@sanitize.register
def _(x: int) -> int:
    return x


@sanitize.register
def _(x: str) -> str | int:
    if x.startswith("ws:"):
        return x[3:]
    try:
        duration = durations.Duration(x).to_seconds()
    except (ScaleFormatError, InvalidTokenError):
        duration = 0
    if duration > 0:
        return int(duration * 1000000)
    date = dateparser.parse(x, settings={"REQUIRE_PARTS": ["day", "month", "year"]})
    if date is not None:
        return int(date.timestamp() * 1000000)
    return f"'{x}'"


@sanitize.register
def _(x: bool) -> str:
    return str(x).upper()


@sanitize.register
def _(x: datetime.datetime) -> int:
    # Someone may ask why I don't use isoformat() here like in the dateparser above.
    # It's because dateparser can be ambiguous for some dates, so it's easier to
    # check that the date was parsed correctly in logs if something went wrong.
    # However, with datetime object, there is no ambiguity, and timestamp can be
    # used directly, so warp10 won't have to transform it back itself.
    return int(x.timestamp() * 1e6)


@sanitize.register
def _(x: datetime.date) -> int:
    # Date cannot be converted easily to a timestamp without making it a datetime.
    x = datetime.datetime(x.year, x.month, x.day, tzinfo=datetime.timezone.utc)
    return int(x.timestamp() * 1e6)


@sanitize.register
def _(x: datetime.timedelta) -> int:
    return int(x.total_seconds() * 1e6)


@sanitize.register
def _(x: dict | list) -> str:
    if isinstance(x, dict):
        symbol_start = "{"
        symbol_end = "}"
    elif isinstance(x, list):
        symbol_start = "["
        symbol_end = "]"
    if len(x) == 0:
        return symbol_start + symbol_end
    separator = " "
    indentation = ""
    if len(str(x)) > 80:
        separator = "\n"
        indentation = " "
    res = f"{symbol_start}{separator}"
    if isinstance(x, dict):
        for key, value in x.items():
            res += f"{indentation}'{key}' {sanitize(value)}{separator}"
    elif isinstance(x, list):
        for value in x:
            res += f"{indentation}{sanitize(value)}{separator}"
    res += symbol_end
    return res


def desanitize(x: list[Any], bind_lgts=True) -> tuple[Any] | gts.GTS | list[gts.GTS]:
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
            return [desanitize(g) for g in x]  # type: ignore # we know desanitize will return a GTS
        else:
            return gts.GTS(x)

    if isinstance(x, list):
        for i, y in enumerate(x):
            x[i] = desanitize(y, bind_lgts=bind_lgts)
        if len(x) == 1:
            return x[0]
        return tuple(x)
    return x
