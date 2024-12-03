from typing import Any, Dict, List, Union

import pandas as pd


def is_lgts(x: Any) -> bool:
    """Is the list a list of GTS?

    Args:
        x: An object

    Returns: True if all the element of the list are GTS.
    """
    if not isinstance(x, List):
        return False
    if len(x) == 0:
        return False
    for element in x:
        if not is_gts(element):
            return False
    return True


def is_gts(x: Any) -> bool:
    """Check if x is a GTS after it has been loaded from a pickle object.

    Args:
        x: The object to check.

    Returns:
        True if x is a GTS.
    """
    if not isinstance(x, Dict) or len(x) == 0:
        return False
    # There is a difference in the output of a GTS depending if it is a pickle or not.
    # Pickled GTS have complete labels, unpickled GTS have only the first letter.
    if all([key in ["c", "l", "a", "la", "v"] for key in x.keys()]):
        return True
    for key in ["classname", "timestamps", "values", "labels"]:
        if key not in x.keys():
            return False
    for key in x.keys():
        if key not in ["classname", "timestamps", "values", "attributes", "labels"]:
            return False
    return True


class GTS(pd.DataFrame):
    def __init__(self, data: Union[Dict, List]):
        if isinstance(data, dict):
            data_df = self._init_gts(data)
            # Convert to timestamps only if higest timestamp is greater than 1 day after
            # epoch
            if len(data_df.index) > 0 and max(data_df.index) > 86400000000:
                data_df.index = pd.to_datetime(data_df.index, unit="us")
        elif isinstance(data, list):
            data_df = self._init_lgts(data)
        elif isinstance(data, pd.DataFrame):
            data_df = data
        else:
            raise TypeError(f"{data} could not be converted to a GTS")
        super().__init__(data_df)  # type: ignore

    def _init_gts(self, gts: Dict) -> pd.DataFrame:
        """Initialize the GTS.

        Args:
            data: The data to initialize the GTS with.
        """
        if not is_gts(gts):
            raise TypeError(f"{gts} is not a GTS")
        if "c" in gts.keys():
            gts["classname"] = gts.pop("c")
            gts["timestamps"] = [d[0] for d in gts["v"]]
            gts["values"] = [d[1] for d in gts["v"]]
            gts.pop("v")
            gts["labels"] = gts.pop("l")
            gts["attributes"] = gts.pop("a")
            gts.pop("la")
        if len(gts["timestamps"]) > 0:
            data = pd.DataFrame(
                gts["values"], index=gts["timestamps"], columns=[gts["classname"]]
            )
        # If there is no data, the classname should not be used as a column name, but
        # as a value instead.
        elif gts["classname"]:
            data = pd.DataFrame({"classname": [gts["classname"]]})
        else:
            data = pd.DataFrame()
        for key, label in gts["labels"].items():
            data[key] = label if len(data) > 0 else [label]
        if "attributes" in gts.keys():
            for key, attribute in gts["attributes"].items():
                data[key] = attribute if len(data) > 0 else [attribute]
        return data

    def _init_lgts(self, lgts) -> pd.DataFrame:
        """Initialize the GTS.

        Args:
            data: The data to initialize the GTS with.
        """
        if not is_lgts(lgts):
            raise TypeError(f"{lgts} is not a list of GTS")
        data = pd.concat([GTS(gts) for gts in lgts])
        # If classname is in the column's name, it means that the GTS was empty
        # and therefore, index can be reset since there won't be timestamps.
        if "classname" in data.columns:
            data.reset_index(inplace=True, drop=True)
        return data
