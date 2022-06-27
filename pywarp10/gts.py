from tkinter import N
from typing import Any, Dict, List

import pandas as pd


def is_lgts(l: List) -> bool:
    """Is the list a list of GTS?

    Args:
        x: An object

    Returns: True if all the element of the list are GTS.
    """
    if not isinstance(l, List):
        return False
    if len(l) == 0:
        return False
    for element in l:
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
    if not isinstance(x, Dict):
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


class GTS:
    def __init__(self, data=Dict):
        self.data = None
        self.classname = None
        self.labels = None
        self.attributes = None
        if not is_gts(data):
            raise TypeError(f"{data} is not a GTS")
        if "c" in data.keys():
            data["classname"] = data.pop("c")
            data["timestamps"] = [d[0] for d in data["v"]]
            data["values"] = [d[1] for d in data["v"]]
            data.pop("v")
            data["labels"] = data.pop("l")
            data["attributes"] = data.pop("a")
            data.pop("la")
        if len(data["timestamps"]) > 0:
            self.data = pd.DataFrame(
                {
                    "timestamps": data["timestamps"],
                    "values": data["values"],
                }
            )
        self.classname = data["classname"]
        self.labels = data["labels"]
        if "attributes" in data.keys():
            self.attributes = data["attributes"]
        # Convert to timestamps only if higest timestamp is greater than 1 day after the epoch
        if self.data is not None and max(self.data["timestamps"]) > 86400000000:
            self.data["timestamps"] = pd.to_datetime(self.data["timestamps"], unit="us")

    def to_pandas(self) -> pd.DataFrame:
        """Convert the GTS to a pandas DataFrame.

        Returns:
            A pandas DataFrame.
        """
        if self.data is not None:
            df = self.data.copy()
            n = len(df)
        else:
            df = pd.DataFrame({"classname": [self.classname]})
            n = 1
        df["classname"] = self.classname
        for label in self.labels:
            df[label] = self.labels[label]
        return df

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        if self.classname:
            name = f"name      : {self.classname}\n"
        else:
            name = ""
        if self.labels:
            labels = "labels    :\n"
            for key, value in self.labels.items():
                labels += f"  {key}={value}\n"
        else:
            labels = ""
        if self.attributes:
            attributes = "attributes:\n"
            for key, value in self.attributes.items():
                attributes += f"  {key}={value}\n"
        else:
            attributes = ""
        if "timestamps" in self.data.columns:
            data = self.data[["timestamps", "values"]].__repr__()
            return f"{name}{labels}{attributes}\n\n{data}"
        else:
            return f"{name}{labels}{attributes}\n\nEmpty GTS"

    def __eq__(self, other: "GTS") -> bool:
        """Check if two GTS are equal.

        Args:
            other: The other GTS to compare with.

        Returns:
            True if the two GTS are equal.
        """
        if isinstance(other, GTS):
            if (
                self.classname == other.classname
                and self.labels == other.labels
                and self.attributes == other.attributes
                and self.data.equals(other.data)
            ):
                return True
        return False


class LGTS(pd.DataFrame):
    def __init__(self, l: List) -> None:
        lgts = []
        for x in l:
            if not is_gts(x):
                raise TypeError("The list is not a list of GTS.")
            lgts.append(GTS(x).to_pandas())
        res = pd.concat(lgts)
        res.replace("", float("NaN"), inplace=True)
        res.dropna(how="all", axis=1, inplace=True)
        super().__init__(res)

    def __eq__(self, other: "LGTS") -> bool:
        """Check if two LGTS are equal.

        Args:
            other: The other LGTS to compare with.

        Returns:
            True if the two LGTS are equal.
        """
        if isinstance(other, LGTS) and self.equals(other):
            return True
        return False

    @staticmethod
    def from_dataframe(
        x: pd.DataFrame, timestamp_col="timestamps", value_col="values"
    ) -> List:
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
        label_col = [col for col in x.columns if col not in [timestamp_col, value_col]]
        if len(label_col) == 0:
            raise ValueError("The dataframe must contain at least one label column.")
        grouped_df = x.groupby(label_col)
        res = []
        for group in grouped_df.groups.keys():
            df = grouped_df.get_group(group)
            labels = {
                str(k): str(l[0])
                for k, l in df[label_col].drop_duplicates().to_dict("list").items()
            }
            ts = df.drop(label_col, axis="columns").to_dict("list")
            ts["timestamps"] = ts.pop(timestamp_col)
            classname = ""
            if value_col != "values":
                classname = value_col
                ts["values"] = ts.pop(classname)
            res.append(
                {
                    "classname": classname,
                    "timestamps": ts["timestamps"],
                    "values": ts["values"],
                    "labels": labels,
                }
            )
        return LGTS(res)
