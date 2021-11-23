from typing import Any, Dict, List
import pandas as pd


def is_lgts(l: List) -> bool:
    """Is the list a list of GTS?

    Args:
        x: An object

    Returns: True if all the element of the list are GTS.
    """
    for element in l:
        if not is_gts(element):
            return False
    return True


def is_gts(x: Any) -> bool:
    """Check if x is a GTS.

    Args:
        x: The object to check.

    Returns:
        True if x is a GTS.
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


class GTS:
    classname = None
    labels = {}
    attributes = {}
    data = None

    def __init__(self, data=None) -> None:
        if is_gts(data):
            self.data = pd.DataFrame(
                {"timestamps": data["timestamps"], "values": data["values"]}
            )
            self.data["timestamps"] = pd.to_datetime(self.data["timestamps"], unit="us")
            self.classname = data["classname"]
            self.labels = data["labels"]
            if "attributes" in data.keys():
                self.attributes = data["attributes"]
        else:
            raise TypeError("The input is not a GTS.")

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        if self.classname:
            name = f"name      : {self.classname}\n"
        else:
            name = ""
        if self.labels:
            labels = "labels    : "
            for key, value in self.labels.items():
                labels += f"{key}={value}, "
            labels += "\n"
        else:
            labels = ""
        if self.attributes:
            attributes = "attributes: "
            for key, value in self.attributes.items():
                attributes += f"{key}={value}, "
        else:
            attributes = ""
        if self.data is not None:
            data = self.data.__repr__()
        return f"{name}{labels}{attributes}\n\n{data}"


class LGTS(pd.DataFrame):
    lgts = []

    def __init__(self, l: List) -> None:
        for element in l:
            if not is_gts(element):
                raise TypeError("The list is not a list of GTS.")
            self.lgts.append(GTS(element))
        res = pd.DataFrame()
        for element in self.lgts:
            if len(element.data) > 0:
                df = element.data
                df["classname"] = element.classname
            else:
                df = pd.DataFrame({"classname": element.classname}, index=[0])
            for label in element.labels:
                df[label] = element.labels[label]
            for attribute in element.attributes:
                df[attribute] = element.attributes[attribute]
            if len(res) == 0:
                res = df
            else:
                res = res.append(df)
        res.replace("", float("NaN"), inplace=True)
        res.dropna(how="all", axis=1, inplace=True)
        super().__init__(res)

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
