import pandas as pd
from sodapy import Socrata
from collections import defaultdict

from .utils import retry
from enum import Enum
from typing import (
    Any,
)

DEFAULT_LIMIT = 10000000  # to exceed the actual amount of rows in case.


class Operation(Enum):
    SUM = "sum"
    MAX = "max"


class Processor():
    def __init__(
        self,
        base_url: str,
        dataset: str,
        limit: int=DEFAULT_LIMIT,
        download: bool=True,
        token: str=None,
    ):
        self.base_url = base_url
        self.dataset = dataset
        self.limit = limit
        self.token = token
        if download:
            self.update()

    @retry(3)
    def update(self):
        client = Socrata(self.base_url, self.token)
        results = client.get(self.dataset, limit=self.limit)
        self.df = pd.DataFrame.from_records(results)

    def calculate(
        self,
        value: str,
        operation: Operation,
        top_n: int=10,
    ) -> Any:
        tmp_df = None
        if operation == Operation.SUM:
            tmp_df = self.df.groupby(["listyear", "town"])[value] \
                            .sum().reset_index() \
                            .sort_values([value], ascending=[0]) \
                            .groupby("listyear").head(top_n) \
                            .reset_index(drop=True)
        elif operation == Operation.MAX:
            tmp_df = self.df.groupby(["listyear", "town"])[value] \
                            .max().reset_index() \
                            .sort_values([value], ascending=[0]) \
                            .groupby("listyear").head(top_n) \
                            .reset_index(drop=True)
        else:
            return {}
        return self.convert_to_dict(tmp_df, value)

    def convert_to_dict(
        self,
        df: pd.DataFrame,
        key: str,
    ) -> Any:
        d = defaultdict(list)
        for index, row in df.iterrows():
            d[int(row["listyear"])].append({
                "town": row["town"],
                key: row[key],
            })
        return d
