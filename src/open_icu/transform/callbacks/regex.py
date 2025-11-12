import polars as pl
from polars import LazyFrame
import re

from open_icu.transform.callbacks.proto import CallbackProtocol
from open_icu.transform.callbacks.regestry import register_callback_class

@register_callback_class
class Split(CallbackProtocol):
    def __init__(self, columns: list[str], suffixes: list[str], regex: str, flags: int = 0) -> None:
        self.columns = columns
        self.suffixes = suffixes
        self.regex = regex
        self.flags = flags
        re.compile(regex, flags)

    def __call__(self, lf: LazyFrame) -> LazyFrame:
        for column in self.columns:
            split_expression = pl.col(column).str.split(self.regex)
            lf = lf.with_columns(split_expression.alias(f"{column}_split"))
            for i, suffix in enumerate(self.suffixes):
                lf = lf.with_columns(
                    pl.col(f"{column}_split").list.get(i).alias(f"{column}_{suffix}")
                )
        return lf
