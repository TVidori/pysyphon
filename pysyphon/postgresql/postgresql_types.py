import pandas as pd
import typing


class IntArray(list):
    def __init__(self, list_):
        if not isinstance(list_, typing.Iterable) & pd.isnull(list_):
            super().__init__([])
        else:
            super().__init__(list_)

    def to_sql_value(self) -> str:
        if len(self) == 0:
            return self.empty_value()
        else:
            return f"ARRAY{self}"

    @staticmethod
    def empty_value() -> str:
        return "ARRAY[]::integer[]"


class FloatArray(list):
    def __init__(self, list_):
        if not isinstance(list_, typing.Iterable) and pd.isnull(list_):
            super().__init__([])
        else:
            super().__init__(list_)

    def to_sql_value(self) -> str:
        if len(self) == 0:
            return self.empty_value()
        else:
            return f"ARRAY{self}"

    @staticmethod
    def empty_value() -> str:
        return "ARRAY[]::real[]"


class VarcharArray(list):
    def __init__(self, list_):
        if not isinstance(list_, typing.Iterable) and pd.isnull(list_):
            super().__init__([])
        else:
            super().__init__(list_)

    def to_sql_value(self) -> str:
        if len(self) == 0:
            return self.empty_value()
        else:
            return f"ARRAY{self}"

    @staticmethod
    def empty_value() -> str:
        return "ARRAY[]::varchar[]"
