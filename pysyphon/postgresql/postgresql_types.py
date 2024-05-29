import pandas as pd
import typing


class IntArray(list):
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
            # Put single quotes as printing directly a python array will use
            #  double quotes if a single quote is in it. Escape single quote
            #  characters by doubling them
            list_with_singe_quote = ", ".join(
                ("'" + item.replace("'", "''") + "'") for item in self
            )
            return f"ARRAY{list_with_singe_quote}"

    @staticmethod
    def empty_value() -> str:
        return "ARRAY[]::varchar[]"
