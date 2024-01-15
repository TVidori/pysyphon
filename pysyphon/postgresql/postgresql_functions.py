import datetime
import pandas as pd
import psycopg2.extensions
import typing
import warnings


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
        return "ARRAY[]::real[]"


def get_connection(
        host: str,
        database: str,
        user: str,
        password: str,
        port: int = 5432,
) -> psycopg2.extensions.connection:
    # TODO: can probably be improved to use "with"
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        port=port,
        password=password,
    )


def load_table_as_dataframe(
        host: str,
        database: str,
        user: str,
        password: str,
        table_name: str,
        port: int = 5432,
) -> pd.DataFrame:
    return load_query_result_as_dataframe(
        host=host,
        database=database,
        user=user,
        password=password,
        query=f'SELECT * FROM {table_name}',
        port=port,
    )


def load_query_result_as_dataframe(
        host: str,
        database: str,
        user: str,
        password: str,
        query: str,
        port: int = 5432,
) -> pd.DataFrame:
    connection = get_connection(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        message='.*pandas only supports SQLAlchemy connectable.*'
    )
    sql_table = pd.read_sql(query, connection)
    connection.close()
    return sql_table


def load_function_result_as_dataframe(
        host: str,
        database: str,
        user: str,
        password: str,
        function_name: str,
        function_input_args: str,
        port: int = 5432,
) -> pd.DataFrame:
    # TODO: to improve to get python type and convert in arguments
    #  or even use custom objects like table row
    connection = get_connection(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    sql_table = pd.read_sql(
        f'SELECT * FROM {function_name}({function_input_args})',
        connection
    )
    connection.close()
    return sql_table


def append_or_update(
        table_name: str,
        row_dicts: dict | list[dict],
        primary_key_column: str | list[str],
) -> str:
    if isinstance(row_dicts, dict):
        list_of_row_dicts = [row_dicts]
    else:
        list_of_row_dicts = row_dicts

    table_header = \
        f"{table_name} (" + ", ".join(list_of_row_dicts[0].keys()) + ")"

    values = "VALUES\n  " + ",\n    ".join([
        "(" + ", ".join(past_values_to_sql(list(row_dict.values()))) + ")"
        for row_dict in list_of_row_dicts
    ])

    if len(primary_key_column) > 0:
        if isinstance(primary_key_column, str):
            primary_key_columns = [primary_key_column]
        else:
            primary_key_columns = primary_key_column
        conflict_line = ", ".join(primary_key_columns)
        update_line = ", ".join([
            f"{key} = EXCLUDED.{key}"
            for key in list_of_row_dicts[0].keys()
            if key not in primary_key_columns
        ])
    else:
        conflict_line = ""
        update_line = ""

    return "\n".join(
        [
            f"INSERT INTO {table_header}",
            values,
        ] + ([] if conflict_line == "" else [
            f"ON CONFLICT ({conflict_line})",
            f"DO UPDATE SET {update_line};"
        ])
    )


def append_if_does_not_exists(
        table_name: str,
        row_dict: dict,
        primary_key_column: str | list[str],
) -> str:
    if isinstance(primary_key_column, str):
        conflict_line = primary_key_column
    else:
        conflict_line = ", ".join(primary_key_column)
    table_header = f"{table_name} (" + ", ".join(row_dict.keys()) + ")"
    value_line = ", ".join(past_values_to_sql(list(row_dict.values())))
    return "\n".join(
        [
            f"INSERT INTO {table_header}",
            f"VALUES ({value_line})",
        ] + ([] if conflict_line == "" else [
            f"ON CONFLICT ({conflict_line})",
            f"DO NOTHING;"
        ])
    )


def insert_list_of_rows_if_does_not_exists(
        table_name: str,
        list_of_rows_dict: list[dict],
        primary_key_column: str | list[str],
) -> str:
    if isinstance(primary_key_column, str):
        conflict_line = primary_key_column
    else:
        conflict_line = ", ".join(primary_key_column)

    rows_lines = ",\n   ".join([
        "(" + ", ".join([
            past_value_to_sql(column_value)
            for column_value in row_dict.values()
        ]) + ")"
        for row_dict in list_of_rows_dict
    ])
    table_header = \
        f"{table_name} (" + ", ".join(list_of_rows_dict[0].keys()) + ")"

    return "\n".join(
        [
            f"INSERT INTO {table_header}",
            f"VALUES",
            f"    {rows_lines}",
        ] + ([] if conflict_line == "" else [
            f"ON CONFLICT ({conflict_line})",
            f"DO NOTHING;"
        ])
    )


def update_given_columns(
        table_name: str,
        row_dict: dict,
        primary_key_column: str,
) -> str:
    if isinstance(primary_key_column, str):
        primary_key_columns = [primary_key_column]
    else:
        primary_key_columns = primary_key_column
    # row_dict needs to contain the columns to update and the primary key to
    # use
    set_line = ", ".join([
        f"{key} = {past_value_to_sql(value)}"
        for key, value in row_dict.items() if key not in primary_key_column
    ])
    where_line = " AND ".join([
        f"{primary_key_loop} = "
        f"{past_value_to_sql(row_dict[primary_key_loop])}"
        for primary_key_loop in primary_key_columns
    ])
    return "\n".join([
        f"UPDATE {table_name}",
        f"SET {set_line}",
        f"WHERE {where_line}",
        f";",
    ])


def select_with_filters(
        table_name: str,
        filter_list: list[tuple[str, str, typing.Any]],
        order_column: str | None = None,
        order_asc: bool = True,
        limit: int | None = None,
) -> str:
    condition_line = " AND ".join([
        get_filter(filter_tuple) for filter_tuple in filter_list
    ])
    query_lines = [f"SELECT * FROM {table_name}", ]
    if condition_line != "":
        query_lines.append(f"WHERE {condition_line}")
    if order_column is not None:
        query_lines.append(
            f"ORDER BY {order_column} " + ("ASC" if order_asc else "DESC")
        )
    if limit is not None:
        query_lines.append(f"LIMIT {limit}")

    return "\n".join(query_lines) + ";"


def get_filter(
        filter_tuple: tuple[str, str, typing.Any],
) -> str:
    column, filter_type, filter_value = filter_tuple
    if filter_value == "now":
        pasted_value = "now()"
    else:
        pasted_value = past_value_to_sql(filter_value)

    return f"{column} {filter_type} {pasted_value}"


def past_values_to_sql(values: list) -> list:
    return [past_value_to_sql(value) for value in values]


def past_value_to_sql(value: typing.Any) -> str:
    if isinstance(value, pd.Series):
        print(f"Error series in: {value}")

    if value is None:
        return "null"
    elif isinstance(value, IntArray):
        return value.to_sql_value()
    elif isinstance(value, FloatArray):
        return value.to_sql_value()
    # TODO: only works for list of string. Use IntArray or FloatArray or
    #  improve process for other types
    elif isinstance(value, list):
        return (
                f"ARRAY[" +
                ", ".join([past_value_to_sql(list_val) for list_val in value]) +
                "]"
        )
    elif not pd.notnull(value):
        return "null"
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif isinstance(value, datetime.datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    elif isinstance(value, bytes):
        return fix_psycopg2_string_representation(
            str(psycopg2.Binary(value))
        )
    else:
        return str(value)


def fix_psycopg2_string_representation(
        string_representation
):
    # Try to identify if \ have been doubled (inconsistency error in
    # psycopg2.Binary() behaviour) to fix them by checking if there is
    # a majority of \\
    single = string_representation.count("\\")
    double = string_representation.count("\\\\")
    if double > (single / 3):
        return string_representation.replace("\\\\", "\\")
    else:
        return string_representation
