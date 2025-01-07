import dataclasses
import logging
import psycopg2.errors
import psycopg2.extensions
import typing

from pysyphon.postgresql import postgresql_functions

LOG = logging.getLogger(__name__)


# TODO: think of making inherit list and be a list of rows
class AbstractTable:
    table_name: str = None
    host: str = None
    user: str = None
    password: str = None
    database_name: str = None
    port: int = 5432
    primary_key_column: str | list[str] | None = None

    def __init_subclass__(cls):
        # This is needed to enforce the children behaviours
        if cls.host is None:
            raise TypeError("Class variable 'host' must be set in subclass")
        if cls.table_name is None:
            raise TypeError(
                "Class variable 'table_name' must be set in subclass"
            )
        if cls.database_name is None:
            raise TypeError(
                "Class variable 'database_name' must be set in subclass"
            )
        if cls.primary_key_column is None:
            raise TypeError(
                "Class variable 'primary_key_column' must be set in subclass"
            )

    @dataclasses.dataclass
    class Row:
        def __str__(self):
            return ",".join([
                str(value).replace('\n', ' ') for value in vars(self).values()
            ])

        def to_list(self) -> list:
            return [value for value in vars(self).values()]

        @classmethod
        def columns(cls) -> list[str]:
            return [field.name for field in dataclasses.fields(cls)]

    @classmethod
    def single_transaction_query(
            cls,
            query: str,
            result_to_fetch: bool = False,
            log_query: bool = False,
    ) -> typing.Any:
        connection = postgresql_functions.get_connection(
            host=cls.host,
            user=cls.user,
            password=cls.password,
            database=cls.database_name,
            port=cls.port,
        )
        if log_query:
            LOG.info(f"SQL query: \n{query}")
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
            except psycopg2.errors.NumericValueOutOfRange as exception:
                print(
                    f"Error: {exception} for: {query}"
                )
                raise exception
            connection.commit()
            if result_to_fetch:
                result = cursor.fetchall()
            else:
                result = None
        connection.close()

        return result

    @classmethod
    def fetch_data_transaction(
            cls,
            query: str,
            log_query: bool = False,
    ) -> list[Row]:
        connection = postgresql_functions.get_connection(
            host=cls.host,
            user=cls.user,
            password=cls.password,
            database=cls.database_name,
            port=cls.port,
        )
        if log_query:
            LOG.info(f"SQL query: \n{query}")
        with connection.cursor() as cursor:
            # If using SELECT *, check that table columns are the same
            #  as python table otherwise, unexpected results could happen
            if query.find("*") >= 0:
                columns = cls.get_table_columns(cursor)
                if columns != cls.Row.columns():
                    raise KeyError(
                        f"Python table and SQL tables don't have the same "
                        f"columns (or not in the same order) for "
                        f"{cls.table_name}. Features in table: {columns} vs "
                        f"features in Python: {cls.Row.columns()}."
                    )

            # Get query results
            cursor.execute(query)
            result = cursor.fetchall()
        connection.close()

        # dataclasses can be built with *args by default
        return [cls.Row(*[
            cls.paste_postgresql_object_to_python(element) for element in list_
        ]) for list_ in result]

    @classmethod
    def append_or_update_single_row(
            cls,
            row: Row,
            connection: psycopg2.extensions.connection = None,
            log_query: bool = False,
    ) -> None:
        query = postgresql_functions.append_or_update(
            table_name=cls.table_name,
            # TODO: Likely that it would be better to send the row directly
            row_dicts=dataclasses.asdict(row),
            primary_key_column=cls.primary_key_column,
        )
        if connection is None:
            cls.single_transaction_query(
                query=query,
                log_query=log_query,
            )
        else:
            raise NotImplementedError

    @classmethod
    def append_or_update_list_of_rows(
            cls,
            rows: list[Row],
            connection: psycopg2.extensions.connection = None,
            log_query: bool = False,
    ) -> None:
        query = postgresql_functions.append_or_update(
            table_name=cls.table_name,
            # TODO: Likely that it would be better to send the row directly
            row_dicts=[dataclasses.asdict(row) for row in rows],
            primary_key_column=cls.primary_key_column,
        )
        if connection is None:
            cls.single_transaction_query(
                query=query,
                log_query=log_query,
            )
        else:
            raise NotImplementedError

    @classmethod
    def append_if_does_not_exists(
            cls,
            row: Row,
            connection: psycopg2.extensions.connection = None,
    ) -> None:
        query = postgresql_functions.append_if_does_not_exists(
            table_name=cls.table_name,
            row_dict=dataclasses.asdict(row),
            primary_key_column=cls.primary_key_column,
        )
        if connection is None:
            cls.single_transaction_query(query)
        else:
            raise NotImplementedError

    @classmethod
    def update_given_columns(
            cls,
            # See if it could use a subset of dataclass instead?
            row_dict: dict,
            connection: psycopg2.extensions.connection = None,
    ) -> None:
        query = postgresql_functions.update_given_columns(
            table_name=cls.table_name,
            row_dict=row_dict,
            primary_key_column=cls.primary_key_column,
        )
        if connection is None:
            cls.single_transaction_query(query)
        else:
            raise NotImplementedError

    @classmethod
    def insert_list_of_rows_if_does_not_exists(
            cls,
            list_of_row: list[Row],
            log_query: bool = False,
            connection: psycopg2.extensions.connection = None,
    ) -> None:
        query = postgresql_functions.insert_list_of_rows_if_does_not_exists(
            table_name=cls.table_name,
            list_of_rows_dict=[
                dataclasses.asdict(game_odds) for game_odds in list_of_row
            ],
            primary_key_column=cls.primary_key_column,
        )
        if connection is None:
            cls.single_transaction_query(
                query=query,
                log_query=log_query,
            )
        else:
            raise NotImplementedError

    @classmethod
    def paste_postgresql_object_to_python(
            cls,
            postgresql_object: typing.Any,
    ) -> typing.Any:
        # Not sure if this should be done or the object should be kept in its
        # native version or cast to byte if the row needs a bytes
        if isinstance(postgresql_object, memoryview):
            return bytes(postgresql_object)
        else:
            return postgresql_object

    @classmethod
    def get_all_columns_as_string(cls) -> str:
        return ", ".join(cls.Row.columns())

    @classmethod
    def load_whole_table(
            cls,
            log_query: bool = False,
            force_check_columns: bool = False,
    ) -> list[Row]:
        columns = "*" if force_check_columns \
            else cls.get_all_columns_as_string()
        return cls.fetch_data_transaction(
            query=f"SELECT {columns} FROM {cls.table_name};",
            log_query=log_query,
        )

    @classmethod
    def load_sample_of_table(
            cls,
            sample_size: int = 10,
            log_query: bool = False,
            force_check_columns: bool = False,
    ) -> list[Row]:
        columns = "*" if force_check_columns \
            else cls.get_all_columns_as_string()
        query = (
            f"SELECT {columns} "
            f"FROM {cls.table_name} "
            f"LIMIT {sample_size}"
        )
        return cls.fetch_data_transaction(
            query=query,
            log_query=log_query,
        )

    @classmethod
    def load_with_filter(
            cls,
            filter_string: str,
            log_query: bool = False,
            force_check_columns: bool = False,
    ) -> list[Row]:
        columns = "*" if force_check_columns \
            else cls.get_all_columns_as_string()
        query = (
            f"SELECT {columns} FROM {cls.table_name} "
            f"WHERE {filter_string}; "
        )
        return cls.fetch_data_transaction(
            query=query,
            log_query=log_query,
        )

    @classmethod
    def load_with_python_parameters(
            cls,
            order_columns: list[str] | None = None,
            filter_string: str | None = None,
            limit: int | None = None,
            offset: int | None = None,
            log_query: bool = False,
            force_check_columns: bool = False,
    ) -> list[Row]:
        columns = "*" if force_check_columns \
            else cls.get_all_columns_as_string()
        order_command = (
            "ORDER BY " + ", ".join(order_columns) + " "
        ) if order_columns is not None else ""
        filter_command = (
            f"WHERE {filter_string} "
        ) if filter_string is not None else ""
        limit_command = f"LIMIT {limit} " if limit is not None else ""
        offset_command = f"OFFSET {offset} " if offset is not None else ""
        query = (
            f"SELECT {columns} "
            f"FROM {cls.table_name} "
            f"{filter_command} "
            f"{order_command} "
            f"{limit_command} "
            f"{offset_command} "
            f"; "
        )
        return cls.fetch_data_transaction(
            query=query,
            log_query=log_query,
        )

    @classmethod
    def get_table_columns(
            cls,
            cursor,
    ) -> list[str]:
        query = (
            f"SELECT column_name "
            f"FROM information_schema.columns "
            f"WHERE table_name = '{cls.table_name}'"
            f"order by ORDINAL_POSITION"
            f";"
        )
        cursor.execute(query)
        # Looks like row results are tuple. Cast them to string
        return [
            column[0] for column in cursor.fetchall()
        ]

    @classmethod
    def to_csv(
            cls,
            path: str,
            rows: list[Row],
    ) -> None:
        with open(path, "w", encoding='utf-8') as writer:
            writer.write(",".join(dataclasses.asdict(rows[0]).keys()) + "\n")
            for row in rows:
                writer.write(f"{row}\n")

    @classmethod
    def from_csv(
            cls,
            path: str,
            header: bool,
    ) -> list[Row]:
        # TODO: need to be implemented. Likely using pandas to read the csv
        raise NotImplementedError
