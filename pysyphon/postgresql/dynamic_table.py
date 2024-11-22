import logging
import psycopg2.errors
import psycopg2.extensions
import typing

from pysyphon.postgresql import postgresql_functions

LOG = logging.getLogger(__name__)


class DynamicTable:
    def __init__(
            self,
            table_name: str = None,
            host: str = None,
            user: str = None,
            password: str = None,
            database_name: str = None,
            port: int = 5432,
            primary_key_columns: str | list[str] | None = None,
    ):
        self.table_name = table_name
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database_name
        self.port = port
        self.primary_key_columns = primary_key_columns

    def append_or_update_list_of_rows(
            self,
            rows_as_dict: list[dict],
            connection: psycopg2.extensions.connection = None,
            log_query: bool = False,
    ) -> None:
        # TODO: add check on columns?
        query = postgresql_functions.append_or_update(
            table_name=self.table_name,
            row_dicts=rows_as_dict,
            primary_key_column=self.primary_key_columns,
        )
        if connection is None:
            self.single_transaction_query(
                query=query,
                log_query=log_query,
            )
        else:
            raise NotImplementedError

    def single_transaction_query(
            self,
            query: str,
            result_to_fetch: bool = False,
            log_query: bool = False,
    ) -> typing.Any:
        connection = postgresql_functions.get_connection(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database_name,
            port=self.port,
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

    def check_if_table_exists(self) -> bool:
        return self.single_transaction_query(
            query=(
                f"SELECT EXISTS ( "
                f"  SELECT FROM information_schema.tables "
                f"  WHERE table_schema = 'public' "
                f"  AND table_name = '{self.table_name}' "
                f"); "
            ),
            result_to_fetch=True,
        )[0][0]

    def create_table_from_dict(
            self,
            columns_dict: dict[str, str],
    ) -> None:
        self.single_transaction_query(
            query=(
                f"CREATE TABLE {self.table_name} (" +
                ", \n".join(
                    [
                        f"  {column_name} {column_type}"
                        for column_name, column_type in columns_dict.items()
                    ]
                ) + ", \n" +
                f"  PRIMARY KEY ({", ".join(self.primary_key_columns)}) "
                f");"
            ),
        )

    def get_column_names(self) -> list[str]:
        columns = self.single_transaction_query(
            query=(
                f"SELECT column_name "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{self.table_name}'; "
            ),
            result_to_fetch=True
        )
        return [column[0] for column in columns]

    def drop_table(self) -> None:
        self.single_transaction_query(
            query=f"DROP TABLE IF EXISTS {self.table_name}"
        )
