from db.migrate import (
    migrate_multiple_tables,
    check_and_update_recent_date,
    update_by_logs_table,
)
from db.sqlserver.config import get_connection
from db.sqlserver.utils import sqlserver_check_primary_keys
from db.postgres.utils import delete_from_pk
from db.postgres.config import get_postgres_connection, get_postgres_engine_string_url
import pandas as pd


def main():
    # TESTES COM A TGFPRO

    sql_conn = get_connection()

    # nunotas = ["11706590", "11706719"]
    postgres_conn = get_postgres_connection()
    postgres_engine = get_postgres_engine_string_url()
    # postgres_cursor = postgres_conn.cursor()
    # delete_from_pk(postgres_cursor, "tgfcab", primary_keys, nunotas)
    # get_logs_table(
    #     log_schema_name="sankhya",
    #     dw_schame_name="sankhya",
    #     prod_schema_name="sankhya_prod",
    #     table_name="tgfpro",
    #     sqlserver_conn=sql_conn,
    #     postgres_conn=postgres_conn,
    # )
    update_by_logs_table(
        log_schema_name="sankhya",
        table_name="tgfpro",
        sqlserver_conn=sql_conn,
        postgres_conn=postgres_conn,
        postgres_engine=postgres_engine,
    )

    # postgres_conn.commit()
    # migrate_multiple_tables()


if __name__ == "__main__":
    main()
