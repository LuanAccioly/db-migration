from db.sqlserver.config import get_connection, get_connection_string_url
from db.postgres.config import (
    get_postgres_connection,
    get_postgres_engine_string_url,
)
from db.migrate import compare_columns_between_databases, migrate_data
import logging
from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main():

    try:
        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_connection_url = get_postgres_engine_string_url()

        compare_columns_between_databases(
            sqlserver_conn=sqlserver_connection,
            postgres_conn=postgres_connection,
            table_name="tgford",
        )

        migrate_data(
            sqlserver_connection, postgres_connection_url, "tgford", "20241101"
        )

    except Exception as e:
        logger.error(f"Erro durante o teste de conexão: {e}")
    finally:
        sqlserver_connection.close()
        postgres_connection.close()


if __name__ == "__main__":
    main()
