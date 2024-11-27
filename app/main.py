from db.sqlserver.config import get_connection, get_connection_string_url
from db.postgres.config import (
    get_postgres_connection,
    get_postgres_engine_string_url,
)
from db.migrate import compare_columns_between_databases, migrate_data
import logging
import json
from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main():
    try:
        with open("app/databases.json", "r") as file:
            tables_to_migrate = json.load(file)
        # Configurar conexões
        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_connection_url = get_postgres_engine_string_url()

        # Iterar sobre a lista de tabelas e processar cada uma
        for table in tables_to_migrate:
            table_name = table["table_name"]
            date_column = table["date_column"]
            date_filter = table["date_filter"]

            logger.info(f"Iniciando migração para a tabela: {table_name}")

            # Comparar colunas entre SQL Server e PostgreSQL
            if compare_columns_between_databases(
                sqlserver_conn=sqlserver_connection,
                postgres_conn=postgres_connection,
                table_name=table_name,
            ):
                # Migrar dados se as colunas forem compatíveis
                migrate_data(
                    sqlserver_connection,
                    postgres_connection_url,
                    table_name,
                    date_filter,
                    date_column,
                )
            else:
                logger.warning(
                    f"Tabela {table_name} não foi migrada devido a diferenças de colunas."
                )

    except Exception as e:
        logger.error(f"Erro durante o processo de migração: {e}")
    finally:
        # Fechar conexões
        sqlserver_connection.close()
        postgres_connection.close()


if __name__ == "__main__":
    main()
