from datetime import datetime, timedelta
import json
import gc
import logging
import pandas as pd
from db.postgres.utils import postgres_check_table_columns
from db.postgres.config import get_postgres_connection, get_postgres_engine_string_url
from db.sqlserver.utils import sqlserver_check_table_columns
from db.sqlserver.config import get_connection, get_connection_string_url
from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def compare_columns_between_databases(sqlserver_conn, postgres_conn, table_name):
    """
    Compara as colunas de uma tabela entre o SQL Server e o PostgreSQL.

    :param sqlserver_conn: Conexão SQL Server.
    :param postgres_conn: Conexão PostgreSQL.
    :param table_name: Nome da tabela a ser comparada.
    :return: Resultado da comparação.
    """
    try:
        # Obter as colunas da tabela no SQL Server e PostgreSQL
        sqlserver_columns = sqlserver_check_table_columns(sqlserver_conn, table_name)
        postgres_columns = postgres_check_table_columns(postgres_conn, table_name)

        # Converter todas as colunas para minúsculas para comparação case-insensitive
        sqlserver_columns = [col.lower() for col in sqlserver_columns]
        postgres_columns = [col.lower() for col in postgres_columns]

        # Verificar se as colunas estão na mesma ordem
        if sqlserver_columns == postgres_columns:
            logger.info(
                f"As colunas da tabela '{table_name}' estão iguais e na mesma ordem em ambos os bancos de dados."
            )
            return True
        else:
            logger.warning(
                f"As colunas da tabela '{table_name}' são diferentes ou estão em ordens diferentes entre os bancos de dados."
            )
            logger.info(f"Colunas no SQL Server: {sqlserver_columns}")
            logger.info(f"Colunas no PostgreSQL: {postgres_columns}")
            return False
    except Exception as e:
        logger.error(f"Erro ao comparar colunas da tabela '{table_name}': {e}")
        raise


def migrate_data(sql_conn, postgres_conn, table_name, date_filter, date_column_name):
    query = f"SELECT * FROM sankhya.{table_name} WHERE {date_column_name} >= '{date_filter}'"
    df = pd.read_sql(query, sql_conn)
    df.columns = df.columns.str.lower()
    logger.info(f"Foram encontrados: {len(df)} registros")

    try:

        df.to_sql(
            table_name,
            postgres_conn,
            schema="raw_sankhya",
            if_exists="append",
            index=False,
        )
        logger.info(
            f"Dados da tabela '{table_name}' inseridos com sucesso na tabela 'raw_sankhya.{table_name}'."
        )
    except Exception as e:
        logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")
    finally:
        del df
        gc.collect()


def update_recent_data(sql_conn, postgres_conn, table_name, date_column_name, days):
    filter_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    try:
        query = f"SELECT * FROM sankhya.{table_name} WHERE {date_column_name} >= '{filter_date}'"
        df = pd.read_sql(query, sql_conn)
        df.columns = df.columns.str.lower()
        logger.info(
            f"Encontrados {len(df)} registros no SQL Server para sincronização."
        )

        # Deletar os dados existentes no PostgreSQL para o mesmo intervalo de tempo
        delete_query = f"""
        DELETE FROM raw_sankhya.{table_name}
        WHERE {date_column_name} >= '{filter_date}';
        """
        with postgres_conn.connect() as conn:
            conn.execute(delete_query)
        logger.info(
            f"Registros no PostgreSQL para '{table_name}' deletados com sucesso a partir de {filter_date}."
        )

        # Inserir os dados do SQL Server no PostgreSQL
        try:
            df.to_sql(
                table_name,
                postgres_conn,
                schema="raw_sankhya",
                if_exists="append",
                index=False,
            )
            logger.info(
                f"Dados sincronizados com sucesso na tabela 'raw_sankhya.{table_name}'."
            )
        except Exception as e:
            logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")

    except Exception as e:
        logger.error(f"Erro ao sincronizar a tabela '{table_name}': {e}")
    finally:
        del df
        gc.collect()


def migrate_multiple_tables():
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
        sqlserver_connection.close()
        postgres_connection.close()


def check_and_update_recent_date(table_name, days, date_column):
    try:

        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_connection_url = get_postgres_engine_string_url()

        if compare_columns_between_databases(
            sqlserver_conn=sqlserver_connection,
            postgres_conn=postgres_connection,
            table_name=table_name,
        ):
            update_recent_data(
                sql_conn=sqlserver_connection,
                postgres_conn=postgres_connection_url,
                table_name=table_name,
                date_column_name=date_column,
                days=days,
            )
        else:
            logger.warning(
                f"Tabela {table_name} não foi migrada devido a diferenças de colunas."
            )

    except Exception as e:
        logger.error(f"Erro durante o processo de migração: {e}")
    finally:
        sqlserver_connection.close()
        postgres_connection.close()
