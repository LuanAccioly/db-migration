import gc
import logging
import pandas as pd
from db.postgres.utils import postgres_check_table_columns
from db.sqlserver.utils import sqlserver_check_table_columns
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
