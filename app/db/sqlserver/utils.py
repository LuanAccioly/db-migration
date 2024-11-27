import logging
from db.sqlserver.config import get_connection
import pandas as pd

from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def check_table_exists_sqlserver(table_name):
    # postgres_conn = get_postgres_connection()
    sqlserver = get_connection()

    query = f"""
    SELECT
        1
    FROM
        INFORMATION_SCHEMA.TABLES
    WHERE
        TABLE_SCHEMA = 'sankhya'
        AND TABLE_NAME = '{table_name}';
    """

    result = sqlserver.execute(query).fetchval()
    logger.info(result)


def sqlserver_check_table_columns(sqlserver_conn, table_name):
    sqlserver = sqlserver_conn.cursor
    query = f"""
    SELECT 
        COLUMN_NAME 
    FROM 
        INFORMATION_SCHEMA.COLUMNS 
    WHERE 
        TABLE_NAME = '{table_name}'
        AND TABLE_SCHEMA = 'sankhya';
    """

    try:

        columns = [row[0] for row in sqlserver_conn.execute(query).fetchall()]
        return columns
    except Exception as e:
        logger.error(f"Erro ao verificar colunas da tabela '{table_name}': {e}")
        raise
