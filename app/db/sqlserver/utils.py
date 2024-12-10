import logging
import pandas as pd
from db.sqlserver.config import get_connection
from logs.log_config import setup_logging
from db.postgres.utils import delete_from_pk


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


def sqlserver_check_primary_keys(sqlserver_conn, table_name):
    query = f"""
    SELECT 
        KCU.COLUMN_NAME AS ColumnName
    FROM 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
    INNER JOIN
        INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
        ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME
    WHERE 
        TC.TABLE_NAME = '{table_name}' 
        AND TC.TABLE_SCHEMA = 'sankhya' 
        AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY';
    """
    try:
        columns = [row[0] for row in sqlserver_conn.execute(query).fetchall()]
        # pk_columns = [col.lower() for col in columns]
        return columns
    except Exception as e:
        logger.error(f"Erro ao verificar PK's da tabela '{table_name}': {e}")
        raise


def get_value_by_pks(
    prod_schema_name, dw_schema_name, table_name, primary_keys, values, sqlserver_conn
):
    if not primary_keys:
        raise ValueError("É necessário especificar ao menos uma chave primária.")

    if not values:
        raise ValueError(
            "É necessário especificar ao menos um valor para a chave primária."
        )

    where_clause = " AND ".join(
        [f"{pk} = '{val}'" for pk, val in zip(primary_keys, values)]
    )

    select_query = f"SELECT * FROM {prod_schema_name}.{dw_schema_name}.{table_name} WHERE {where_clause};"

    logger.info(f"Query gerada: {select_query}")

    select_df = pd.read_sql(select_query, sqlserver_conn)
    logger.info(select_df.values)


def update_values_by_pk(
    sqlserver_conn,
    schema_name,
    table_name,
    primary_keys,
    source_values,
    operation_type,
    last_SyncTableId=None,
):
    if not primary_keys:
        raise ValueError("É necessário especificar ao menos uma chave primária.")

    if len(source_values) <= 0:
        logger.info("Nenhum valor para fazer update de DhIntegração.")
        return

    # Converte ["11706590", "11706719"] -> ('11706590', '11706719')
    # formatted_values = ", ".join(f"'{v}'" for v in source_values)

    # Monta a expressão para a chave primária
    if len(primary_keys) == 1:
        logger.debug("Montando expressão de chave primaria")
        pk_expression = primary_keys[0]
        logger.debug("Expressão de chave primaria montada")
    else:
        logger.debug("Montando expressão de parametros de pks")
        formatted_pks = ", ".join(f"{v}" for v in primary_keys)
        pk_expression = f"CONCAT_WS('|', {formatted_pks})"
        logger.debug("Expressão de parametros de pks montada")

    if operation_type.lower() == "delete":
        update_DhIntegracao_query = f"""
        UPDATE LogSincronizacaoDW.{schema_name}.{table_name}
        SET DhIntegracao = GETDATE()
        WHERE {pk_expression} IN ({source_values});
        """
    elif operation_type.lower() == "update":
        update_DhIntegracao_query = f"""
        UPDATE LogSincronizacaoDW.{schema_name}.{table_name}
        SET DhIntegracao = GETDATE()
        WHERE {pk_expression} IN ({source_values})
        AND SyncTableId <= {last_SyncTableId};
        """

    logger.debug(update_DhIntegracao_query)
    sqlserver_conn.execute(update_DhIntegracao_query)
    sqlserver_conn.commit()
