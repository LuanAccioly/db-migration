import logging
from sqlalchemy import text
from db.postgres.config import (
    get_postgres_engine_string_url,
)

from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def check_table_exists_postgres(table_name):
    try:
        # Obtém a conexão com o PostgreSQL
        engine = get_postgres_engine_string_url()
        conn = engine.connect()

        # Consulta SQL para verificar se a tabela existe
        query = text(
            """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'sankhya'  -- ou o schema desejado
        AND table_name = :table_name
        """
        )

        # Executa a consulta passando o nome da tabela como parâmetro
        result = conn.execute(query, {"table_name": table_name}).fetchone()

        print(result)

    except Exception as e:
        logger.error(f"Erro ao verificar a existência da tabela: {e}")
    finally:
        conn.close()


def postgres_check_table_columns(postgres_conn, table_name):
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
        # Executando a consulta corretamente com SQLAlchemy
        postgres_conn.execute(query)
        rows = postgres_conn.fetchall()
        columns = [row[0] for row in rows]
        return columns
    except Exception as e:
        logger.error(f"Erro ao verificar colunas da tabela '{table_name}': {e}")
        raise


def delete_from_pk(
    postgres_cursor, schema_name, table_name, primary_keys, source_values
):
    if not primary_keys:
        raise ValueError("É necessário especificar ao menos uma chave primária.")

    if source_values == "":
        return

    delete_query = f"""
    DELETE FROM {schema_name}.{table_name}
    WHERE {primary_keys} IN ({source_values});
    """
    try:
        postgres_cursor.execute(delete_query, source_values)
        logger.info(
            f"Dados deletados com sucesso no PostgreSQL para a tabela '{schema_name}.{table_name}'."
        )
        logger.debug(f"Query executada: {delete_query}")
    except Exception as e:
        logger.error(
            f"Erro ao deletar dados na tabela '{schema_name}.{table_name}': {e}"
        )
        raise
