import time
from datetime import datetime, timedelta
import json
import gc
import logging
import pandas as pd
from sqlalchemy import Table, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete
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
        postgres_cursor = postgres_conn.cursor()

        # Obter as colunas da tabela no SQL Server e PostgreSQL
        sqlserver_columns = sqlserver_check_table_columns(sqlserver_conn, table_name)
        postgres_columns = postgres_check_table_columns(postgres_cursor, table_name)

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


def migrate_data(sql_conn, postgres_engine, table_name, date_filter, date_column_name):
    query = f"SELECT * FROM sankhya.{table_name} WHERE {date_column_name} >= '{date_filter}'"
    df = pd.read_sql(query, sql_conn)
    df.columns = df.columns.str.lower()
    logger.info(f"Foram encontrados: {len(df)} registros")

    with postgres_engine.connect() as postgres_conn:
        transaction = postgres_conn.begin()
        try:
            df.to_sql(
                table_name,
                postgres_conn,
                schema="sankhya",
                if_exists="append",
                index=False,
            )
            transaction.commit()
            logger.info(
                f"Dados da tabela '{table_name}' inseridos com sucesso na tabela 'sankhya.{table_name}'."
            )
        except Exception as e:
            logger.error(f"Erro ao inserir dados no PostgreSQL: {e}")
        finally:
            del df
            gc.collect()


def update_recent_data(
    sql_conn, postgres_conn, postgres_engine_url, table_name, date_column_name, days
):
    filter_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    logger.info(
        f"Iniciando sincronização dos dados para a tabela '{table_name}' com base na data {filter_date}."
    )

    try:
        # Conectar ao banco PostgreSQL e preparar para operações
        postgres_cursor = postgres_conn.cursor()
        logger.info(
            f"Conexão estabelecida com o banco PostgreSQL para a tabela '{table_name}'."
        )

        # Carregar dados do SQL Server
        query = f"SELECT * FROM sankhya.{table_name} WHERE {date_column_name} >= '{filter_date}'"
        logger.info(f"Executando consulta no SQL Server: {query}")

        df = pd.read_sql(query, sql_conn)
        df.columns = df.columns.str.lower()
        logger.info(
            f"Encontrados {len(df)} registros no SQL Server para sincronização."
        )

        # Deletar registros antigos no PostgreSQL
        delete_query = f"""
        DELETE FROM sankhya.{table_name}
        WHERE {date_column_name} >= '{filter_date}';
        """
        logger.info(f"Executando exclusão no PostgreSQL com a consulta: {delete_query}")
        postgres_cursor.execute(delete_query)
        postgres_conn.commit()
        logger.info(
            f"Dados deletados com sucesso no PostgreSQL para a tabela 'sankhya.{table_name}' a partir de {filter_date}."
        )

        with postgres_engine_url.connect() as postgres_conn_url:
            transaction = postgres_conn_url.begin()
            try:
                logger.info(f"Iniciando inserção dos dados no PostgreSQL.")
                df.to_sql(
                    table_name,
                    postgres_conn_url,
                    schema="sankhya",
                    if_exists="append",
                    index=False,
                )
                transaction.commit()
                logger.info(
                    f"Dados sincronizados com sucesso na tabela 'sankhya.{table_name}'. Total de {len(df)} registros inseridos."
                )

            except Exception as insert_error:
                transaction.rollback()
                logger.error(
                    f"Erro ao inserir dados no PostgreSQL para a tabela '{table_name}': {insert_error}"
                )
                raise

    except Exception as e:
        logger.error(
            f"Erro durante o processo de sincronização para a tabela '{table_name}': {e}"
        )
        raise
    finally:
        # Limpeza de memória
        if sql_conn:
            sql_conn.close()
        if postgres_conn:
            postgres_conn.close()
        if postgres_conn_url:
            postgres_conn_url.close()


def migrate_multiple_tables():
    try:
        with open("app/databases.json", "r") as file:
            tables_to_migrate = json.load(file)
        # Configurar conexões
        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_engine_url = get_postgres_engine_string_url()

        # ADICIONAR WITH AQUI

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
                    postgres_engine_url,
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
        # postgres_engine_url.close()
        postgres_connection.close()


def check_and_update_recent_date(table_name, days, date_column):
    try:

        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_engine_url = get_postgres_engine_string_url()

        if compare_columns_between_databases(
            sqlserver_conn=sqlserver_connection,
            postgres_conn=postgres_connection,
            table_name=table_name,
        ):
            update_recent_data(
                sql_conn=sqlserver_connection,
                postgres_engine_url=postgres_engine_url,
                postgres_conn=postgres_connection,
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
        raise
    # finally:

    # sqlserver_connection.close()
    # postgres_connection.close()
    # postgres_engine_url.close()
