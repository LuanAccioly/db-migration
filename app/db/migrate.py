import time
from datetime import datetime, timedelta
import json
import gc
import logging
import pandas as pd
from sqlalchemy import Table, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import delete
from db.postgres.utils import postgres_check_table_columns, delete_from_pk
from db.postgres.config import get_postgres_connection, get_postgres_engine_string_url
from db.sqlserver.utils import (
    sqlserver_check_table_columns,
    sqlserver_check_primary_keys,
    update_values_by_pk,
)
from db.sqlserver.config import get_connection, get_connection_string_url
from logs.log_config import setup_logging
from classes.classes import ColumnsMismatchError

setup_logging()
logger = logging.getLogger(__name__)


def compare_columns_between_databases(sqlserver_conn, postgres_conn, table_name):
    """
    Compara as colunas de uma tabela entre o SQL Server e o PostgreSQL.

    :param sqlserver_conn: Conexão SQL Server.
    :param postgres_conn: Conexão PostgreSQL.
    :param table_name: Nome da tabela a ser comparada.
    :raises ColumnsMismatchError: Quando as colunas da tabela são diferentes entre os bancos de dados.
    :return: True se as colunas forem iguais e na mesma ordem.
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
            # Lançar exceção personalizada se houver discrepâncias
            raise ColumnsMismatchError(table_name, sqlserver_columns, postgres_columns)

    except Exception as e:
        logger.error(f"Erro ao comparar colunas da tabela '{table_name}': {e}")
        raise


def update_recent_data(
    sql_conn,
    postgres_conn,
    postgres_engine_url,
    table_name,
    date_column_name,
    days,
    primary_keys,
):
    filter_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
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
        query = f"SELECT * FROM sankhya.{table_name} WHERE {date_column_name} BETWEEN '{filter_date}' AND '{tomorrow}'"
        logger.info(f"Executando consulta no SQL Server: {query}")

        df = pd.read_sql(query, sql_conn)
        pks_values = df[primary_keys].astype(str).agg("|".join, axis=1)
        df.columns = df.columns.str.lower()
        logger.info(
            f"Encontrados {len(df)} registros no SQL Server para sincronização."
        )

        # delete_from_pk(postgres_cursor, table_name, primary_keys, pks_values)
        postgres_conn.commit()

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


def update_full_table(
    sql_conn,
    postgres_conn,
    postgres_engine_url,
    table_name,
):
    """
    Realiza a carga completa de dados de uma tabela do SQL Server para o PostgreSQL.

    :param sql_conn: Conexão com o banco SQL Server.
    :param postgres_conn: Conexão com o banco PostgreSQL.
    :param postgres_engine_url: URL do engine SQLAlchemy para o PostgreSQL.
    :param table_name: Nome da tabela a ser sincronizada.
    """
    logger.info(f"Iniciando carga completa dos dados para a tabela '{table_name}'.")

    try:
        # Conectar ao banco PostgreSQL e preparar para operações
        postgres_cursor = postgres_conn.cursor()
        logger.info(
            f"Conexão estabelecida com o banco PostgreSQL para a tabela '{table_name}'."
        )

        # Carregar dados do SQL Server
        query = f"SELECT * FROM sankhya.{table_name}"
        logger.info(f"Executando consulta no SQL Server: {query}")

        df = pd.read_sql(query, sql_conn)
        df.columns = df.columns.str.lower()
        logger.info(
            f"Encontrados {len(df)} registros no SQL Server para sincronização."
        )

        # Remover todos os registros existentes na tabela PostgreSQL
        delete_query = f"DELETE FROM sankhya.{table_name}"
        postgres_cursor.execute(delete_query)
        postgres_conn.commit()
        logger.info(
            f"Todos os registros da tabela 'sankhya.{table_name}' foram removidos no PostgreSQL."
        )

        # Inserir os dados no PostgreSQL
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
            f"Erro durante o processo de carga completa para a tabela '{table_name}': {e}"
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
            primary_keys = sqlserver_check_primary_keys(
                sqlserver_connection, table_name
            )
            update_recent_data(
                sql_conn=sqlserver_connection,
                postgres_engine_url=postgres_engine_url,
                postgres_conn=postgres_connection,
                table_name=table_name,
                date_column_name=date_column,
                days=days,
                primary_keys=primary_keys,
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


def full_load(table_name):
    try:
        sqlserver_connection = get_connection()
        postgres_connection = get_postgres_connection()
        postgres_engine_url = get_postgres_engine_string_url()

        if compare_columns_between_databases(
            sqlserver_conn=sqlserver_connection,
            postgres_conn=postgres_connection,
            table_name=table_name,
        ):
            primary_keys = sqlserver_check_primary_keys(
                sqlserver_connection, table_name
            )
            update_full_table(
                sql_conn=sqlserver_connection,
                postgres_engine_url=postgres_engine_url,
                postgres_conn=postgres_connection,
                table_name=table_name,
            )
        else:
            logger.warning(
                f"Tabela {table_name} não foi migrada devido a diferenças de colunas."
            )

    except Exception as e:
        logger.error(f"Erro durante o processo de migração: {e}")
        raise


def log_update(
    log_schema_name, table_name, sqlserver_conn, postgres_conn, postgres_engine
):

    # Consulta inicial para identificar colunas e registros sem DhIntegracao
    pks_query = f"""
    SELECT TOP 1 * 
    FROM LogSincronizacaoDW.{log_schema_name}.{table_name}
    WHERE DhIntegracao IS NULL
    """
    logs_df = pd.read_sql(pks_query, sqlserver_conn)

    if logs_df.empty:
        logger.warning(
            f"Não foi encontrado nenhum registro pendente de integração em LogSincronizacaoDW.{log_schema_name}.{table_name}"
        )
        return {"pks": "", "pks_values": [""], "last_sync_table_id": None}

    # Identificação de chaves primárias
    default_columns = ["SyncTableId", "TipoOperacao", "DhOperacao", "DhIntegracao"]
    primary_keys = [col for col in logs_df.columns if col not in default_columns]

    # Consulta para obter valores únicos e último SyncTableId
    query = f"""
    SELECT {', '.join(primary_keys)}, MAX(DhOperacao) AS DhOperacao, SyncTableId
    FROM LogSincronizacaoDW.{log_schema_name}.{table_name}
    GROUP BY {', '.join(primary_keys)}, SyncTableId
    """
    pk_values_df = pd.read_sql(query, sqlserver_conn)

    if pk_values_df.empty:
        logger.warning("pk_values_df está vazio, nenhuma consulta gerada.")
        return {"pks": "", "pks_values": []}

    last_sync_table_id = pk_values_df["SyncTableId"].max()
    logger.info(f"Último SyncTableId: {last_sync_table_id}")

    # Remoção da coluna DhOperacao para o processamento
    pk_values_df = pk_values_df.drop("DhOperacao", axis=1)

    # Geração da expressão para as chaves primárias
    if len(primary_keys) == 1:
        pk_expression = primary_keys[0]
        primary_key_values = ", ".join(map(str, pk_values_df[primary_keys[0]].tolist()))
    else:
        pk_expression = "CONCAT_WS('|', " + ", ".join(primary_keys) + ")"
        pks_values = pk_values_df[primary_keys].astype(str).agg("|".join, axis=1)
        primary_key_values = ", ".join([f"'{v}'" for v in pks_values])

    # Consulta para buscar os valores a serem inseridos no PostgreSQL
    get_values_query = f"""
    SELECT * 
    FROM sankhya_prod.{log_schema_name}.{table_name}
    WHERE {pk_expression} IN ({primary_key_values})
    """
    values_to_insert = pd.read_sql(get_values_query, sqlserver_conn)

    # Exclusão dos registros existentes no PostgreSQL
    with postgres_conn.cursor() as postgres_cursor:
        delete_from_pk(
            postgres_cursor=postgres_cursor,
            schema_name="sankhya",
            table_name=table_name,
            primary_keys=pk_expression,
            source_values=primary_key_values,
        )
        postgres_conn.commit()

    # Inserção dos novos valores no PostgreSQL
    with postgres_engine.connect() as postgres_url_conn:
        transaction = postgres_url_conn.begin()
        try:
            values_to_insert.columns = values_to_insert.columns.str.lower()
            values_to_insert.to_sql(
                table_name,
                postgres_url_conn,
                schema="sankhya",
                if_exists="append",
                index=False,
            )
            transaction.commit()
            logger.info("Dados inseridos com sucesso.")
        except Exception as e:
            transaction.rollback()
            logger.error(f"Erro ao inserir os dados: {e}")
            raise

    return {
        "pks": primary_keys,
        "pks_values": primary_key_values,
        "last_sync_table_id": last_sync_table_id,
    }


def log_delete(
    log_schema_name,
    table_name,
    sqlserver_conn,
    postgres_conn,
):
    query = f"""
        SELECT *
        FROM LogSincronizacaoDW.{log_schema_name}.{table_name}
        WHERE DhIntegracao IS NULL
        AND TipoOperacao = 'D'
    """
    logs_df = pd.read_sql(query, sqlserver_conn)
    postgres_cursor = postgres_conn.cursor()

    default_columns = ["SyncTableId", "TipoOperacao", "DhOperacao", "DhIntegracao"]
    primary_keys = [col for col in logs_df.columns if col not in default_columns]
    pks_values = logs_df[primary_keys].astype(str).agg("|".join, axis=1)

    if len(primary_keys) == 1:
        logger.info(f"Primary key única: {primary_keys[0]}")
        primary_key_values = ", ".join(map(str, logs_df[primary_keys[0]].tolist()))
        pk_expression = primary_keys[0]
    else:
        logger.info(f"Primary key múltipla: {primary_keys}")
        # Transforma [(pk1, pk2), (pk1, pk2)] em ['pk1|pk2', 'pk1|pk2']
        pks_values = logs_df[primary_keys].astype(str).agg("|".join, axis=1)

        # Transforma ['pk1|pk2', 'pk1|pk2'] em "'pk1|pk2', 'pk1|pk2'"
        primary_key_values = ", ".join([f"'{v}'" for v in pks_values])
        pk_expression = "CONCAT_WS('|', " + ", ".join(primary_keys) + ")"

    delete_from_pk(
        postgres_cursor=postgres_cursor,
        schema_name="sankhya",
        table_name=table_name,
        primary_keys=pk_expression,
        source_values=primary_key_values,
    )
    postgres_conn.commit()
    logger.info("Dados deletados com sucesso")
    return {"pks": primary_keys, "pks_values": pks_values}


def update_by_logs_table(
    log_schema_name, table_name, sqlserver_conn, postgres_conn, postgres_engine
):
    try:
        # ================== Verificação de Colunas ==================
        if not compare_columns_between_databases(
            sqlserver_conn=sqlserver_conn,
            postgres_conn=postgres_conn,
            table_name=table_name,
        ):
            logger.warning(
                f"Tabela {table_name} não foi processada devido a diferenças de colunas."
            )
            return

        # ========================== PASSO 1 ==========================
        # =================== Deletando os Deletados ==================
        logger.info("Deletando os registros de DELETE")
        deleted_rows = log_delete(
            log_schema_name,
            table_name,
            sqlserver_conn,
            postgres_conn,
        )
        if not deleted_rows["pks_values"].empty:
            update_values_by_pk(
                sqlserver_conn=sqlserver_conn,
                schema_name=log_schema_name,
                table_name=table_name,
                primary_keys=deleted_rows["pks"],
                source_values=deleted_rows["pks_values"],
                operation_type="delete",
            )

        # ========================== PASSO 2 ==========================
        # ================== Update dos mais recentes ==================
        updated_rows = log_update(
            sqlserver_conn=sqlserver_conn,
            log_schema_name=log_schema_name,
            table_name=table_name,
            postgres_conn=postgres_conn,
            postgres_engine=postgres_engine,
        )
        if updated_rows["pks_values"] == [""]:
            return

        update_values_by_pk(
            sqlserver_conn=sqlserver_conn,
            schema_name=log_schema_name,
            table_name=table_name,
            primary_keys=updated_rows["pks"],
            source_values=updated_rows["pks_values"],
            operation_type="update",
            last_SyncTableId=updated_rows["last_sync_table_id"],
        )
    except Exception as e:
        logger.error(f"Erro durante a execução de update_by_logs_table: {e}")
        raise
