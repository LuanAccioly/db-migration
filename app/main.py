import argparse
import sys
from db.migrate import update_by_logs_table
from db.sqlserver.config import get_connection
from db.postgres.config import get_postgres_connection, get_postgres_engine_string_url
import logging

# Configurando o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_arguments():
    """
    Parse argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(
        description="Executa operações de sincronização entre SQL Server e PostgreSQL."
    )
    parser.add_argument(
        "operation",
        type=str,
        choices=["update"],
        help="Operação a ser realizada. Atualmente, apenas 'update' é suportado.",
    )
    parser.add_argument(
        "log_schema_name",
        type=str,
        help="Nome do schema de log no banco de dados.",
    )
    parser.add_argument(
        "table_name",
        type=str,
        help="Nome da tabela a ser sincronizada.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    sql_conn = None
    postgres_conn = None

    try:
        # Conexões com os bancos de dados
        logger.info("Estabelecendo conexão com SQL Server...")
        sql_conn = get_connection()

        logger.info("Estabelecendo conexão com PostgreSQL...")
        postgres_conn = get_postgres_connection()
        postgres_engine = get_postgres_engine_string_url()

        # Realizando a operação
        if args.operation == "update":
            logger.info(
                f"Iniciando atualização para a tabela {args.log_schema_name}.{args.table_name}..."
            )
            update_by_logs_table(
                log_schema_name=args.log_schema_name,
                table_name=args.table_name,
                sqlserver_conn=sql_conn,
                postgres_conn=postgres_conn,
                postgres_engine=postgres_engine,
            )
            logger.info("Atualização concluída com sucesso.")
        else:
            logger.error(f"Operação '{args.operation}' não suportada.")

    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        sys.exit(1)

    finally:
        # Fechando conexões, se abertas
        if sql_conn:
            logger.info("Fechando conexão com SQL Server.")
            sql_conn.close()

        if postgres_conn:
            logger.info("Fechando conexão com PostgreSQL.")
            postgres_conn.close()


if __name__ == "__main__":
    main()
