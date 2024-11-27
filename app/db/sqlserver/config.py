import pyodbc
import os
import logging
from dotenv import load_dotenv
from logs.log_config import setup_logging
from sqlalchemy import create_engine

# Carrega as variáveis de ambiente
load_dotenv()


setup_logging()
logger = logging.getLogger(__name__)


# Função para obter a conexão com o banco de dados
def get_connection():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={os.getenv('SQL_SERVER_HOST')};"
            f"DATABASE={os.getenv('SQL_SERVER_DATABASE')};"
            f"UID={os.getenv('SQL_SERVER_USERNAME')};"
            f"PWD={os.getenv('SQL_SERVER_PASSWORD')};"
            "TrustServerCertificate=yes;"
        )
        logger.info("SQL SERVER conectado com sucesso!")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Erro ao conectar ao SQL SERVER: {e}")
        raise  # Lança o erro para ser tratado externamente


def get_connection_string_url():
    try:
        # Criação da string de conexão para SQLAlchemy
        connection_string = (
            f"mssql+pyodbc://{os.getenv('SQL_SERVER_USERNAME')}:{os.getenv('SQL_SERVER_PASSWORD')}@"
            f"{os.getenv('SQL_SERVER_HOST')}/{os.getenv('SQL_SERVER_DATABASE')}?driver=ODBC+Driver+18+for+SQL+Server"
        )

        # Criação do engine com o SQLAlchemy
        engine = create_engine(connection_string, fast_executemany=True)

        conn = engine.connect()
        logger.info("Conectado via string URL no SQL SERVER")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao SQL Server via SQLAlchemy: {e}")
        raise  # Lança o erro para ser tratado externamente
