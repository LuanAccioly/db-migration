import psycopg2
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from dotenv import load_dotenv
from logs.log_config import setup_logging

load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)
# Parâmetros da conexão


def get_postgres_connection():
    try:
        # Conectando ao banco de dados
        conn = psycopg2.connect(
            database=os.getenv("POSTGRES_DATABASE"),
            user=os.getenv("POSTGRES_USER"),
            host=os.getenv("POSTGRES_HOST"),
            password=os.getenv("POSTGRES_PASSWORD"),
            port=5432,
        )
        logger.info("Postgres conectado com sucesso!")

        return conn

    except psycopg2.Error as e:
        logger.error(f"Erro ao conectar ao Postgres: {e}")
        raise


def get_postgres_engine_string_url():
    try:
        # Conectando ao banco de dados via SQLAlchemy
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}/{os.getenv('POSTGRES_DATABASE')}"
        )
        connection = engine.connect()
        logger.info("Postgres conectado com sucesso via SQLAlchemy!")
        return connection
    except Exception as e:
        logger.error(f"Erro ao conectar ao Postgres: {e}")
        raise
