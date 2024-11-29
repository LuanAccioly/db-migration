from fastapi import APIRouter, HTTPException
from classes.classes import MigrationRequest
from typing import Dict
from db.migrate import check_and_update_recent_date
import logging
from logs.log_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/")
async def hello():
    return {"message": "My name is Chicken Little"}


@router.post("/load_days")
def update_table(request: MigrationRequest) -> Dict[str, str]:
    table_name = request.table_name
    days = request.days
    date_column = request.date_column

    try:
        # Chama a função para verificar e atualizar a tabela
        check_and_update_recent_date(
            days=days, table_name=table_name, date_column=date_column
        )

        # Log de sucesso
        success_message = f"Update feito com sucesso na tabela '{table_name}', trazendo dados dos últimos {days} dias baseados na coluna '{date_column}'."
        logging.info(success_message)

        return {"status": "success", "message": success_message}

    except Exception as e:
        # Log de erro
        error_message = f"Falha ao atualizar tabela '{table_name}' com dados dos últimos {days} dias baseados na coluna '{date_column}'. Error: {str(e)}"
        logging.error(error_message)

        # Lança um erro HTTP com detalhes
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": error_message, "error": str(e)},
        )
