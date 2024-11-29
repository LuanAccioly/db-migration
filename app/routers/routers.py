from fastapi import APIRouter, HTTPException
from classes.classes import MigrationRequest
from typing import Dict

router = APIRouter()


@router.get("/")
async def hello():
    return {"message": "My name is Chicken Little"}


@router.post("/load_days")
def migrate_table(request: MigrationRequest) -> Dict[str, str]:
    table_name = request.table_name
    days = request.days
    date_column = request.date_column

    return {"message": f"{table_name}, {days}, {date_column}"}
