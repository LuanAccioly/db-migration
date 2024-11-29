from pydantic import BaseModel, Field


class MigrationRequest(BaseModel):
    table_name: str = Field(..., description="Nome da tabela a ser processada")
    date_column: str = Field(
        ..., description="Nome da coluna que será utilizada como parametro de data"
    )
    days: int = Field(..., gt=0, description="Quantidade de dias para buscar os dados")
