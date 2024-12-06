from pydantic import BaseModel, Field


class MigrationRequest(BaseModel):
    table_name: str = Field(..., description="Nome da tabela a ser processada")
    date_column: str = Field(
        ..., description="Nome da coluna que será utilizada como parametro de data"
    )
    days: int = Field(..., gt=0, description="Quantidade de dias para buscar os dados")


class ColumnsMismatchError(Exception):
    """
    Exceção personalizada para discrepâncias nas colunas de tabelas entre bancos de dados.
    """

    def __init__(self, table_name, sqlserver_columns, postgres_columns):
        self.table_name = table_name
        self.sqlserver_columns = sqlserver_columns
        self.postgres_columns = postgres_columns
        super().__init__(
            f"Colunas discrepantes na tabela '{table_name}':\n"
            f"SQL Server: {sqlserver_columns}\n"
            f"PostgreSQL: {postgres_columns}"
        )
