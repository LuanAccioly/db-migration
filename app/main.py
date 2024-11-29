from db.migrate import migrate_multiple_tables, check_and_update_recent_date
from db.postgres.utils import check_table_exists_postgres, postgres_check_table_columns
from db.postgres.config import get_postgres_connection


def main():
    engine = get_postgres_connection()
    conn = engine.cursor()
    # check_and_update_recent_date(days=10, table_name="tgfcot", date_column="dtalter")
    # check_table_exists_postgres("tcbint")
    print(postgres_check_table_columns(conn, "tcbint"))


if __name__ == "__main__":
    main()
