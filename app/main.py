from db.migrate import migrate_multiple_tables, check_and_update_recent_date


def main():
    # check_and_update_recent_date(days=10, table_name="tgfcot", date_column="dtalter")
    migrate_multiple_tables()


if __name__ == "__main__":
    main()
