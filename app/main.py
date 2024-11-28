from db.migrate import migrate_multiple_tables, check_and_update_recent_date


def main():
    check_and_update_recent_date(days=10, table_name="tgfite", date_column="dtalter")


if __name__ == "__main__":
    main()
