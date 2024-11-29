from db.migrate import migrate_multiple_tables, check_and_update_recent_date


def main():
    migrate_multiple_tables()


if __name__ == "__main__":
    main()
