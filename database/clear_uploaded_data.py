from pathlib import Path
import sys

import mysql.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ENGINEERING_DIR = PROJECT_ROOT / "data-engineering"
if str(DATA_ENGINEERING_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_ENGINEERING_DIR))

from config.db_config import DB_CONFIG


TABLES_TO_CLEAR = [
    "validation_results",
    "validation_run_log",
    "survey_responses",
    "task_callcycle_deviation",
    "task_location_checkin",
    "task_location_checkout",
    "task_osa_pack_coc_mh",
    "task_primary_shelf_placement",
    "task_quality",
    "task_secondary_placement",
    "task_sos",
    "task_store_conditions",
    "visits",
    "products",
    "stores",
    "employees",
]


def table_exists(cursor, table_name):
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
        """,
        (DB_CONFIG["database"], table_name),
    )
    return cursor.fetchone() is not None


def count_rows(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    return int(cursor.fetchone()[0])


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        print(f"Connected to database: {DB_CONFIG['database']}")
        print("Clearing uploaded data while keeping table structure...")

        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for table_name in TABLES_TO_CLEAR:
            if not table_exists(cursor, table_name):
                print(f"skipped {table_name}: table does not exist")
                continue

            before_count = count_rows(cursor, table_name)
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            print(f"cleared {table_name}: {before_count} rows removed")

        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()

        print("Done. Validation rule definitions were not deleted.")

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
