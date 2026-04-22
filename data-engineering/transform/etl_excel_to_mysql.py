import mysql.connector

from config.db_config import DB_CONFIG
from load.load_base_tables import (
    load_employees,
    load_products,
    load_stores,
    load_visits,
)
from load.load_task_tables import full_refresh_database, load_task_tables
from transform.build_base_tables import (
    build_employees_dataframe,
    build_products_dataframe,
    build_stores_dataframe,
    build_visits_dataframe,
)
from transform.build_task_tables import build_tagged_task_dataframe, build_task_table_batches

FULL_REFRESH_ON_EACH_RUN = False


def run_etl(source_df, full_refresh=None, logger=print):
    if full_refresh is None:
        full_refresh = FULL_REFRESH_ON_EACH_RUN

    df = source_df.copy()

    logger("Preparing ETL from in-memory dataframe...")
    logger(f"Columns: {df.columns.tolist()}")
    logger(f"Total rows: {len(df)}")

    employees_df = build_employees_dataframe(df)
    stores_df = build_stores_dataframe(df)
    products_df = build_products_dataframe(df)
    visits_df = build_visits_dataframe(df)

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()

    try:
        if full_refresh:
            full_refresh_database(cursor, db)

        emp_map = load_employees(db, cursor, employees_df, logger=logger)
        store_map = load_stores(db, cursor, stores_df, logger=logger)
        prod_map = load_products(db, cursor, products_df, logger=logger)
        visit_map = load_visits(db, cursor, visits_df, emp_map, store_map, logger=logger)

        logger("\n--- Building task tables ---")
        tagged_df = build_tagged_task_dataframe(df, visit_map, prod_map)
        logger(f"  {len(tagged_df)} rows matched to task tables")

        task_batches = build_task_table_batches(tagged_df)
        affected_visit_ids = sorted({int(v) for v in visit_map.values() if v is not None})

        logger("\n--- Loading task tables ---")
        load_task_tables(db, cursor, task_batches, affected_visit_ids, logger=logger)

        logger("\nDone!")
        return {
            "rows": len(df),
            "employees": len(emp_map),
            "stores": len(store_map),
            "products": len(prod_map),
            "visits": len(visit_map),
        }

    except Exception:
        db.rollback()
        raise

    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    raise SystemExit("Run main.py instead.")
