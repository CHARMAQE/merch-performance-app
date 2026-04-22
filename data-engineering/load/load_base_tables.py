import pandas as pd

from load.load_task_tables import get_all_task_tables, table_exists
from transform.etl_helpers import to_sql_value


def delete_visit_payload(cursor, visit_id):
    for table_name in get_all_task_tables():
        if table_exists(cursor, table_name):
            cursor.execute(f"DELETE FROM {table_name} WHERE visit_id=%s", (visit_id,))

    if table_exists(cursor, "survey_responses"):
        cursor.execute("DELETE FROM survey_responses WHERE visit_id=%s", (visit_id,))

    cursor.execute("DELETE FROM visits WHERE visit_id=%s", (visit_id,))


def load_employees(db, cursor, employees_df, logger=print):
    logger("\n--- Loading employees ---")

    emp_map = {}

    for _, row in employees_df.iterrows():
        code = to_sql_value(row["employee_code"])
        username = to_sql_value(row["username"])

        cursor.execute("SELECT employee_id FROM employees WHERE employee_code=%s", (code,))
        result = cursor.fetchone()

        if result:
            emp_map[code] = result[0]
        else:
            cursor.execute(
                "INSERT INTO employees (employee_code, username) VALUES (%s, %s)",
                (code, username),
            )
            emp_map[code] = cursor.lastrowid

    db.commit()
    logger(f"  {len(emp_map)} employees")

    return emp_map


def load_stores(db, cursor, stores_df, logger=print):
    logger("--- Loading stores ---")

    store_map = {}

    for _, row in stores_df.iterrows():
        code = to_sql_value(row["store_code"])

        cursor.execute("SELECT store_id FROM stores WHERE store_code=%s", (code,))
        result = cursor.fetchone()

        if result:
            store_map[code] = result[0]
        else:
            cursor.execute(
                """
                INSERT INTO stores
                (store_code, store_name, store_city, store_state, store_region, store_format)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    to_sql_value(row["store_code"]),
                    to_sql_value(row["store_name"]),
                    to_sql_value(row["store_city"]),
                    to_sql_value(row["store_state"]),
                    to_sql_value(row["store_region"]),
                    to_sql_value(row["store_format"]),
                ),
            )
            store_map[code] = cursor.lastrowid

    db.commit()
    logger(f"  {len(store_map)} stores")

    return store_map


def load_products(db, cursor, products_df, logger=print):
    logger("--- Loading products ---")

    prod_map = {}

    for _, row in products_df.iterrows():
        code = to_sql_value(row["product_code"])

        cursor.execute("SELECT product_id FROM products WHERE product_code=%s", (code,))
        result = cursor.fetchone()

        if result:
            prod_map[code] = result[0]
        else:
            cursor.execute(
                """
                INSERT INTO products
                (product_code, barcode, product_description, brand, category, sub_category)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    to_sql_value(row["product_code"]),
                    to_sql_value(row["barcode"]),
                    to_sql_value(row["product_description"]),
                    to_sql_value(row["brand"]),
                    to_sql_value(row["category"]),
                    to_sql_value(row["sub_category"]),
                ),
            )
            prod_map[code] = cursor.lastrowid

    db.commit()
    logger(f"  {len(prod_map)} products")

    return prod_map


def load_visits(db, cursor, visits_df, emp_map, store_map, logger=print):
    logger("--- Loading visits ---")

    visit_map = {}

    for _, row in visits_df.iterrows():
        employee_code = to_sql_value(row["employee_code"])
        store_code = to_sql_value(row["store_code"])
        visit_date = to_sql_value(row["visit_date"])
        year = to_sql_value(row["year"])
        month = to_sql_value(row["month"])
        latitude = to_sql_value(row["latitude"])
        longitude = to_sql_value(row["longitude"])
        map_link = to_sql_value(row["map_link"])

        emp_id = emp_map.get(employee_code)
        store_id = store_map.get(store_code)

        if emp_id is None or store_id is None or pd.isna(visit_date):
            continue

        cursor.execute(
            "SELECT visit_id FROM visits WHERE visit_date=%s AND employee_id=%s AND store_id=%s LIMIT 1",
            (visit_date, emp_id, store_id),
        )
        existing_visit = cursor.fetchone()

        if existing_visit:
            delete_visit_payload(cursor, int(existing_visit[0]))

        cursor.execute(
            """
            INSERT INTO visits
            (visit_date, year, month, employee_id, store_id, latitude, longitude, map_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                visit_date,
                year,
                month,
                emp_id,
                store_id,
                latitude,
                longitude,
                map_link,
            ),
        )

        visit_map[(str(visit_date), employee_code, store_code)] = cursor.lastrowid

    db.commit()
    logger(f"  {len(visit_map)} visits")

    return visit_map
