import pandas as pd

from load.load_task_tables import get_all_task_tables, table_exists
from transform.etl_helpers import to_sql_value


def _clean_tuple(row):
    return tuple(to_sql_value(v) for v in row)


def _fetch_map(cursor, table_name, id_col, code_col):
    cursor.execute(f"SELECT {id_col}, {code_col} FROM {table_name}")
    return {str(code): int(row_id) for row_id, code in cursor.fetchall() if code is not None}


def _executemany_chunks(cursor, sql, rows, chunk_size=3000):
    total = 0
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        cursor.executemany(sql, chunk)
        total += len(chunk)
    return total


def delete_visit_payload_batch(cursor, visit_ids):
    if not visit_ids:
        return

    visit_ids = [int(v) for v in visit_ids]
    ph = ",".join(["%s"] * len(visit_ids))

    for table_name in get_all_task_tables():
        if table_exists(cursor, table_name):
            cursor.execute(f"DELETE FROM {table_name} WHERE visit_id IN ({ph})", visit_ids)

    if table_exists(cursor, "survey_responses"):
        cursor.execute(f"DELETE FROM survey_responses WHERE visit_id IN ({ph})", visit_ids)


def load_employees(db, cursor, employees_df, logger=print):
    logger("\n--- Loading employees fast ---")
    rows = [
        _clean_tuple((r.employee_code, r.username))
        for r in employees_df.itertuples(index=False)
        if pd.notna(r.employee_code)
    ]
    if rows:
        _executemany_chunks(
            cursor,
            """
            INSERT INTO employees (employee_code, username)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE username = VALUES(username)
            """,
            rows,
        )
    db.commit()
    emp_map = _fetch_map(cursor, "employees", "employee_id", "employee_code")
    logger(f"  {len(emp_map)} employees in DB")
    return emp_map


def load_stores(db, cursor, stores_df, logger=print):
    logger("--- Loading stores fast ---")
    rows = [
        _clean_tuple((
            r.store_code, r.store_name, r.store_city, r.store_state, r.store_region, r.store_format
        ))
        for r in stores_df.itertuples(index=False)
        if pd.notna(r.store_code)
    ]
    if rows:
        _executemany_chunks(
            cursor,
            """
            INSERT INTO stores (store_code, store_name, store_city, store_state, store_region, store_format)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                store_name = VALUES(store_name),
                store_city = VALUES(store_city),
                store_state = VALUES(store_state),
                store_region = VALUES(store_region),
                store_format = VALUES(store_format)
            """,
            rows,
        )
    db.commit()
    store_map = _fetch_map(cursor, "stores", "store_id", "store_code")
    logger(f"  {len(store_map)} stores in DB")
    return store_map


def load_products(db, cursor, products_df, logger=print):
    logger("--- Loading products fast ---")
    rows = [
        _clean_tuple((
            r.product_code, r.barcode, r.product_description, r.brand, r.category, r.sub_category
        ))
        for r in products_df.itertuples(index=False)
        if pd.notna(r.product_code)
    ]
    if rows:
        _executemany_chunks(
            cursor,
            """
            INSERT INTO products (product_code, barcode, product_description, brand, category, sub_category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                barcode = VALUES(barcode),
                product_description = VALUES(product_description),
                brand = VALUES(brand),
                category = VALUES(category),
                sub_category = VALUES(sub_category)
            """,
            rows,
        )
    db.commit()
    prod_map = _fetch_map(cursor, "products", "product_id", "product_code")
    logger(f"  {len(prod_map)} products in DB")
    return prod_map


def load_visits(db, cursor, visits_df, emp_map, store_map, logger=print):
    logger("--- Loading visits fast ---")

    rows = []
    key_rows = []

    for r in visits_df.itertuples(index=False):
        employee_code = to_sql_value(r.employee_code)
        store_code = to_sql_value(r.store_code)
        emp_id = emp_map.get(str(employee_code))
        store_id = store_map.get(str(store_code))

        if emp_id is None or store_id is None or pd.isna(r.visit_date):
            continue

        visit_date = to_sql_value(r.visit_date)
        rows.append(_clean_tuple((
            visit_date, r.year, r.month, emp_id, store_id, r.latitude, r.longitude, r.map_link
        )))
        key_rows.append((str(visit_date), employee_code, store_code, visit_date, emp_id, store_id))

    if rows:
        _executemany_chunks(
            cursor,
            """
            INSERT INTO visits (visit_date, year, month, employee_id, store_id, latitude, longitude, map_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                year = VALUES(year),
                month = VALUES(month),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                map_link = VALUES(map_link)
            """,
            rows,
        )
    db.commit()

    visit_map = {}
    affected_visit_ids = []
    for visit_date_str, employee_code, store_code, visit_date, emp_id, store_id in key_rows:
        cursor.execute(
            "SELECT visit_id FROM visits WHERE visit_date=%s AND employee_id=%s AND store_id=%s LIMIT 1",
            (visit_date, emp_id, store_id),
        )
        result = cursor.fetchone()
        if result:
            visit_id = int(result[0])
            visit_map[(visit_date_str, employee_code, store_code)] = visit_id
            affected_visit_ids.append(visit_id)

    delete_visit_payload_batch(cursor, affected_visit_ids)
    db.commit()

    logger(f"  {len(visit_map)} visits affected")
    return visit_map
