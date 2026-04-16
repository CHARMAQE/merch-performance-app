import re
import mysql.connector
import pandas as pd

from config.db_config import DB_CONFIG
from config.paths import RAW_DEFAULT_FILE
from transform.etl_helpers import clean_text, clean_float, question_to_column
from transform.etl_constants import (
    TASK_TABLE_MAP,
    TITLE_TABLE_MAP,
    LOCATION_TABLES,
    TABLES_WITH_PRODUCT,
    TABLES_WITH_TITLE,
    TABLES_WITH_TASK,
)

EXCEL_FILE = RAW_DEFAULT_FILE
FULL_REFRESH_ON_EACH_RUN = False


def get_task_rows(df, task_table_map, title_table_map):
    df = df.copy()
    df["_target_table"] = None

    for task_name, table_name in task_table_map.items():
        mask = df["task"].astype(str).str.strip() == task_name
        df.loc[mask, "_target_table"] = table_name

    unmatched = df["_target_table"].isna()
    for title_key, table_name in title_table_map.items():
        mask = unmatched & df["title"].astype(str).str.upper().str.contains(title_key, na=False)
        df.loc[mask, "_target_table"] = table_name
        unmatched = df["_target_table"].isna()

    return df[df["_target_table"].notna()]


def table_exists(cursor, table_name):
    cursor.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s LIMIT 1",
        (DB_CONFIG["database"], table_name),
    )
    return cursor.fetchone() is not None


def fk_exists(cursor, table_name, constraint_name):
    cursor.execute(
        "SELECT 1 FROM information_schema.table_constraints "
        "WHERE table_schema=%s AND table_name=%s AND constraint_name=%s "
        "AND constraint_type='FOREIGN KEY' LIMIT 1",
        (DB_CONFIG["database"], table_name, constraint_name),
    )
    return cursor.fetchone() is not None


def get_existing_columns(cursor, table_name):
    cursor.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s",
        (DB_CONFIG["database"], table_name),
    )
    return {r[0] for r in cursor.fetchall()}


def get_all_task_tables():
    return sorted(set(TASK_TABLE_MAP.values()) | set(TITLE_TABLE_MAP.values()))


def full_refresh_database(cursor, db):
    task_tables = get_all_task_tables()
    base_tables = ["visits", "products", "stores", "employees"]
    all_tables = task_tables + base_tables

    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    try:
        for table_name in all_tables:
            if table_exists(cursor, table_name):
                cursor.execute(f"TRUNCATE TABLE {table_name}")
        db.commit()
        print("Full refresh: all existing data cleared.")
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")


def delete_visit_payload(cursor, visit_id):
    for table_name in get_all_task_tables():
        if table_exists(cursor, table_name):
            cursor.execute(f"DELETE FROM {table_name} WHERE visit_id=%s", (visit_id,))
    cursor.execute("DELETE FROM visits WHERE visit_id=%s", (visit_id,))


def ensure_task_table_structure(cursor, table_name, has_product, has_title, has_task, has_location_meta):
    if not table_exists(cursor, table_name):
        columns = ["id INT AUTO_INCREMENT PRIMARY KEY", "visit_id INT"]
        if has_product:
            columns.append("product_id INT")
        if has_title:
            columns.append("title TEXT")
        if has_task:
            columns.append("task VARCHAR(100)")
        if has_location_meta:
            columns.append("latitude DECIMAL(10,6)")
            columns.append("longitude DECIMAL(10,6)")
            columns.append("map_link TEXT")
        columns.append("response_date DATE")

        create_sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n)"
        cursor.execute(create_sql)

    existing_cols = get_existing_columns(cursor, table_name)

    wanted = {"visit_id", "response_date"}
    if has_product:
        wanted.add("product_id")
    if has_title:
        wanted.add("title")
    if has_task:
        wanted.add("task")
    if has_location_meta:
        wanted.update({"latitude", "longitude", "map_link"})

    type_map = {
        "visit_id": "INT",
        "product_id": "INT",
        "title": "TEXT",
        "task": "VARCHAR(100)",
        "latitude": "DECIMAL(10,6)",
        "longitude": "DECIMAL(10,6)",
        "map_link": "TEXT",
        "response_date": "DATE",
    }

    for col in wanted:
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {type_map[col]}")

    fk_visit = f"fk_{table_name}_visit"
    if not fk_exists(cursor, table_name, fk_visit):
        cursor.execute(
            f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_visit} "
            f"FOREIGN KEY (visit_id) REFERENCES visits(visit_id)"
        )

    if has_product:
        fk_product = f"fk_{table_name}_product"
        if not fk_exists(cursor, table_name, fk_product):
            cursor.execute(
                f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_product} "
                f"FOREIGN KEY (product_id) REFERENCES products(product_id)"
            )


def ensure_question_columns(cursor, table_name, question_col_map):
    existing_cols = get_existing_columns(cursor, table_name)
    for _, col in question_col_map.items():
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")


def ensure_sos_table(cursor, table_name):
    if not table_exists(cursor, table_name):
        cursor.execute(
            f"""CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                visit_id INT,
                task VARCHAR(100),
                response_date DATE,
                qst TEXT,
                qst_value TEXT,
                total TEXT
            )"""
        )

    existing_cols = get_existing_columns(cursor, table_name)
    sos_cols = {
        "visit_id": "INT",
        "task": "VARCHAR(100)",
        "response_date": "DATE",
        "qst": "TEXT",
        "qst_value": "TEXT",
        "total": "TEXT",
    }
    for col, typ in sos_cols.items():
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {typ}")

    fk_visit = f"fk_{table_name}_visit"
    if not fk_exists(cursor, table_name, fk_visit):
        cursor.execute(
            f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_visit} "
            f"FOREIGN KEY (visit_id) REFERENCES visits(visit_id)"
        )


def run_etl(excel_file=EXCEL_FILE, full_refresh=None, logger=print):
    if full_refresh is None:
        full_refresh = FULL_REFRESH_ON_EACH_RUN

    logger("Reading Excel file...")
    df = pd.read_excel(excel_file)
    df.columns = df.columns.str.strip().str.lower()
    logger(f"Columns: {df.columns.tolist()}")
    logger(f"Total rows: {len(df)}")

    df["date"] = pd.to_datetime(df["dateid"].astype(str), format="%Y%m%d", errors="coerce")
    df["responsedate"] = pd.to_datetime(
        pd.to_numeric(df["responsedate"], errors="coerce"),
        unit="D", origin="1899-12-30", errors="coerce"
    )

    db = mysql.connector.connect(**DB_CONFIG)
    cursor = db.cursor()

    try:
        if full_refresh:
            full_refresh_database(cursor, db)

        logger("\n--- Loading employees ---")
        emp_unique = df[["employeecode", "username"]].drop_duplicates(subset="employeecode")
        emp_map = {}

        for _, row in emp_unique.iterrows():
            code = clean_text(row["employeecode"])
            if not code:
                continue
            cursor.execute("SELECT employee_id FROM employees WHERE employee_code=%s", (code,))
            result = cursor.fetchone()
            if result:
                emp_map[code] = result[0]
            else:
                cursor.execute(
                    "INSERT INTO employees (employee_code, username) VALUES (%s, %s)",
                    (code, clean_text(row["username"]))
                )
                emp_map[code] = cursor.lastrowid
        db.commit()
        logger(f"  {len(emp_map)} employees")

        logger("--- Loading stores ---")
        store_cols = ["storecode", "storename", "storecity", "storestate", "storeregion", "storeformat"]
        store_unique = df[store_cols].drop_duplicates(subset="storecode")
        store_map = {}

        for _, row in store_unique.iterrows():
            code = clean_text(row["storecode"])
            if not code:
                continue
            cursor.execute("SELECT store_id FROM stores WHERE store_code=%s", (code,))
            result = cursor.fetchone()
            if result:
                store_map[code] = result[0]
            else:
                cursor.execute(
                    """INSERT INTO stores
                    (store_code, store_name, store_city, store_state, store_region, store_format)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                    (
                        code,
                        clean_text(row["storename"]),
                        clean_text(row["storecity"]),
                        clean_text(row["storestate"]),
                        clean_text(row["storeregion"]),
                        clean_text(row["storeformat"]),
                    )
                )
                store_map[code] = cursor.lastrowid
        db.commit()
        logger(f"  {len(store_map)} stores")

        logger("--- Loading products ---")
        prod_cols = ["productcode", "productbarcode", "productdescription", "brandname", "category", "subcategory"]
        prod_unique = df[prod_cols].drop_duplicates(subset="productcode")
        prod_map = {}

        for _, row in prod_unique.iterrows():
            code = clean_text(row["productcode"])
            if not code:
                continue
            cursor.execute("SELECT product_id FROM products WHERE product_code=%s", (code,))
            result = cursor.fetchone()
            if result:
                prod_map[code] = result[0]
            else:
                cursor.execute(
                    """INSERT INTO products
                    (product_code, barcode, product_description, brand, category, sub_category)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                    (
                        code,
                        clean_text(row["productbarcode"]),
                        clean_text(row["productdescription"]),
                        clean_text(row["brandname"]),
                        clean_text(row["category"]),
                        clean_text(row["subcategory"]),
                    )
                )
                prod_map[code] = cursor.lastrowid
        db.commit()
        logger(f"  {len(prod_map)} products")

        logger("--- Loading visits ---")
        visit_unique = df[["date", "year", "month", "employeecode", "storecode", "latitude", "longitude"]].drop_duplicates(
            subset=["date", "employeecode", "storecode"]
        )
        visit_map = {}

        for _, row in visit_unique.iterrows():
            emp_code = clean_text(row["employeecode"])
            store_code = clean_text(row["storecode"])
            emp_id = emp_map.get(emp_code)
            store_id = store_map.get(store_code)
            visit_date = row["date"].date() if pd.notna(row["date"]) else None
            lat = clean_float(row["latitude"])
            lon = clean_float(row["longitude"])
            map_link = f"https://www.google.com/maps?q={lat},{lon}" if lat is not None and lon is not None else None

            if emp_id is None or store_id is None or visit_date is None:
                continue

            cursor.execute(
                "SELECT visit_id FROM visits WHERE visit_date=%s AND employee_id=%s AND store_id=%s LIMIT 1",
                (visit_date, emp_id, store_id),
            )
            existing_visit = cursor.fetchone()
            if existing_visit:
                delete_visit_payload(cursor, int(existing_visit[0]))

            cursor.execute(
                "INSERT INTO visits (visit_date, year, month, employee_id, store_id, latitude, longitude, map_link) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (visit_date, row["year"], clean_text(row["month"]), emp_id, store_id, lat, lon, map_link)
            )
            visit_map[(str(visit_date), emp_code, store_code)] = cursor.lastrowid

        db.commit()
        logger(f"  {len(visit_map)} visits")

        affected_visit_ids = sorted({int(v) for v in visit_map.values() if pd.notna(v)})

        logger("\n--- Creating pivoted task tables ---")

        tagged_df = get_task_rows(df, TASK_TABLE_MAP, TITLE_TABLE_MAP)
        logger(f"  {len(tagged_df)} rows matched to task tables")

        tagged_df = tagged_df.copy()
        tagged_df["_visit_id"] = tagged_df.apply(
            lambda r: visit_map.get((
                str(r["date"].date()) if pd.notna(r["date"]) else None,
                clean_text(r["employeecode"]),
                clean_text(r["storecode"])
            )),
            axis=1
        )
        tagged_df["_product_id"] = tagged_df["productcode"].apply(lambda x: prod_map.get(clean_text(x)))
        tagged_df = tagged_df[tagged_df["_visit_id"].notna()]

        for table_name in sorted(tagged_df["_target_table"].unique()):
            table_df = tagged_df[tagged_df["_target_table"] == table_name].copy()

            if table_name == "task_secondary_placement":
                table_df = table_df[
                    ~table_df["task"].astype(str).str.upper().str.contains("GLACE", na=False)
                ]

            if table_df.empty:
                logger(f"\n  {table_name}: 0 rows after filters, skipped")
                continue

            has_product = table_name in TABLES_WITH_PRODUCT
            has_title = table_name in TABLES_WITH_TITLE
            has_task = table_name in TABLES_WITH_TASK
            has_location_meta = table_name in LOCATION_TABLES

            if table_name == "task_sos":
                logger(f"\n  {table_name}: custom reshape ({len(table_df)} source rows)")

                ensure_sos_table(cursor, table_name)

                if affected_visit_ids:
                    ph = ",".join(["%s"] * len(affected_visit_ids))
                    cursor.execute(f"DELETE FROM {table_name} WHERE visit_id IN ({ph})", affected_visit_ids)

                count = 0
                for vid, group_df in table_df.groupby("_visit_id", dropna=False):
                    if pd.isna(vid):
                        continue

                    vid = int(vid)
                    group_df = group_df.sort_index()

                    resp_dates = group_df["responsedate"].dropna()
                    resp_date = resp_dates.iloc[0].date() if len(resp_dates) > 0 else None

                    pending_rows = []
                    current_total = None

                    for _, row in group_df.iterrows():
                        qst = clean_text(row["question"])
                        qst_value = clean_text(row["response"])
                        task_value = clean_text(row["task"])

                        if not qst:
                            continue

                        q_upper = qst.upper()

                        if "TOTAL" in q_upper and "UNILEVER SOS" not in q_upper:
                            current_total = qst_value
                            continue

                        if "UNILEVER SOS" in q_upper:
                            sos_total = None
                            if qst_value:
                                m = re.search(r"\$\s*([0-9]+(?:\.[0-9]+)?)", qst_value)
                                if m:
                                    sos_total = m.group(1)

                            block_total = sos_total if sos_total is not None else current_total

                            for pending_task, pending_qst, pending_val in pending_rows:
                                cursor.execute(
                                    f"INSERT INTO {table_name} (visit_id, task, response_date, qst, qst_value, total) "
                                    f"VALUES (%s,%s,%s,%s,%s,%s)",
                                    (vid, pending_task, resp_date, pending_qst, pending_val, block_total)
                                )
                                count += 1

                            pending_rows = []
                            current_total = None
                            continue

                        pending_rows.append((task_value, qst, qst_value))

                db.commit()
                logger(f"    -> {count} rows inserted")
                continue

            questions = table_df["question"].dropna().astype(str).str.strip().unique()
            questions = sorted([q for q in questions if q])

            col_map = {}
            used_cols = set()
            for q in questions:
                col = question_to_column(q)
                base = col
                i = 2
                while col in used_cols:
                    suffix = f"_{i}"
                    col = f"{base[:64 - len(suffix)]}{suffix}"
                    i += 1
                col_map[q] = col
                used_cols.add(col)

            logger(f"\n  {table_name}: {len(questions)} questions, {len(table_df)} rows")

            ensure_task_table_structure(cursor, table_name, has_product, has_title, has_task, has_location_meta)
            ensure_question_columns(cursor, table_name, col_map)

            if affected_visit_ids:
                ph = ",".join(["%s"] * len(affected_visit_ids))
                cursor.execute(f"DELETE FROM {table_name} WHERE visit_id IN ({ph})", affected_visit_ids)

            group_cols = ["_visit_id"]
            if has_product:
                group_cols.append("_product_id")
            if has_task:
                group_cols.append("task")

            count = 0
            for group_key, group_df in table_df.groupby(group_cols, dropna=False):
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)

                vid = group_key[0]
                pid = group_key[1] if has_product else None

                vid = int(vid)
                pid = int(pid) if pd.notna(pid) and pid is not None else None

                resp_dates = group_df["responsedate"].dropna()
                resp_date = resp_dates.iloc[0].date() if len(resp_dates) > 0 else None

                q_responses = {}
                for _, row in group_df.iterrows():
                    q = clean_text(row["question"])
                    if q and q in col_map:
                        q_responses[col_map[q]] = clean_text(row["response"])

                insert_cols = ["visit_id"]
                insert_vals = [vid]

                if has_product:
                    insert_cols.append("product_id")
                    insert_vals.append(pid)

                if has_title:
                    title_vals = group_df["title"].dropna().astype(str).str.strip()
                    insert_cols.append("title")
                    insert_vals.append(title_vals.iloc[0] if len(title_vals) > 0 else None)

                if has_task:
                    task_vals = group_df["task"].dropna().astype(str).str.strip()
                    insert_cols.append("task")
                    insert_vals.append(task_vals.iloc[0] if len(task_vals) > 0 else None)

                if has_location_meta:
                    lat = clean_float(group_df["latitude"].dropna().iloc[0]) if group_df["latitude"].notna().any() else None
                    lon = clean_float(group_df["longitude"].dropna().iloc[0]) if group_df["longitude"].notna().any() else None
                    map_link = f"https://www.google.com/maps?q={lat},{lon}" if lat is not None and lon is not None else None
                    insert_cols.extend(["latitude", "longitude", "map_link"])
                    insert_vals.extend([lat, lon, map_link])

                insert_cols.append("response_date")
                insert_vals.append(resp_date)

                for q in questions:
                    col = col_map[q]
                    insert_cols.append(col)
                    insert_vals.append(q_responses.get(col))

                placeholders = ",".join(["%s"] * len(insert_vals))
                col_str = ",".join(insert_cols)
                cursor.execute(
                    f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})",
                    insert_vals
                )
                count += 1

            db.commit()
            logger(f"    -> {count} pivoted rows inserted")

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
    run_etl()
    print("Process finished.")