import re

import pandas as pd

from config.db_config import DB_CONFIG
from transform.etl_constants import TASK_TABLE_MAP, TITLE_TABLE_MAP
from transform.etl_helpers import clean_float, clean_text


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
    payload_tables = ["validation_results", "survey_responses"] + task_tables
    base_tables = ["visits", "products", "stores", "employees"]
    all_tables = payload_tables + base_tables

    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    try:
        for table_name in all_tables:
            if table_exists(cursor, table_name):
                cursor.execute(f"TRUNCATE TABLE {table_name}")
        db.commit()
        print("Full refresh: all existing data cleared.")
    finally:
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")


def ensure_task_table_structure(
    cursor,
    table_name,
    has_product,
    has_title,
    has_task,
    has_location_meta,
):
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


def load_sos_table(db, cursor, batch, affected_visit_ids, logger=print):
    table_name = batch["table_name"]
    table_df = batch["table_df"]

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
                    match = re.search(r"\$\s*([0-9]+(?:\.[0-9]+)?)", qst_value)
                    if match:
                        sos_total = match.group(1)

                block_total = sos_total if sos_total is not None else current_total

                for pending_task, pending_qst, pending_val in pending_rows:
                    cursor.execute(
                        f"INSERT INTO {table_name} (visit_id, task, response_date, qst, qst_value, total) "
                        f"VALUES (%s, %s, %s, %s, %s, %s)",
                        (vid, pending_task, resp_date, pending_qst, pending_val, block_total),
                    )
                    count += 1

                pending_rows = []
                current_total = None
                continue

            pending_rows.append((task_value, qst, qst_value))

    db.commit()
    logger(f"    -> {count} rows inserted")


def load_standard_task_table(db, cursor, batch, affected_visit_ids, logger=print):
    table_name = batch["table_name"]
    table_df = batch["table_df"]
    has_product = batch["has_product"]
    has_title = batch["has_title"]
    has_task = batch["has_task"]
    has_location_meta = batch["has_location_meta"]
    questions = batch["questions"]
    question_col_map = batch["question_col_map"]

    logger(f"\n  {table_name}: {len(questions)} questions, {len(table_df)} rows")

    ensure_task_table_structure(
        cursor,
        table_name,
        has_product,
        has_title,
        has_task,
        has_location_meta,
    )
    ensure_question_columns(cursor, table_name, question_col_map)

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
            if q and q in question_col_map:
                q_responses[question_col_map[q]] = clean_text(row["response"])

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
            col = question_col_map[q]
            insert_cols.append(col)
            insert_vals.append(q_responses.get(col))

        placeholders = ",".join(["%s"] * len(insert_vals))
        col_str = ",".join(insert_cols)

        cursor.execute(
            f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})",
            insert_vals,
        )
        count += 1

    db.commit()
    logger(f"    -> {count} pivoted rows inserted")


def load_task_tables(db, cursor, task_batches, affected_visit_ids, logger=print):
    for batch in task_batches:
        if batch["table_name"] == "task_sos":
            load_sos_table(db, cursor, batch, affected_visit_ids, logger=logger)
        else:
            load_standard_task_table(db, cursor, batch, affected_visit_ids, logger=logger)
