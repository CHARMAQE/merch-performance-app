import math
import mysql.connector
import pandas as pd

from config.db_config import DB_CONFIG

INSERT_SQL = """
INSERT INTO survey_responses (
    visit_id,
    employee_code,
    store_code,
    product_code,
    task,
    title,
    question,
    response,
    response_datetime,
    latitude,
    longitude
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def fetch_visit_lookup_dataframe() -> pd.DataFrame:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT v.visit_id, v.visit_date, e.employee_code, s.store_code
        FROM visits v
        JOIN employees e ON e.employee_id = v.employee_id
        JOIN stores s ON s.store_id = v.store_id
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(rows, columns=["visit_id", "visit_date", "employee_code", "store_code"])


def _clean_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _iter_insert_rows(df: pd.DataFrame):
    cols = [
        "visit_id", "employee_code", "store_code", "product_code", "task", "title",
        "question", "response", "response_datetime", "latitude", "longitude",
    ]
    for row in df[cols].itertuples(index=False, name=None):
        row = list(row)
        if row[0] is not None and not pd.isna(row[0]):
            row[0] = int(row[0])
        yield tuple(_clean_value(v) for v in row)


def load_survey_responses(df: pd.DataFrame, batch_size: int = 5000) -> int:
    """
    Faster than the old version because:
    - uses itertuples instead of iterrows
    - inserts in batches
    - commits once per batch, not row by row
    """
    if df.empty:
        return 0

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0
    batch = []

    try:
        for row in _iter_insert_rows(df):
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(INSERT_SQL, batch)
                conn.commit()
                inserted += len(batch)
                batch.clear()

        if batch:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()
            inserted += len(batch)

        return inserted

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
