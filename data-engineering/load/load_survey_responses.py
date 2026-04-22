import mysql.connector
import pandas as pd

from config.db_config import DB_CONFIG


def _nullify(value):
    if pd.isna(value):
        return None
    return value


def fetch_visit_lookup_dataframe() -> pd.DataFrame:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            v.visit_id,
            v.visit_date,
            e.employee_code,
            s.store_code
        FROM visits v
        JOIN employees e ON e.employee_id = v.employee_id
        JOIN stores s ON s.store_id = v.store_id
        """
    )
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return pd.DataFrame(
        rows,
        columns=["visit_id", "visit_date", "employee_code", "store_code"],
    )


def _build_insert_row(row) -> tuple:
    visit_id = _nullify(row.get("visit_id"))
    employee_code = _nullify(row.get("employee_code"))
    store_code = _nullify(row.get("store_code"))
    product_code = _nullify(row.get("product_code"))
    task = _nullify(row.get("task"))
    title = _nullify(row.get("title"))
    question = _nullify(row.get("question"))
    response = _nullify(row.get("response"))
    response_datetime = _nullify(row.get("response_datetime"))
    latitude = _nullify(row.get("latitude"))
    longitude = _nullify(row.get("longitude"))

    if response_datetime is not None:
        response_datetime = pd.Timestamp(response_datetime).to_pydatetime()

    if visit_id is not None:
        visit_id = int(visit_id)

    return (
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
        longitude,
    )


def load_survey_responses(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    rows_to_insert = [_build_insert_row(row) for _, row in df.iterrows()]

    cursor.executemany(
        """
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
        """,
        rows_to_insert,
    )

    conn.commit()
    cursor.close()
    conn.close()

    return len(rows_to_insert)
