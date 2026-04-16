import mysql.connector
import pandas as pd

from config.db_config import DB_CONFIG


def _nullify(value):
    if pd.isna(value):
        return None
    return value


def load_survey_responses(df: pd.DataFrame) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0

    cursor.execute("DELETE FROM survey_responses")

    for _, row in df.iterrows():
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

        cursor.execute("""
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
        """, (
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
        ))
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()
    return inserted