import pandas as pd
import mysql.connector

from config.db_config import DB_CONFIG
from transform.etl_helpers import clean_text, clean_float


def build_survey_responses_dataframe(excel_file: str) -> pd.DataFrame:
    df = pd.read_excel(excel_file)
    df.columns = df.columns.str.strip().str.lower()

    df["date"] = pd.to_datetime(df["dateid"].astype(str), format="%Y%m%d", errors="coerce")
    df["responsedate"] = pd.to_datetime(
        pd.to_numeric(df["responsedate"], errors="coerce"),
        unit="D", origin="1899-12-30", errors="coerce"
    )

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT v.visit_id, v.visit_date, e.employee_code, s.store_code
        FROM visits v
        JOIN employees e ON e.employee_id = v.employee_id
        JOIN stores s ON s.store_id = v.store_id
    """)
    visit_rows = cursor.fetchall()
    cursor.close()
    conn.close()

    visit_map = {
        (str(visit_date), employee_code, store_code): visit_id
        for visit_id, visit_date, employee_code, store_code in visit_rows
    }

    df["visit_id"] = df.apply(
        lambda r: visit_map.get((
            str(r["date"].date()) if pd.notna(r["date"]) else None,
            clean_text(r["employeecode"]),
            clean_text(r["storecode"])
        )),
        axis=1
    )

    out = pd.DataFrame({
        "visit_id": df["visit_id"],
        "employee_code": df["employeecode"].apply(clean_text),
        "store_code": df["storecode"].apply(clean_text),
        "product_code": df["productcode"].apply(clean_text),
        "task": df["task"].apply(clean_text),
        "title": df["title"].apply(clean_text),
        "question": df["question"].apply(clean_text),
        "response": df["response"].apply(clean_text),
        "response_datetime": df["responsedate"],
        "latitude": df["latitude"].apply(clean_float),
        "longitude": df["longitude"].apply(clean_float),
    })

    out = out[out["visit_id"].notna()].copy()
    out = out.where(pd.notna(out), None)
    return out