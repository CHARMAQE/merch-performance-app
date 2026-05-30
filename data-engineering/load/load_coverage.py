import mysql.connector
import pandas as pd

from config.db_config import DB_CONFIG
from transform.build_coverage import COVERAGE_COLUMNS
from transform.etl_helpers import to_sql_value


CREATE_FACT_COVERAGE_SQL = """
CREATE TABLE IF NOT EXISTS fact_coverage (
    coverage_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    visit_date DATE NOT NULL,
    year INT,
    month VARCHAR(20),
    dateid INT,
    employee_code VARCHAR(50) NOT NULL,
    username VARCHAR(100),
    role VARCHAR(100),
    l1name VARCHAR(150),
    l2name VARCHAR(150),
    l3name VARCHAR(150),
    store_code VARCHAR(50) NOT NULL,
    store_name VARCHAR(150),
    store_region VARCHAR(100),
    store_state VARCHAR(100),
    store_city VARCHAR(100),
    store_format VARCHAR(100),
    call_cycle_type VARCHAR(100),
    call_status VARCHAR(100),
    is_planned TINYINT(1) NOT NULL DEFAULT 0,
    is_adhoc TINYINT(1) NOT NULL DEFAULT 0,
    is_done TINYINT(1) NOT NULL DEFAULT 0,
    rejection TINYINT(1) NOT NULL DEFAULT 0,
    deviation TINYINT(1) NOT NULL DEFAULT 0,
    not_visited TINYINT(1) NOT NULL DEFAULT 0,
    task_assigned INT,
    task_done INT,
    task_per DECIMAL(8,4),
    master_latitude DECIMAL(10,6),
    master_longitude DECIMAL(10,6),
    start_time DATETIME,
    start_latitude DECIMAL(10,6),
    start_longitude DECIMAL(10,6),
    start_distance_meters DECIMAL(12,3),
    end_time DATETIME,
    end_latitude DECIMAL(10,6),
    end_longitude DECIMAL(10,6),
    end_distance_meters DECIMAL(12,3),
    time_mm INT,
    time_hh DECIMAL(10,4),
    reason VARCHAR(255),
    user_attendance VARCHAR(100),
    superior_attendance VARCHAR(100),
    final_user_attendance VARCHAR(100),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_fact_coverage_natural_key (visit_date, employee_code, store_code),
    KEY idx_fact_coverage_visit_date (visit_date),
    KEY idx_fact_coverage_employee (employee_code),
    KEY idx_fact_coverage_store (store_code),
    KEY idx_fact_coverage_status (is_planned, is_done, not_visited, deviation)
)
"""


def _clean_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    return to_sql_value(value)


def _iter_rows(df: pd.DataFrame):
    for row in df[COVERAGE_COLUMNS].itertuples(index=False, name=None):
        yield tuple(_clean_value(value) for value in row)


def _fetch_existing_keys(cursor, df: pd.DataFrame) -> set[tuple[str, str, str]]:
    dates = sorted({row.isoformat() for row in df["visit_date"].dropna()})
    if not dates:
        return set()

    placeholders = ",".join(["%s"] * len(dates))
    cursor.execute(
        f"""
        SELECT visit_date, employee_code, store_code
        FROM fact_coverage
        WHERE visit_date IN ({placeholders})
        """,
        dates,
    )
    return {
        (visit_date.isoformat(), str(employee_code), str(store_code))
        for visit_date, employee_code, store_code in cursor.fetchall()
    }


def _count_insert_update_candidates(cursor, df: pd.DataFrame) -> tuple[int, int]:
    existing_keys = _fetch_existing_keys(cursor, df)
    incoming_keys = {
        (row.visit_date.isoformat(), str(row.employee_code), str(row.store_code))
        for row in df[["visit_date", "employee_code", "store_code"]].itertuples(index=False)
    }
    updated = len(incoming_keys & existing_keys)
    inserted = len(incoming_keys - existing_keys)
    return inserted, updated


def load_coverage(df: pd.DataFrame, batch_size: int = 3000) -> dict:
    if df.empty:
        return {"rows": 0, "inserted": 0, "updated": 0}

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    columns_sql = ", ".join(COVERAGE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(COVERAGE_COLUMNS))
    update_sql = ",\n    ".join(
        f"{col} = VALUES({col})"
        for col in COVERAGE_COLUMNS
        if col not in {"visit_date", "employee_code", "store_code"}
    )

    insert_sql = f"""
    INSERT INTO fact_coverage ({columns_sql})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE
    {update_sql}
    """

    try:
        cursor.execute(CREATE_FACT_COVERAGE_SQL)
        inserted, updated = _count_insert_update_candidates(cursor, df)

        batch = []
        loaded = 0
        for row in _iter_rows(df):
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                loaded += len(batch)
                batch.clear()

        if batch:
            cursor.executemany(insert_sql, batch)
            loaded += len(batch)

        conn.commit()
        return {"rows": loaded, "inserted": inserted, "updated": updated}

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
