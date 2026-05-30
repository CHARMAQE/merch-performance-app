from pathlib import Path

import mysql.connector

from config.db_config import DB_CONFIG
from transform.etl_helpers import to_sql_value


CREATE_ETL_RUN_LOG_SQL = """
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_type VARCHAR(40) NOT NULL,
    source_mode VARCHAR(40) NULL,
    file_name VARCHAR(255) NULL,
    file_path TEXT NULL,
    date_from DATE NULL,
    date_to DATE NULL,
    rows_read INT NULL,
    rows_loaded INT NULL,
    rows_inserted INT NULL,
    rows_updated INT NULL,
    affected_visits INT NULL,
    survey_responses INT NULL,
    validation_enabled TINYINT(1) NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    error_message TEXT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    INDEX idx_etl_run_source_type (source_type),
    INDEX idx_etl_run_status (status),
    INDEX idx_etl_run_started_at (started_at),
    INDEX idx_etl_run_date_range (date_from, date_to)
)
"""


def _execute(sql, params=None, fetch_id=False):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        cursor.execute(CREATE_ETL_RUN_LOG_SQL)
        cursor.execute(sql, params or ())
        run_id = cursor.lastrowid if fetch_id else None
        conn.commit()
        return run_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def start_etl_run(
    source_type: str,
    source_mode: str | None,
    file_path: str | None,
    validation_enabled: bool = False,
) -> int:
    path = Path(file_path) if file_path else None
    return _execute(
        """
        INSERT INTO etl_run_log (
            source_type,
            source_mode,
            file_name,
            file_path,
            validation_enabled,
            status
        )
        VALUES (%s, %s, %s, %s, %s, 'RUNNING')
        """,
        (
            source_type,
            source_mode,
            path.name if path else None,
            str(path) if path else None,
            1 if validation_enabled else 0,
        ),
        fetch_id=True,
    )


def finish_etl_run(
    run_id: int,
    status: str,
    date_from=None,
    date_to=None,
    rows_read: int | None = None,
    rows_loaded: int | None = None,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    affected_visits: int | None = None,
    survey_responses: int | None = None,
    error_message: str | None = None,
) -> None:
    _execute(
        """
        UPDATE etl_run_log
        SET
            date_from = %s,
            date_to = %s,
            rows_read = %s,
            rows_loaded = %s,
            rows_inserted = %s,
            rows_updated = %s,
            affected_visits = %s,
            survey_responses = %s,
            status = %s,
            error_message = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
        """,
        (
            to_sql_value(date_from),
            to_sql_value(date_to),
            rows_read,
            rows_loaded,
            rows_inserted,
            rows_updated,
            affected_visits,
            survey_responses,
            status,
            error_message[:5000] if error_message else None,
            run_id,
        ),
    )
