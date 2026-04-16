import hashlib
import sys
from datetime import datetime
from pathlib import Path

import mysql.connector

from config.db_config import DB_CONFIG
from config.paths import INBOUND_DIR, ARCHIVE_SUCCESS_DIR, ARCHIVE_FAILED_DIR
from transform.etl_excel_to_mysql import run_etl
from transform.build_survey_responses import build_survey_responses_dataframe
from load.load_survey_responses import load_survey_responses

ALLOWED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_latest_excel_file(folder: Path) -> Path | None:
    if not folder.exists():
        return None

    files = [
        p for p in folder.rglob("*")
        if p.is_file() and p.suffix.lower() in ALLOWED_EXTENSIONS
    ]

    if not files:
        return None

    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0]


def ensure_dirs():
    ARCHIVE_SUCCESS_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_FAILED_DIR.mkdir(parents=True, exist_ok=True)


def db_conn():
    return mysql.connector.connect(**DB_CONFIG)


def insert_run_log(cursor, source_file: str):
    cursor.execute(
        """
        INSERT INTO etl_run_log (started_at, status, source_file)
        VALUES (%s, %s, %s)
        """,
        (datetime.now(), "RUNNING", source_file),
    )
    return cursor.lastrowid


def finish_run_log(cursor, run_id: int, status: str, metrics: dict | None = None, error_message: str | None = None):
    metrics = metrics or {}
    cursor.execute(
        """
        UPDATE etl_run_log
        SET finished_at=%s,
            status=%s,
            rows_loaded=%s,
            employees_loaded=%s,
            stores_loaded=%s,
            products_loaded=%s,
            visits_loaded=%s,
            error_message=%s
        WHERE run_id=%s
        """,
        (
            datetime.now(),
            status,
            metrics.get("rows"),
            metrics.get("employees"),
            metrics.get("stores"),
            metrics.get("products"),
            metrics.get("visits"),
            error_message,
            run_id,
        ),
    )


def already_loaded(cursor, file_hash: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM etl_file_registry WHERE file_hash=%s AND status='SUCCESS' LIMIT 1",
        (file_hash,),
    )
    return cursor.fetchone() is not None


def upsert_file_registry(cursor, file_name: str, file_hash: str, file_size: int, modified_at: datetime, status: str, error_message: str | None = None):
    cursor.execute(
        """
        INSERT INTO etl_file_registry (file_name, file_hash, file_size, file_modified_at, status, loaded_at, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            file_name=VALUES(file_name),
            file_size=VALUES(file_size),
            file_modified_at=VALUES(file_modified_at),
            status=VALUES(status),
            loaded_at=VALUES(loaded_at),
            error_message=VALUES(error_message)
        """,
        (
            file_name,
            file_hash,
            file_size,
            modified_at,
            status,
            datetime.now() if status == "SUCCESS" else None,
            error_message,
        ),
    )


def move_file(src: Path, dst_dir: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = dst_dir / f"{src.stem}_{ts}{src.suffix}"
    src.rename(target)
    return target


def main():
    ensure_dirs()

    file_path = get_latest_excel_file(INBOUND_DIR)
    if file_path is None:
        print(f"No Excel file found in: {INBOUND_DIR}")
        return 0

    file_hash = sha256_file(file_path)
    file_size = file_path.stat().st_size
    modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)

    conn = db_conn()
    cursor = conn.cursor()

    run_id = insert_run_log(cursor, str(file_path.name))
    conn.commit()

    try:
        if already_loaded(cursor, file_hash):
            finish_run_log(cursor, run_id, "SUCCESS", metrics={"rows": 0}, error_message="Skipped: file already loaded")
            upsert_file_registry(cursor, file_path.name, file_hash, file_size, modified_at, "SUCCESS", "Skipped: file already loaded")
            conn.commit()
            move_file(file_path, ARCHIVE_SUCCESS_DIR)
            print("Skipped duplicate file (already loaded).")
            return 0

        metrics = run_etl(excel_file=str(file_path), full_refresh=False, logger=print)

        survey_df = build_survey_responses_dataframe(str(file_path))
        survey_count = load_survey_responses(survey_df)
        print(f"survey_responses loaded: {survey_count}")

        finish_run_log(cursor, run_id, "SUCCESS", metrics=metrics)
        upsert_file_registry(cursor, file_path.name, file_hash, file_size, modified_at, "SUCCESS")
        conn.commit()

        moved = move_file(file_path, ARCHIVE_SUCCESS_DIR)
        print(f"ETL success. Archived: {moved}")
        return 0

    except Exception as exc:
        err = str(exc)[:4000]
        finish_run_log(cursor, run_id, "FAILED", error_message=err)
        upsert_file_registry(cursor, file_path.name, file_hash, file_size, modified_at, "FAILED", err)
        conn.commit()

        try:
            moved = move_file(file_path, ARCHIVE_FAILED_DIR)
            print(f"ETL failed. File moved to failed archive: {moved}")
        except Exception:
            print("ETL failed and could not move file to failed archive.")
        print(err)
        return 1

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())