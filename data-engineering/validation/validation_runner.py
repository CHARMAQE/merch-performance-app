from datetime import datetime
import mysql.connector

from config.db_config import DB_CONFIG
from validation.engine.validation_engine import run_all_validations


def insert_validation_run(cursor) -> int:
    cursor.execute(
        """
        INSERT INTO validation_run_log (started_at, status)
        VALUES (%s, %s)
        """,
        (datetime.now(), "RUNNING"),
    )
    return cursor.lastrowid


def finish_validation_run(cursor, run_id: int, status: str, rules_executed: int, issues_found: int, error_message: str | None = None):
    cursor.execute(
        """
        UPDATE validation_run_log
        SET finished_at=%s,
            status=%s,
            rules_executed=%s,
            issues_found=%s,
            error_message=%s
        WHERE run_id=%s
        """,
        (
            datetime.now(),
            status,
            rules_executed,
            issues_found,
            error_message,
            run_id,
        ),
    )


def _normalize_target_visit_ids(target_visit_ids: list[int] | None) -> list[int] | None:
    if target_visit_ids is None:
        return None

    return sorted({int(visit_id) for visit_id in target_visit_ids})


def main(target_visit_ids: list[int] | None = None):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    normalized_target_visit_ids = _normalize_target_visit_ids(target_visit_ids)

    run_id = insert_validation_run(cursor)
    conn.commit()

    try:
        results = run_all_validations(run_id, target_visit_ids=normalized_target_visit_ids)

        rules_executed = len(results)
        issues_found = sum(results.values())

        finish_validation_run(
            cursor,
            run_id=run_id,
            status="SUCCESS",
            rules_executed=rules_executed,
            issues_found=issues_found,
        )
        conn.commit()

        print("Validation finished successfully.")
        print(f"Run ID: {run_id}")
        if normalized_target_visit_ids is None:
            print("Validation scope: full database")
        else:
            print(f"Validation scope: {len(normalized_target_visit_ids)} uploaded visits")
        for rule_code, count in results.items():
            print(f"{rule_code}: {count}")
        print(f"Total issues found: {issues_found}")

    except Exception as exc:
        finish_validation_run(
            cursor,
            run_id=run_id,
            status="FAILED",
            rules_executed=0,
            issues_found=0,
            error_message=str(exc)[:4000],
        )
        conn.commit()
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
