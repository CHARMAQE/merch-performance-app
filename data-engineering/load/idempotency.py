from __future__ import annotations

import re
from typing import Iterable

from config.db_config import DB_CONFIG


def normalize_visit_ids(visit_ids: Iterable[int] | None) -> list[int]:
    if not visit_ids:
        return []

    normalized = set()
    for visit_id in visit_ids:
        if visit_id is None:
            continue
        normalized.add(int(visit_id))

    return sorted(normalized)


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", identifier):
        raise ValueError(f"Unsafe SQL identifier: {identifier}")
    return f"`{identifier}`"


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
        """,
        (DB_CONFIG["database"], table_name),
    )
    return cursor.fetchone() is not None


def _delete_by_visit_ids(cursor, table_name: str, visit_ids: list[int], chunk_size: int = 1000) -> int:
    if not visit_ids:
        return 0

    deleted = 0
    quoted_table = _quote_identifier(table_name)

    for index in range(0, len(visit_ids), chunk_size):
        chunk = visit_ids[index:index + chunk_size]
        placeholders = ",".join(["%s"] * len(chunk))
        cursor.execute(
            f"DELETE FROM {quoted_table} WHERE visit_id IN ({placeholders})",
            chunk,
        )
        deleted += max(cursor.rowcount, 0)

    return deleted


def discover_task_tables(cursor) -> tuple[list[str], list[str]]:
    cursor.execute(
        """
        SELECT
            t.table_name,
            MAX(CASE WHEN c.column_name = 'visit_id' THEN 1 ELSE 0 END) AS has_visit_id
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        WHERE t.table_schema = %s
          AND t.table_type = 'BASE TABLE'
          AND LEFT(t.table_name, 5) = 'task_'
        GROUP BY t.table_name
        ORDER BY t.table_name
        """,
        (DB_CONFIG["database"],),
    )

    deletable = []
    skipped = []
    for table_name, has_visit_id in cursor.fetchall():
        if int(has_visit_id or 0) == 1:
            deletable.append(table_name)
        else:
            skipped.append(table_name)

    return deletable, skipped


def cleanup_task_tables_for_visits(cursor, visit_ids: Iterable[int] | None, logger=print) -> dict:
    normalized_visit_ids = normalize_visit_ids(visit_ids)
    deleted_by_table = {}
    skipped_tables = []

    if not normalized_visit_ids:
        logger("Idempotency cleanup: no affected visit_ids for task tables.")
        return {
            "task_rows_deleted": 0,
            "task_rows_deleted_by_table": deleted_by_table,
            "task_tables_skipped": skipped_tables,
        }

    task_tables, skipped_tables = discover_task_tables(cursor)
    logger(f"Idempotency cleanup: checking {len(task_tables)} task tables with visit_id.")

    for table_name in task_tables:
        deleted = _delete_by_visit_ids(cursor, table_name, normalized_visit_ids)
        deleted_by_table[table_name] = deleted
        logger(f"Idempotency cleanup: deleted {deleted} old rows from {table_name}.")

    for table_name in skipped_tables:
        logger(f"Idempotency cleanup: skipped {table_name}; no visit_id column.")

    return {
        "task_rows_deleted": sum(deleted_by_table.values()),
        "task_rows_deleted_by_table": deleted_by_table,
        "task_tables_skipped": skipped_tables,
    }


def cleanup_survey_responses_for_visits(cursor, visit_ids: Iterable[int] | None, logger=print) -> int:
    normalized_visit_ids = normalize_visit_ids(visit_ids)
    if not normalized_visit_ids:
        logger("Idempotency cleanup: no affected visit_ids for survey_responses.")
        return 0

    if not _table_exists(cursor, "survey_responses"):
        logger("Idempotency cleanup: survey_responses table does not exist yet.")
        return 0

    deleted = _delete_by_visit_ids(cursor, "survey_responses", normalized_visit_ids)
    logger(f"Idempotency cleanup: deleted {deleted} old rows from survey_responses.")
    return deleted


def cleanup_existing_data_for_visits(db, cursor, visit_ids: Iterable[int] | None, logger=print) -> dict:
    normalized_visit_ids = normalize_visit_ids(visit_ids)
    logger(f"Idempotency cleanup: affected visit_ids = {len(normalized_visit_ids)}.")

    task_result = cleanup_task_tables_for_visits(cursor, normalized_visit_ids, logger=logger)
    survey_responses_deleted = cleanup_survey_responses_for_visits(
        cursor,
        normalized_visit_ids,
        logger=logger,
    )
    db.commit()

    return {
        "affected_visit_ids": normalized_visit_ids,
        "survey_responses_deleted": survey_responses_deleted,
        **task_result,
    }
