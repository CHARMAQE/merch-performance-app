import mysql.connector

from config.db_config import DB_CONFIG
from validation.engine.registry import REGISTERED_RULES


UPSERT_RULE_SQL = """
INSERT INTO validation_rules (
    rule_code,
    rule_name,
    description,
    source_table,
    severity,
    is_active
)
VALUES (%s, %s, %s, %s, %s, TRUE)
ON DUPLICATE KEY UPDATE
    rule_name = VALUES(rule_name),
    description = VALUES(description),
    source_table = VALUES(source_table),
    severity = VALUES(severity)
"""


def _sync_registered_rules(cursor) -> None:
    payloads = [
        (
            rule.RULE_CODE,
            rule.RULE_NAME,
            rule.RULE_DESCRIPTION,
            rule.SOURCE_TABLE,
            rule.SEVERITY,
        )
        for rule in REGISTERED_RULES
    ]

    if payloads:
        cursor.executemany(UPSERT_RULE_SQL, payloads)


def _load_active_rule_codes(cursor) -> set[str]:
    cursor.execute(
        """
        SELECT rule_code
        FROM validation_rules
        WHERE is_active = TRUE
        """
    )
    return {row[0] for row in cursor.fetchall()}


def run_all_validations(run_id: int, target_visit_ids: list[int] | None = None) -> dict:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        _sync_registered_rules(cursor)
        conn.commit()
        active_rule_codes = _load_active_rule_codes(cursor)
    finally:
        cursor.close()
        conn.close()

    results = {}

    for rule in REGISTERED_RULES:
        if rule.RULE_CODE not in active_rule_codes:
            continue

        results[rule.RULE_CODE] = rule.run(run_id, target_visit_ids=target_visit_ids)

    return results
