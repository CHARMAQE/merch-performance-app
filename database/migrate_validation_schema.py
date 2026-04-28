from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

import mysql.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ENGINEERING_DIR = PROJECT_ROOT / "data-engineering"
if str(DATA_ENGINEERING_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_ENGINEERING_DIR))

from config.db_config import DB_CONFIG


KNOWN_RULES = {
    "OSA_UNUSUAL_NON_BY_BANNER": {
        "rule_name": "OSA unusual non by banner",
        "description": (
            "Flags OSA answers marked 'Non' when the same banner, product, and "
            "question show a strong weekly 'Oui' pattern."
        ),
        "source_table": "survey_responses",
        "severity": "MEDIUM",
    },
    "GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH": {
        "rule_name": "GPS inconsistent check-in same store month",
        "description": (
            "Flags repeated same-store monthly visits whose GPS position is far "
            "from the merchandiser's normal GPS cluster for that store."
        ),
        "source_table": "visits",
        "severity": "MEDIUM",
    },
    "GPS_STORE_LOCATION_INCONSISTENCY_GT": {
        "rule_name": "GPS store location inconsistency GT",
        "description": (
            "Legacy GT GPS validation migrated from the old validation schema."
        ),
        "source_table": "visits",
        "severity": "HIGH",
    },
}

VALIDATION_RESULTS_COLUMNS = {
    "validation_id",
    "run_id",
    "rule_code",
    "entity_type",
    "entity_id",
    "visit_id",
    "store_code",
    "employee_code",
    "product_code",
    "question",
    "actual_value",
    "expected_value",
    "metric_value",
    "message",
    "severity",
    "details_json",
    "detected_at",
}

INSERT_RULE_SQL = """
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

INSERT_RESULT_SQL = """
INSERT INTO validation_results (
    run_id,
    rule_code,
    entity_type,
    entity_id,
    visit_id,
    store_code,
    employee_code,
    product_code,
    question,
    actual_value,
    expected_value,
    metric_value,
    message,
    severity,
    details_json,
    detected_at
)
VALUES (
    %(run_id)s,
    %(rule_code)s,
    %(entity_type)s,
    %(entity_id)s,
    %(visit_id)s,
    %(store_code)s,
    %(employee_code)s,
    %(product_code)s,
    %(question)s,
    %(actual_value)s,
    %(expected_value)s,
    %(metric_value)s,
    %(message)s,
    %(severity)s,
    %(details_json)s,
    %(detected_at)s
)
"""


@dataclass(frozen=True)
class TableState:
    exists: bool
    columns: set[str]


def get_table_state(cursor, table_name: str) -> TableState:
    cursor.execute("SHOW TABLES LIKE %s", (table_name,))
    exists = cursor.fetchone() is not None
    if not exists:
        return TableState(False, set())

    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = {row["Field"] for row in cursor.fetchall()}
    return TableState(True, columns)


def is_already_migrated(state: TableState) -> bool:
    return state.exists and VALIDATION_RESULTS_COLUMNS.issubset(state.columns)


def create_validation_run_log(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_run_log (
            run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            started_at DATETIME NOT NULL,
            finished_at DATETIME NULL,
            status VARCHAR(20) NOT NULL,
            rules_executed INT NULL,
            issues_found INT NULL,
            error_message TEXT NULL
        )
        """
    )


def create_validation_rules(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_rules (
            rule_code VARCHAR(100) PRIMARY KEY,
            rule_name VARCHAR(150) NOT NULL,
            description TEXT NULL,
            source_table VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        """
    )


def create_validation_results(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE validation_results (
            validation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
            run_id BIGINT NOT NULL,
            rule_code VARCHAR(100) NOT NULL,
            entity_type VARCHAR(50) NOT NULL,
            entity_id VARCHAR(100) NULL,
            visit_id INT NULL,
            store_code VARCHAR(50) NULL,
            employee_code VARCHAR(50) NULL,
            product_code VARCHAR(50) NULL,
            question TEXT NULL,
            actual_value TEXT NULL,
            expected_value TEXT NULL,
            metric_value DECIMAL(12,4) NULL,
            message TEXT NOT NULL,
            severity VARCHAR(20) NOT NULL,
            details_json JSON NULL,
            detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_rule_code (rule_code),
            INDEX idx_run_id (run_id),
            INDEX idx_entity_type (entity_type),
            INDEX idx_visit_id (visit_id),
            INDEX idx_store_code (store_code),
            INDEX idx_employee_code (employee_code),
            INDEX idx_product_code (product_code),
            INDEX idx_detected_at (detected_at),
            CONSTRAINT fk_validation_results_run
                FOREIGN KEY (run_id) REFERENCES validation_run_log(run_id),
            CONSTRAINT fk_validation_results_rule
                FOREIGN KEY (rule_code) REFERENCES validation_rules(rule_code)
        )
        """
    )


def rule_metadata(rule_code: str) -> tuple[str, str, str, str]:
    known = KNOWN_RULES.get(rule_code)
    if known:
        return (
            known["rule_name"],
            known["description"],
            known["source_table"],
            known["severity"],
        )

    return (
        rule_code,
        "Legacy validation rule migrated from the old validation schema.",
        "unknown",
        "MEDIUM",
    )


def derive_severity(row: dict) -> str:
    banner = (row.get("banner") or "").upper()
    message = (row.get("message") or "").upper()

    if any(token in banner for token in ("CRITICAL", "HIGH")):
        return "HIGH"
    if any(token in message for token in ("CRITICAL:", "HIGH:")):
        return "HIGH"
    if "MEDIUM" in banner or "MEDIUM:" in message:
        return "MEDIUM"
    if row.get("rule_code") == "OSA_UNUSUAL_NON_BY_BANNER":
        return "MEDIUM"
    return "MEDIUM"


def parse_distance_meters(row: dict) -> float | None:
    candidates = [
        row.get("response") or "",
        row.get("message") or "",
    ]
    patterns = [
        r"distance_meters=(\d+(?:\.\d+)?)",
        r"is (\d+(?:\.\d+)?) meters",
    ]

    for text in candidates:
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return float(match.group(1))
    return None


def map_entity_type(row: dict) -> str:
    if row.get("rule_code") == "OSA_UNUSUAL_NON_BY_BANNER":
        return "survey_response"
    return "visit"


def map_expected_value(row: dict) -> str | None:
    if row.get("rule_code") == "OSA_UNUSUAL_NON_BY_BANNER":
        return "Oui"
    return None


def map_metric_value(row: dict) -> float | None:
    availability_rate = row.get("availability_rate")
    if availability_rate is not None:
        return float(availability_rate)
    return parse_distance_meters(row)


def map_entity_id(row: dict) -> str | None:
    visit_id = row.get("visit_id")
    product_code = row.get("product_code")
    rule_code = row.get("rule_code")

    if rule_code == "OSA_UNUSUAL_NON_BY_BANNER":
        if visit_id is not None and product_code:
            return f"{visit_id}:{product_code}"
        if visit_id is not None:
            return str(visit_id)
        return product_code

    if visit_id is not None:
        return str(visit_id)
    return None


def build_details_json(row: dict) -> str:
    details = {
        "legacy_validation_id": row.get("validation_id"),
        "legacy_banner": row.get("banner"),
        "legacy_response": row.get("response"),
        "legacy_no_count": row.get("no_count"),
        "legacy_yes_count": row.get("yes_count"),
        "legacy_total_answers": row.get("total_answers"),
        "legacy_availability_rate": (
            float(row["availability_rate"])
            if row.get("availability_rate") is not None
            else None
        ),
        "migrated_from_old_schema": True,
    }

    distance_meters = parse_distance_meters(row)
    if distance_meters is not None:
        details["distance_meters"] = distance_meters

    if row.get("rule_code") == "OSA_UNUSUAL_NON_BY_BANNER":
        details["banner"] = row.get("banner")
        details["yes_count"] = row.get("yes_count")
        details["no_count"] = row.get("no_count")
        details["total_answers"] = row.get("total_answers")
        details["availability_rate"] = (
            float(row["availability_rate"])
            if row.get("availability_rate") is not None
            else None
        )

    return json.dumps(details)


def migrate_rows(cursor, backup_table: str, migration_run_id: int) -> int:
    cursor.execute(f"SELECT * FROM {backup_table} ORDER BY validation_id")
    rows = cursor.fetchall()

    payloads = []
    for row in rows:
        payloads.append(
            {
                "run_id": migration_run_id,
                "rule_code": row["rule_code"],
                "entity_type": map_entity_type(row),
                "entity_id": map_entity_id(row),
                "visit_id": row.get("visit_id"),
                "store_code": row.get("store_code"),
                "employee_code": row.get("employee_code"),
                "product_code": row.get("product_code"),
                "question": row.get("question"),
                "actual_value": row.get("response"),
                "expected_value": map_expected_value(row),
                "metric_value": map_metric_value(row),
                "message": row["message"],
                "severity": derive_severity(row),
                "details_json": build_details_json(row),
                "detected_at": row["detected_at"],
            }
        )

    if payloads:
        cursor.executemany(INSERT_RESULT_SQL, payloads)
    return len(payloads)


def main() -> None:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        current_state = get_table_state(cursor, "validation_results")
        if is_already_migrated(current_state):
            print("validation_results is already using the generic migrated schema. No action taken.")
            return

        if not current_state.exists:
            print("validation_results does not exist yet. Creating the new validation tables only.")
            create_validation_run_log(cursor)
            create_validation_rules(cursor)
            create_validation_results(cursor)
            conn.commit()
            print("Created validation_run_log, validation_rules, and validation_results.")
            return

        backup_table = f"validation_results_legacy_backup_{datetime.now():%Y%m%d_%H%M%S}"

        cursor.execute("SELECT DISTINCT rule_code FROM validation_results ORDER BY rule_code")
        existing_rule_codes = [row["rule_code"] for row in cursor.fetchall()]
        cursor.execute("SELECT COUNT(*) AS row_count FROM validation_results")
        legacy_row_count = int(cursor.fetchone()["row_count"])

        print(f"Backing up old validation_results to {backup_table}...")
        cursor.execute(f"RENAME TABLE validation_results TO {backup_table}")

        create_validation_run_log(cursor)
        create_validation_rules(cursor)

        rule_payloads = []
        for rule_code in existing_rule_codes:
            rule_name, description, source_table, severity = rule_metadata(rule_code)
            rule_payloads.append(
                (rule_code, rule_name, description, source_table, severity)
            )
        if rule_payloads:
            cursor.executemany(INSERT_RULE_SQL, rule_payloads)

        cursor.execute(
            """
            INSERT INTO validation_run_log (
                started_at,
                finished_at,
                status,
                rules_executed,
                issues_found,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.now(),
                datetime.now(),
                "MIGRATED",
                len(existing_rule_codes),
                legacy_row_count,
                f"Legacy validation results migrated from {backup_table}",
            ),
        )
        migration_run_id = int(cursor.lastrowid)

        create_validation_results(cursor)
        migrated_count = migrate_rows(cursor, backup_table, migration_run_id)

        conn.commit()
        print(f"Migration completed successfully.")
        print(f"Backup table: {backup_table}")
        print(f"Migration run_id: {migration_run_id}")
        print(f"Rules migrated: {len(existing_rule_codes)}")
        print(f"Rows migrated: {migrated_count}")

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
