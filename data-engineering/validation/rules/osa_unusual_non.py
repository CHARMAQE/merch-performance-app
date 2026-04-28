import json
import mysql.connector

from config.db_config import DB_CONFIG


RULE_CODE = "OSA_UNUSUAL_NON_BY_BANNER"
RULE_NAME = "OSA unusual non by banner"
RULE_DESCRIPTION = (
    "Flags OSA answers marked 'Non' when the same banner, product, and question "
    "show a strong weekly 'Oui' pattern."
)
SOURCE_TABLE = "survey_responses"
SEVERITY = "MEDIUM"
ENTITY_TYPE = "survey_response"


def _banner_case_sql() -> str:
    return """
    CASE
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE MARKET%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ACIMA%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE%' THEN 'MARJANE'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR MARKET%' THEN 'CARREFOUR MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR%' THEN 'CARREFOUR'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATACADAO%' THEN 'ATACADAO'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATTACADAO%' THEN 'ATACADAO'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'ASWAK ASSALAM%' THEN 'ASWAK ASSALAM'
        
        ELSE 'OTHER'
    END
    """


def _normalize_target_visit_ids(target_visit_ids: list[int] | None) -> list[int] | None:
    if target_visit_ids is None:
        return None

    return sorted({int(visit_id) for visit_id in target_visit_ids})


def run(run_id: int, target_visit_ids: list[int] | None = None) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    normalized_target_visit_ids = _normalize_target_visit_ids(target_visit_ids)

    if normalized_target_visit_ids == []:
        cursor.close()
        conn.close()
        return 0

    banner_case = _banner_case_sql()
    target_filter_sql = ""
    query_params: tuple[int, ...] = ()

    if normalized_target_visit_ids is not None:
        placeholders = ",".join(["%s"] * len(normalized_target_visit_ids))
        target_filter_sql = f" AND b.visit_id IN ({placeholders})"
        query_params = tuple(normalized_target_visit_ids)

    query = f"""
    WITH osa_base AS (
        SELECT
            sr.visit_id,
            sr.employee_code,
            sr.store_code,
            sr.product_code,
            sr.question,
            sr.response,
            sr.response_datetime,
            {banner_case} AS banner,
            YEAR(sr.response_datetime) AS year_num,
            WEEK(sr.response_datetime, 1) AS week_num
        FROM survey_responses sr
        JOIN visits v ON v.visit_id = sr.visit_id
        JOIN stores s ON s.store_id = v.store_id
        WHERE sr.product_code IS NOT NULL
          AND sr.response IN ('Oui', 'Non')
          AND UPPER(sr.question) LIKE '%DISPONIBLE%'
    ),
    osa_weekly AS (
        SELECT
            year_num,
            week_num,
            banner,
            product_code,
            question,
            COUNT(*) AS total_answers,
            SUM(CASE WHEN response = 'Oui' THEN 1 ELSE 0 END) AS yes_count,
            SUM(CASE WHEN response = 'Non' THEN 1 ELSE 0 END) AS no_count,
            ROUND(
                SUM(CASE WHEN response = 'Oui' THEN 1 ELSE 0 END) * 100.0 /
                NULLIF(COUNT(*), 0),
                2
            ) AS availability_rate
        FROM osa_base
        GROUP BY
            year_num,
            week_num,
            banner,
            product_code,
            question
        HAVING COUNT(*) >= 10
            AND SUM(CASE WHEN response = 'Oui' THEN 1 ELSE 0 END) >= 8
    )
    SELECT
        b.visit_id,
        b.employee_code,
        b.store_code,
        b.product_code,
        b.banner,
        b.question,
        b.response,
        w.total_answers,
        w.yes_count,
        w.no_count,
        w.availability_rate
    FROM osa_base b
    JOIN osa_weekly w
      ON b.year_num = w.year_num
     AND b.week_num = w.week_num
     AND b.banner = w.banner
     AND b.product_code = w.product_code
     AND b.question = w.question
    WHERE b.response = 'Non'
      AND w.availability_rate >= 80
      {target_filter_sql}
    """

    if query_params:
        cursor.execute(query, query_params)
    else:
        cursor.execute(query)
    rows = cursor.fetchall()

    inserted = 0

    insert_sql = """
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
        details_json
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
        %(details_json)s
    )
    """

    payloads = []

    for row in rows:
        details = {
            "banner": row["banner"],
            "weekly_expected_response": "Oui",
            "yes_count": int(row["yes_count"]),
            "no_count": int(row["no_count"]),
            "total_answers": int(row["total_answers"]),
            "availability_rate": float(row["availability_rate"]),
        }
        payloads.append(
            {
                "run_id": run_id,
                "rule_code": RULE_CODE,
                "entity_type": ENTITY_TYPE,
                "entity_id": f"{row['visit_id']}:{row['product_code']}",
                "visit_id": row["visit_id"],
                "store_code": row["store_code"],
                "employee_code": row["employee_code"],
                "product_code": row["product_code"],
                "question": row["question"],
                "actual_value": row["response"],
                "expected_value": "Oui",
                "metric_value": float(row["availability_rate"]),
                "message": (
                    f"OSA response is 'Non' while weekly availability for SKU {row['product_code']} "
                    f"in banner {row['banner']} is {row['availability_rate']}%."
                ),
                "severity": SEVERITY,
                "details_json": json.dumps(details),
            }
        )

    if payloads:
        cursor.executemany(insert_sql, payloads)
        inserted = len(payloads)

    conn.commit()
    cursor.close()
    conn.close()
    return inserted


def run_osa_unusual_non_validation(run_id: int, target_visit_ids: list[int] | None = None) -> int:
    return run(run_id, target_visit_ids=target_visit_ids)
