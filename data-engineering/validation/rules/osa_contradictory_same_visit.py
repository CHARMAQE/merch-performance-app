import mysql.connector

from config.db_config import DB_CONFIG


RULE_CODE = "OSA_CONTRADICTORY_RESPONSE_SAME_VISIT"


def _banner_case_sql() -> str:
    return """
    CASE
        WHEN UPPER(s.store_name) LIKE 'MARJANE%' THEN 'MARJANE'
        WHEN UPPER(s.store_name) LIKE 'MARJANE MARKET%' THEN 'MARJANE MARKET'
        WHEN UPPER(s.store_name) LIKE 'CARREFOUR%' THEN 'CARREFOUR'
        WHEN UPPER(s.store_name) LIKE 'ATACADAO%' THEN 'ATACADAO'
        WHEN UPPER(s.store_name) LIKE 'BIM%' THEN 'BIM'
        WHEN UPPER(s.store_name) LIKE 'ASWAK ASSALAM%' THEN 'ASWAK ASSALAM'
        ELSE 'OTHER'
    END
    """


def run_osa_contradictory_same_visit_validation(run_id: int) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    banner_case = _banner_case_sql()

    query = f"""
    SELECT
        sr.visit_id,
        sr.employee_code,
        sr.store_code,
        sr.product_code,
        {banner_case} AS banner,
        sr.question,
        COUNT(DISTINCT sr.response) AS distinct_responses,
        GROUP_CONCAT(DISTINCT sr.response ORDER BY sr.response SEPARATOR ', ') AS response_list
    FROM survey_responses sr
    JOIN visits v ON v.visit_id = sr.visit_id
    JOIN stores s ON s.store_id = v.store_id
    WHERE sr.product_code IS NOT NULL
      AND sr.response IN ('Oui', 'Non')
      AND UPPER(sr.question) LIKE '%DISPONIBLE%'
    GROUP BY
        sr.visit_id,
        sr.employee_code,
        sr.store_code,
        sr.product_code,
        banner,
        sr.question
    HAVING COUNT(DISTINCT sr.response) > 1
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    inserted = 0

    insert_sql = """
    INSERT INTO validation_results (
        run_id,
        rule_code,
        severity,
        visit_id,
        employee_code,
        store_code,
        product_code,
        banner,
        question,
        response_value,
        issue_message,
        context_json
    )
    VALUES (
        %(run_id)s,
        %(rule_code)s,
        %(severity)s,
        %(visit_id)s,
        %(employee_code)s,
        %(store_code)s,
        %(product_code)s,
        %(banner)s,
        %(question)s,
        %(response_value)s,
        %(issue_message)s,
        %(context_json)s
    )
    """

    for row in rows:
        payload = {
            "run_id": run_id,
            "rule_code": RULE_CODE,
            "severity": "HIGH",
            "visit_id": row["visit_id"],
            "employee_code": row["employee_code"],
            "store_code": row["store_code"],
            "product_code": row["product_code"],
            "banner": row["banner"],
            "question": row["question"],
            "response_value": row["response_list"],
            "issue_message": (
                f"Contradictory OSA responses found for SKU {row['product_code']} "
                f"in the same visit: {row['response_list']}."
            ),
            "context_json": (
                "{"
                f"\"distinct_responses\": {int(row['distinct_responses'])}, "
                f"\"responses\": \"{row['response_list']}\""
                "}"
            ),
        }

        cursor.execute(insert_sql, payload)
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()
    return inserted