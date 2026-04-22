import mysql.connector

from config.db_config import DB_CONFIG


RULE_CODE = "OSA_UNUSUAL_NON_BY_BANNER"


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


def run_osa_unusual_non_validation(_run_id: int) -> int:
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    banner_case = _banner_case_sql()

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
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    inserted = 0

    insert_sql = """
    INSERT INTO validation_results (
        rule_code,
        visit_id,
        store_code,
        employee_code,
        product_code,
        banner,
        question,
        response,
        message,
        no_count,
        yes_count,
        total_answers,
        availability_rate
    )
    VALUES (
        %(rule_code)s,
        %(visit_id)s,
        %(store_code)s,
        %(employee_code)s,
        %(product_code)s,
        %(banner)s,
        %(question)s,
        %(response)s,
        %(message)s,
        %(no_count)s,
        %(yes_count)s,
        %(total_answers)s,
        %(availability_rate)s
    )
    """

    for row in rows:
        payload = {
            "rule_code": RULE_CODE,
            "visit_id": row["visit_id"],
            "store_code": row["store_code"],
            "employee_code": row["employee_code"],
            "product_code": row["product_code"],
            "banner": row["banner"],
            "question": row["question"],
            "response": row["response"],
            "message": (
                f"OSA response is 'Non' while weekly availability for SKU {row['product_code']} "
                f"in banner {row['banner']} is {row['availability_rate']}%."
            ),
            "no_count": int(row["no_count"]),
            "yes_count": int(row["yes_count"]),
            "total_answers": int(row["total_answers"]),
            "availability_rate": float(row["availability_rate"]),
        }

        cursor.execute(insert_sql, payload)
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()
    return inserted
