-- Active: 1776681738854@@127.0.0.1@3306@unilever_db
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM stores;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM visits;
SELECT COUNT(*) FROM survey_responses;


SELECT * FROM validation_run_log ORDER BY run_id DESC;
SELECT rule_code, COUNT(*) FROM validation_results GROUP BY rule_code;
SELECT * FROM validation_results ORDER BY detected_at DESC LIMIT 20;


SELECT
    rule_code,
    visit_id,
    store_code,
    employee_code,
    product_code,
    banner,
    detected_at,
    question,
    response,
    message,
    no_count,
    yes_count,
    total_answers,
    availability_rate
FROM validation_results
WHERE rule_code = 'OSA_UNUSUAL_NON_BY_BANNER'
LIMIT 20;

SHOW TABLES;

SELECT *
FROM validation_run_log
ORDER BY run_id DESC;


SELECT
    s.store_name,
    CASE
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE MARKET%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ACIMA%' THEN 'MARJANE MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'MARJANE%' THEN 'MARJANE'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR MARKET%' THEN 'CARREFOUR MARKET'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'CARREFOUR%' THEN 'CARREFOUR'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATACADAO%' THEN 'ATACADAO'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'ATTACADAO%' THEN 'ATACADAO'

        WHEN UPPER(TRIM(s.store_name)) LIKE 'ASWAK ASSALAM%' THEN 'ASWAK ASSALAM'
        WHEN UPPER(TRIM(s.store_name)) LIKE 'BIM%' THEN 'BIM'

        ELSE 'OTHER'
    END AS banner
FROM stores s
ORDER BY s.store_name;

SELECT
    banner,
    COUNT(*) AS issue_count
FROM validation_results
GROUP BY banner
ORDER BY issue_count DESC;




select DISTINCT(banner) from stores;