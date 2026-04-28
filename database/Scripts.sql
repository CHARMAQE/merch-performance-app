-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM stores;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM visits;
SELECT COUNT(*) FROM survey_responses;


SELECT * FROM validation_rules ORDER BY rule_code;
SELECT * FROM validation_run_log ORDER BY run_id DESC;
SELECT rule_code, COUNT(*) FROM validation_results GROUP BY rule_code;
SELECT
    run_id,
    rule_code,
    COUNT(*) AS issue_count
FROM validation_results
GROUP BY run_id, rule_code
ORDER BY run_id DESC, rule_code;
SELECT * FROM validation_results ORDER BY detected_at DESC LIMIT 20;


SELECT
    run_id,
    rule_code,
    entity_type,
    entity_id,
    visit_id,
    store_code,
    employee_code,
    product_code,
    detected_at,
    question,
    actual_value,
    expected_value,
    metric_value,
    severity,
    message,
    JSON_UNQUOTE(JSON_EXTRACT(details_json, '$.banner')) AS banner,
    JSON_EXTRACT(details_json, '$.yes_count') AS yes_count,
    JSON_EXTRACT(details_json, '$.no_count') AS no_count,
    JSON_EXTRACT(details_json, '$.total_answers') AS total_answers,
    JSON_EXTRACT(details_json, '$.availability_rate') AS availability_rate
FROM validation_results
WHERE rule_code = 'OSA_UNUSUAL_NON_BY_BANNER'
ORDER BY detected_at DESC
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

        ELSE 'OTHER'
    END AS banner
FROM stores s
ORDER BY s.store_name;

SELECT
    JSON_UNQUOTE(JSON_EXTRACT(details_json, '$.banner')) AS banner,
    COUNT(*) AS issue_count
FROM validation_results
WHERE rule_code = 'OSA_UNUSUAL_NON_BY_BANNER'
GROUP BY banner
ORDER BY issue_count DESC;




SELECT *
FROM validation_results
WHERE rule_code = 'GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH'
ORDER BY metric_value DESC, detected_at DESC;

SELECT
    run_id,
    visit_id,
    store_code,
    employee_code,
    metric_value AS distance_meters,
    severity,
    question,
    actual_value,
    expected_value,
    message,
    JSON_EXTRACT(details_json, '$.visit_latitude') AS visit_latitude,
    JSON_EXTRACT(details_json, '$.visit_longitude') AS visit_longitude,
    JSON_EXTRACT(details_json, '$.median_latitude') AS median_latitude,
    JSON_EXTRACT(details_json, '$.median_longitude') AS median_longitude,
    JSON_EXTRACT(details_json, '$.total_visits_in_group') AS total_visits_in_group
FROM validation_results
WHERE rule_code = 'GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH'
ORDER BY detected_at DESC;
