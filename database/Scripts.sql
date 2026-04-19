-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
SELECT COUNT(*) FROM employees;
SELECT COUNT(*) FROM stores;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM visits;
SELECT COUNT(*) FROM survey_responses;


SELECT * FROM validation_run_log ORDER BY run_id DESC;
SELECT rule_code, COUNT(*) FROM validation_results GROUP BY rule_code;
SELECT * FROM validation_results ORDER BY validation_id DESC LIMIT 20;


SELECT
    validation_id,
    rule_code,
    visit_id,
    username,
    store_code,
    product_code,
    banner,
    response_value,
    issue_message,
    context_json
FROM validation_results
WHERE rule_code = 'OSA_UNUSUAL_NON_BY_BANNER'
LIMIT 20;

SHOW TABLES;

SELECT *
FROM validation_run_log
ORDER BY run_id DESC;