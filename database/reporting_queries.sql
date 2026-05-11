-- Reporting queries used for quick local analytics checks.
-- Power BI can use the same source tables directly: visits, stores,
-- employees, survey_responses, validation_run_log, and validation_results.

SELECT COUNT(*) AS stores_count FROM stores;

SELECT COUNT(*) AS visits_count FROM visits;

SELECT COUNT(*) AS survey_responses_count FROM survey_responses;

SELECT
    rule_code,
    COUNT(*) AS issue_count
FROM validation_results
GROUP BY rule_code
ORDER BY issue_count DESC;
