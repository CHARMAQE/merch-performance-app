USE unilever_db;

-- Indexes used by the monthly GPS validation rule.
CREATE INDEX idx_visits_employee_store_date
ON visits (employee_id, store_id, visit_date);

CREATE INDEX idx_visits_gps
ON visits (latitude, longitude);

SELECT
    vr.validation_id,
    vr.rule_code,
    vr.severity,
    vr.employee_code,
    e.username,
    vr.store_code,
    s.store_name,
    vr.visit_id,
    v.visit_date,
    JSON_UNQUOTE(JSON_EXTRACT(vr.context_json, '$.distance_meters')) AS distance_meters,
    ROUND(
        CAST(JSON_UNQUOTE(JSON_EXTRACT(vr.context_json, '$.distance_meters')) AS DECIMAL(10,2)) / 1000,
        2
    ) AS distance_km,
    vr.issue_message,
    vr.detected_at
FROM validation_results vr
LEFT JOIN employees e
    ON e.employee_code = vr.employee_code
LEFT JOIN stores s
    ON s.store_code = vr.store_code
LEFT JOIN visits v
    ON v.visit_id = vr.visit_id
WHERE vr.rule_code = 'GPS_INCONSISTENT_CHECKIN_SAME_STORE_MONTH'
ORDER BY distance_meters DESC;