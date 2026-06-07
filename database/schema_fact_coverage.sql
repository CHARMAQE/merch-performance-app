CREATE TABLE IF NOT EXISTS fact_coverage (
    coverage_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    visit_date DATE NOT NULL,
    year INT,
    month VARCHAR(20),
    dateid INT,
    employee_code VARCHAR(50) NOT NULL,
    username VARCHAR(100),
    role VARCHAR(100),
    l1name VARCHAR(150),
    l2name VARCHAR(150),
    l3name VARCHAR(150),
    store_code VARCHAR(50) NOT NULL,
    store_name VARCHAR(150),
    store_region VARCHAR(100),
    store_state VARCHAR(100),
    store_city VARCHAR(100),
    store_format VARCHAR(100),
    call_cycle_type VARCHAR(100),
    call_status VARCHAR(100),
    is_planned TINYINT(1) NOT NULL DEFAULT 0,
    is_adhoc TINYINT(1) NOT NULL DEFAULT 0,
    is_done TINYINT(1) NOT NULL DEFAULT 0,
    rejection TINYINT(1) NOT NULL DEFAULT 0,
    deviation TINYINT(1) NOT NULL DEFAULT 0,
    not_visited TINYINT(1) NOT NULL DEFAULT 0,
    task_assigned INT,
    task_done INT,
    task_per DECIMAL(8,4),
    master_latitude DECIMAL(10,6),
    master_longitude DECIMAL(10,6),
    start_time DATETIME,
    start_latitude DECIMAL(10,6),
    start_longitude DECIMAL(10,6),
    start_distance_meters DECIMAL(12,3),
    end_time DATETIME,
    end_latitude DECIMAL(10,6),
    end_longitude DECIMAL(10,6),
    end_distance_meters DECIMAL(12,3),
    time_mm INT,
    time_hh DECIMAL(10,4),
    reason VARCHAR(255),
    user_attendance VARCHAR(100),
    superior_attendance VARCHAR(100),
    final_user_attendance VARCHAR(100),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_fact_coverage_natural_key (visit_date, employee_code, store_code),
    KEY idx_fact_coverage_visit_date (visit_date),
    KEY idx_fact_coverage_employee (employee_code),
    KEY idx_fact_coverage_store (store_code),
    KEY idx_fact_coverage_status (is_planned, is_done, not_visited, deviation)
);

CREATE OR REPLACE VIEW vw_fact_coverage_visit_match AS
SELECT
    fc.*,
    v.visit_id,
    CASE
        WHEN v.visit_id IS NOT NULL THEN 'MATCHED'
        ELSE 'NO_DATA_DUMP_VISIT'
    END AS match_status
FROM fact_coverage fc
LEFT JOIN employees e
    ON e.employee_code = fc.employee_code
LEFT JOIN stores s
    ON s.store_code = fc.store_code
LEFT JOIN visits v
    ON v.visit_date = fc.visit_date
   AND v.employee_id = e.employee_id
   AND v.store_id = s.store_id;
