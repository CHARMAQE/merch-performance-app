-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
CREATE TABLE IF NOT EXISTS validation_rules (
    rule_id INT AUTO_INCREMENT PRIMARY KEY,
    rule_code VARCHAR(100) NOT NULL UNIQUE,
    rule_name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    severity_default VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS validation_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status VARCHAR(20) NOT NULL, -- RUNNING | SUCCESS | FAILED
    rules_executed INT NULL,
    issues_found INT NULL,
    error_message TEXT NULL
);

CREATE TABLE IF NOT EXISTS validation_results (
    validation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    rule_code VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- LOW | MEDIUM | HIGH
    visit_id INT NULL,
    employee_code VARCHAR(50) NULL,
    store_code VARCHAR(50) NULL,
    product_code VARCHAR(50) NULL,
    banner VARCHAR(100) NULL,
    question TEXT NULL,
    response_value TEXT NULL,
    issue_message TEXT NOT NULL,
    context_json JSON NULL,
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN', -- OPEN | REVIEWED | RESOLVED
    FOREIGN KEY (run_id) REFERENCES validation_run_log(run_id),
    INDEX idx_rule_code (rule_code),
    INDEX idx_visit_id (visit_id),
    INDEX idx_employee_code (employee_code),
    INDEX idx_store_code (store_code),
    INDEX idx_product_code (product_code),
    INDEX idx_banner (banner),
    INDEX idx_status (status)
);

INSERT INTO validation_rules (rule_code, rule_name, description, severity_default, is_active)
VALUES
('OSA_UNUSUAL_NON_BY_BANNER', 'OSA unusual NON by banner', 'Flags NON responses when weekly SKU availability is high in the same banner.', 'MEDIUM', 1),
('OSA_CONTRADICTORY_RESPONSE_SAME_VISIT', 'OSA contradictory response in same visit', 'Flags cases where the same visit and SKU contain both OUI and NON.', 'HIGH', 1)
ON DUPLICATE KEY UPDATE
rule_name = VALUES(rule_name),
description = VALUES(description),
severity_default = VALUES(severity_default),
is_active = VALUES(is_active);