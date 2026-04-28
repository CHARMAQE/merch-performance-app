-- Active: 1776681738854@@127.0.0.1@3306@unilever_db
CREATE DATABASE IF NOT EXISTS unilever_db;
USE unilever_db;

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_code VARCHAR(20) NOT NULL,
    username VARCHAR(100),
    UNIQUE KEY uq_employees_employee_code (employee_code)
);

CREATE TABLE IF NOT EXISTS stores (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    store_code VARCHAR(50) NOT NULL,
    store_name VARCHAR(150),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_region VARCHAR(100),
    store_format VARCHAR(100),
    UNIQUE KEY uq_stores_store_code (store_code)
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_code VARCHAR(50) NOT NULL,
    barcode VARCHAR(50),
    product_description VARCHAR(200),
    brand VARCHAR(100),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    UNIQUE KEY uq_products_product_code (product_code)
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id INT AUTO_INCREMENT PRIMARY KEY,
    visit_date DATE NOT NULL,
    year INT,
    month VARCHAR(20),
    employee_id INT NOT NULL,
    store_id INT NOT NULL,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    map_link TEXT,
    UNIQUE KEY uq_visits_natural_key (visit_date, employee_id, store_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE IF NOT EXISTS survey_responses (
    response_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    visit_id INT NOT NULL,
    employee_code VARCHAR(50) NOT NULL,
    store_code VARCHAR(50) NOT NULL,
    product_code VARCHAR(50) NULL,
    task VARCHAR(255) NULL,
    title VARCHAR(255) NULL,
    question TEXT NOT NULL,
    response TEXT NULL,
    response_datetime DATETIME NULL,
    latitude DECIMAL(10,6) NULL,
    longitude DECIMAL(10,6) NULL,
    KEY idx_survey_visit (visit_id),
    KEY idx_survey_employee (employee_code),
    KEY idx_survey_store (store_code),
    KEY idx_survey_product (product_code),
    KEY idx_survey_response_datetime (response_datetime),
    CONSTRAINT fk_survey_visit
        FOREIGN KEY (visit_id) REFERENCES visits(visit_id)
);

-- ============================================
-- VALIDATION TABLES
-- ============================================

-- If you are migrating from the old validation schema, recreate these tables
-- in this order because validation_results depends on the other two tables.
-- DROP TABLE IF EXISTS validation_results;
-- DROP TABLE IF EXISTS validation_run_log;
-- DROP TABLE IF EXISTS validation_rules;

CREATE TABLE IF NOT EXISTS validation_rules (
    rule_code VARCHAR(100) PRIMARY KEY,
    rule_name VARCHAR(150) NOT NULL,
    description TEXT NULL,
    source_table VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS validation_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status VARCHAR(20) NOT NULL,
    rules_executed INT NULL,
    issues_found INT NULL,
    error_message TEXT NULL
);

CREATE TABLE IF NOT EXISTS validation_results (
    validation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    rule_code VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(100) NULL,
    visit_id INT NULL,
    store_code VARCHAR(50) NULL,
    employee_code VARCHAR(50) NULL,
    product_code VARCHAR(50) NULL,
    question TEXT NULL,
    actual_value TEXT NULL,
    expected_value TEXT NULL,
    metric_value DECIMAL(12,4) NULL,
    message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    details_json JSON NULL,
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rule_code (rule_code),
    INDEX idx_run_id (run_id),
    INDEX idx_entity_type (entity_type),
    INDEX idx_visit_id (visit_id),
    INDEX idx_store_code (store_code),
    INDEX idx_employee_code (employee_code),
    INDEX idx_product_code (product_code),
    INDEX idx_detected_at (detected_at),
    CONSTRAINT fk_validation_results_run
        FOREIGN KEY (run_id) REFERENCES validation_run_log(run_id),
    CONSTRAINT fk_validation_results_rule
        FOREIGN KEY (rule_code) REFERENCES validation_rules(rule_code)
);

SHOW COLUMNS FROM validation_results;

-- ============================================
-- NOTE
-- ============================================
-- task_* tables are created dynamically by the ETL.
-- They are not created here on purpose.
-- DROP DATABASE IF EXISTS unilever_db;
