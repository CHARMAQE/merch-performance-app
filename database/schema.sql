-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
CREATE DATABASE IF NOT EXISTS unilever_db;
USE unilever_db;

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_code VARCHAR(20),
    username VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS stores (
    store_id INT AUTO_INCREMENT PRIMARY KEY,
    store_code VARCHAR(50),
    store_name VARCHAR(150),
    store_city VARCHAR(100),
    store_state VARCHAR(100),
    store_region VARCHAR(100),
    store_format VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_code VARCHAR(50),
    barcode VARCHAR(50),
    product_description VARCHAR(200),
    brand VARCHAR(100),
    category VARCHAR(100),
    sub_category VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id INT AUTO_INCREMENT PRIMARY KEY,
    visit_date DATE,
    year INT,
    month VARCHAR(20),
    employee_id INT,
    store_id INT,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    map_link TEXT,
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

-- DROP TABLE IF EXISTS validation_results;


-- ALTER TABLE validation_results
-- ADD COLUMN validation_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;


CREATE TABLE validation_results (
    validation_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    rule_code VARCHAR(100) NOT NULL,
    visit_id INT NULL,
    store_code VARCHAR(50) NULL,
    employee_code VARCHAR(50) NULL,
    product_code VARCHAR(50) NULL,
    banner VARCHAR(100) NULL,
    detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    question TEXT NULL,
    response TEXT NULL,
    message TEXT NOT NULL,
    no_count INT NULL,
    yes_count INT NULL,
    total_answers INT NULL,
    availability_rate DECIMAL(5,2) NULL,
    INDEX idx_rule_code (rule_code),
    INDEX idx_visit_id (visit_id),
    INDEX idx_store_code (store_code),
    INDEX idx_employee_code (employee_code),
    INDEX idx_product_code (product_code),
    INDEX idx_banner (banner),
    INDEX idx_detected_at (detected_at)
);


SHOW COLUMNS FROM validation_results;
-- ============================================
-- LOGS FOR VALIDATION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS validation_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status VARCHAR(20) NOT NULL,
    rules_executed INT NULL,
    issues_found INT NULL,
    error_message TEXT NULL
);

-- ============================================
-- NOTE
-- ============================================
-- task_* tables are created dynamically by the ETL.
-- They are not created here on purpose.
-- DROP DATABASE IF EXISTS unilever_db;