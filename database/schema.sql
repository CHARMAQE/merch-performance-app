-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
-- ============================================
-- SMOLLAN TASKS DATABASE - SCHEMA
-- ============================================

CREATE DATABASE IF NOT EXISTS unilever_db;
USE unilever_db;

-- ============================================
-- BASE TABLES
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

-- ============================================
-- TASK TABLES ARE CREATED BY python.py
-- Each question becomes a column header
-- Responses fill the rows per visit
-- ============================================

-- Full Reset

-- DROP DATABASE IF EXISTS unilever_db;


-- Check duplicates by visit key in SQL:

-- SELECT v.visit_date, e.employee_code, s.store_code, COUNT(*) cnt
-- FROM visits v
-- JOIN employees e ON e.employee_id = v.employee_id
-- JOIN stores s ON s.store_id = v.store_id
-- GROUP BY v.visit_date, e.employee_code, s.store_code
-- HAVING COUNT(*) > 1;

-- ============================================
-- ETL FILE REGISTRY
-- ============================================

CREATE TABLE IF NOT EXISTS etl_file_registry (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_hash CHAR(64) NOT NULL,
    file_size BIGINT NULL,
    file_modified_at DATETIME NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING|SUCCESS|FAILED
    loaded_at DATETIME NULL,
    error_message TEXT NULL,
    UNIQUE KEY uk_file_hash (file_hash)
);

-- ============================================
-- ETL RUN LOG
-- ============================================

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status VARCHAR(20) NOT NULL, -- RUNNING|SUCCESS|FAILED
    source_file VARCHAR(255) NULL,
    rows_loaded INT NULL,
    employees_loaded INT NULL,
    stores_loaded INT NULL,
    products_loaded INT NULL,
    visits_loaded INT NULL,
    error_message TEXT NULL
);
