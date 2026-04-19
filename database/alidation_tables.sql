-- Active: 1776577465055@@127.0.0.1@3306@unilever_db
CREATE TABLE IF NOT EXISTS survey_responses (
    response_id INT AUTO_INCREMENT PRIMARY KEY,

    visit_id INT,
    employee_code VARCHAR(50),
    store_code VARCHAR(50),

    product_code VARCHAR(50) NULL,

    task VARCHAR(255),
    title VARCHAR(255),

    question TEXT,
    response TEXT,

    response_datetime DATETIME,

    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),

    INDEX idx_visit (visit_id),
    INDEX idx_employee (employee_code),
    INDEX idx_store (store_code),
    INDEX idx_product (product_code),

    CONSTRAINT fk_survey_visit
        FOREIGN KEY (visit_id) REFERENCES visits(visit_id)
);