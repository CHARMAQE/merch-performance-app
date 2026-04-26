USE unilever_db;

-- Run this one time before testing the optimized ETL.
-- These indexes make lookup and duplicate detection much faster.

ALTER TABLE employees
  ADD UNIQUE KEY uq_employees_code (employee_code);

ALTER TABLE stores
  ADD UNIQUE KEY uq_stores_code (store_code);

ALTER TABLE products
  ADD UNIQUE KEY uq_products_code (product_code);

ALTER TABLE visits
  ADD UNIQUE KEY uq_visits_business_key (visit_date, employee_id, store_id),
  ADD KEY idx_visits_date (visit_date),
  ADD KEY idx_visits_employee_store (employee_id, store_id);

ALTER TABLE survey_responses
  ADD KEY idx_survey_visit_product (visit_id, product_code),
  ADD KEY idx_survey_task_question (task, question(100));
