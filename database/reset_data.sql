USE unilever_db;

SET FOREIGN_KEY_CHECKS = 0;

DELETE FROM validation_results;
-- DELETE FROM validation_run_log;

DROP TABLE IF EXISTS task_callcycle_deviation;
DROP TABLE IF EXISTS task_location_checkin;
DROP TABLE IF EXISTS task_location_checkout;
DROP TABLE IF EXISTS task_osa_pack_coc_mh;
DROP TABLE IF EXISTS task_primary_shelf_placement;
DROP TABLE IF EXISTS task_quality;
DROP TABLE IF EXISTS task_secondary_placement;
DROP TABLE IF EXISTS task_sos;
DROP TABLE IF EXISTS task_store_conditions;

DELETE FROM survey_responses;
DELETE FROM visits;
DELETE FROM products;
DELETE FROM stores;
DELETE FROM employees;

SET FOREIGN_KEY_CHECKS = 1;
