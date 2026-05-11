-- Active: 1776681738854@@127.0.0.1@3306@unilever_db
USE unilever_db;

-- Demo supervisor accounts for the mobile app.
-- Passwords are plain demo values for now. Replace with hashed passwords later.
INSERT INTO supervisors (
    full_name,
    username,
    password_hash,
    phone,
    email,
    region,
    active
)
VALUES
    ('Admin', 'admin', '1234', NULL, NULL, 'All Regions', TRUE),
    ('Casablanca Supervisor', 'casa_sup', '1234', NULL, NULL, 'Casablanca', TRUE),
    ('Rabat Supervisor', 'rabat_sup', '1234', NULL, NULL, 'Rabat', TRUE),
    ('Marrakech Supervisor', 'marrakech_sup', '1234', NULL, NULL, 'Marrakech', TRUE),
    ('Tanger Supervisor', 'tanger_sup', '1234', NULL, NULL, 'Tanger', TRUE),
    ('Fes Supervisor', 'fes_sup', '1234', NULL, NULL, 'Fes', TRUE),
    ('Agadir Supervisor', 'agadir_sup', '1234', NULL, NULL, 'Agadir', TRUE)
ON DUPLICATE KEY UPDATE
    full_name = VALUES(full_name),
    password_hash = VALUES(password_hash),
    region = VALUES(region),
    active = VALUES(active);

-- Assign stores to demo supervisors.
-- Casablanca uses store_region because the project treats the supervisor scope
-- as the full Casablanca operational region, not only StoreCity = CASABLANCA.
DELETE ss
FROM supervisor_stores ss
JOIN supervisors sup
    ON sup.supervisor_id = ss.supervisor_id
WHERE sup.username = 'casa_sup';

INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_region)) = 'CASABLANCA'
WHERE sup.username = 'casa_sup';

-- Other demo supervisors still use store_city because their names are city-based
-- while the available store_region values are broader operational regions
-- such as NORTH, SOUTH, and ATLANTIC.
-- INSERT IGNORE prevents duplicate assignments if this script is run more than once.
INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_city)) LIKE '%RABAT%'
WHERE sup.username = 'rabat_sup';

INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_city)) LIKE '%MARRAKECH%'
WHERE sup.username = 'marrakech_sup';

INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_city)) LIKE '%TANGER%'
WHERE sup.username = 'tanger_sup';

INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_city)) IN ('FES', 'FEZ')
WHERE sup.username = 'fes_sup';

INSERT IGNORE INTO supervisor_stores (supervisor_id, store_id)
SELECT sup.supervisor_id, s.store_id
FROM supervisors sup
JOIN stores s
    ON UPPER(TRIM(s.store_city)) LIKE '%AGADIR%'
WHERE sup.username = 'agadir_sup';

-- Quick check: stores assigned to each demo supervisor.
SELECT
    sup.username,
    sup.full_name,
    sup.region,
    COUNT(ss.store_id) AS assigned_stores
FROM supervisors sup
LEFT JOIN supervisor_stores ss
    ON ss.supervisor_id = sup.supervisor_id
WHERE sup.username IN (
    'admin',
    'casa_sup',
    'rabat_sup',
    'marrakech_sup',
    'tanger_sup',
    'fes_sup',
    'agadir_sup'
)
GROUP BY sup.supervisor_id, sup.username, sup.full_name, sup.region
ORDER BY sup.username;
