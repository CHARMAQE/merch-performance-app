-- Active: 1776681738854@@127.0.0.1@3306@unilever_db
USE unilever_db;

SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Prototype Unilever client assignment list for the mobile app.
-- Raw test password for every account: 1234
-- Stored value is BCrypt, not plain text.
SET @demo_password_hash = _utf8mb4'$2a$10$crw1vES16nLtEW4fYs3H3On7K0GMDNVawfRV5tQm1e9PXT.qd1t7u' COLLATE utf8mb4_unicode_ci;

DROP TEMPORARY TABLE IF EXISTS prototype_supervisor_candidates;
CREATE TEMPORARY TABLE prototype_supervisor_candidates (
    city_key VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    slot_number INT NOT NULL,
    supervisor_code VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    full_name VARCHAR(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    username VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    email VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    phone VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    city VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    region VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    role VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_supervisor_candidates (
    city_key,
    slot_number,
    supervisor_code,
    full_name,
    username,
    email,
    phone,
    city,
    region,
    role
)
VALUES
    ('CASABLANCA', 1, 'SUP-CASA-001', 'Yassine El Amrani', 'casa_sup_1', 'yassine.elamrani@unilever.test', '+212 6 10 00 00 01', 'Casablanca', 'Casablanca-Settat', 'CLIENT_SUPERVISOR'),
    ('CASABLANCA', 2, 'SUP-CASA-002', 'Sara Lahlou', 'casa_sup_2', 'sara.lahlou@unilever.test', '+212 6 10 00 00 02', 'Casablanca', 'Casablanca-Settat', 'CLIENT_SUPERVISOR'),
    ('CASABLANCA', 3, 'SUP-CASA-003', 'Mehdi Bennani', 'casa_sup_3', 'mehdi.bennani@unilever.test', '+212 6 10 00 00 03', 'Casablanca', 'Casablanca-Settat', 'CLIENT_SUPERVISOR'),
    ('CASABLANCA', 4, 'SUP-CASA-004', 'Hajar Alaoui', 'casa_sup_4', 'hajar.alaoui@unilever.test', '+212 6 10 00 00 04', 'Casablanca', 'Casablanca-Settat', 'CLIENT_SUPERVISOR'),
    ('RABAT', 1, 'SUP-RABAT-001', 'Salma Benjelloun', 'rabat_sup_1', 'salma.benjelloun@unilever.test', '+212 6 10 00 00 05', 'Rabat', 'Rabat-Sale-Kenitra', 'CLIENT_SUPERVISOR'),
    ('RABAT', 2, 'SUP-RABAT-002', 'Anas Ouazzani', 'rabat_sup_2', 'anas.ouazzani@unilever.test', '+212 6 10 00 00 06', 'Rabat', 'Rabat-Sale-Kenitra', 'CLIENT_SUPERVISOR'),
    ('MARRAKECH', 1, 'SUP-MARRAKECH-001', 'Nadia El Fassi', 'marrakech_sup_1', 'nadia.elfassi@unilever.test', '+212 6 10 00 00 07', 'Marrakech', 'Marrakech-Safi', 'CLIENT_SUPERVISOR'),
    ('MARRAKECH', 2, 'SUP-MARRAKECH-002', 'Hicham Barakat', 'marrakech_sup_2', 'hicham.barakat@unilever.test', '+212 6 10 00 00 08', 'Marrakech', 'Marrakech-Safi', 'CLIENT_SUPERVISOR'),
    ('TANGER', 1, 'SUP-TANGER-001', 'Omar Bakkali', 'tanger_sup_1', 'omar.bakkali@unilever.test', '+212 6 10 00 00 09', 'Tanger', 'Tanger-Tetouan-Al Hoceima', 'CLIENT_SUPERVISOR'),
    ('FES', 1, 'SUP-FES-001', 'Imane Chraibi', 'fes_sup_1', 'imane.chraibi@unilever.test', '+212 6 10 00 00 10', 'Fes', 'Fes-Meknes', 'CLIENT_SUPERVISOR'),
    ('AGADIR', 1, 'SUP-AGADIR-001', 'Khalid Ait Lahcen', 'agadir_sup_1', 'khalid.aitlahcen@unilever.test', '+212 6 10 00 00 11', 'Agadir', 'Souss-Massa', 'CLIENT_SUPERVISOR');

DROP TEMPORARY TABLE IF EXISTS prototype_legacy_usernames;
CREATE TEMPORARY TABLE prototype_legacy_usernames (
    username VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_legacy_usernames (username)
VALUES
    ('admin'),
    ('casa_sup'),
    ('rabat_sup'),
    ('marrakech_sup'),
    ('tanger_sup'),
    ('fes_sup'),
    ('agadir_sup'),
    ('supervisor'),
    ('casa_sup_1'),
    ('casa_sup_2'),
    ('casa_sup_3'),
    ('casa_sup_4'),
    ('rabat_sup_1'),
    ('rabat_sup_2'),
    ('marrakech_sup_1'),
    ('marrakech_sup_2'),
    ('tanger_sup_1'),
    ('fes_sup_1'),
    ('agadir_sup_1');

-- Remove fake prototype assignments and accounts only.
DELETE ss
FROM supervisor_stores ss
JOIN supervisors sup
    ON sup.supervisor_id = ss.supervisor_id
LEFT JOIN prototype_legacy_usernames legacy
    ON legacy.username COLLATE utf8mb4_unicode_ci = sup.username COLLATE utf8mb4_unicode_ci
LEFT JOIN prototype_supervisor_candidates candidate
    ON candidate.username COLLATE utf8mb4_unicode_ci = sup.username COLLATE utf8mb4_unicode_ci
WHERE ss.assignment_source COLLATE utf8mb4_unicode_ci = _utf8mb4'FAKE_CLIENT_LIST' COLLATE utf8mb4_unicode_ci
   OR legacy.username IS NOT NULL
   OR candidate.username IS NOT NULL
   OR LOWER(COALESCE(sup.email COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%@unilever.test' COLLATE utf8mb4_unicode_ci;

DELETE sup
FROM supervisors sup
LEFT JOIN prototype_legacy_usernames legacy
    ON legacy.username COLLATE utf8mb4_unicode_ci = sup.username COLLATE utf8mb4_unicode_ci
LEFT JOIN prototype_supervisor_candidates candidate
    ON candidate.username COLLATE utf8mb4_unicode_ci = sup.username COLLATE utf8mb4_unicode_ci
WHERE legacy.username IS NOT NULL
   OR candidate.username IS NOT NULL
   OR LOWER(COALESCE(sup.email COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%@unilever.test' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci IN (_utf8mb4'SUP-ADMIN' COLLATE utf8mb4_unicode_ci)
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-CASA-%' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-RABAT-%' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-MARRAKECH-%' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-TANGER-%' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-FES-%' COLLATE utf8mb4_unicode_ci
   OR sup.supervisor_code COLLATE utf8mb4_unicode_ci LIKE _utf8mb4'SUP-AGADIR-%' COLLATE utf8mb4_unicode_ci;

DROP TEMPORARY TABLE IF EXISTS prototype_city_store_candidates;
CREATE TEMPORARY TABLE prototype_city_store_candidates (
    city_key VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    city_priority INT NOT NULL,
    store_id INT NOT NULL,
    store_code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_city_store_candidates (city_key, city_priority, store_id, store_code)
SELECT _utf8mb4'CASABLANCA' COLLATE utf8mb4_unicode_ci AS city_key,
       1 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%CASA%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%CASA%' COLLATE utf8mb4_unicode_ci

UNION ALL
SELECT _utf8mb4'RABAT' COLLATE utf8mb4_unicode_ci AS city_key,
       2 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%RABAT%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%RABAT%' COLLATE utf8mb4_unicode_ci

UNION ALL
SELECT _utf8mb4'MARRAKECH' COLLATE utf8mb4_unicode_ci AS city_key,
       3 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%MARRAKECH%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%MARRAKESH%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%MARRAKECH%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%MARRAKESH%' COLLATE utf8mb4_unicode_ci

UNION ALL
SELECT _utf8mb4'TANGER' COLLATE utf8mb4_unicode_ci AS city_key,
       4 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%TANGER%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%TANGIER%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%TANGER%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%TANGIER%' COLLATE utf8mb4_unicode_ci

UNION ALL
SELECT _utf8mb4'FES' COLLATE utf8mb4_unicode_ci AS city_key,
       5 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%FES%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%FEZ%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%FES%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%FEZ%' COLLATE utf8mb4_unicode_ci

UNION ALL
SELECT _utf8mb4'AGADIR' COLLATE utf8mb4_unicode_ci AS city_key,
       6 AS city_priority,
       s.store_id,
       s.store_code COLLATE utf8mb4_unicode_ci AS store_code
FROM stores s
WHERE UPPER(COALESCE(s.store_city COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%AGADIR%' COLLATE utf8mb4_unicode_ci
   OR UPPER(COALESCE(s.store_region COLLATE utf8mb4_unicode_ci, _utf8mb4'' COLLATE utf8mb4_unicode_ci))
        LIKE _utf8mb4'%AGADIR%' COLLATE utf8mb4_unicode_ci;

DROP TEMPORARY TABLE IF EXISTS prototype_city_store_unique;
CREATE TEMPORARY TABLE prototype_city_store_unique (
    city_key VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    store_id INT NOT NULL,
    store_code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_city_store_unique (city_key, store_id, store_code)
SELECT city_key, store_id, store_code
FROM (
    SELECT
        city_key,
        store_id,
        store_code,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY city_priority, city_key COLLATE utf8mb4_unicode_ci
        ) AS city_match_rank
    FROM prototype_city_store_candidates
) ranked_city_matches
WHERE city_match_rank = 1;

DROP TEMPORARY TABLE IF EXISTS prototype_city_stores;
CREATE TEMPORARY TABLE prototype_city_stores (
    city_key VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    store_id INT NOT NULL,
    store_code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    store_rank BIGINT NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_city_stores (city_key, store_id, store_code, store_rank)
SELECT
    city_key,
    store_id,
    store_code,
    ROW_NUMBER() OVER (
        PARTITION BY city_key COLLATE utf8mb4_unicode_ci
        ORDER BY store_code COLLATE utf8mb4_unicode_ci, store_id
    ) AS store_rank
FROM prototype_city_store_unique;

DROP TEMPORARY TABLE IF EXISTS prototype_city_supervisor_counts;
CREATE TEMPORARY TABLE prototype_city_supervisor_counts (
    city_key VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    store_count BIGINT NOT NULL,
    supervisor_count BIGINT NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_city_supervisor_counts (city_key, store_count, supervisor_count)
SELECT
    city_key,
    store_count,
    CASE
        WHEN city_key COLLATE utf8mb4_unicode_ci = _utf8mb4'CASABLANCA' COLLATE utf8mb4_unicode_ci THEN LEAST(4, store_count)
        WHEN city_key COLLATE utf8mb4_unicode_ci = _utf8mb4'RABAT' COLLATE utf8mb4_unicode_ci
            AND store_count > 100 THEN 2
        WHEN city_key COLLATE utf8mb4_unicode_ci = _utf8mb4'MARRAKECH' COLLATE utf8mb4_unicode_ci
            AND store_count > 100 THEN 2
        ELSE 1
    END AS supervisor_count
FROM (
    SELECT
        MIN(city_key COLLATE utf8mb4_unicode_ci) AS city_key,
        COUNT(*) AS store_count
    FROM prototype_city_stores
    GROUP BY city_key COLLATE utf8mb4_unicode_ci
) city_counts
WHERE store_count > 0;

DROP TEMPORARY TABLE IF EXISTS prototype_supervisors;
CREATE TEMPORARY TABLE prototype_supervisors (
    supervisor_code VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    full_name VARCHAR(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    username VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL PRIMARY KEY,
    email VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    phone VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    city VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    region VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    role VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

INSERT INTO prototype_supervisors (
    supervisor_code,
    full_name,
    username,
    email,
    phone,
    city,
    region,
    role
)
VALUES
    ('SUP-ADMIN', 'Unilever Mobile Admin', 'admin', 'mobile.admin@unilever.test', NULL, 'National', 'All Regions', 'ADMIN');

INSERT INTO prototype_supervisors (
    supervisor_code,
    full_name,
    username,
    email,
    phone,
    city,
    region,
    role
)
SELECT
    candidate.supervisor_code,
    candidate.full_name,
    candidate.username,
    candidate.email,
    candidate.phone,
    candidate.city,
    candidate.region,
    candidate.role
FROM prototype_supervisor_candidates candidate
JOIN prototype_city_supervisor_counts city_counts
    ON city_counts.city_key COLLATE utf8mb4_unicode_ci = candidate.city_key COLLATE utf8mb4_unicode_ci
WHERE candidate.slot_number <= city_counts.supervisor_count;

INSERT INTO supervisors (
    supervisor_code,
    full_name,
    username,
    email,
    phone,
    city,
    region,
    role,
    password_hash,
    active
)
SELECT
    proto.supervisor_code,
    proto.full_name,
    proto.username,
    proto.email,
    proto.phone,
    proto.city,
    proto.region,
    proto.role,
    @demo_password_hash,
    TRUE
FROM prototype_supervisors proto;

-- Fair split: stores are ordered by store_code and assigned round-robin to
-- the active supervisor slots for the same city bucket.
INSERT INTO supervisor_stores (
    supervisor_id,
    store_id,
    assignment_source,
    active
)
SELECT
    sup.supervisor_id,
    city_stores.store_id,
    _utf8mb4'FAKE_CLIENT_LIST' COLLATE utf8mb4_unicode_ci,
    TRUE
FROM prototype_city_stores city_stores
JOIN prototype_city_supervisor_counts city_counts
    ON city_counts.city_key COLLATE utf8mb4_unicode_ci = city_stores.city_key COLLATE utf8mb4_unicode_ci
JOIN prototype_supervisor_candidates candidate
    ON candidate.city_key COLLATE utf8mb4_unicode_ci = city_stores.city_key COLLATE utf8mb4_unicode_ci
   AND candidate.slot_number = MOD(city_stores.store_rank - 1, city_counts.supervisor_count) + 1
JOIN supervisors sup
    ON sup.username COLLATE utf8mb4_unicode_ci = candidate.username COLLATE utf8mb4_unicode_ci
WHERE city_counts.supervisor_count > 0
  AND sup.active = TRUE;

-- Quick check: city split size and stores assigned to each prototype supervisor.
SELECT
    city_key,
    store_count,
    supervisor_count,
    CEIL(store_count / supervisor_count) AS max_expected_stores_per_supervisor
FROM prototype_city_supervisor_counts
ORDER BY city_key COLLATE utf8mb4_unicode_ci;

SELECT
    sup.email,
    sup.username,
    sup.supervisor_code,
    sup.full_name,
    sup.city,
    sup.region,
    sup.role,
    COALESCE(SUM(CASE WHEN ss.active = TRUE THEN 1 ELSE 0 END), 0) AS assigned_stores
FROM supervisors sup
LEFT JOIN supervisor_stores ss
    ON ss.supervisor_id = sup.supervisor_id
JOIN prototype_supervisors proto
    ON proto.username COLLATE utf8mb4_unicode_ci = sup.username COLLATE utf8mb4_unicode_ci
GROUP BY
    sup.supervisor_id,
    sup.email,
    sup.username,
    sup.supervisor_code,
    sup.full_name,
    sup.city,
    sup.region,
    sup.role
ORDER BY sup.role COLLATE utf8mb4_unicode_ci,
    sup.city COLLATE utf8mb4_unicode_ci,
    sup.username COLLATE utf8mb4_unicode_ci;

DROP TEMPORARY TABLE IF EXISTS prototype_supervisors;
DROP TEMPORARY TABLE IF EXISTS prototype_city_supervisor_counts;
DROP TEMPORARY TABLE IF EXISTS prototype_city_stores;
DROP TEMPORARY TABLE IF EXISTS prototype_city_store_unique;
DROP TEMPORARY TABLE IF EXISTS prototype_city_store_candidates;
DROP TEMPORARY TABLE IF EXISTS prototype_legacy_usernames;
DROP TEMPORARY TABLE IF EXISTS prototype_supervisor_candidates;
