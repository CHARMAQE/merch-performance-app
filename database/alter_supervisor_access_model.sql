-- Active: 1776681738854@@127.0.0.1@3306@unilever_db
USE unilever_db;

-- Add the mobile supervisor access-model columns without dropping existing data.
DELIMITER $$

DROP PROCEDURE IF EXISTS mp_add_column_if_missing $$
CREATE PROCEDURE mp_add_column_if_missing(
    IN table_name_value VARCHAR(64),
    IN column_name_value VARCHAR(64),
    IN column_definition_value TEXT
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = table_name_value
          AND COLUMN_NAME = column_name_value
    ) THEN
        SET @ddl = CONCAT(
            'ALTER TABLE `',
            table_name_value,
            '` ADD COLUMN `',
            column_name_value,
            '` ',
            column_definition_value
        );
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END $$

DROP PROCEDURE IF EXISTS mp_add_index_if_missing $$
CREATE PROCEDURE mp_add_index_if_missing(
    IN table_name_value VARCHAR(64),
    IN index_name_value VARCHAR(64),
    IN index_definition_value TEXT
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = table_name_value
          AND INDEX_NAME = index_name_value
    ) THEN
        SET @ddl = CONCAT(
            'ALTER TABLE `',
            table_name_value,
            '` ADD ',
            index_definition_value
        );
        PREPARE stmt FROM @ddl;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END $$

DELIMITER ;

CALL mp_add_column_if_missing(
    'supervisors',
    'supervisor_code',
    'VARCHAR(40) NULL AFTER `supervisor_id`'
);

CALL mp_add_column_if_missing(
    'supervisors',
    'city',
    'VARCHAR(120) NULL AFTER `phone`'
);

CALL mp_add_column_if_missing(
    'supervisors',
    'role',
    'VARCHAR(40) NOT NULL DEFAULT ''CLIENT_SUPERVISOR'' AFTER `region`'
);

CALL mp_add_column_if_missing(
    'supervisors',
    'created_at',
    'TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP AFTER `active`'
);

CALL mp_add_column_if_missing(
    'supervisors',
    'updated_at',
    'TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `created_at`'
);

UPDATE supervisors
SET supervisor_code = CONCAT('SUP-', supervisor_id)
WHERE supervisor_code IS NULL
   OR TRIM(supervisor_code) = '';

UPDATE supervisors sup
JOIN (
    SELECT supervisor_code
    FROM supervisors
    WHERE supervisor_code IS NOT NULL
      AND TRIM(supervisor_code) <> ''
    GROUP BY supervisor_code
    HAVING COUNT(*) > 1
) duplicate_codes
    ON duplicate_codes.supervisor_code = sup.supervisor_code
SET sup.supervisor_code = CONCAT('SUP-', sup.supervisor_id);

UPDATE supervisors
SET role = CASE
        WHEN LOWER(TRIM(username)) = 'admin' THEN 'ADMIN'
        ELSE 'CLIENT_SUPERVISOR'
    END
WHERE role IS NULL
   OR TRIM(role) = '';

CALL mp_add_index_if_missing(
    'supervisors',
    'uq_supervisors_supervisor_code',
    'UNIQUE KEY `uq_supervisors_supervisor_code` (`supervisor_code`)'
);

CALL mp_add_column_if_missing(
    'supervisor_stores',
    'assignment_source',
    'VARCHAR(80) NULL DEFAULT ''FAKE_CLIENT_LIST'' AFTER `store_id`'
);

CALL mp_add_column_if_missing(
    'supervisor_stores',
    'assigned_at',
    'TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP AFTER `assignment_source`'
);

CALL mp_add_column_if_missing(
    'supervisor_stores',
    'active',
    'BOOLEAN NOT NULL DEFAULT TRUE AFTER `assigned_at`'
);

UPDATE supervisor_stores
SET assignment_source = 'FAKE_CLIENT_LIST'
WHERE assignment_source IS NULL
   OR TRIM(assignment_source) = '';

UPDATE supervisor_stores
SET active = TRUE
WHERE active IS NULL;

DROP PROCEDURE IF EXISTS mp_add_column_if_missing;
DROP PROCEDURE IF EXISTS mp_add_index_if_missing;
