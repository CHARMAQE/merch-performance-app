USE unilever_db;

-- Review status values used by the back-office workflow:
-- PENDING, REVIEWED, CONFIRMED, IGNORED, NEEDS_ACTION
--
-- This migration is additive only. It keeps existing validation_results rows
-- and initializes review_status to PENDING through the column default.

DELIMITER //

CREATE PROCEDURE add_validation_review_fields()
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'validation_results'
          AND column_name = 'review_status'
    ) THEN
        ALTER TABLE validation_results
            ADD COLUMN review_status VARCHAR(30) NOT NULL DEFAULT 'PENDING';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'validation_results'
          AND column_name = 'review_comment'
    ) THEN
        ALTER TABLE validation_results
            ADD COLUMN review_comment TEXT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'validation_results'
          AND column_name = 'reviewed_by'
    ) THEN
        ALTER TABLE validation_results
            ADD COLUMN reviewed_by VARCHAR(100) NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'validation_results'
          AND column_name = 'reviewed_at'
    ) THEN
        ALTER TABLE validation_results
            ADD COLUMN reviewed_at DATETIME NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'validation_results'
          AND index_name = 'idx_review_status'
    ) THEN
        ALTER TABLE validation_results
            ADD INDEX idx_review_status (review_status);
    END IF;
END//

DELIMITER ;

CALL add_validation_review_fields();
DROP PROCEDURE add_validation_review_fields;
