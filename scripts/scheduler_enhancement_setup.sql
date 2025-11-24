-- =============================================================================
-- Script: scheduler_enhancement_setup.sql
-- Description: Phase 0 & Phase 1 - Cleanup old system and create new scheduler schema
-- Dependencies: Requires custom_ilm_setup.sql to have been run first
-- =============================================================================

-- =============================================================================
-- PHASE 0: CLEANUP OLD SYSTEM
-- =============================================================================

PROMPT ========================================
PROMPT Phase 0: Cleaning up old system
PROMPT ========================================

-- Drop old execution window function if exists
BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION is_execution_window_open';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped function is_execution_window_open');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -4043 THEN
            DBMS_OUTPUT.PUT_LINE('  (Function is_execution_window_open does not exist)');
        ELSE
            RAISE;
        END IF;
END;
/

-- Delete old config entries
DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START';
DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END';
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Deleted EXECUTION_WINDOW_START and EXECUTION_WINDOW_END from dwh_ilm_config');

-- =============================================================================
-- PHASE 1: CREATE NEW SCHEMA OBJECTS
-- =============================================================================

PROMPT ========================================
PROMPT Phase 1: Creating new schema objects
PROMPT ========================================

-- -----------------------------------------------------------------------------
-- Table: dwh_ilm_execution_schedules
-- Purpose: Store ILM execution schedules with per-day time windows
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_execution_schedules (
    schedule_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_name       VARCHAR2(100) NOT NULL UNIQUE,
    enabled             CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),

    -- Execution Windows: One column per day (NULL = don't run that day)
    -- Format: 'HH24:MI-HH24:MI' (e.g., '22:00-06:00' for 10 PM to 6 AM)
    monday_hours        VARCHAR2(11),
    tuesday_hours       VARCHAR2(11),
    wednesday_hours     VARCHAR2(11),
    thursday_hours      VARCHAR2(11),
    friday_hours        VARCHAR2(11),
    saturday_hours      VARCHAR2(11),
    sunday_hours        VARCHAR2(11),

    -- Execution Control
    batch_cooldown_minutes  NUMBER DEFAULT 5 CHECK (batch_cooldown_minutes >= 0),

    -- Resumability
    enable_checkpointing    CHAR(1) DEFAULT 'Y' CHECK (enable_checkpointing IN ('Y','N')),
    checkpoint_frequency    NUMBER DEFAULT 5 CHECK (checkpoint_frequency > 0),

    created_date        DATE DEFAULT SYSDATE,
    modified_date       DATE DEFAULT SYSDATE,

    -- Validation: Ensure correct time format HH24:MI-HH24:MI
    CONSTRAINT chk_monday_hours CHECK (monday_hours IS NULL OR REGEXP_LIKE(monday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_tuesday_hours CHECK (tuesday_hours IS NULL OR REGEXP_LIKE(tuesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_wednesday_hours CHECK (wednesday_hours IS NULL OR REGEXP_LIKE(wednesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_thursday_hours CHECK (thursday_hours IS NULL OR REGEXP_LIKE(thursday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_friday_hours CHECK (friday_hours IS NULL OR REGEXP_LIKE(friday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_saturday_hours CHECK (saturday_hours IS NULL OR REGEXP_LIKE(saturday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_sunday_hours CHECK (sunday_hours IS NULL OR REGEXP_LIKE(sunday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$'))
);

COMMENT ON TABLE cmr.dwh_ilm_execution_schedules IS
'ILM execution schedules with per-day time windows.
Each day column contains time range in HH24:MI-HH24:MI format (e.g., ''22:00-06:00'').
NULL value = no execution on that day.';

COMMENT ON COLUMN cmr.dwh_ilm_execution_schedules.monday_hours IS
'Execution window for Monday in format HH24:MI-HH24:MI (e.g., ''22:00-06:00'').
NULL = no execution on Monday.
Window can span midnight (e.g., ''22:00-06:00'' runs from 10 PM Monday to 6 AM Tuesday).';

COMMENT ON COLUMN cmr.dwh_ilm_execution_schedules.batch_cooldown_minutes IS
'Pause between batches in minutes.
0 = Continuous execution (run next batch immediately)
>0 = Pause N minutes between batches (e.g., 5 = 5-minute cooldown)
Batch size controlled globally by MAX_CONCURRENT_OPERATIONS in dwh_ilm_config.
Window close time provides natural duration limit.';

DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_ilm_execution_schedules');

-- -----------------------------------------------------------------------------
-- Table: dwh_ilm_execution_state
-- Purpose: Track batch execution state for resumability
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_execution_state (
    state_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_batch_id  VARCHAR2(50) NOT NULL UNIQUE,
    schedule_id         NUMBER NOT NULL,

    -- Execution Progress
    start_time          TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_checkpoint     TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR2(20) DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, INTERRUPTED, FAILED

    -- Work Tracking
    last_queue_id       NUMBER,       -- Last queue_id processed (for resumption)
    operations_completed NUMBER DEFAULT 0,
    operations_total    NUMBER,       -- Total pending when batch started
    operations_remaining NUMBER GENERATED ALWAYS AS (operations_total - operations_completed) VIRTUAL,

    -- Timing
    elapsed_seconds     NUMBER,

    CONSTRAINT fk_exec_state_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_ilm_execution_schedules(schedule_id)
);

COMMENT ON TABLE cmr.dwh_ilm_execution_state IS
'Tracks execution state of ILM batches for resumability and monitoring.
Each batch execution creates one row. Status tracks progress: RUNNING, COMPLETED, INTERRUPTED, FAILED.';

COMMENT ON COLUMN cmr.dwh_ilm_execution_state.execution_batch_id IS
'Unique batch identifier in format BATCH_YYYYMMDD_HH24MISS_NNN.
Used to link queue items and enable resumption after interruption.';

COMMENT ON COLUMN cmr.dwh_ilm_execution_state.last_queue_id IS
'Last queue_id successfully processed. Used to resume from checkpoint after interruption.
NULL = batch not started processing yet.';

DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_ilm_execution_state');

-- -----------------------------------------------------------------------------
-- Indexes for Performance
-- -----------------------------------------------------------------------------

CREATE INDEX idx_exec_state_status ON cmr.dwh_ilm_execution_state(status, start_time);
CREATE INDEX idx_exec_state_batch ON cmr.dwh_ilm_execution_state(execution_batch_id);
CREATE INDEX idx_exec_state_schedule ON cmr.dwh_ilm_execution_state(schedule_id, status);

DBMS_OUTPUT.PUT_LINE('✓ Created indexes on dwh_ilm_execution_state');

-- -----------------------------------------------------------------------------
-- Enhance Queue Table (Add Batch Tracking)
-- -----------------------------------------------------------------------------

-- Check if column already exists
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM all_tab_columns
    WHERE table_name = 'DWH_ILM_EVALUATION_QUEUE'
    AND owner = 'CMR'
    AND column_name = 'EXECUTION_BATCH_ID';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_evaluation_queue ADD (
            execution_batch_id  VARCHAR2(50),
            batch_sequence      NUMBER
        )';
        DBMS_OUTPUT.PUT_LINE('✓ Added batch tracking columns to dwh_ilm_evaluation_queue');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  (Batch tracking columns already exist in dwh_ilm_evaluation_queue)');
    END IF;
END;
/

-- Create index on batch columns if needed
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_queue_batch ON cmr.dwh_ilm_evaluation_queue(execution_batch_id, batch_sequence)';
    DBMS_OUTPUT.PUT_LINE('✓ Created index idx_queue_batch');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN  -- Name already used
            DBMS_OUTPUT.PUT_LINE('  (Index idx_queue_batch already exists)');
        ELSE
            RAISE;
        END IF;
END;
/

COMMENT ON COLUMN cmr.dwh_ilm_evaluation_queue.execution_batch_id IS
'Links queue item to execution batch for tracking and resumption.
Populated when item is picked up by batch execution.';

COMMENT ON COLUMN cmr.dwh_ilm_evaluation_queue.batch_sequence IS
'Order of execution within batch (1, 2, 3, ...).
Used to resume from checkpoint in correct order.';

-- -----------------------------------------------------------------------------
-- Insert Default Schedule
-- -----------------------------------------------------------------------------

MERGE INTO cmr.dwh_ilm_execution_schedules dst
USING (
    SELECT 'DEFAULT_SCHEDULE' AS schedule_name FROM dual
) src
ON (dst.schedule_name = src.schedule_name)
WHEN NOT MATCHED THEN
    INSERT (
        schedule_name,
        monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours,
        saturday_hours, sunday_hours,
        batch_cooldown_minutes,
        enable_checkpointing, checkpoint_frequency
    ) VALUES (
        'DEFAULT_SCHEDULE',
        '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00',  -- Mon-Fri
        NULL, NULL,                                                                   -- No weekends
        5,                                                                            -- 5-minute cooldown
        'Y', 5                                                                        -- Checkpoint every 5 ops
    );

COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Inserted DEFAULT_SCHEDULE (or already exists)');

-- -----------------------------------------------------------------------------
-- Summary
-- -----------------------------------------------------------------------------

PROMPT ========================================
PROMPT Schema Setup Complete!
PROMPT ========================================

SELECT 'dwh_ilm_execution_schedules' AS table_name, COUNT(*) AS row_count
FROM cmr.dwh_ilm_execution_schedules
UNION ALL
SELECT 'dwh_ilm_execution_state', COUNT(*)
FROM cmr.dwh_ilm_execution_state;

PROMPT
PROMPT Next Steps:
PROMPT 1. Run scheduler_enhancement_engine.sql to create execution engine package
PROMPT 2. Run scheduler_enhancement_scheduler.sql to create DBMS_SCHEDULER objects
PROMPT 3. Run scheduler_enhancement_monitoring.sql to create monitoring views
PROMPT
