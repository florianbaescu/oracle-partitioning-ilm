-- =============================================================================
-- Script: scheduler_enhancement_setup.sql
-- Description: Complete scheduler setup with schedule conditions framework
-- Dependencies: Requires custom_ilm_setup.sql to have been run first
--
-- This script consolidates:
-- - Schema migration (old → new generalized tables)
-- - Schedule conditions table creation
-- - Monitoring views
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ============================================================================
PROMPT Scheduler Enhancement Setup - Complete Installation
PROMPT ============================================================================
PROMPT
PROMPT This script will:
PROMPT 1. Cleanup old system (drop old tables if they exist)
PROMPT 2. Create new generalized tables (dwh_execution_*)
PROMPT 3. Create schedule conditions table (NEW)
PROMPT 4. Insert default schedule
PROMPT 5. Create monitoring views
PROMPT
PROMPT ============================================================================

-- =============================================================================
-- PHASE 0: CLEANUP OLD SYSTEM
-- =============================================================================

PROMPT
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
BEGIN
    DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START';
    DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END';
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('✓ Deleted EXECUTION_WINDOW_START and EXECUTION_WINDOW_END from dwh_ilm_config');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_ilm_config does not exist, skipping)');
        ELSE
            RAISE;
        END IF;
END;
/

-- =============================================================================
-- PHASE 1: DROP OLD TABLES (if they exist)
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 1: Dropping old tables
PROMPT ========================================

-- Drop old execution state table first (child table, has FK)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_execution_state CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: dwh_ilm_execution_state');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_ilm_execution_state does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            RAISE;
        END IF;
END;
/

-- Drop old execution schedules table (parent table)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_execution_schedules CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: dwh_ilm_execution_schedules');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_ilm_execution_schedules does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            RAISE;
        END IF;
END;
/

-- Drop new tables if they exist (for clean reinstall)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_execution_state CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: dwh_execution_state');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_execution_state does not exist, skipping)');
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_schedule_conditions CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: dwh_schedule_conditions');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_schedule_conditions does not exist, skipping)');
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_execution_schedules CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: dwh_execution_schedules');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (Table dwh_execution_schedules does not exist, skipping)');
        END IF;
END;
/

-- =============================================================================
-- PHASE 2: CREATE NEW SCHEMA OBJECTS
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 2: Creating new schema objects
PROMPT ========================================

-- -----------------------------------------------------------------------------
-- Table 1: dwh_execution_schedules (Generalized)
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_execution_schedules (
    schedule_id             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_name           VARCHAR2(100) NOT NULL UNIQUE,
    schedule_type           VARCHAR2(50) DEFAULT 'ILM' NOT NULL,
    schedule_description    VARCHAR2(500),
    enabled                 CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),

    -- Execution Windows: One column per day (NULL = don't run that day)
    -- Format: 'HH24:MI-HH24:MI' (e.g., '22:00-06:00' for 10 PM to 6 AM)
    monday_hours            VARCHAR2(11),
    tuesday_hours           VARCHAR2(11),
    wednesday_hours         VARCHAR2(11),
    thursday_hours          VARCHAR2(11),
    friday_hours            VARCHAR2(11),
    saturday_hours          VARCHAR2(11),
    sunday_hours            VARCHAR2(11),

    -- Execution Control
    batch_cooldown_minutes  NUMBER DEFAULT 5 CHECK (batch_cooldown_minutes >= 0),

    -- Resumability
    enable_checkpointing    CHAR(1) DEFAULT 'Y' CHECK (enable_checkpointing IN ('Y','N')),
    checkpoint_frequency    NUMBER DEFAULT 5 CHECK (checkpoint_frequency > 0),

    -- Audit
    created_by              VARCHAR2(50) DEFAULT USER,
    created_date            DATE DEFAULT SYSDATE,
    modified_by             VARCHAR2(50) DEFAULT USER,
    modified_date           DATE DEFAULT SYSDATE,

    -- Validation: Ensure correct time format HH24:MI-HH24:MI
    CONSTRAINT chk_monday_hours CHECK (monday_hours IS NULL OR REGEXP_LIKE(monday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_tuesday_hours CHECK (tuesday_hours IS NULL OR REGEXP_LIKE(tuesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_wednesday_hours CHECK (wednesday_hours IS NULL OR REGEXP_LIKE(wednesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_thursday_hours CHECK (thursday_hours IS NULL OR REGEXP_LIKE(thursday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_friday_hours CHECK (friday_hours IS NULL OR REGEXP_LIKE(friday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_saturday_hours CHECK (saturday_hours IS NULL OR REGEXP_LIKE(saturday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_sunday_hours CHECK (sunday_hours IS NULL OR REGEXP_LIKE(sunday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$'))
);

CREATE INDEX idx_exec_sched_type ON cmr.dwh_execution_schedules(schedule_type, enabled);

COMMENT ON TABLE cmr.dwh_execution_schedules IS
'Centralized execution schedules with per-day time windows.
Supports multiple schedule types (ILM, BACKUP, ETL, etc.).
Each day column contains time range in HH24:MI-HH24:MI format.
NULL value = no execution on that day.';

COMMENT ON COLUMN cmr.dwh_execution_schedules.schedule_type IS
'Type of scheduled operation: ILM, BACKUP, ETL, MAINTENANCE, etc.
Allows same scheduling framework to be used for different job types.';

COMMENT ON COLUMN cmr.dwh_execution_schedules.batch_cooldown_minutes IS
'Pause between batches in minutes.
0 = Continuous execution (run next batch immediately)
>0 = Pause N minutes between batches (e.g., 5 = 5-minute cooldown)';

DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_execution_schedules');

-- -----------------------------------------------------------------------------
-- Table 2: dwh_schedule_conditions (NEW)
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_schedule_conditions (
    condition_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_id             NUMBER NOT NULL,
    condition_name          VARCHAR2(100) NOT NULL,
    condition_type          VARCHAR2(20) NOT NULL CHECK (condition_type IN ('SQL', 'FUNCTION', 'PLSQL')),
    condition_code          CLOB NOT NULL,
    evaluation_order        NUMBER DEFAULT 1 NOT NULL CHECK (evaluation_order > 0),
    logical_operator        VARCHAR2(10) DEFAULT 'AND' CHECK (logical_operator IN ('AND', 'OR')),
    enabled                 CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),
    fail_on_error           CHAR(1) DEFAULT 'Y' CHECK (fail_on_error IN ('Y','N')),
    description             VARCHAR2(500),

    -- Evaluation History
    last_evaluation_time    TIMESTAMP,
    last_evaluation_result  CHAR(1) CHECK (last_evaluation_result IN ('Y','N')),
    last_evaluation_error   VARCHAR2(4000),
    evaluation_count        NUMBER DEFAULT 0,
    success_count           NUMBER DEFAULT 0,
    failure_count           NUMBER DEFAULT 0,

    -- Audit
    created_by              VARCHAR2(50) DEFAULT USER,
    created_date            DATE DEFAULT SYSDATE,
    modified_by             VARCHAR2(50) DEFAULT USER,
    modified_date           DATE DEFAULT SYSDATE,

    CONSTRAINT fk_sched_cond_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_execution_schedules(schedule_id)
        ON DELETE CASCADE,

    CONSTRAINT uk_sched_cond_name
        UNIQUE (schedule_id, condition_name)
);

CREATE INDEX idx_sched_cond_schedule ON cmr.dwh_schedule_conditions(schedule_id, evaluation_order);
CREATE INDEX idx_sched_cond_enabled ON cmr.dwh_schedule_conditions(schedule_id, enabled);

COMMENT ON TABLE cmr.dwh_schedule_conditions IS
'Custom conditions that gate schedule execution.
Each condition must evaluate to TRUE (or be skipped if disabled).
Multiple conditions per schedule are evaluated in order with logical operators.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.condition_type IS
'Type of condition code:
- SQL: SELECT statement returning 1 (TRUE) or 0 (FALSE)
- FUNCTION: PL/SQL function call returning BOOLEAN
- PLSQL: PL/SQL anonymous block with RETURN statement';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.condition_code IS
'Executable code for condition evaluation:
- SQL: SELECT 1 FROM DUAL WHERE <condition>
- FUNCTION: RETURN my_check_function(param1, param2)
- PLSQL: BEGIN ... RETURN <boolean_expression>; END;';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.evaluation_order IS
'Order in which conditions are evaluated (1, 2, 3...).
Lower numbers evaluated first.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.logical_operator IS
'How to combine with NEXT condition:
- AND: Both this and next must be TRUE
- OR: Either this or next must be TRUE
Last condition''s operator is ignored.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.fail_on_error IS
'How to handle evaluation errors:
- Y: Treat evaluation error as FALSE (fail-safe, prevent execution)
- N: Ignore errors and treat as TRUE (continue execution)';

DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_schedule_conditions');

-- -----------------------------------------------------------------------------
-- Table 3: dwh_execution_state (Generalized)
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_execution_state (
    state_id                NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_batch_id      VARCHAR2(50) NOT NULL UNIQUE,
    schedule_id             NUMBER NOT NULL,

    -- Execution Progress
    start_time              TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_checkpoint         TIMESTAMP,
    end_time                TIMESTAMP,
    status                  VARCHAR2(20) DEFAULT 'RUNNING' CHECK (status IN ('RUNNING','COMPLETED','INTERRUPTED','FAILED')),

    -- Work Tracking
    last_queue_id           NUMBER,       -- Last queue_id processed (for resumption)
    operations_completed    NUMBER DEFAULT 0,
    operations_total        NUMBER,       -- Total pending when batch started
    operations_remaining    NUMBER GENERATED ALWAYS AS (operations_total - operations_completed) VIRTUAL,

    -- Timing
    elapsed_seconds         NUMBER,

    CONSTRAINT fk_exec_state_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_execution_schedules(schedule_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_exec_state_schedule ON cmr.dwh_execution_state(schedule_id, status);
CREATE INDEX idx_exec_state_status ON cmr.dwh_execution_state(status, start_time);
-- Note: execution_batch_id already has UNIQUE constraint which creates an index

COMMENT ON TABLE cmr.dwh_execution_state IS
'Tracks batch execution state for checkpointing and resumability.
One row per batch execution. Supports interruption and resume.';

COMMENT ON COLUMN cmr.dwh_execution_state.execution_batch_id IS
'Unique batch identifier in format BATCH_YYYYMMDD_HH24MISS_NNN.
Used to link queue items and enable resumption after interruption.';

COMMENT ON COLUMN cmr.dwh_execution_state.last_queue_id IS
'Last queue_id successfully processed. Used to resume from checkpoint after interruption.
NULL = batch not started processing yet.';

DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_execution_state');

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

-- =============================================================================
-- PHASE 3: INSERT DEFAULT DATA
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 3: Inserting default data
PROMPT ========================================

-- Insert default schedule
INSERT INTO cmr.dwh_execution_schedules (
    schedule_name,
    schedule_type,
    schedule_description,
    enabled,
    monday_hours,
    tuesday_hours,
    wednesday_hours,
    thursday_hours,
    friday_hours,
    saturday_hours,
    sunday_hours,
    batch_cooldown_minutes,
    enable_checkpointing,
    checkpoint_frequency
) VALUES (
    'DEFAULT_SCHEDULE',
    'ILM',
    'Default ILM execution schedule',
    'Y',
    '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00',  -- Mon-Fri: 10 PM to 6 AM
    NULL, NULL,                                                                   -- No weekends
    5,                                                                            -- 5-minute cooldown between batches
    'Y',                                                                          -- Enable checkpointing
    5                                                                             -- Checkpoint every 5 operations
);

COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Inserted DEFAULT_SCHEDULE');

-- =============================================================================
-- PHASE 4: CREATE MONITORING VIEWS
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 4: Creating monitoring views
PROMPT ========================================

-- -----------------------------------------------------------------------------
-- View: v_dwh_active_batches
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_active_batches AS
SELECT
    es.execution_batch_id,
    es.status,
    es.start_time,
    es.last_checkpoint,
    ROUND((EXTRACT(SECOND FROM (SYSTIMESTAMP - es.start_time)) +
           EXTRACT(MINUTE FROM (SYSTIMESTAMP - es.start_time)) * 60 +
           EXTRACT(HOUR FROM (SYSTIMESTAMP - es.start_time)) * 3600) / 60, 2) AS elapsed_minutes,
    es.operations_completed,
    es.operations_total,
    es.operations_remaining,
    ROUND(es.operations_completed * 100.0 / NULLIF(es.operations_total, 0), 1) AS pct_complete,
    sch.schedule_name,
    sch.schedule_type,
    sch.batch_cooldown_minutes,
    cfg.config_value AS max_operations_per_batch
FROM cmr.dwh_execution_state es
JOIN cmr.dwh_execution_schedules sch ON sch.schedule_id = es.schedule_id
CROSS JOIN (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'MAX_CONCURRENT_OPERATIONS') cfg
WHERE es.status IN ('RUNNING', 'INTERRUPTED')
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_active_batches IS
'Shows currently running or interrupted batches with progress metrics.
Use this view to monitor active batch executions in real-time.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_active_batches');

-- -----------------------------------------------------------------------------
-- View: v_dwh_schedule_stats
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_schedule_stats AS
SELECT
    sch.schedule_id,
    sch.schedule_name,
    sch.schedule_type,
    sch.enabled,
    sch.monday_hours,
    sch.tuesday_hours,
    sch.wednesday_hours,
    sch.thursday_hours,
    sch.friday_hours,
    sch.saturday_hours,
    sch.sunday_hours,
    sch.batch_cooldown_minutes,
    COUNT(DISTINCT es.execution_batch_id) AS total_batches,
    SUM(CASE WHEN es.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_batches,
    SUM(CASE WHEN es.status = 'INTERRUPTED' THEN 1 ELSE 0 END) AS interrupted_batches,
    SUM(CASE WHEN es.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_batches,
    SUM(CASE WHEN es.status = 'RUNNING' THEN 1 ELSE 0 END) AS running_batches,
    ROUND(AVG(es.operations_completed), 1) AS avg_ops_per_batch,
    SUM(es.operations_completed) AS total_operations_completed,
    ROUND(AVG(es.elapsed_seconds / 60), 1) AS avg_duration_minutes,
    MAX(es.start_time) AS last_execution_time,
    MIN(es.start_time) AS first_execution_time
FROM cmr.dwh_execution_schedules sch
LEFT JOIN cmr.dwh_execution_state es ON es.schedule_id = sch.schedule_id
GROUP BY
    sch.schedule_id, sch.schedule_name, sch.schedule_type, sch.enabled,
    sch.monday_hours, sch.tuesday_hours, sch.wednesday_hours,
    sch.thursday_hours, sch.friday_hours, sch.saturday_hours, sch.sunday_hours,
    sch.batch_cooldown_minutes
ORDER BY last_execution_time DESC NULLS LAST;

COMMENT ON TABLE cmr.v_dwh_schedule_stats IS
'Statistics and effectiveness metrics for execution schedules.
Shows batch completion rates, average operations per batch, and execution history.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_schedule_stats');

-- -----------------------------------------------------------------------------
-- View: v_dwh_batch_progress
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_batch_progress AS
SELECT
    es.execution_batch_id,
    es.status,
    sch.schedule_name,
    sch.schedule_type,
    es.start_time,
    es.last_checkpoint,
    es.end_time,
    ROUND(es.elapsed_seconds / 60, 2) AS elapsed_minutes,
    es.operations_completed,
    es.operations_total,
    es.operations_remaining,
    ROUND(es.operations_completed * 100.0 / NULLIF(es.operations_total, 0), 1) AS pct_complete,
    CASE
        WHEN es.status = 'RUNNING' THEN
            ROUND((es.operations_remaining * (es.elapsed_seconds / NULLIF(es.operations_completed, 0))) / 60, 1)
        ELSE NULL
    END AS estimated_minutes_remaining
FROM cmr.dwh_execution_state es
JOIN cmr.dwh_execution_schedules sch ON sch.schedule_id = es.schedule_id
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_batch_progress IS
'Detailed batch execution progress with estimated completion time.
Shows checkpoint information and completion percentage for all batches.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_batch_progress');

-- -----------------------------------------------------------------------------
-- View: v_dwh_current_window
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_current_window AS
SELECT
    schedule_id,
    schedule_name,
    schedule_type,
    enabled,
    TRIM(TO_CHAR(SYSDATE, 'DAY')) AS current_day,
    CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
        WHEN 'MONDAY'    THEN monday_hours
        WHEN 'TUESDAY'   THEN tuesday_hours
        WHEN 'WEDNESDAY' THEN wednesday_hours
        WHEN 'THURSDAY'  THEN thursday_hours
        WHEN 'FRIDAY'    THEN friday_hours
        WHEN 'SATURDAY'  THEN saturday_hours
        WHEN 'SUNDAY'    THEN sunday_hours
    END AS today_window,
    TO_CHAR(SYSDATE, 'HH24:MI') AS current_time,
    CASE
        WHEN enabled = 'N' THEN 'DISABLED'
        WHEN CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
            WHEN 'MONDAY'    THEN monday_hours
            WHEN 'TUESDAY'   THEN tuesday_hours
            WHEN 'WEDNESDAY' THEN wednesday_hours
            WHEN 'THURSDAY'  THEN thursday_hours
            WHEN 'FRIDAY'    THEN friday_hours
            WHEN 'SATURDAY'  THEN saturday_hours
            WHEN 'SUNDAY'    THEN sunday_hours
        END IS NULL THEN 'NO_EXECUTION_TODAY'
        ELSE 'CHECK_TIME_WINDOW'
    END AS window_status
FROM cmr.dwh_execution_schedules
ORDER BY schedule_name;

COMMENT ON TABLE cmr.v_dwh_current_window IS
'Shows current execution window status for all schedules.
Use this view to check if schedule should run today and what time window is configured.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_current_window');

-- -----------------------------------------------------------------------------
-- View: v_dwh_queue_summary
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_queue_summary AS
SELECT
    COUNT(*) AS total_queue_items,
    SUM(CASE WHEN execution_status = 'PENDING' AND eligible = 'Y' THEN 1 ELSE 0 END) AS pending_eligible,
    SUM(CASE WHEN execution_status = 'PENDING' AND eligible = 'N' THEN 1 ELSE 0 END) AS pending_not_eligible,
    SUM(CASE WHEN execution_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN execution_status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped,
    MIN(evaluation_date) AS oldest_evaluation,
    MAX(evaluation_date) AS newest_evaluation,
    ROUND(EXTRACT(DAY FROM (SYSTIMESTAMP - MIN(evaluation_date))) +
          EXTRACT(HOUR FROM (SYSTIMESTAMP - MIN(evaluation_date))) / 24 +
          EXTRACT(MINUTE FROM (SYSTIMESTAMP - MIN(evaluation_date))) / 1440, 1) AS oldest_age_days
FROM cmr.dwh_ilm_evaluation_queue;

COMMENT ON TABLE cmr.v_dwh_queue_summary IS
'Summary of ILM evaluation queue status.
Shows counts by execution status and identifies backlog age.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_queue_summary');

-- -----------------------------------------------------------------------------
-- View: v_dwh_schedule_conditions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_schedule_conditions AS
SELECT
    sc.condition_id,
    sc.schedule_id,
    sch.schedule_name,
    sch.schedule_type,
    sc.condition_name,
    sc.condition_type,
    sc.evaluation_order,
    sc.logical_operator,
    sc.enabled,
    sc.fail_on_error,
    sc.description,
    -- Evaluation metrics
    sc.last_evaluation_time,
    sc.last_evaluation_result,
    CASE sc.last_evaluation_result
        WHEN 'Y' THEN 'PASSED'
        WHEN 'N' THEN 'FAILED'
        ELSE 'NOT_EVALUATED'
    END AS last_result_text,
    sc.last_evaluation_error,
    sc.evaluation_count,
    sc.success_count,
    sc.failure_count,
    ROUND(sc.success_count * 100.0 / NULLIF(sc.evaluation_count, 0), 1) AS success_rate_pct,
    -- Time since last evaluation
    CASE
        WHEN sc.last_evaluation_time IS NOT NULL THEN
            ROUND((EXTRACT(SECOND FROM (SYSTIMESTAMP - sc.last_evaluation_time)) +
                   EXTRACT(MINUTE FROM (SYSTIMESTAMP - sc.last_evaluation_time)) * 60 +
                   EXTRACT(HOUR FROM (SYSTIMESTAMP - sc.last_evaluation_time)) * 3600) / 60, 1)
        ELSE NULL
    END AS minutes_since_last_eval,
    sc.created_by,
    sc.created_date,
    sc.modified_by,
    sc.modified_date
FROM cmr.dwh_schedule_conditions sc
JOIN cmr.dwh_execution_schedules sch ON sch.schedule_id = sc.schedule_id
ORDER BY sch.schedule_name, sc.evaluation_order;

COMMENT ON TABLE cmr.v_dwh_schedule_conditions IS
'Shows all schedule conditions with evaluation history and metrics.
Use this view to monitor condition effectiveness and troubleshoot failures.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_schedule_conditions');

-- -----------------------------------------------------------------------------
-- View: v_dwh_condition_failures
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_condition_failures AS
SELECT
    sc.condition_id,
    sch.schedule_name,
    sch.schedule_type,
    sc.condition_name,
    sc.condition_type,
    sc.evaluation_order,
    sc.enabled,
    sc.fail_on_error,
    sc.last_evaluation_time,
    sc.last_evaluation_error,
    sc.failure_count,
    sc.success_count,
    sc.evaluation_count,
    ROUND(sc.failure_count * 100.0 / NULLIF(sc.evaluation_count, 0), 1) AS failure_rate_pct,
    CASE
        WHEN sc.last_evaluation_time IS NOT NULL THEN
            ROUND((EXTRACT(SECOND FROM (SYSTIMESTAMP - sc.last_evaluation_time)) +
                   EXTRACT(MINUTE FROM (SYSTIMESTAMP - sc.last_evaluation_time)) * 60 +
                   EXTRACT(HOUR FROM (SYSTIMESTAMP - sc.last_evaluation_time)) * 3600) / 60, 1)
        ELSE NULL
    END AS minutes_since_failure
FROM cmr.dwh_schedule_conditions sc
JOIN cmr.dwh_execution_schedules sch ON sch.schedule_id = sc.schedule_id
WHERE sc.enabled = 'Y'
AND sc.last_evaluation_result = 'N'
ORDER BY sc.last_evaluation_time DESC;

COMMENT ON TABLE cmr.v_dwh_condition_failures IS
'Shows currently failing conditions that are blocking schedule execution.
Use this view to identify and troubleshoot condition failures.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_condition_failures');

-- -----------------------------------------------------------------------------
-- View: v_dwh_schedule_readiness
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_schedule_readiness AS
SELECT
    sch.schedule_id,
    sch.schedule_name,
    sch.schedule_type,
    sch.enabled AS schedule_enabled,
    -- Current window
    TRIM(TO_CHAR(SYSDATE, 'DAY')) AS current_day,
    CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
        WHEN 'MONDAY'    THEN sch.monday_hours
        WHEN 'TUESDAY'   THEN sch.tuesday_hours
        WHEN 'WEDNESDAY' THEN sch.wednesday_hours
        WHEN 'THURSDAY'  THEN sch.thursday_hours
        WHEN 'FRIDAY'    THEN sch.friday_hours
        WHEN 'SATURDAY'  THEN sch.saturday_hours
        WHEN 'SUNDAY'    THEN sch.sunday_hours
    END AS today_window,
    -- Running batches
    (SELECT COUNT(*)
     FROM cmr.dwh_execution_state es
     WHERE es.schedule_id = sch.schedule_id
     AND es.status = 'RUNNING') AS running_batches,
    -- Pending work
    (SELECT COUNT(*)
     FROM cmr.dwh_ilm_evaluation_queue q
     WHERE q.execution_status = 'PENDING'
     AND q.eligible = 'Y') AS pending_work,
    -- Conditions
    (SELECT COUNT(*)
     FROM cmr.dwh_schedule_conditions sc
     WHERE sc.schedule_id = sch.schedule_id
     AND sc.enabled = 'Y') AS total_conditions,
    (SELECT COUNT(*)
     FROM cmr.dwh_schedule_conditions sc
     WHERE sc.schedule_id = sch.schedule_id
     AND sc.enabled = 'Y'
     AND sc.last_evaluation_result = 'N') AS failing_conditions,
    -- Overall readiness
    CASE
        WHEN sch.enabled = 'N' THEN 'NOT_READY:SCHEDULE_DISABLED'
        WHEN (SELECT COUNT(*)
              FROM cmr.dwh_execution_state es
              WHERE es.schedule_id = sch.schedule_id
              AND es.status = 'RUNNING') > 0 THEN 'NOT_READY:ALREADY_RUNNING'
        WHEN (SELECT COUNT(*)
              FROM cmr.dwh_ilm_evaluation_queue q
              WHERE q.execution_status = 'PENDING'
              AND q.eligible = 'Y') = 0 THEN 'NOT_READY:NO_PENDING_WORK'
        WHEN CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
            WHEN 'MONDAY'    THEN sch.monday_hours
            WHEN 'TUESDAY'   THEN sch.tuesday_hours
            WHEN 'WEDNESDAY' THEN sch.wednesday_hours
            WHEN 'THURSDAY'  THEN sch.thursday_hours
            WHEN 'FRIDAY'    THEN sch.friday_hours
            WHEN 'SATURDAY'  THEN sch.saturday_hours
            WHEN 'SUNDAY'    THEN sch.sunday_hours
        END IS NULL THEN 'NOT_READY:NO_WINDOW_TODAY'
        WHEN (SELECT COUNT(*)
              FROM cmr.dwh_schedule_conditions sc
              WHERE sc.schedule_id = sch.schedule_id
              AND sc.enabled = 'Y'
              AND sc.last_evaluation_result = 'N') > 0 THEN 'NOT_READY:CONDITIONS_FAILING'
        ELSE 'READY'
    END AS readiness_status
FROM cmr.dwh_execution_schedules sch
ORDER BY sch.schedule_name;

COMMENT ON TABLE cmr.v_dwh_schedule_readiness IS
'Overall schedule readiness check including all gates (enabled, window, work, conditions).
Use this view to understand why a schedule is or is not executing.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_schedule_readiness');

-- -----------------------------------------------------------------------------
-- View: v_dwh_recent_batches
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_recent_batches AS
SELECT
    es.execution_batch_id,
    sch.schedule_name,
    sch.schedule_type,
    es.status,
    es.start_time,
    es.end_time,
    ROUND(es.elapsed_seconds / 60, 2) AS duration_minutes,
    es.operations_completed,
    es.operations_total,
    ROUND(es.operations_completed * 100.0 / NULLIF(es.operations_total, 0), 1) AS pct_complete,
    -- Operations per minute
    CASE
        WHEN es.elapsed_seconds > 0 THEN
            ROUND((es.operations_completed * 60.0) / es.elapsed_seconds, 2)
        ELSE NULL
    END AS ops_per_minute
FROM cmr.dwh_execution_state es
JOIN cmr.dwh_execution_schedules sch ON sch.schedule_id = es.schedule_id
WHERE es.start_time >= SYSTIMESTAMP - INTERVAL '7' DAY  -- Last 7 days
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_recent_batches IS
'Recent batch execution history (last 7 days).
Shows performance metrics including operations per minute.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_recent_batches');

-- =============================================================================
-- PHASE 5: VERIFICATION
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 5: Verification
PROMPT ========================================

DECLARE
    v_sched_count NUMBER;
    v_state_count NUMBER;
    v_cond_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_sched_count FROM cmr.dwh_execution_schedules;
    SELECT COUNT(*) INTO v_state_count FROM cmr.dwh_execution_state;
    SELECT COUNT(*) INTO v_cond_count FROM cmr.dwh_schedule_conditions;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Table Row Counts:');
    DBMS_OUTPUT.PUT_LINE('  dwh_execution_schedules: ' || v_sched_count);
    DBMS_OUTPUT.PUT_LINE('  dwh_execution_state: ' || v_state_count);
    DBMS_OUTPUT.PUT_LINE('  dwh_schedule_conditions: ' || v_cond_count || ' (new table, empty)');
END;
/

-- =============================================================================
-- SUMMARY
-- =============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Scheduler Enhancement Setup Complete!
PROMPT ============================================================================
PROMPT
PROMPT Tables Created:
PROMPT   ✓ dwh_execution_schedules (generalized, with schedule_type column)
PROMPT   ✓ dwh_execution_state (generalized)
PROMPT   ✓ dwh_schedule_conditions (NEW - condition framework)
PROMPT
PROMPT Default Data Inserted:
PROMPT   ✓ DEFAULT_SCHEDULE (ILM type, Mon-Fri 22:00-06:00)
PROMPT
PROMPT Views Created:
PROMPT   ✓ v_dwh_active_batches
PROMPT   ✓ v_dwh_schedule_stats
PROMPT   ✓ v_dwh_batch_progress
PROMPT   ✓ v_dwh_current_window
PROMPT   ✓ v_dwh_queue_summary
PROMPT   ✓ v_dwh_recent_batches
PROMPT   ✓ v_dwh_schedule_conditions (NEW)
PROMPT   ✓ v_dwh_condition_failures (NEW)
PROMPT   ✓ v_dwh_schedule_readiness (NEW)
PROMPT
PROMPT Next Steps:
PROMPT   1. Run scheduler_enhancement_engine.sql to create execution engine package
PROMPT   2. Run scheduler_enhancement_scheduler.sql to create DBMS_SCHEDULER jobs
PROMPT   3. (Optional) Review examples: scheduler_conditions_examples.sql
PROMPT
PROMPT ============================================================================
