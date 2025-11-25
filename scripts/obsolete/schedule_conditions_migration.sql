-- =============================================================================
-- Script: schedule_conditions_migration.sql
-- Description: Migrate from ILM-specific to generalized schedule framework
-- Dependencies: Requires scheduler_enhancement_setup.sql to have been run first
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET TIMING ON

PROMPT ============================================================================
PROMPT Schedule Conditions Framework Migration
PROMPT ============================================================================
PROMPT
PROMPT This script will:
PROMPT 1. Rename old tables (dwh_ilm_execution_* -> *_old)
PROMPT 2. Create new generalized tables (dwh_execution_*)
PROMPT 3. Create new dwh_schedule_conditions table
PROMPT 4. Migrate existing data
PROMPT 5. Create indexes and constraints
PROMPT
PROMPT ============================================================================

-- =============================================================================
-- PHASE 1: RENAME OLD TABLES
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 1: Renaming old tables
PROMPT ========================================

-- Rename execution state table first (child table, has FK)
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_execution_state RENAME TO dwh_ilm_execution_state_old';
    DBMS_OUTPUT.PUT_LINE('✓ Renamed dwh_ilm_execution_state -> dwh_ilm_execution_state_old');
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

-- Rename execution schedules table (parent table)
BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_execution_schedules RENAME TO dwh_ilm_execution_schedules_old';
    DBMS_OUTPUT.PUT_LINE('✓ Renamed dwh_ilm_execution_schedules -> dwh_ilm_execution_schedules_old');
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

-- =============================================================================
-- PHASE 2: CREATE NEW TABLES
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 2: Creating new tables
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
CREATE INDEX idx_exec_state_batch ON cmr.dwh_execution_state(execution_batch_id);

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

-- =============================================================================
-- PHASE 3: MIGRATE DATA
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 3: Migrating data
PROMPT ========================================

-- Migrate schedules
DECLARE
    v_rows_migrated NUMBER := 0;
BEGIN
    INSERT INTO cmr.dwh_execution_schedules (
        schedule_name, schedule_type, enabled,
        monday_hours, tuesday_hours, wednesday_hours,
        thursday_hours, friday_hours, saturday_hours, sunday_hours,
        batch_cooldown_minutes, enable_checkpointing, checkpoint_frequency,
        created_date
    )
    SELECT
        schedule_name,
        'ILM' AS schedule_type,  -- All existing schedules are ILM type
        enabled,
        monday_hours, tuesday_hours, wednesday_hours,
        thursday_hours, friday_hours, saturday_hours, sunday_hours,
        batch_cooldown_minutes, enable_checkpointing, checkpoint_frequency,
        created_date
    FROM cmr.dwh_ilm_execution_schedules_old;

    v_rows_migrated := SQL%ROWCOUNT;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('✓ Migrated ' || v_rows_migrated || ' schedules from dwh_ilm_execution_schedules_old');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (No old schedule data to migrate)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR migrating schedules: ' || SQLERRM);
            RAISE;
        END IF;
END;
/

-- Migrate execution state
DECLARE
    v_rows_migrated NUMBER := 0;
BEGIN
    -- First, get schedule_id mapping
    INSERT INTO cmr.dwh_execution_state (
        execution_batch_id,
        schedule_id,
        start_time,
        last_checkpoint,
        end_time,
        status,
        last_queue_id,
        operations_completed,
        operations_total,
        elapsed_seconds
    )
    SELECT
        old_state.execution_batch_id,
        new_sched.schedule_id,  -- Map to new schedule_id
        old_state.start_time,
        old_state.last_checkpoint,
        old_state.end_time,
        old_state.status,
        old_state.last_queue_id,
        old_state.operations_completed,
        old_state.operations_total,
        old_state.elapsed_seconds
    FROM cmr.dwh_ilm_execution_state_old old_state
    JOIN cmr.dwh_ilm_execution_schedules_old old_sched
        ON old_state.schedule_id = old_sched.schedule_id
    JOIN cmr.dwh_execution_schedules new_sched
        ON old_sched.schedule_name = new_sched.schedule_name;

    v_rows_migrated := SQL%ROWCOUNT;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('✓ Migrated ' || v_rows_migrated || ' execution state records from dwh_ilm_execution_state_old');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('  (No old execution state data to migrate)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR migrating execution state: ' || SQLERRM);
            RAISE;
        END IF;
END;
/

-- =============================================================================
-- PHASE 4: VERIFICATION
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Phase 4: Verification
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
PROMPT Migration Complete!
PROMPT ============================================================================
PROMPT
PROMPT Tables Created:
PROMPT   ✓ dwh_execution_schedules (generalized, with schedule_type column)
PROMPT   ✓ dwh_execution_state (generalized)
PROMPT   ✓ dwh_schedule_conditions (NEW - condition framework)
PROMPT
PROMPT Old Tables Renamed:
PROMPT   • dwh_ilm_execution_schedules_old (backup)
PROMPT   • dwh_ilm_execution_state_old (backup)
PROMPT
PROMPT Next Steps:
PROMPT   1. Update package pck_dwh_ilm_execution_engine to use new table names
PROMPT   2. Add evaluate_schedule_conditions() function to package
PROMPT   3. Update should_execute_now() to check conditions
PROMPT   4. Update monitoring views to use new table names
PROMPT   5. Test with sample conditions
PROMPT
PROMPT After verification (e.g., 1 week):
PROMPT   DROP TABLE cmr.dwh_ilm_execution_schedules_old PURGE;
PROMPT   DROP TABLE cmr.dwh_ilm_execution_state_old PURGE;
PROMPT
PROMPT ============================================================================
