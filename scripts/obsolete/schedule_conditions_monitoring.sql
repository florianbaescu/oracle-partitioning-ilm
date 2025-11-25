-- =============================================================================
-- Script: schedule_conditions_monitoring.sql
-- Description: Create/Update monitoring views for schedule conditions framework
-- Dependencies: schedule_conditions_migration.sql must be run first
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ============================================================================
PROMPT Schedule Conditions Framework - Monitoring Views
PROMPT ============================================================================

-- =============================================================================
-- UPDATE EXISTING VIEWS TO USE NEW TABLE NAMES
-- =============================================================================

PROMPT
PROMPT Updating existing monitoring views to use new table names...
PROMPT

-- -----------------------------------------------------------------------------
-- View: v_dwh_active_batches (renamed from v_dwh_ilm_active_batches)
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

DBMS_OUTPUT.PUT_LINE('✓ Created/Updated view: v_dwh_active_batches');

-- -----------------------------------------------------------------------------
-- View: v_dwh_schedule_stats (renamed from v_dwh_ilm_schedule_stats)
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

DBMS_OUTPUT.PUT_LINE('✓ Created/Updated view: v_dwh_schedule_stats');

-- -----------------------------------------------------------------------------
-- View: v_dwh_batch_progress (renamed from v_dwh_ilm_batch_progress)
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

DBMS_OUTPUT.PUT_LINE('✓ Created/Updated view: v_dwh_batch_progress');

-- -----------------------------------------------------------------------------
-- View: v_dwh_current_window (renamed from v_dwh_ilm_current_window)
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

DBMS_OUTPUT.PUT_LINE('✓ Created/Updated view: v_dwh_current_window');

-- =============================================================================
-- NEW VIEWS FOR SCHEDULE CONDITIONS
-- =============================================================================

PROMPT
PROMPT Creating new schedule conditions monitoring views...
PROMPT

-- -----------------------------------------------------------------------------
-- View: v_dwh_schedule_conditions
-- Purpose: Current condition configuration and status
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
-- Purpose: Conditions that are currently failing
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
-- Purpose: Overall schedule readiness including conditions
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

-- =============================================================================
-- SUMMARY
-- =============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Monitoring Views Created/Updated Successfully!
PROMPT ============================================================================
PROMPT
PROMPT Updated Views (renamed, now generalized):
PROMPT   ✓ v_dwh_active_batches
PROMPT   ✓ v_dwh_schedule_stats
PROMPT   ✓ v_dwh_batch_progress
PROMPT   ✓ v_dwh_current_window
PROMPT
PROMPT New Views (schedule conditions):
PROMPT   ✓ v_dwh_schedule_conditions - All conditions with metrics
PROMPT   ✓ v_dwh_condition_failures - Currently failing conditions
PROMPT   ✓ v_dwh_schedule_readiness - Overall readiness status
PROMPT
PROMPT Usage Examples:
PROMPT   -- Check current readiness
PROMPT   SELECT * FROM cmr.v_dwh_schedule_readiness;
PROMPT
PROMPT   -- Monitor condition status
PROMPT   SELECT schedule_name, condition_name, last_result_text, success_rate_pct
PROMPT   FROM cmr.v_dwh_schedule_conditions;
PROMPT
PROMPT   -- Identify failing conditions
PROMPT   SELECT * FROM cmr.v_dwh_condition_failures;
PROMPT
PROMPT ============================================================================
