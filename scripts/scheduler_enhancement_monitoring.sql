-- =============================================================================
-- Script: scheduler_enhancement_monitoring.sql
-- Description: Phase 4 - Create monitoring views for scheduler enhancement
-- Dependencies: scheduler_enhancement_setup.sql must be run first
-- =============================================================================

PROMPT ========================================
PROMPT Phase 4: Creating Monitoring Views
PROMPT ========================================

-- =============================================================================
-- View: v_dwh_ilm_active_batches
-- Purpose: Monitor currently running or interrupted batches
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_active_batches AS
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
    sch.batch_cooldown_minutes,
    cfg.config_value AS max_operations_per_batch
FROM cmr.dwh_ilm_execution_state es
JOIN cmr.dwh_ilm_execution_schedules sch ON sch.schedule_id = es.schedule_id
CROSS JOIN (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'MAX_CONCURRENT_OPERATIONS') cfg
WHERE es.status IN ('RUNNING', 'INTERRUPTED')
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_ilm_active_batches IS
'Shows currently running or interrupted ILM batches with progress metrics.
Use this view to monitor active batch executions in real-time.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_active_batches');

-- =============================================================================
-- View: v_dwh_ilm_schedule_stats
-- Purpose: Schedule effectiveness and execution history
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_schedule_stats AS
SELECT
    sch.schedule_id,
    sch.schedule_name,
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
FROM cmr.dwh_ilm_execution_schedules sch
LEFT JOIN cmr.dwh_ilm_execution_state es ON es.schedule_id = sch.schedule_id
GROUP BY
    sch.schedule_id, sch.schedule_name, sch.enabled,
    sch.monday_hours, sch.tuesday_hours, sch.wednesday_hours,
    sch.thursday_hours, sch.friday_hours, sch.saturday_hours, sch.sunday_hours,
    sch.batch_cooldown_minutes
ORDER BY last_execution_time DESC NULLS LAST;

COMMENT ON TABLE cmr.v_dwh_ilm_schedule_stats IS
'Statistics and effectiveness metrics for ILM schedules.
Shows batch completion rates, average operations per batch, and execution history.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_schedule_stats');

-- =============================================================================
-- View: v_dwh_ilm_batch_progress
-- Purpose: Detailed batch execution progress
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_batch_progress AS
SELECT
    es.execution_batch_id,
    es.status,
    sch.schedule_name,
    es.start_time,
    es.last_checkpoint,
    es.end_time,
    ROUND(es.elapsed_seconds / 60, 2) AS elapsed_minutes,
    es.operations_completed,
    es.operations_total,
    es.operations_remaining,
    ROUND(es.operations_completed * 100.0 / NULLIF(es.operations_total, 0), 1) AS pct_complete,
    es.last_queue_id,
    -- Estimate time remaining based on average speed
    CASE
        WHEN es.status = 'RUNNING' AND es.operations_completed > 0 THEN
            ROUND((es.operations_remaining * (es.elapsed_seconds / es.operations_completed)) / 60, 1)
        ELSE NULL
    END AS estimated_minutes_remaining,
    -- Queue items processed by this batch
    (SELECT COUNT(*)
     FROM cmr.dwh_ilm_evaluation_queue q
     WHERE q.execution_batch_id = es.execution_batch_id) AS queue_items_tagged
FROM cmr.dwh_ilm_execution_state es
JOIN cmr.dwh_ilm_execution_schedules sch ON sch.schedule_id = es.schedule_id
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_ilm_batch_progress IS
'Detailed progress tracking for all ILM batches (current and historical).
Includes estimated time remaining for running batches based on current speed.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_batch_progress');

-- =============================================================================
-- View: v_dwh_ilm_queue_summary
-- Purpose: Current queue status and backlog
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_queue_summary AS
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

COMMENT ON TABLE cmr.v_dwh_ilm_queue_summary IS
'Summary of ILM evaluation queue status.
Shows counts by execution status and identifies backlog age.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_queue_summary');

-- =============================================================================
-- View: v_dwh_ilm_current_window
-- Purpose: Show current execution window status
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_current_window AS
SELECT
    sch.schedule_name,
    sch.enabled,
    TRIM(TO_CHAR(SYSDATE, 'DAY')) AS current_day,
    TO_CHAR(SYSDATE, 'HH24:MI') AS current_time,
    CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
        WHEN 'MONDAY'    THEN sch.monday_hours
        WHEN 'TUESDAY'   THEN sch.tuesday_hours
        WHEN 'WEDNESDAY' THEN sch.wednesday_hours
        WHEN 'THURSDAY'  THEN sch.thursday_hours
        WHEN 'FRIDAY'    THEN sch.friday_hours
        WHEN 'SATURDAY'  THEN sch.saturday_hours
        WHEN 'SUNDAY'    THEN sch.sunday_hours
    END AS today_window,
    CASE
        WHEN sch.enabled = 'N' THEN 'SCHEDULE_DISABLED'
        WHEN CASE TRIM(TO_CHAR(SYSDATE, 'DAY'))
                 WHEN 'MONDAY'    THEN sch.monday_hours
                 WHEN 'TUESDAY'   THEN sch.tuesday_hours
                 WHEN 'WEDNESDAY' THEN sch.wednesday_hours
                 WHEN 'THURSDAY'  THEN sch.thursday_hours
                 WHEN 'FRIDAY'    THEN sch.friday_hours
                 WHEN 'SATURDAY'  THEN sch.saturday_hours
                 WHEN 'SUNDAY'    THEN sch.sunday_hours
             END IS NULL THEN 'NO_WINDOW_TODAY'
        ELSE 'WINDOW_CONFIGURED'
    END AS window_status,
    sch.batch_cooldown_minutes,
    (SELECT COUNT(*)
     FROM cmr.dwh_ilm_execution_state es
     WHERE es.schedule_id = sch.schedule_id
     AND es.status = 'RUNNING') AS currently_running_batches
FROM cmr.dwh_ilm_execution_schedules sch
ORDER BY sch.schedule_name;

COMMENT ON TABLE cmr.v_dwh_ilm_current_window IS
'Shows current execution window status for all schedules.
Helps quickly determine if ILM should be running right now.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_current_window');

-- =============================================================================
-- View: v_dwh_ilm_recent_batches
-- Purpose: Show recent batch execution history
-- =============================================================================

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_recent_batches AS
SELECT
    es.execution_batch_id,
    sch.schedule_name,
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
FROM cmr.dwh_ilm_execution_state es
JOIN cmr.dwh_ilm_execution_schedules sch ON sch.schedule_id = es.schedule_id
WHERE es.start_time >= SYSTIMESTAMP - INTERVAL '7' DAY  -- Last 7 days
ORDER BY es.start_time DESC;

COMMENT ON TABLE cmr.v_dwh_ilm_recent_batches IS
'Recent batch execution history (last 7 days).
Shows performance metrics including operations per minute.';

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_recent_batches');

-- =============================================================================
-- Summary and Usage Examples
-- =============================================================================

PROMPT ========================================
PROMPT Monitoring Views Created!
PROMPT ========================================
PROMPT
PROMPT Available Views:
PROMPT 1. v_dwh_ilm_active_batches        - Currently running/interrupted batches
PROMPT 2. v_dwh_ilm_schedule_stats        - Schedule effectiveness metrics
PROMPT 3. v_dwh_ilm_batch_progress        - Detailed batch progress with estimates
PROMPT 4. v_dwh_ilm_queue_summary         - Queue status and backlog
PROMPT 5. v_dwh_ilm_current_window        - Current execution window status
PROMPT 6. v_dwh_ilm_recent_batches        - Recent batch history (7 days)
PROMPT
PROMPT Example Queries:
PROMPT
PROMPT -- Check if anything is running right now
PROMPT SELECT * FROM cmr.v_dwh_ilm_active_batches;
PROMPT
PROMPT -- See current window status
PROMPT SELECT * FROM cmr.v_dwh_ilm_current_window;
PROMPT
PROMPT -- Check queue backlog
PROMPT SELECT * FROM cmr.v_dwh_ilm_queue_summary;
PROMPT
PROMPT -- Schedule performance
PROMPT SELECT * FROM cmr.v_dwh_ilm_schedule_stats;
PROMPT
PROMPT -- Recent execution history
PROMPT SELECT * FROM cmr.v_dwh_ilm_recent_batches;
PROMPT
PROMPT Next: Test the implementation with scheduler_enhancement_test.sql
PROMPT
