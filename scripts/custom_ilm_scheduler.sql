-- =============================================================================
-- Custom ILM Scheduler Jobs
-- Automates policy evaluation and execution
-- =============================================================================

-- =============================================================================
-- SECTION 1: SCHEDULER PROGRAMS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Program: Refresh Partition Access Tracking
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_REFRESH_ACCESS_TRACKING',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                dwh_refresh_partition_access_tracking();
            END;',
        enabled => TRUE,
        comments => 'Refreshes custom partition access tracking statistics'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Program: Evaluate ILM Policies
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_EVALUATE_POLICIES',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                pck_dwh_ilm_policy_engine.evaluate_all_policies();
            END;',
        enabled => TRUE,
        comments => 'Evaluates all active ILM policies and identifies eligible partitions'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Program: Execute Pending ILM Actions
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_EXECUTE_ACTIONS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            DECLARE
                v_enabled VARCHAR2(1);
            BEGIN
                -- Check if auto execution is enabled
                SELECT config_value INTO v_enabled
                FROM dwh_ilm_config
                WHERE config_key = ''ENABLE_AUTO_EXECUTION'';

                IF v_enabled = ''Y'' THEN
                    pck_dwh_ilm_execution_engine.execute_pending_actions(p_max_operations => 10);
                ELSE
                    DBMS_OUTPUT.PUT_LINE(''Automatic execution is disabled'');
                END IF;
            END;',
        enabled => TRUE,
        comments => 'Executes pending ILM actions within configured window'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Program: Cleanup Old Logs
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_CLEANUP_LOGS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                cleanup_execution_logs();
            END;',
        enabled => TRUE,
        comments => 'Cleans up old execution logs based on retention policy'
    );
END;
/


-- =============================================================================
-- SECTION 2: SCHEDULER JOBS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Job: Refresh Access Tracking (Daily)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_JOB_REFRESH_ACCESS',
        program_name => 'ILM_REFRESH_ACCESS_TRACKING',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=1; BYMINUTE=0',
        enabled => TRUE,
        comments => 'Daily refresh of partition access tracking at 1 AM'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Job: Evaluate Policies (Daily)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_JOB_EVALUATE',
        program_name => 'ILM_EVALUATE_POLICIES',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=0',
        enabled => TRUE,
        comments => 'Daily policy evaluation at 2 AM'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Job: Execute Actions (Every 2 hours during execution window)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_JOB_EXECUTE',
        program_name => 'ILM_EXECUTE_ACTIONS',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=2',
        enabled => TRUE,
        comments => 'Execute pending ILM actions every 2 hours (checks execution window internally)'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Job: Cleanup Logs (Weekly)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_JOB_CLEANUP',
        program_name => 'ILM_CLEANUP_LOGS',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY; BYDAY=SUN; BYHOUR=3; BYMINUTE=0',
        enabled => TRUE,
        comments => 'Weekly cleanup of old logs on Sunday at 3 AM'
    );
END;
/


-- =============================================================================
-- SECTION 3: JOB MONITORING VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View scheduler job status
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_scheduler_status AS
SELECT
    job_name,
    enabled,
    state,
    last_start_date,
    last_run_duration,
    next_run_date,
    run_count,
    failure_count,
    CASE
        WHEN failure_count > 0 AND run_count > 0 THEN
            ROUND(failure_count / run_count * 100, 2)
        ELSE 0
    END AS failure_rate_pct
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB_%'
ORDER BY job_name;


-- -----------------------------------------------------------------------------
-- View job run history
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_job_history AS
SELECT
    log_id,
    job_name,
    log_date,
    status,
    EXTRACT(SECOND FROM run_duration) +
    EXTRACT(MINUTE FROM run_duration) * 60 +
    EXTRACT(HOUR FROM run_duration) * 3600 AS duration_seconds,
    error#,
    SUBSTR(additional_info, 1, 200) AS additional_info
FROM user_scheduler_job_run_details
WHERE job_name LIKE 'ILM_JOB_%'
ORDER BY log_date DESC;


-- =============================================================================
-- SECTION 4: MANUAL JOB CONTROL PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Stop all ILM jobs
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_stop_ilm_jobs AS
BEGIN
    FOR job IN (
        SELECT job_name
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'ILM_JOB_%'
        AND enabled = 'TRUE'
    ) LOOP
        DBMS_SCHEDULER.DISABLE(name => job.job_name);
        DBMS_OUTPUT.PUT_LINE('Disabled job: ' || job.job_name);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('All ILM jobs stopped');
END;
/


-- -----------------------------------------------------------------------------
-- Start all ILM jobs
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_start_ilm_jobs AS
BEGIN
    FOR job IN (
        SELECT job_name
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'ILM_JOB_%'
        AND enabled = 'FALSE'
    ) LOOP
        DBMS_SCHEDULER.ENABLE(name => job.job_name);
        DBMS_OUTPUT.PUT_LINE('Enabled job: ' || job.job_name);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('All ILM jobs started');
END;
/


-- -----------------------------------------------------------------------------
-- Run job immediately
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_run_ilm_job_now(
    p_job_name VARCHAR2
) AS
BEGIN
    DBMS_SCHEDULER.RUN_JOB(
        job_name => p_job_name,
        use_current_session => FALSE
    );

    DBMS_OUTPUT.PUT_LINE('Job started: ' || p_job_name);
    DBMS_OUTPUT.PUT_LINE('Check v_ilm_job_history for status');
END;
/


-- =============================================================================
-- SECTION 5: COMPLETE ILM WORKFLOW PROCEDURE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Run complete ILM cycle manually
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_run_ilm_cycle(
    p_max_executions NUMBER DEFAULT NULL
) AS
    v_start_time TIMESTAMP := SYSTIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Starting Manual ILM Cycle');
    DBMS_OUTPUT.PUT_LINE('Start Time: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 1: Refresh access tracking
    DBMS_OUTPUT.PUT_LINE('STEP 1: Refreshing partition access tracking...');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    dwh_refresh_partition_access_tracking();
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 2: Evaluate policies
    DBMS_OUTPUT.PUT_LINE('STEP 2: Evaluating ILM policies...');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    pck_dwh_ilm_policy_engine.evaluate_all_policies();
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 3: Execute pending actions
    DBMS_OUTPUT.PUT_LINE('STEP 3: Executing pending actions...');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    pck_dwh_ilm_execution_engine.execute_pending_actions(p_max_executions);
    DBMS_OUTPUT.PUT_LINE('');

    v_end_time := SYSTIMESTAMP;
    v_duration := EXTRACT(SECOND FROM (v_end_time - v_start_time)) +
                 EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                 EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('ILM Cycle Complete');
    DBMS_OUTPUT.PUT_LINE('End Time: ' || TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Duration: ' || ROUND(v_duration, 2) || ' seconds');
    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/


-- =============================================================================
-- SECTION 6: VERIFICATION AND STATUS
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT ILM Scheduler Setup Complete!
PROMPT ========================================
PROMPT

PROMPT Scheduled Jobs:
SELECT
    job_name,
    enabled,
    next_run_date,
    repeat_interval
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB_%'
ORDER BY job_name;

PROMPT
PROMPT ========================================
PROMPT Quick Start Commands:
PROMPT ========================================
PROMPT
PROMPT -- Run ILM cycle manually:
PROMPT EXEC dwh_run_ilm_cycle();
PROMPT
PROMPT -- Run specific job now:
PROMPT EXEC dwh_run_ilm_job_now('ILM_JOB_EVALUATE');
PROMPT
PROMPT -- Stop all ILM jobs:
PROMPT EXEC dwh_stop_ilm_jobs();
PROMPT
PROMPT -- Start all ILM jobs:
PROMPT EXEC dwh_start_ilm_jobs();
PROMPT
PROMPT -- Check job status:
PROMPT SELECT * FROM v_ilm_scheduler_status;
PROMPT
PROMPT -- Check job history:
PROMPT SELECT * FROM v_ilm_job_history;
PROMPT
PROMPT ========================================
