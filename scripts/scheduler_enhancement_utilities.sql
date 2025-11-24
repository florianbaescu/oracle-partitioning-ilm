-- =============================================================================
-- Script: scheduler_enhancement_utilities.sql
-- Description: Additional scheduler utilities (access tracking, cleanup, alerts)
-- Dependencies: scheduler_enhancement_scheduler.sql must be run first
-- =============================================================================

PROMPT ========================================
PROMPT Creating Additional Scheduler Utilities
PROMPT ========================================

-- =============================================================================
-- SECTION 1: ADDITIONAL SCHEDULER PROGRAMS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Program: Refresh Partition Access Tracking
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM(program_name => 'DWH_ILM_REFRESH_ACCESS_TRACKING', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_REFRESH_ACCESS_TRACKING',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                dwh_refresh_partition_access_tracking();
            END;',
        enabled => TRUE,
        comments => 'Refreshes custom partition access tracking statistics'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created program: DWH_ILM_REFRESH_ACCESS_TRACKING');
END;
/

-- -----------------------------------------------------------------------------
-- Program: Evaluate ILM Policies
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM(program_name => 'DWH_ILM_EVALUATE_POLICIES', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_EVALUATE_POLICIES',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                pck_dwh_ilm_policy_engine.evaluate_all_policies();
            END;',
        enabled => TRUE,
        comments => 'Evaluates all active ILM policies and identifies eligible partitions'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created program: DWH_ILM_EVALUATE_POLICIES');
END;
/

-- -----------------------------------------------------------------------------
-- Program: Cleanup Old Logs
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM(program_name => 'DWH_ILM_CLEANUP_LOGS', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_CLEANUP_LOGS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                dwh_cleanup_execution_logs();
            END;',
        enabled => TRUE,
        comments => 'Cleans up old execution logs based on retention policy'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created program: DWH_ILM_CLEANUP_LOGS');
END;
/

-- -----------------------------------------------------------------------------
-- Program: Check for Failures
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM(program_name => 'DWH_ILM_CHECK_FAILURES', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_CHECK_FAILURES',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                dwh_check_ilm_failures(p_hours_back => 24);
            END;',
        enabled => TRUE,
        comments => 'Monitors for ILM failures and sends alert emails'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created program: DWH_ILM_CHECK_FAILURES');
END;
/

-- =============================================================================
-- SECTION 2: ADDITIONAL SCHEDULER JOBS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Job: Refresh Access Tracking (Daily at 1 AM)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_JOB(job_name => 'DWH_ILM_JOB_REFRESH_ACCESS', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_REFRESH_ACCESS',
        program_name => 'DWH_ILM_REFRESH_ACCESS_TRACKING',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=1; BYMINUTE=0',
        enabled => FALSE,  -- Start disabled
        auto_drop => FALSE,
        comments => 'Daily refresh of partition access tracking at 1 AM'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created job: DWH_ILM_JOB_REFRESH_ACCESS (DISABLED)');
END;
/

-- -----------------------------------------------------------------------------
-- Job: Evaluate Policies (Daily at 2 AM)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_JOB(job_name => 'DWH_ILM_JOB_EVALUATE', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_EVALUATE',
        program_name => 'DWH_ILM_EVALUATE_POLICIES',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2; BYMINUTE=0',
        enabled => FALSE,  -- Start disabled
        auto_drop => FALSE,
        comments => 'Daily policy evaluation at 2 AM'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created job: DWH_ILM_JOB_EVALUATE (DISABLED)');
END;
/

-- -----------------------------------------------------------------------------
-- Job: Cleanup Logs (Weekly on Sunday at 3 AM)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_JOB(job_name => 'DWH_ILM_JOB_CLEANUP', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_CLEANUP',
        program_name => 'DWH_ILM_CLEANUP_LOGS',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=WEEKLY; BYDAY=SUN; BYHOUR=3; BYMINUTE=0',
        enabled => FALSE,  -- Start disabled
        auto_drop => FALSE,
        comments => 'Weekly cleanup of old logs on Sunday at 3 AM'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created job: DWH_ILM_JOB_CLEANUP (DISABLED)');
END;
/

-- -----------------------------------------------------------------------------
-- Job: Monitor Failures (Every 4 hours)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.DROP_JOB(job_name => 'DWH_ILM_JOB_MONITOR_FAILURES', force => TRUE);
EXCEPTION
    WHEN OTHERS THEN NULL;
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_MONITOR_FAILURES',
        program_name => 'DWH_ILM_CHECK_FAILURES',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=4',
        enabled => FALSE,  -- Start disabled (enable after email config)
        auto_drop => FALSE,
        comments => 'Monitor for ILM failures every 4 hours and send alerts if threshold exceeded'
    );
    DBMS_OUTPUT.PUT_LINE('✓ Created job: DWH_ILM_JOB_MONITOR_FAILURES (DISABLED - enable after email config)');
END;
/

-- =============================================================================
-- SECTION 3: HELPER PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Procedure: Stop All ILM Jobs
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_stop_ilm_jobs AS
    v_count NUMBER := 0;
BEGIN
    FOR job IN (
        SELECT job_name
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'DWH_ILM_JOB_%'
        AND enabled = 'TRUE'
    ) LOOP
        DBMS_SCHEDULER.DISABLE(name => job.job_name);
        DBMS_OUTPUT.PUT_LINE('Disabled job: ' || job.job_name);
        v_count := v_count + 1;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No enabled ILM jobs found');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Stopped ' || v_count || ' ILM jobs');
    END IF;
END;
/

DBMS_OUTPUT.PUT_LINE('✓ Created procedure: dwh_stop_ilm_jobs');

-- -----------------------------------------------------------------------------
-- Procedure: Start All ILM Jobs
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_start_ilm_jobs AS
    v_count NUMBER := 0;
BEGIN
    FOR job IN (
        SELECT job_name
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'DWH_ILM_JOB_%'
        AND enabled = 'FALSE'
    ) LOOP
        DBMS_SCHEDULER.ENABLE(name => job.job_name);
        DBMS_OUTPUT.PUT_LINE('Enabled job: ' || job.job_name);
        v_count := v_count + 1;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('All ILM jobs are already enabled');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Started ' || v_count || ' ILM jobs');
    END IF;
END;
/

DBMS_OUTPUT.PUT_LINE('✓ Created procedure: dwh_start_ilm_jobs');

-- -----------------------------------------------------------------------------
-- Procedure: Run ILM Job Immediately
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
    DBMS_OUTPUT.PUT_LINE('Check v_dwh_ilm_job_history for status');
END;
/

DBMS_OUTPUT.PUT_LINE('✓ Created procedure: dwh_run_ilm_job_now');

-- -----------------------------------------------------------------------------
-- Procedure: Run Complete ILM Cycle Manually
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_run_ilm_cycle(
    p_max_operations NUMBER DEFAULT NULL
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
    BEGIN
        dwh_refresh_partition_access_tracking();
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 2: Evaluate policies
    DBMS_OUTPUT.PUT_LINE('STEP 2: Evaluating ILM policies...');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    BEGIN
        pck_dwh_ilm_policy_engine.evaluate_all_policies();
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 3: Execute pending actions (using new continuous execution)
    DBMS_OUTPUT.PUT_LINE('STEP 3: Executing pending actions...');
    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    BEGIN
        pck_dwh_ilm_execution_engine.execute_pending_actions(
            p_schedule_name => 'DEFAULT_SCHEDULE',
            p_force_run => TRUE  -- Bypass schedule checks
        );
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
    END;
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

DBMS_OUTPUT.PUT_LINE('✓ Created procedure: dwh_run_ilm_cycle');

-- =============================================================================
-- SECTION 4: ADDITIONAL MONITORING VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View: Scheduler Job Status
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_dwh_ilm_scheduler_status AS
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
WHERE job_name LIKE 'DWH_ILM_JOB_%'
ORDER BY job_name;

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_scheduler_status');

-- -----------------------------------------------------------------------------
-- View: Job Run History
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_dwh_ilm_job_history AS
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
WHERE job_name LIKE 'DWH_ILM_JOB_%'
ORDER BY log_date DESC;

DBMS_OUTPUT.PUT_LINE('✓ Created view: v_dwh_ilm_job_history');

-- =============================================================================
-- SECTION 5: ALERT/NOTIFICATION PROCEDURES (Stubs - to be implemented)
-- =============================================================================

PROMPT
PROMPT Note: Alert/notification procedures (dwh_send_ilm_alert, dwh_check_ilm_failures,
PROMPT       dwh_notify_job_failure) require email configuration and are defined in
PROMPT       the original custom_ilm_scheduler.sql. Copy those procedures if needed.
PROMPT

-- =============================================================================
-- SUMMARY
-- =============================================================================

PROMPT ========================================
PROMPT Additional Scheduler Utilities Created!
PROMPT ========================================
PROMPT
PROMPT Programs Created:
PROMPT - DWH_ILM_REFRESH_ACCESS_TRACKING
PROMPT - DWH_ILM_EVALUATE_POLICIES
PROMPT - DWH_ILM_CLEANUP_LOGS
PROMPT - DWH_ILM_CHECK_FAILURES
PROMPT
PROMPT Jobs Created (all DISABLED by default):
PROMPT - DWH_ILM_JOB_REFRESH_ACCESS  (Daily at 1 AM)
PROMPT - DWH_ILM_JOB_EVALUATE        (Daily at 2 AM)
PROMPT - DWH_ILM_JOB_CLEANUP         (Weekly Sunday at 3 AM)
PROMPT - DWH_ILM_JOB_MONITOR_FAILURES (Every 4 hours)
PROMPT
PROMPT Helper Procedures:
PROMPT - dwh_stop_ilm_jobs()
PROMPT - dwh_start_ilm_jobs()
PROMPT - dwh_run_ilm_job_now(p_job_name)
PROMPT - dwh_run_ilm_cycle()
PROMPT
PROMPT Views:
PROMPT - v_dwh_ilm_scheduler_status
PROMPT - v_dwh_ilm_job_history
PROMPT
PROMPT Usage:
PROMPT
PROMPT -- Run complete ILM cycle manually
PROMPT EXEC dwh_run_ilm_cycle();
PROMPT
PROMPT -- Enable all jobs
PROMPT EXEC dwh_start_ilm_jobs();
PROMPT
PROMPT -- Enable specific jobs
PROMPT EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_REFRESH_ACCESS');
PROMPT EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EVALUATE');
PROMPT EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EXECUTE');
PROMPT
PROMPT -- Monitor jobs
PROMPT SELECT * FROM v_dwh_ilm_scheduler_status;
PROMPT SELECT * FROM v_dwh_ilm_job_history;
PROMPT
