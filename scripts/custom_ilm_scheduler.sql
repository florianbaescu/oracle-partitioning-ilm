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
                FROM cmr.dwh_ilm_config
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
                dwh_cleanup_execution_logs();
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
WHERE job_name LIKE 'ILM_JOB_%'
ORDER BY job_name;


-- -----------------------------------------------------------------------------
-- View job run history
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
-- SECTION 6: ERROR NOTIFICATION SYSTEM
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Procedure: Send ILM Error Notification
-- Purpose: Send email notification when ILM jobs fail
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_send_ilm_alert(
    p_alert_type VARCHAR2,
    p_subject VARCHAR2,
    p_message CLOB
) AS
    v_email_enabled VARCHAR2(1);
    v_recipients VARCHAR2(4000);
    v_sender VARCHAR2(200);
    v_smtp_server VARCHAR2(200);
    v_final_subject VARCHAR2(500);
    v_message_text VARCHAR2(32767);
BEGIN
    -- Check if email notifications are enabled
    BEGIN
        SELECT config_value INTO v_email_enabled
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_email_enabled := 'N';
    END;

    IF v_email_enabled != 'Y' THEN
        DBMS_OUTPUT.PUT_LINE('Email notifications are disabled');
        RETURN;
    END IF;

    -- Get email configuration
    BEGIN
        SELECT config_value INTO v_recipients
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ALERT_EMAIL_RECIPIENTS';

        SELECT config_value INTO v_sender
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ALERT_EMAIL_SENDER';

        SELECT config_value INTO v_smtp_server
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'SMTP_SERVER';

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: Email configuration incomplete');
            DBMS_OUTPUT.PUT_LINE('Required config keys: ALERT_EMAIL_RECIPIENTS, ALERT_EMAIL_SENDER, SMTP_SERVER');
            RETURN;
    END;

    -- Construct subject with alert type prefix
    v_final_subject := '[ILM ' || p_alert_type || '] ' || p_subject;

    -- Convert CLOB to VARCHAR2 (truncate if necessary)
    v_message_text := SUBSTR(p_message, 1, 32767);

    -- Send email using UTL_MAIL
    BEGIN
        UTL_MAIL.SEND(
            sender => v_sender,
            recipients => v_recipients,
            subject => v_final_subject,
            message => v_message_text,
            mime_type => 'text/plain; charset=utf-8'
        );

        DBMS_OUTPUT.PUT_LINE('Alert email sent successfully');
        DBMS_OUTPUT.PUT_LINE('  To: ' || v_recipients);
        DBMS_OUTPUT.PUT_LINE('  Subject: ' || v_final_subject);

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR sending email: ' || SQLERRM);
            -- Log to execution log as well
            INSERT INTO cmr.dwh_ilm_execution_log (
                policy_id, policy_name, action_type,
                status, error_message
            ) VALUES (
                -1, 'EMAIL_NOTIFICATION', 'NOTIFY',
                'FAILED', 'Failed to send email: ' || SQLERRM
            );
            COMMIT;
    END;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Unexpected error in dwh_send_ilm_alert: ' || SQLERRM);
END;
/


-- -----------------------------------------------------------------------------
-- Procedure: Check for Failures and Send Alerts
-- Purpose: Monitor for recent failures and send consolidated alert
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_check_ilm_failures(
    p_hours_back NUMBER DEFAULT 24
) AS
    v_failure_count NUMBER;
    v_alert_message CLOB;
    v_threshold NUMBER;
    v_last_alert_time TIMESTAMP;
    v_alert_interval NUMBER; -- hours between alerts
    CURSOR c_recent_failures IS
        SELECT
            execution_id,
            policy_name,
            partition_name,
            action_type,
            execution_start,
            error_message
        FROM cmr.dwh_ilm_execution_log
        WHERE status = 'FAILED'
        AND execution_start > SYSTIMESTAMP - INTERVAL '1' HOUR * p_hours_back
        ORDER BY execution_start DESC;
BEGIN
    -- Get alert threshold
    BEGIN
        SELECT TO_NUMBER(config_value) INTO v_threshold
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ALERT_FAILURE_THRESHOLD';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_threshold := 3; -- Default: alert if 3+ failures
    END;

    -- Get alert interval (minimum hours between alerts)
    BEGIN
        SELECT TO_NUMBER(config_value) INTO v_alert_interval
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ALERT_INTERVAL_HOURS';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_alert_interval := 4; -- Default: alert max every 4 hours
    END;

    -- Check last alert time
    BEGIN
        SELECT MAX(execution_start) INTO v_last_alert_time
        FROM cmr.dwh_ilm_execution_log
        WHERE policy_name = 'FAILURE_ALERT'
        AND action_type = 'NOTIFY';

        -- If alert was sent recently, skip
        IF v_last_alert_time IS NOT NULL AND
           v_last_alert_time > SYSTIMESTAMP - INTERVAL '1' HOUR * v_alert_interval THEN
            DBMS_OUTPUT.PUT_LINE('Alert was sent recently (' ||
                TO_CHAR(v_last_alert_time, 'YYYY-MM-DD HH24:MI:SS') ||
                '), skipping');
            RETURN;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_last_alert_time := NULL;
    END;

    -- Count recent failures
    SELECT COUNT(*) INTO v_failure_count
    FROM cmr.dwh_ilm_execution_log
    WHERE status = 'FAILED'
    AND execution_start > SYSTIMESTAMP - INTERVAL '1' HOUR * p_hours_back;

    -- Check if threshold exceeded
    IF v_failure_count >= v_threshold THEN

        -- Build alert message
        DBMS_LOB.CREATETEMPORARY(v_alert_message, TRUE);

        DBMS_LOB.APPEND(v_alert_message,
            'ILM Failure Alert' || CHR(10) ||
            '=================' || CHR(10) || CHR(10) ||
            'Database: ' || SYS_CONTEXT('USERENV', 'DB_NAME') || CHR(10) ||
            'Instance: ' || SYS_CONTEXT('USERENV', 'INSTANCE_NAME') || CHR(10) ||
            'Time: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) ||
            'Period: Last ' || p_hours_back || ' hours' || CHR(10) ||
            'Failure Count: ' || v_failure_count || ' (threshold: ' || v_threshold || ')' || CHR(10) || CHR(10) ||
            'Recent Failures:' || CHR(10) ||
            '----------------' || CHR(10)
        );

        -- Add failure details
        FOR rec IN c_recent_failures LOOP
            DBMS_LOB.APPEND(v_alert_message,
                CHR(10) || 'Execution ID: ' || rec.execution_id || CHR(10) ||
                '  Policy: ' || NVL(rec.policy_name, 'N/A') || CHR(10) ||
                '  Partition: ' || NVL(rec.partition_name, 'N/A') || CHR(10) ||
                '  Action: ' || NVL(rec.action_type, 'N/A') || CHR(10) ||
                '  Time: ' || TO_CHAR(rec.execution_start, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) ||
                '  Error: ' || SUBSTR(rec.error_message, 1, 200) || CHR(10)
            );
        END LOOP;

        DBMS_LOB.APPEND(v_alert_message,
            CHR(10) || CHR(10) ||
            'Recommended Actions:' || CHR(10) ||
            '-------------------' || CHR(10) ||
            '1. Review execution logs: SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = ''FAILED''' || CHR(10) ||
            '2. Check job history: SELECT * FROM v_ilm_job_history WHERE status != ''SUCCEEDED''' || CHR(10) ||
            '3. Verify scheduler jobs: SELECT * FROM v_ilm_scheduler_status' || CHR(10) ||
            '4. Review operations runbook: docs/operations/OPERATIONS_RUNBOOK.md' || CHR(10)
        );

        -- Send alert
        dwh_send_ilm_alert(
            p_alert_type => 'FAILURE',
            p_subject => v_failure_count || ' ILM failures in last ' || p_hours_back || ' hours',
            p_message => v_alert_message
        );

        -- Log that alert was sent
        INSERT INTO cmr.dwh_ilm_execution_log (
            policy_id, policy_name, action_type,
            execution_start, status, error_message
        ) VALUES (
            -1, 'FAILURE_ALERT', 'NOTIFY',
            SYSTIMESTAMP, 'SUCCESS',
            'Sent alert for ' || v_failure_count || ' failures'
        );
        COMMIT;

        DBMS_LOB.FREETEMPORARY(v_alert_message);

    ELSE
        DBMS_OUTPUT.PUT_LINE('No alert needed: ' || v_failure_count ||
            ' failures (threshold: ' || v_threshold || ')');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR in dwh_check_ilm_failures: ' || SQLERRM);
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- Procedure: Send Job Failure Notification
-- Purpose: Called by scheduler when a job fails (via notification attributes)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_notify_job_failure(
    p_job_name VARCHAR2,
    p_error_message VARCHAR2
) AS
    v_alert_message CLOB;
    v_recent_runs NUMBER;
BEGIN
    -- Build alert message
    DBMS_LOB.CREATETEMPORARY(v_alert_message, TRUE);

    -- Get recent run count
    BEGIN
        SELECT COUNT(*) INTO v_recent_runs
        FROM user_scheduler_job_run_details
        WHERE job_name = p_job_name
        AND log_date > SYSTIMESTAMP - INTERVAL '1' DAY;
    EXCEPTION
        WHEN OTHERS THEN
            v_recent_runs := 0;
    END;

    DBMS_LOB.APPEND(v_alert_message,
        'ILM Scheduler Job Failure' || CHR(10) ||
        '=========================' || CHR(10) || CHR(10) ||
        'Database: ' || SYS_CONTEXT('USERENV', 'DB_NAME') || CHR(10) ||
        'Instance: ' || SYS_CONTEXT('USERENV', 'INSTANCE_NAME') || CHR(10) ||
        'Time: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) ||
        'Job Name: ' || p_job_name || CHR(10) ||
        'Recent Runs (24h): ' || v_recent_runs || CHR(10) || CHR(10) ||
        'Error Message:' || CHR(10) ||
        '--------------' || CHR(10) ||
        SUBSTR(p_error_message, 1, 1000) || CHR(10) || CHR(10) ||
        'Recommended Actions:' || CHR(10) ||
        '-------------------' || CHR(10) ||
        '1. Check job history: SELECT * FROM v_ilm_job_history WHERE job_name = ''' || p_job_name || '''' || CHR(10) ||
        '2. Review scheduler status: SELECT * FROM v_ilm_scheduler_status' || CHR(10) ||
        '3. Check execution logs: SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = ''FAILED''' || CHR(10) ||
        '4. If persistent, disable job: EXEC DBMS_SCHEDULER.DISABLE(''' || p_job_name || ''')' || CHR(10)
    );

    -- Send alert
    dwh_send_ilm_alert(
        p_alert_type => 'JOB FAILURE',
        p_subject => 'Scheduler job failed: ' || p_job_name,
        p_message => v_alert_message
    );

    DBMS_LOB.FREETEMPORARY(v_alert_message);

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR in dwh_notify_job_failure: ' || SQLERRM);
END;
/


-- =============================================================================
-- SECTION 7: SCHEDULER PROGRAM FOR FAILURE MONITORING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Program: Check for Failures
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_CHECK_FAILURES',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            BEGIN
                dwh_check_ilm_failures(p_hours_back => 24);
            END;',
        enabled => TRUE,
        comments => 'Monitors for ILM failures and sends alert emails'
    );
END;
/


-- -----------------------------------------------------------------------------
-- Job: Monitor Failures (Every 4 hours)
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_JOB_MONITOR_FAILURES',
        program_name => 'ILM_CHECK_FAILURES',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=4',
        enabled => FALSE, -- Disabled by default, enable after email config
        comments => 'Monitor for ILM failures every 4 hours and send alerts if threshold exceeded'
    );
END;
/


-- =============================================================================
-- SECTION 8: VERIFICATION AND STATUS
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
