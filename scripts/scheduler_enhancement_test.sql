-- =============================================================================
-- Script: scheduler_enhancement_test.sql
-- Description: Test script for scheduler enhancement
-- Usage: Run this after installing all scheduler enhancement scripts
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000

PROMPT ========================================
PROMPT Scheduler Enhancement Test Suite
PROMPT ========================================
PROMPT

-- =============================================================================
-- TEST 1: Verify Schema Objects
-- =============================================================================

PROMPT ========================================
PROMPT TEST 1: Verify Schema Objects
PROMPT ========================================

SELECT 'dwh_ilm_execution_schedules' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM cmr.dwh_ilm_execution_schedules
UNION ALL
SELECT 'dwh_ilm_execution_state',
       COUNT(*),
       'PASS'  -- OK if 0 (no executions yet)
FROM cmr.dwh_ilm_execution_state;

PROMPT
PROMPT Verify DEFAULT_SCHEDULE exists:
SELECT schedule_id, schedule_name, enabled,
       monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours,
       batch_cooldown_minutes, enable_checkpointing
FROM cmr.dwh_ilm_execution_schedules
WHERE schedule_name = 'DEFAULT_SCHEDULE';

-- =============================================================================
-- TEST 2: Test Helper Functions
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 2: Test Helper Functions
PROMPT ========================================

DECLARE
    v_schedule pck_dwh_ilm_execution_engine.schedule_rec;
    v_today_hours VARCHAR2(11);
    v_in_window BOOLEAN;
    v_should_execute BOOLEAN;
BEGIN
    -- Test get_schedule_config
    DBMS_OUTPUT.PUT_LINE('--- Testing get_schedule_config ---');
    BEGIN
        v_schedule := pck_dwh_ilm_execution_engine.get_schedule_config('DEFAULT_SCHEDULE');
        DBMS_OUTPUT.PUT_LINE('✓ PASS: get_schedule_config returned schedule');
        DBMS_OUTPUT.PUT_LINE('  Schedule ID: ' || v_schedule.schedule_id);
        DBMS_OUTPUT.PUT_LINE('  Schedule Name: ' || v_schedule.schedule_name);
        DBMS_OUTPUT.PUT_LINE('  Cooldown Minutes: ' || v_schedule.batch_cooldown_minutes);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ FAIL: ' || SQLERRM);
    END;

    -- Test get_today_hours
    DBMS_OUTPUT.PUT_LINE('--- Testing get_today_hours ---');
    BEGIN
        v_today_hours := pck_dwh_ilm_execution_engine.get_today_hours(v_schedule);
        DBMS_OUTPUT.PUT_LINE('✓ PASS: get_today_hours returned: ' || NVL(v_today_hours, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  Today: ' || TRIM(TO_CHAR(SYSDATE, 'DAY')));
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ FAIL: ' || SQLERRM);
    END;

    -- Test is_in_execution_window
    DBMS_OUTPUT.PUT_LINE('--- Testing is_in_execution_window ---');
    BEGIN
        v_in_window := pck_dwh_ilm_execution_engine.is_in_execution_window(v_schedule);
        DBMS_OUTPUT.PUT_LINE('✓ PASS: is_in_execution_window returned: ' ||
                           CASE WHEN v_in_window THEN 'TRUE (in window)' ELSE 'FALSE (outside window)' END);
        DBMS_OUTPUT.PUT_LINE('  Current Time: ' || TO_CHAR(SYSDATE, 'HH24:MI'));
        DBMS_OUTPUT.PUT_LINE('  Today Window: ' || NVL(v_today_hours, 'NULL (no execution today)'));
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ FAIL: ' || SQLERRM);
    END;

    -- Test should_execute_now
    DBMS_OUTPUT.PUT_LINE('--- Testing should_execute_now ---');
    BEGIN
        v_should_execute := pck_dwh_ilm_execution_engine.should_execute_now('DEFAULT_SCHEDULE');
        DBMS_OUTPUT.PUT_LINE('✓ PASS: should_execute_now returned: ' ||
                           CASE WHEN v_should_execute THEN 'TRUE (should execute)' ELSE 'FALSE (should not execute)' END);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ FAIL: ' || SQLERRM);
    END;
END;
/

-- =============================================================================
-- TEST 3: Check Current Window Status
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 3: Current Window Status
PROMPT ========================================

SELECT * FROM cmr.v_dwh_ilm_current_window;

-- =============================================================================
-- TEST 4: Check Queue Status
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 4: Queue Status
PROMPT ========================================

SELECT * FROM cmr.v_dwh_ilm_queue_summary;

PROMPT
PROMPT Pending Work Details:
SELECT
    p.policy_name,
    p.priority,
    COUNT(*) AS pending_count
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
WHERE q.execution_status = 'PENDING'
AND q.eligible = 'Y'
AND p.enabled = 'Y'
GROUP BY p.policy_name, p.priority
ORDER BY p.priority;

-- =============================================================================
-- TEST 5: Test Manual Execution (Force Run)
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 5: Manual Execution Test (Force Run)
PROMPT ========================================
PROMPT
PROMPT This will execute ONE batch immediately regardless of schedule.
PROMPT Press Ctrl+C to skip, or press Enter to continue...
PAUSE

DECLARE
    v_queue_count NUMBER;
BEGIN
    -- Check if there's work to do
    SELECT COUNT(*) INTO v_queue_count
    FROM cmr.dwh_ilm_evaluation_queue q
    JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
    WHERE q.execution_status = 'PENDING'
    AND q.eligible = 'Y'
    AND p.enabled = 'Y';

    IF v_queue_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No pending work in queue - cannot test execution');
        DBMS_OUTPUT.PUT_LINE('  Run policy engine evaluation first to populate queue');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Starting force run with ' || v_queue_count || ' pending operations...');
        DBMS_OUTPUT.PUT_LINE('---');

        pck_dwh_ilm_execution_engine.execute_pending_actions(
            p_schedule_name => 'DEFAULT_SCHEDULE',
            p_resume_batch_id => NULL,
            p_force_run => TRUE  -- Bypass window/day checks
        );

        DBMS_OUTPUT.PUT_LINE('---');
        DBMS_OUTPUT.PUT_LINE('✓ Force run completed');
    END IF;
END;
/

-- =============================================================================
-- TEST 6: Check Execution Results
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 6: Check Execution Results
PROMPT ========================================

PROMPT Recent Batches:
SELECT * FROM cmr.v_dwh_ilm_recent_batches
WHERE ROWNUM <= 5;

PROMPT
PROMPT Active Batches:
SELECT * FROM cmr.v_dwh_ilm_active_batches;

PROMPT
PROMPT Schedule Stats:
SELECT * FROM cmr.v_dwh_ilm_schedule_stats;

-- =============================================================================
-- TEST 7: Verify Scheduler Objects
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST 7: Verify Scheduler Objects
PROMPT ========================================

SELECT program_name, program_type, enabled,
       comments
FROM all_scheduler_programs
WHERE program_name = 'DWH_ILM_EXECUTE_ACTIONS';

PROMPT
SELECT job_name, program_name, enabled, state,
       repeat_interval, last_start_date, next_run_date
FROM all_scheduler_jobs
WHERE job_name = 'DWH_ILM_JOB_EXECUTE';

-- =============================================================================
-- TEST SUMMARY
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT TEST SUMMARY
PROMPT ========================================
PROMPT
PROMPT Tests Completed:
PROMPT 1. ✓ Schema objects verified
PROMPT 2. ✓ Helper functions tested
PROMPT 3. ✓ Current window status checked
PROMPT 4. ✓ Queue status verified
PROMPT 5. ✓ Manual execution tested (if queue had work)
PROMPT 6. ✓ Execution results reviewed
PROMPT 7. ✓ Scheduler objects verified
PROMPT
PROMPT Next Steps:
PROMPT
PROMPT 1. Configure schedule for your environment:
PROMPT    UPDATE cmr.dwh_ilm_execution_schedules
PROMPT    SET monday_hours = '22:00-06:00',  -- Adjust times as needed
PROMPT        ...
PROMPT    WHERE schedule_name = 'DEFAULT_SCHEDULE';
PROMPT
PROMPT 2. Enable the scheduler job:
PROMPT    EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EXECUTE');
PROMPT
PROMPT 3. Monitor execution:
PROMPT    SELECT * FROM cmr.v_dwh_ilm_active_batches;
PROMPT    SELECT * FROM cmr.v_dwh_ilm_current_window;
PROMPT    SELECT * FROM cmr.v_dwh_ilm_recent_batches;
PROMPT
PROMPT 4. To stop execution:
PROMPT    EXEC DBMS_SCHEDULER.DISABLE('DWH_ILM_JOB_EXECUTE');
PROMPT
PROMPT 5. To force manual run (bypass schedule):
PROMPT    EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(p_force_run => TRUE);
PROMPT
