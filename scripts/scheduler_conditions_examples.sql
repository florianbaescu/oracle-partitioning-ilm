-- =============================================================================
-- Script: scheduler_conditions_examples.sql
-- Description: Example schedule conditions for various use cases
-- Dependencies: scheduler_enhancement_setup.sql must be run first
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ============================================================================
PROMPT Schedule Conditions Framework - Examples
PROMPT ============================================================================
PROMPT
PROMPT This script provides working examples of schedule conditions.
PROMPT These examples demonstrate SQL, FUNCTION, and PLSQL condition types.
PROMPT
PROMPT Examples are commented out by default. Uncomment to test.
PROMPT ============================================================================

-- =============================================================================
-- EXAMPLE 1: CPU Threshold Check (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 1: CPU Threshold - Only run if CPU < 70%
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'CPU_THRESHOLD_70PCT',
    'SQL',
    'SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
     FROM v$sysmetric
     WHERE metric_name = ''CPU Usage Per Sec''
     AND group_id = 2',  -- Last 60 seconds
    1,
    'AND',
    'Y',
    'Y',
    'Only run if average CPU usage < 70% over last minute'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 2: Memory Threshold Check (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 2: Memory Threshold - Only run if SGA free memory > 20%
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'MEMORY_THRESHOLD_20PCT',
    'SQL',
    'SELECT CASE
        WHEN (free_memory / total_memory * 100) > 20 THEN 1
        ELSE 0
     END
     FROM (
        SELECT
            (SELECT SUM(bytes) FROM v$sgastat WHERE name = ''free memory'') AS free_memory,
            (SELECT SUM(value) FROM v$sga) AS total_memory
        FROM DUAL
     )',
    2,
    'AND',
    'Y',
    'Y',
    'Only run if SGA free memory > 20%'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 3: Backup Coordination (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 3: Backup Coordination - Wait for backup completion
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'NO_BACKUP_RUNNING',
    'SQL',
    'SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
     FROM v$rman_backup_job_details
     WHERE start_time >= TRUNC(SYSDATE)
     AND (status = ''RUNNING'' OR end_time IS NULL)',
    3,
    'AND',
    'Y',
    'Y',
    'Only run if no RMAN backup is currently running today'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 4: Business Calendar Check (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 4: Business Calendar - Only run on business days
PROMPT

/*
-- First, create a company holidays table (example)
CREATE TABLE company_holidays (
    holiday_date DATE PRIMARY KEY,
    holiday_name VARCHAR2(100),
    holiday_type VARCHAR2(50)
);

-- Insert example holidays
INSERT INTO company_holidays VALUES (DATE '2025-01-01', 'New Year''s Day', 'PUBLIC');
INSERT INTO company_holidays VALUES (DATE '2025-12-25', 'Christmas Day', 'PUBLIC');
COMMIT;

-- Now create the condition
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'BUSINESS_DAY_CHECK',
    'SQL',
    'SELECT CASE
        WHEN TO_CHAR(SYSDATE, ''DY'') NOT IN (''SAT'', ''SUN'')
        AND NOT EXISTS (
            SELECT 1 FROM company_holidays
            WHERE holiday_date = TRUNC(SYSDATE)
        )
        THEN 1 ELSE 0 END
     FROM DUAL',
    4,
    'AND',
    'Y',
    'Y',
    'Only run on business days (weekdays excluding company holidays)'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 5: Queue Size Check (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 5: Queue Size - Only run if sufficient pending work
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'MIN_QUEUE_SIZE_100',
    'SQL',
    'SELECT CASE WHEN COUNT(*) >= 100 THEN 1 ELSE 0 END
     FROM cmr.dwh_ilm_evaluation_queue
     WHERE execution_status = ''PENDING''
     AND eligible = ''Y''',
    5,
    'AND',
    'Y',
    'Y',
    'Only run if at least 100 operations are pending (batch efficiency)'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 6: Tablespace Free Space Check (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 6: Tablespace Free Space - Ensure sufficient space
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'TABLESPACE_FREE_SPACE_20PCT',
    'SQL',
    'SELECT CASE WHEN MIN(pct_free) > 20 THEN 1 ELSE 0 END
     FROM (
        SELECT tablespace_name,
               100 - ROUND((SUM(bytes) / SUM(maxbytes)) * 100, 2) AS pct_free
        FROM dba_data_files
        WHERE tablespace_name IN (''USERS'', ''DATA'', ''INDEXES'')
        GROUP BY tablespace_name
     )',
    6,
    'AND',
    'Y',
    'Y',
    'Only run if all critical tablespaces have > 20% free space'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 7: Active Session Count (SQL Type)
-- =============================================================================

PROMPT
PROMPT Example 7: Active Session Count - Avoid running during peak load
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'ACTIVE_SESSIONS_BELOW_50',
    'SQL',
    'SELECT CASE WHEN COUNT(*) < 50 THEN 1 ELSE 0 END
     FROM v$session
     WHERE status = ''ACTIVE''
     AND username IS NOT NULL
     AND type = ''USER''',
    7,
    'AND',
    'Y',
    'Y',
    'Only run if fewer than 50 active user sessions'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 8: Custom Function (FUNCTION Type)
-- =============================================================================

PROMPT
PROMPT Example 8: Custom Function - Complex business logic
PROMPT

/*
-- First, create a custom function
CREATE OR REPLACE FUNCTION check_system_ready_for_ilm
RETURN BOOLEAN
AS
    v_cpu NUMBER;
    v_memory NUMBER;
    v_io NUMBER;
BEGIN
    -- Check CPU
    SELECT AVG(value) INTO v_cpu
    FROM v$sysmetric
    WHERE metric_name = 'CPU Usage Per Sec'
    AND group_id = 2;

    -- Check Memory
    SELECT AVG(value) INTO v_memory
    FROM v$sysmetric
    WHERE metric_name = 'Memory Usage'
    AND group_id = 2;

    -- Check I/O
    SELECT AVG(value) INTO v_io
    FROM v$sysmetric
    WHERE metric_name = 'Physical Read Total IO Requests Per Sec'
    AND group_id = 2;

    -- Return TRUE if all thresholds met
    RETURN (v_cpu < 70 AND v_memory < 80 AND v_io < 1000);
EXCEPTION
    WHEN OTHERS THEN
        RETURN FALSE;
END;
/

-- Now create the condition
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'SYSTEM_READY_CHECK',
    'FUNCTION',
    'RETURN check_system_ready_for_ilm()',
    8,
    'AND',
    'Y',
    'Y',
    'Check multiple system metrics using custom function'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 9: First Monday of Month Check (PLSQL Type)
-- =============================================================================

PROMPT
PROMPT Example 9: First Monday of Month - Complex date logic
PROMPT

/*
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'FIRST_MONDAY_OF_MONTH',
    'PLSQL',
    'DECLARE
        v_day_of_week VARCHAR2(10);
        v_day_of_month NUMBER;
    BEGIN
        v_day_of_week := TO_CHAR(SYSDATE, ''DY'');
        v_day_of_month := TO_NUMBER(TO_CHAR(SYSDATE, ''DD''));

        v_result := (v_day_of_week = ''MON'' AND v_day_of_month BETWEEN 1 AND 7);
        RETURN v_result;
    END;',
    9,
    'OR',  -- This is OR so it can run any day OR first Monday
    'Y',
    'N',  -- fail_on_error = N (not critical)
    'Special handling on first Monday of month'
);
COMMIT;
*/

-- =============================================================================
-- EXAMPLE 10: Multi-Condition with OR Logic (Complex)
-- =============================================================================

PROMPT
PROMPT Example 10: OR Logic - Run if EITHER low CPU OR off-peak hours
PROMPT

/*
-- Condition A: Low CPU (normal execution)
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'CONDITION_A_LOW_CPU',
    'SQL',
    'SELECT CASE WHEN AVG(value) < 50 THEN 1 ELSE 0 END
     FROM v$sysmetric
     WHERE metric_name = ''CPU Usage Per Sec''
     AND group_id = 2',
    10,
    'OR',  -- A OR B
    'Y',
    'Y',
    'Run if CPU is very low (< 50%)'
);

-- Condition B: Off-peak hours (late night)
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'CONDITION_B_OFF_PEAK',
    'SQL',
    'SELECT CASE
        WHEN TO_NUMBER(TO_CHAR(SYSDATE, ''HH24'')) >= 23 OR
             TO_NUMBER(TO_CHAR(SYSDATE, ''HH24'')) <= 5
        THEN 1 ELSE 0 END
     FROM DUAL',
    11,
    'AND',
    'Y',
    'Y',
    'Run during off-peak hours (11 PM - 5 AM)'
);

-- Logic: (A OR B) - Run if EITHER condition is true
COMMIT;
*/

-- =============================================================================
-- TESTING YOUR CONDITIONS
-- =============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Testing Schedule Conditions
PROMPT ============================================================================
PROMPT
PROMPT To test your conditions:
PROMPT
PROMPT 1. View all conditions:
PROMPT    SELECT * FROM cmr.v_dwh_schedule_conditions;
PROMPT
PROMPT 2. Check condition evaluation results:
PROMPT    SELECT schedule_name, condition_name, last_result_text, success_rate_pct
PROMPT    FROM cmr.v_dwh_schedule_conditions
PROMPT    ORDER BY evaluation_order;
PROMPT
PROMPT 3. Test evaluation function directly:
PROMPT    DECLARE
PROMPT        v_result BOOLEAN;
PROMPT    BEGIN
PROMPT        v_result := pck_dwh_ilm_execution_engine.evaluate_schedule_conditions(1);
PROMPT        DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result THEN 'TRUE' ELSE 'FALSE' END);
PROMPT    END;
PROMPT    /
PROMPT
PROMPT 4. Check schedule readiness:
PROMPT    SELECT * FROM cmr.v_dwh_schedule_readiness;
PROMPT
PROMPT 5. View failing conditions:
PROMPT    SELECT * FROM cmr.v_dwh_condition_failures;
PROMPT
PROMPT ============================================================================

-- =============================================================================
-- CLEANUP (if testing)
-- =============================================================================

PROMPT
PROMPT To remove all example conditions:
PROMPT    DELETE FROM cmr.dwh_schedule_conditions WHERE condition_name LIKE '%EXAMPLE%';
PROMPT    COMMIT;
PROMPT
