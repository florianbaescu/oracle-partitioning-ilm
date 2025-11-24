-- =============================================================================
-- Script: scheduler_enhancement_scheduler.sql
-- Description: Phase 3 - Create DBMS_SCHEDULER program and job
-- Dependencies: scheduler_enhancement_engine.sql must be run first
-- =============================================================================

PROMPT ========================================
PROMPT Phase 3: Creating Scheduler Objects
PROMPT ========================================

-- =============================================================================
-- Drop Old Scheduler Objects (if they exist)
-- =============================================================================

BEGIN
    DBMS_SCHEDULER.DROP_JOB(job_name => 'DWH_ILM_JOB_EXECUTE', force => TRUE);
    DBMS_OUTPUT.PUT_LINE('✓ Dropped old job: DWH_ILM_JOB_EXECUTE');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -27475 THEN  -- Object does not exist
            DBMS_OUTPUT.PUT_LINE('  (Old job DWH_ILM_JOB_EXECUTE does not exist)');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM(program_name => 'DWH_ILM_EXECUTE_ACTIONS', force => TRUE);
    DBMS_OUTPUT.PUT_LINE('✓ Dropped old program: DWH_ILM_EXECUTE_ACTIONS');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -27476 THEN  -- Object does not exist
            DBMS_OUTPUT.PUT_LINE('  (Old program DWH_ILM_EXECUTE_ACTIONS does not exist)');
        ELSE
            RAISE;
        END IF;
END;
/

-- =============================================================================
-- Create New Scheduler Program (Enhanced with Schedule Check)
-- =============================================================================

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_EXECUTE_ACTIONS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            DECLARE
                v_should_run BOOLEAN;
            BEGIN
                -- Enable DBMS_OUTPUT for logging
                DBMS_OUTPUT.ENABLE(1000000);

                -- Check if should run based on schedule config
                v_should_run := pck_dwh_ilm_execution_engine.should_execute_now(
                    p_schedule_name => ''DEFAULT_SCHEDULE''
                );

                IF v_should_run THEN
                    DBMS_OUTPUT.PUT_LINE(''['' || TO_CHAR(SYSTIMESTAMP, ''HH24:MI:SS'') || ''] Starting ILM execution...'');

                    pck_dwh_ilm_execution_engine.execute_pending_actions(
                        p_schedule_name => ''DEFAULT_SCHEDULE'',
                        p_resume_batch_id => NULL,
                        p_force_run => FALSE
                    );

                    DBMS_OUTPUT.PUT_LINE(''['' || TO_CHAR(SYSTIMESTAMP, ''HH24:MI:SS'') || ''] ILM execution completed'');
                ELSE
                    DBMS_OUTPUT.PUT_LINE(''['' || TO_CHAR(SYSTIMESTAMP, ''HH24:MI:SS'') || ''] Not scheduled to run at this time'');
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE(''[ERROR] '' || SQLERRM);
                    RAISE;
            END;',
        enabled => TRUE,
        comments => 'ILM Execution Engine - Checks schedule config and runs continuous execution if appropriate'
    );

    DBMS_OUTPUT.PUT_LINE('✓ Created program: DWH_ILM_EXECUTE_ACTIONS');
END;
/

-- =============================================================================
-- Create New Scheduler Job (Hourly Check)
-- =============================================================================

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_EXECUTE',
        program_name => 'DWH_ILM_EXECUTE_ACTIONS',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=1',  -- Check every hour
        enabled => FALSE,  -- Start disabled, enable manually after testing
        auto_drop => FALSE,
        comments => 'ILM Execution Job - Checks hourly if execution should start based on schedule configuration'
    );

    DBMS_OUTPUT.PUT_LINE('✓ Created job: DWH_ILM_JOB_EXECUTE (DISABLED - enable manually after testing)');
END;
/

-- =============================================================================
-- Summary and Instructions
-- =============================================================================

PROMPT ========================================
PROMPT Scheduler Objects Created!
PROMPT ========================================
PROMPT
PROMPT Program: DWH_ILM_EXECUTE_ACTIONS
PROMPT   - Checks if should run based on schedule config
PROMPT   - Runs continuous execution loop if appropriate
PROMPT
PROMPT Job: DWH_ILM_JOB_EXECUTE
PROMPT   - Status: DISABLED (for safety)
PROMPT   - Frequency: Every hour (FREQ=HOURLY; INTERVAL=1)
PROMPT   - Action: Calls should_execute_now() then execute_pending_actions()
PROMPT
PROMPT How It Works:
PROMPT 1. Job wakes every hour
PROMPT 2. Checks: Is there running batch? Is there work? Are we in window?
PROMPT 3. If YES to all: Starts continuous execution (LOOP until queue empty or window closes)
PROMPT 4. If NO: Exits immediately (waits for next hourly check)
PROMPT
PROMPT To Enable the Job:
PROMPT   EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EXECUTE');
PROMPT
PROMPT To Disable the Job:
PROMPT   EXEC DBMS_SCHEDULER.DISABLE('DWH_ILM_JOB_EXECUTE');
PROMPT
PROMPT To Run Manually (for testing):
PROMPT   EXEC DBMS_SCHEDULER.RUN_JOB('DWH_ILM_JOB_EXECUTE', use_current_session => TRUE);
PROMPT
PROMPT Or call directly:
PROMPT   EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(p_force_run => TRUE);
PROMPT
PROMPT Next: Run scheduler_enhancement_monitoring.sql to create monitoring views
PROMPT
