-- =============================================================================
-- Script: cleanup_old_scheduler_objects.sql
-- Description: Drop old/renamed scheduler objects (rerunnable, safe)
-- Location: tmp/ folder (temporary cleanup script)
-- Dependencies: None
--
-- Purpose: Clean up old scheduler objects that existed BEFORE migration
-- Safe to run multiple times - no errors if objects don't exist
--
-- IMPORTANT: This drops the ORIGINAL objects, not the _old backup tables
-- created by the migration. The _old tables should be kept for rollback.
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ============================================================================
PROMPT Cleanup Old Scheduler Objects
PROMPT ============================================================================
PROMPT
PROMPT This script will drop old/renamed scheduler objects that existed BEFORE
PROMPT the migration to the new generalized schema:
PROMPT
PROMPT   1. Old tables (if migration hasn't been run yet):
PROMPT      - dwh_ilm_execution_schedules
PROMPT      - dwh_ilm_execution_state
PROMPT
PROMPT   2. Old function:
PROMPT      - is_execution_window_open
PROMPT
PROMPT   3. Old monitoring views (v_dwh_ilm_*):
PROMPT      - v_dwh_ilm_active_batches
PROMPT      - v_dwh_ilm_schedule_stats
PROMPT      - v_dwh_ilm_batch_progress
PROMPT      - v_dwh_ilm_current_window
PROMPT      - v_dwh_ilm_recent_batches
PROMPT      - v_dwh_ilm_queue_summary
PROMPT
PROMPT All drops are safe - no errors if objects don't exist
PROMPT
PROMPT NOTE: This script does NOT drop the *_old backup tables created
PROMPT       by the migration script. Those should be kept for rollback.
PROMPT
PROMPT ============================================================================

-- =============================================================================
-- SECTION 1: DROP OLD TABLES (ORIGINAL TABLES BEFORE MIGRATION)
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Section 1: Dropping old tables
PROMPT ========================================

-- Check if new tables exist first
DECLARE
    v_new_tables_exist NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_new_tables_exist
    FROM all_tables
    WHERE owner = 'CMR'
    AND table_name IN ('DWH_EXECUTION_SCHEDULES', 'DWH_EXECUTION_STATE');

    IF v_new_tables_exist = 2 THEN
        DBMS_OUTPUT.PUT_LINE('✓ New tables exist (dwh_execution_schedules, dwh_execution_state)');
        DBMS_OUTPUT.PUT_LINE('  Safe to drop old tables if they exist...');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: New tables do not exist yet!');
        DBMS_OUTPUT.PUT_LINE('  Run scheduler_enhancement_setup.sql first before dropping old tables.');
        DBMS_OUTPUT.PUT_LINE('');
    END IF;
END;
/

-- Drop old execution state table (child table first)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_execution_state CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: cmr.dwh_ilm_execution_state');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (Table cmr.dwh_ilm_execution_state does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping dwh_ilm_execution_state: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop old execution schedules table (parent table)
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_execution_schedules CASCADE CONSTRAINTS PURGE';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped table: cmr.dwh_ilm_execution_schedules');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (Table cmr.dwh_ilm_execution_schedules does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping dwh_ilm_execution_schedules: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- =============================================================================
-- SECTION 2: DROP OLD FUNCTION
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Section 2: Dropping old function
PROMPT ========================================

-- Drop old execution window function
BEGIN
    EXECUTE IMMEDIATE 'DROP FUNCTION is_execution_window_open';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped function: is_execution_window_open');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -4043 THEN  -- ORA-04043: object does not exist
            DBMS_OUTPUT.PUT_LINE('  (Function is_execution_window_open does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping is_execution_window_open: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- =============================================================================
-- SECTION 3: DROP OLD MONITORING VIEWS (v_dwh_ilm_*)
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Section 3: Dropping old monitoring views
PROMPT ========================================

-- Drop v_dwh_ilm_active_batches (renamed to v_dwh_active_batches)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_active_batches';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_active_batches');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_active_batches does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_active_batches: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop v_dwh_ilm_schedule_stats (renamed to v_dwh_schedule_stats)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_schedule_stats';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_schedule_stats');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_schedule_stats does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_schedule_stats: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop v_dwh_ilm_batch_progress (renamed to v_dwh_batch_progress)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_batch_progress';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_batch_progress');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_batch_progress does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_batch_progress: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop v_dwh_ilm_queue_summary (may still exist if created separately)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_queue_summary';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_queue_summary');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_queue_summary does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_queue_summary: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop v_dwh_ilm_current_window (renamed to v_dwh_current_window)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_current_window';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_current_window');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_current_window does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_current_window: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- Drop v_dwh_ilm_recent_batches (may exist from old installation)
BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_recent_batches';
    DBMS_OUTPUT.PUT_LINE('✓ Dropped view: cmr.v_dwh_ilm_recent_batches');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN  -- ORA-00942: table or view does not exist
            DBMS_OUTPUT.PUT_LINE('  (View cmr.v_dwh_ilm_recent_batches does not exist, skipping)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR dropping v_dwh_ilm_recent_batches: ' || SQLERRM);
            -- Don't raise, continue with other drops
        END IF;
END;
/

-- =============================================================================
-- SECTION 4: VERIFICATION - LIST REMAINING OLD OBJECTS
-- =============================================================================

PROMPT
PROMPT ========================================
PROMPT Section 4: Verification
PROMPT ========================================

-- Check for any remaining old scheduler objects
DECLARE
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Checking for remaining old objects:');
    DBMS_OUTPUT.PUT_LINE('');

    -- Check for old tables (original names)
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR'
    AND table_name IN ('DWH_ILM_EXECUTION_SCHEDULES', 'DWH_ILM_EXECUTION_STATE');

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Found ' || v_count || ' old tables still existing:');
        FOR rec IN (
            SELECT table_name
            FROM all_tables
            WHERE owner = 'CMR'
            AND table_name IN ('DWH_ILM_EXECUTION_SCHEDULES', 'DWH_ILM_EXECUTION_STATE')
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  - ' || rec.table_name);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  These tables should be dropped manually if new tables exist.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ No old tables found (original names)');
    END IF;

    -- Check for backup tables created by migration
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR'
    AND table_name IN ('DWH_ILM_EXECUTION_SCHEDULES_OLD', 'DWH_ILM_EXECUTION_STATE_OLD');

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('ℹ INFO: Found ' || v_count || ' backup tables (created by migration):');
        FOR rec IN (
            SELECT table_name
            FROM all_tables
            WHERE owner = 'CMR'
            AND table_name IN ('DWH_ILM_EXECUTION_SCHEDULES_OLD', 'DWH_ILM_EXECUTION_STATE_OLD')
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  - ' || rec.table_name);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  These backup tables are kept for rollback. Drop manually if no longer needed.');
    END IF;

    -- Check for new tables
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR'
    AND table_name IN ('DWH_EXECUTION_SCHEDULES', 'DWH_EXECUTION_STATE', 'DWH_SCHEDULE_CONDITIONS');

    IF v_count = 3 THEN
        DBMS_OUTPUT.PUT_LINE('✓ New tables exist (dwh_execution_schedules, dwh_execution_state, dwh_schedule_conditions)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Not all new tables exist (' || v_count || '/3 found)');
        DBMS_OUTPUT.PUT_LINE('  Run scheduler_enhancement_setup.sql to create new schema.');
    END IF;

    -- Check for old function
    SELECT COUNT(*) INTO v_count
    FROM all_objects
    WHERE owner = USER
    AND object_name = 'IS_EXECUTION_WINDOW_OPEN'
    AND object_type = 'FUNCTION';

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Function IS_EXECUTION_WINDOW_OPEN still exists');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ No old function found');
    END IF;

    -- Check for old views
    SELECT COUNT(*) INTO v_count
    FROM all_views
    WHERE owner = 'CMR'
    AND view_name LIKE 'V_DWH_ILM_%';

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Found ' || v_count || ' old views still existing:');
        FOR rec IN (
            SELECT view_name
            FROM all_views
            WHERE owner = 'CMR'
            AND view_name LIKE 'V_DWH_ILM_%'
            ORDER BY view_name
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  - ' || rec.view_name);
        END LOOP;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ No old views found');
    END IF;

    -- Check for new views
    SELECT COUNT(*) INTO v_count
    FROM all_views
    WHERE owner = 'CMR'
    AND view_name IN ('V_DWH_ACTIVE_BATCHES', 'V_DWH_SCHEDULE_STATS', 'V_DWH_BATCH_PROGRESS',
                      'V_DWH_CURRENT_WINDOW', 'V_DWH_QUEUE_SUMMARY', 'V_DWH_SCHEDULE_CONDITIONS',
                      'V_DWH_CONDITION_FAILURES', 'V_DWH_SCHEDULE_READINESS');

    IF v_count >= 8 THEN
        DBMS_OUTPUT.PUT_LINE('✓ New views exist (' || v_count || '/8 monitoring views found)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('ℹ INFO: Only ' || v_count || '/8 new views found');
    END IF;

    DBMS_OUTPUT.PUT_LINE('');
END;
/

-- =============================================================================
-- SUMMARY
-- =============================================================================

PROMPT
PROMPT ============================================================================
PROMPT Cleanup Complete!
PROMPT ============================================================================
PROMPT
PROMPT Objects Dropped (if they existed BEFORE migration):
PROMPT
PROMPT Tables (original names):
PROMPT   • cmr.dwh_ilm_execution_schedules
PROMPT   • cmr.dwh_ilm_execution_state
PROMPT
PROMPT Function:
PROMPT   • is_execution_window_open
PROMPT
PROMPT Views:
PROMPT   • cmr.v_dwh_ilm_active_batches
PROMPT   • cmr.v_dwh_ilm_schedule_stats
PROMPT   • cmr.v_dwh_ilm_batch_progress
PROMPT   • cmr.v_dwh_ilm_queue_summary
PROMPT   • cmr.v_dwh_ilm_current_window
PROMPT   • cmr.v_dwh_ilm_recent_batches
PROMPT
PROMPT Objects NOT Dropped (kept for rollback):
PROMPT   • cmr.dwh_ilm_execution_schedules_old (backup created by migration)
PROMPT   • cmr.dwh_ilm_execution_state_old (backup created by migration)
PROMPT
PROMPT New Objects (should exist after migration):
PROMPT   ✓ cmr.dwh_execution_schedules
PROMPT   ✓ cmr.dwh_execution_state
PROMPT   ✓ cmr.dwh_schedule_conditions
PROMPT   ✓ cmr.v_dwh_active_batches (and 7 other v_dwh_* views)
PROMPT
PROMPT This script can be safely re-run multiple times.
PROMPT
PROMPT ============================================================================
