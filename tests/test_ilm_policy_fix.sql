-- =============================================================================
-- Test Script: Verify ILM Policy Creation Fix for Tiered Templates
-- =============================================================================
--
-- This script tests the fix for apply_ilm_policies to ensure it correctly
-- creates ILM policies for both tiered and non-tiered templates.
--
-- Background:
--   - Bug: Tiered templates have JSON structure {"tier_config":{...}, "policies":[...]}
--   - Previous code used JSON_TABLE path '$[*]' which only works for non-tiered
--   - This caused zero policies to be created for tiered templates
--   - Fix: Use UNION ALL with both '$.policies[*]' and '$[*]' paths
--
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200

PROMPT ========================================
PROMPT Test: ILM Policy Creation Fix
PROMPT ========================================
PROMPT

-- =============================================================================
-- Step 1: Clean up existing policies for test table
-- =============================================================================
PROMPT Step 1: Cleaning up existing test data...

DELETE FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y';

COMMIT;

SELECT COUNT(*) as policies_before_test
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y';

PROMPT Expected: 0 policies
PROMPT

-- =============================================================================
-- Step 2: Manually call apply_ilm_policies for TEST_SALES_3Y
-- =============================================================================
PROMPT Step 2: Applying ILM policies using FACT_TABLE_STANDARD_TIERED template...
PROMPT

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get task ID for TEST_SALES_3Y
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Call apply_ilm_policies
    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Task not found. Run tests/test_tiered_partitioning.sql first.');
        RAISE;
END;
/

PROMPT

-- =============================================================================
-- Step 3: Verify policies were created
-- =============================================================================
PROMPT Step 3: Verifying policies were created...
PROMPT

SELECT COUNT(*) as policies_created
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y';

PROMPT Expected: 2 policies (TEST_SALES_3Y_TIER_WARM, TEST_SALES_3Y_TIER_COLD)
PROMPT

-- Show policy details
PROMPT Policy Details:
SELECT
    policy_name,
    policy_type,
    action_type,
    age_months,
    target_tablespace,
    compression_type,
    enabled,
    priority
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y'
ORDER BY priority;

PROMPT
PROMPT Expected policies:
PROMPT   1. TEST_SALES_3Y_TIER_WARM: MOVE to TBS_WARM at 24 months, BASIC compression
PROMPT   2. TEST_SALES_3Y_TIER_COLD: MOVE to TBS_COLD at 60 months, OLTP compression
PROMPT

-- =============================================================================
-- Step 4: Validate ILM policies
-- =============================================================================
PROMPT Step 4: Running ILM policy validation...
PROMPT

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get task ID
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1;

    -- Call validate_ilm_policies
    cmr.pck_dwh_table_migration_executor.validate_ilm_policies(v_task_id);
END;
/

PROMPT

-- =============================================================================
-- Step 5: Check validation results in execution log
-- =============================================================================
PROMPT Step 5: Checking validation results...
PROMPT

SELECT
    step_number,
    step_name,
    status,
    error_message,
    TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS') as execution_time
FROM cmr.dwh_migration_execution_log
WHERE task_id = (
    SELECT task_id FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1
)
AND step_name = 'Validate ILM Policies'
ORDER BY step_number DESC
FETCH FIRST 1 ROW ONLY;

PROMPT Expected: status = 'SUCCESS' (not 'FAILED')
PROMPT

-- Show validation details
PROMPT Validation Details (last run):
SELECT
    DBMS_LOB.SUBSTR(sql_statement, 2000, 1) as validation_report
FROM cmr.dwh_migration_execution_log
WHERE task_id = (
    SELECT task_id FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1
)
AND step_name = 'Validate ILM Policies'
ORDER BY step_number DESC
FETCH FIRST 1 ROW ONLY;

PROMPT

-- =============================================================================
-- Step 6: Test non-tiered template (backward compatibility)
-- =============================================================================
PROMPT ========================================
PROMPT Step 6: Testing Non-Tiered Template
PROMPT ========================================
PROMPT

-- Clean up for non-tiered test
DELETE FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y';
COMMIT;

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get task ID for non-tiered test
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test Non-Tiered'
    AND ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Testing non-tiered template: FACT_TABLE_STANDARD');
    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Call apply_ilm_policies
    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Note: Non-tiered test task not found (optional test)');
        DBMS_OUTPUT.PUT_LINE('      Run tests/test_tiered_partitioning.sql to create it');
END;
/

PROMPT
PROMPT Non-tiered template policies:
SELECT
    policy_name,
    policy_type,
    action_type,
    COALESCE(TO_CHAR(age_months), TO_CHAR(age_days)) as age,
    target_tablespace,
    compression_type
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y'
ORDER BY priority;

PROMPT

-- =============================================================================
-- Summary
-- =============================================================================
PROMPT ========================================
PROMPT Test Summary
PROMPT ========================================
PROMPT
PROMPT Tests completed:
PROMPT   1. Tiered template (FACT_TABLE_STANDARD_TIERED) - Should create 2 policies
PROMPT   2. Validation should pass (not fail with "No ILM policies found")
PROMPT   3. Non-tiered template (FACT_TABLE_STANDARD) - Should create policies
PROMPT
PROMPT If all tests passed:
PROMPT   - Tiered templates now correctly parse policies from $.policies[*]
PROMPT   - Non-tiered templates continue to work with $[*]
PROMPT   - Fix is backward compatible
PROMPT
PROMPT ========================================
