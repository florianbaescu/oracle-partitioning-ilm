-- =============================================================================
-- Test Cleanup Script
-- =============================================================================
-- Description: Cleans up test tables, policies, and data
-- Usage: Called by test_suite_runner.sql
-- =============================================================================

DECLARE
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Cleaning up test data...');

    -- Drop test tables
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.test_sales_fact PURGE';
        DBMS_OUTPUT.PUT_LINE('  ✓ Dropped test_sales_fact');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN  -- Ignore table doesn't exist
                DBMS_OUTPUT.PUT_LINE('  ⚠ Error dropping test_sales_fact: ' || SQLERRM);
            END IF;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.test_customer_dim PURGE';
        DBMS_OUTPUT.PUT_LINE('  ✓ Dropped test_customer_dim');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN
                DBMS_OUTPUT.PUT_LINE('  ⚠ Error dropping test_customer_dim: ' || SQLERRM);
            END IF;
    END;

    -- Clean up partition tracking for test tables
    DELETE FROM cmr.dwh_ilm_partition_access
    WHERE table_owner = 'CMR'
    AND table_name IN ('TEST_SALES_FACT', 'TEST_CUSTOMER_DIM');

    SELECT SQL%ROWCOUNT INTO v_count;
    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  ✓ Removed ' || v_count || ' partition tracking records');
    END IF;

    -- Clean up any test policies that weren't cleaned up
    DELETE FROM cmr.dwh_ilm_policies
    WHERE policy_name LIKE 'TEST_%';

    SELECT SQL%ROWCOUNT INTO v_count;
    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  ✓ Removed ' || v_count || ' test policies');
    END IF;

    -- Clean up test evaluation queue entries
    DELETE FROM cmr.dwh_ilm_evaluation_queue
    WHERE policy_id IN (
        SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name LIKE 'TEST_%'
    );

    -- Clean up test execution log entries
    DELETE FROM cmr.dwh_ilm_execution_log
    WHERE policy_name LIKE 'TEST_%';

    -- Clean up test migration tasks
    DELETE FROM cmr.dwh_migration_analysis
    WHERE task_id IN (
        SELECT task_id FROM cmr.dwh_migration_tasks WHERE table_name LIKE 'TEST_%'
    );

    DELETE FROM cmr.dwh_migration_tasks
    WHERE table_name LIKE 'TEST_%';

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Test cleanup completed successfully');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR in test cleanup: ' || SQLERRM);
        -- Don't raise - allow test suite to complete
END;
/
