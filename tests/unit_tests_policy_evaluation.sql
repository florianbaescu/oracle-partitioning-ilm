-- =============================================================================
-- Unit Tests: Policy Evaluation
-- =============================================================================

DECLARE
    v_policy_id NUMBER;
    v_queue_count NUMBER;
BEGIN

    -- Test 1: Evaluate policies should identify eligible partitions
    BEGIN
        -- Create test policy (old partitions should be compressed)
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_EVAL_COMPRESS_OLD', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 30,  -- Partitions older than 30 days
            'QUERY LOW', 100, 'Y'
        ) RETURNING policy_id INTO v_policy_id;
        COMMIT;

        -- Run evaluation
        dwh_evaluate_policies_for_queue();

        -- Check if eligible partitions were queued
        SELECT COUNT(*) INTO v_queue_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = v_policy_id
        AND eligible = 'Y';

        IF v_queue_count > 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Evaluate identifies eligible partitions', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Policy evaluation found ' || v_queue_count || ' eligible partitions');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Evaluate identifies eligible partitions', 'FAILED', 'No eligible partitions found', 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] No eligible partitions found');
        END IF;

        -- Cleanup
        DELETE FROM cmr.dwh_ilm_evaluation_queue WHERE policy_id = v_policy_id;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Evaluate identifies eligible partitions', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Policy evaluation: ' || SQLERRM);
    END;

    -- Test 2: Disabled policies should not be evaluated
    BEGIN
        -- Create disabled policy
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_EVAL_DISABLED', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 30,
            'QUERY LOW', 100, 'N'  -- Disabled
        ) RETURNING policy_id INTO v_policy_id;
        COMMIT;

        -- Run evaluation
        dwh_evaluate_policies_for_queue();

        -- Check that no partitions were queued for disabled policy
        SELECT COUNT(*) INTO v_queue_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = v_policy_id;

        IF v_queue_count = 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Disabled policies not evaluated', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Disabled policies not evaluated');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Disabled policies not evaluated', 'FAILED', 'Found ' || v_queue_count || ' queued items for disabled policy', 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Disabled policy was evaluated');
        END IF;

        -- Cleanup
        DELETE FROM cmr.dwh_ilm_evaluation_queue WHERE policy_id = v_policy_id;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_EVALUATION', 'Disabled policies not evaluated', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Disabled policy test: ' || SQLERRM);
    END;

END;
/
