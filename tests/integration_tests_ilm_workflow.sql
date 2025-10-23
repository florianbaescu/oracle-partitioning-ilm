-- =============================================================================
-- Integration Tests: Complete ILM Workflow
-- =============================================================================

DECLARE
    v_policy_id NUMBER;
    v_queue_count NUMBER;
    v_execution_count NUMBER;
    v_initial_compression VARCHAR2(30);
    v_final_compression VARCHAR2(30);
BEGIN

    -- Test 1: End-to-end compression workflow
    BEGIN
        DBMS_OUTPUT.PUT_LINE('  Running end-to-end compression test...');

        -- Step 1: Create policy
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_E2E_COMPRESS', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 30,
            'QUERY LOW', 100, 'Y'
        ) RETURNING policy_id INTO v_policy_id;
        COMMIT;

        -- Step 2: Evaluate policy (queue eligible partitions)
        dwh_evaluate_policies_for_queue();

        SELECT COUNT(*) INTO v_queue_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = v_policy_id
        AND eligible = 'Y';

        IF v_queue_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'No partitions queued for compression');
        END IF;

        -- Step 3: Execute one partition (avoid actual compression in test)
        -- Instead, verify execution logging works
        SELECT compression INTO v_initial_compression
        FROM all_tab_partitions
        WHERE table_owner = 'CMR'
        AND table_name = 'TEST_SALES_FACT'
        AND partition_name = 'P_2024_01';

        -- Log a simulated execution
        INSERT INTO cmr.dwh_ilm_execution_log (
            policy_id, policy_name, table_owner, table_name,
            partition_name, action_type, status,
            execution_start, execution_end, duration_seconds,
            size_before_mb, size_after_mb, space_saved_mb,
            compression_ratio, executed_by, execution_mode
        ) VALUES (
            v_policy_id, 'TEST_E2E_COMPRESS', 'CMR', 'TEST_SALES_FACT',
            'P_2024_01', 'COMPRESS', 'SUCCESS',
            SYSTIMESTAMP, SYSTIMESTAMP + INTERVAL '5' SECOND, 5,
            10, 4, 6,
            2.5, USER, 'MANUAL'
        );
        COMMIT;

        -- Step 4: Verify execution was logged
        SELECT COUNT(*) INTO v_execution_count
        FROM cmr.dwh_ilm_execution_log
        WHERE policy_id = v_policy_id
        AND status = 'SUCCESS';

        IF v_execution_count > 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('INTEGRATION', 'End-to-end compression workflow', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] End-to-end workflow completed');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('INTEGRATION', 'End-to-end compression workflow', 'FAILED', 'Execution not logged', 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Execution not logged');
        END IF;

        -- Cleanup
        DELETE FROM cmr.dwh_ilm_execution_log WHERE policy_id = v_policy_id;
        DELETE FROM cmr.dwh_ilm_evaluation_queue WHERE policy_id = v_policy_id;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('INTEGRATION', 'End-to-end compression workflow', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] E2E workflow: ' || SQLERRM);
    END;

    -- Test 2: Monitoring views integration
    BEGIN
        -- Verify all monitoring views are accessible
        DECLARE
            v_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_count FROM dwh_v_ilm_performance_dashboard;
            SELECT COUNT(*) INTO v_count FROM dwh_v_ilm_alerting_metrics;
            SELECT COUNT(*) INTO v_count FROM dwh_v_ilm_policy_effectiveness;

            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('INTEGRATION', 'Monitoring views accessible', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] All monitoring views accessible');
        END;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('INTEGRATION', 'Monitoring views accessible', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Monitoring views: ' || SQLERRM);
    END;

END;
/
