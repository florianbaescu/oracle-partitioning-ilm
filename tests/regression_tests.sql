-- =============================================================================
-- Regression Tests: Bug Fixes and Edge Cases
-- =============================================================================

DECLARE
    v_count NUMBER;
    v_config_value VARCHAR2(200);
BEGIN

    -- Test 1: SQLERRM not used in DML (Bug fixed in v1.0)
    BEGIN
        -- Test that analyze_table handles errors properly without SQLERRM in DML
        -- This is a regression test for ORA-00904 error
        DECLARE
            v_task_id NUMBER;
        BEGIN
            -- Create a migration task for test table
            INSERT INTO cmr.dwh_migration_tasks (
                project_id, table_owner, table_name, analysis_status
            ) VALUES (
                1, 'CMR', 'TEST_SALES_FACT', 'PENDING'
            ) RETURNING task_id INTO v_task_id;
            COMMIT;

            -- Run analysis (should not fail with SQLERRM in DML)
            dwh_analyze_table(v_task_id);

            -- If we get here, test passed
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('REGRESSION', 'SQLERRM not used in DML statements', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] SQLERRM not in DML');

            -- Cleanup
            DELETE FROM cmr.dwh_migration_analysis WHERE task_id = v_task_id;
            DELETE FROM cmr.dwh_migration_tasks WHERE task_id = v_task_id;
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -904 THEN
                    -- ORA-00904 = SQLERRM used in DML
                    ROLLBACK;
                    INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                    VALUES ('REGRESSION', 'SQLERRM not used in DML statements', 'FAILED', 'ORA-00904: SQLERRM used in DML', 0);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE('  [FAILED] SQLERRM in DML detected');
                ELSE
                    RAISE;
                END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'SQLERRM not used in DML statements', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] SQLERRM test: ' || SQLERRM);
    END;

    -- Test 2: Config table independence (Bug fixed in v1.0)
    BEGIN
        -- Verify that config table exists and can be queried
        SELECT COUNT(*) INTO v_count
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'HOT_THRESHOLD_DAYS';

        IF v_count > 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('REGRESSION', 'Config table accessible', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Config table accessible');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'Config table accessible', 'FAILED', 'Config key not found', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Config key not found');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'Config table accessible', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Config table: ' || SQLERRM);
    END;

    -- Test 3: Parallel degree applied correctly (Bug fixed in v2.0)
    BEGIN
        -- Get parallel degree config
        SELECT config_value INTO v_config_value
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'DEFAULT_PARALLEL_DEGREE';

        IF v_config_value IS NOT NULL AND TO_NUMBER(v_config_value) > 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('REGRESSION', 'Parallel degree configuration valid', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Parallel degree config valid');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'Parallel degree configuration valid', 'FAILED', 'Invalid parallel degree: ' || v_config_value, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid parallel degree');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'Parallel degree configuration valid', 'FAILED', 'Config key not found', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Parallel degree config not found');
        WHEN OTHERS THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('REGRESSION', 'Parallel degree configuration valid', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Parallel degree test: ' || SQLERRM);
    END;

END;
/
