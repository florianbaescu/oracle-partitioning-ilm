-- =============================================================================
-- Unit Tests: Policy Validation
-- =============================================================================
-- Description: Tests for ILM policy validation logic
-- =============================================================================

DECLARE
    v_test_passed BOOLEAN;
    v_error_msg VARCHAR2(4000);
    v_policy_id NUMBER;
    v_validation_count NUMBER;
BEGIN

    -- Test 1: Valid compression policy should be accepted
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_VALID_COMPRESS', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N'
        ) RETURNING policy_id INTO v_policy_id;

        v_test_passed := TRUE;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Valid compression policy accepted', 'PASSED', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [PASSED] Valid compression policy accepted');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_VALIDATION', 'Valid compression policy accepted', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Valid compression policy accepted: ' || SQLERRM);
    END;

    -- Test 2: Invalid table name should be rejected
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_INVALID_TABLE', 'CMR', 'NONEXISTENT_TABLE',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'Y'
        );
        -- Should not reach here
        ROLLBACK;
        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Invalid table name rejected', 'FAILED', 'Policy was accepted when it should have been rejected', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid table name rejected: Policy was accepted');
    EXCEPTION
        WHEN OTHERS THEN
            -- Expected to fail
            ROLLBACK;
            IF SQLERRM LIKE '%table%not exist%' OR SQLERRM LIKE '%table%not found%' OR SQLERRM LIKE '%ORA-20001%' THEN
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Invalid table name rejected', 'PASSED', 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [PASSED] Invalid table name rejected');
            ELSE
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Invalid table name rejected', 'FAILED', 'Unexpected error: ' || SQLERRM, 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid table name rejected: ' || SQLERRM);
            END IF;
    END;

    -- Test 3: Invalid compression type should be rejected
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_INVALID_COMPRESS', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'INVALID_TYPE', 100, 'Y'
        );
        -- Should not reach here
        ROLLBACK;
        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Invalid compression type rejected', 'FAILED', 'Invalid compression type was accepted', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid compression type rejected: Type was accepted');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            IF SQLERRM LIKE '%compression%' OR SQLERRM LIKE '%ORA-20002%' THEN
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Invalid compression type rejected', 'PASSED', 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [PASSED] Invalid compression type rejected');
            ELSE
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Invalid compression type rejected', 'FAILED', 'Unexpected error: ' || SQLERRM, 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid compression type rejected: ' || SQLERRM);
            END IF;
    END;

    -- Test 4: Negative age_days should be rejected
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_NEGATIVE_AGE', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', -10,
            'QUERY LOW', 100, 'Y'
        );
        -- Should not reach here
        ROLLBACK;
        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Negative age_days rejected', 'FAILED', 'Negative age was accepted', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [FAILED] Negative age_days rejected: Negative value was accepted');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            IF SQLERRM LIKE '%age%' OR SQLERRM LIKE '%ORA-20003%' OR SQLERRM LIKE '%check constraint%' THEN
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Negative age_days rejected', 'PASSED', 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [PASSED] Negative age_days rejected');
            ELSE
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                VALUES ('POLICY_VALIDATION', 'Negative age_days rejected', 'FAILED', 'Unexpected error: ' || SQLERRM, 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [FAILED] Negative age_days rejected: ' || SQLERRM);
            END IF;
    END;

    -- Test 5: Valid tiering policy should be accepted
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            target_tablespace, priority, enabled
        ) VALUES (
            'TEST_VALID_TIERING', 'CMR', 'TEST_SALES_FACT',
            'TIERING', 'MOVE', 365,
            'USERS', 100, 'N'
        ) RETURNING policy_id INTO v_policy_id;

        v_test_passed := TRUE;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Valid tiering policy accepted', 'PASSED', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [PASSED] Valid tiering policy accepted');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_VALIDATION', 'Valid tiering policy accepted', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Valid tiering policy accepted: ' || SQLERRM);
    END;

    -- Test 6: Policy with both age_days and age_months should be valid
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days, age_months,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_AGE_DAYS_MONTHS', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 365, 12,
            'QUERY HIGH', 100, 'N'
        ) RETURNING policy_id INTO v_policy_id;

        v_test_passed := TRUE;
        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
        VALUES ('POLICY_VALIDATION', 'Policy with age_days and age_months accepted', 'PASSED', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [PASSED] Policy with age_days and age_months accepted');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_VALIDATION', 'Policy with age_days and age_months accepted', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Policy with age_days and age_months accepted: ' || SQLERRM);
    END;

    -- Test 7: Duplicate policy name should be rejected
    BEGIN
        -- Insert first policy
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled
        ) VALUES (
            'TEST_DUPLICATE_NAME', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N'
        ) RETURNING policy_id INTO v_policy_id;

        -- Try to insert duplicate
        BEGIN
            INSERT INTO cmr.dwh_ilm_policies (
                policy_name, table_owner, table_name,
                policy_type, action_type, age_days,
                compression_type, priority, enabled
            ) VALUES (
                'TEST_DUPLICATE_NAME', 'CMR', 'TEST_CUSTOMER_DIM',
                'COMPRESSION', 'COMPRESS', 180,
                'QUERY HIGH', 90, 'N'
            );
            -- Should not reach here
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_VALIDATION', 'Duplicate policy name rejected', 'FAILED', 'Duplicate name was accepted', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Duplicate policy name rejected: Duplicate was accepted');
        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK;
                IF SQLCODE = -1 OR SQLERRM LIKE '%unique%' THEN
                    INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
                    VALUES ('POLICY_VALIDATION', 'Duplicate policy name rejected', 'PASSED', 0);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE('  [PASSED] Duplicate policy name rejected');
                ELSE
                    INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                    VALUES ('POLICY_VALIDATION', 'Duplicate policy name rejected', 'FAILED', 'Unexpected error: ' || SQLERRM, 0);
                    COMMIT;
                    DBMS_OUTPUT.PUT_LINE('  [FAILED] Duplicate policy name rejected: ' || SQLERRM);
                END IF;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('POLICY_VALIDATION', 'Duplicate policy name rejected', 'FAILED', 'Setup error: ' || SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Duplicate policy name rejected setup: ' || SQLERRM);
    END;

END;
/
