-- =============================================================================
-- ILM Framework Test Suite Runner
-- =============================================================================
-- Description: Main test runner script that executes all tests
-- Usage: @test_suite_runner.sql
-- Prerequisites: ILM framework must be installed
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF
SET VERIFY OFF
SET LINESIZE 200
SET PAGESIZE 1000

-- Test Results Variables
VAR g_total_tests NUMBER
VAR g_passed_tests NUMBER
VAR g_failed_tests NUMBER
VAR g_test_run_id NUMBER

-- Test Results Table (create if not exists)
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE cmr.dwh_test_results (
        test_run_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        test_category VARCHAR2(50),
        test_name VARCHAR2(200),
        test_status VARCHAR2(20),
        error_message VARCHAR2(4000),
        execution_time_ms NUMBER,
        test_date TIMESTAMP DEFAULT SYSTIMESTAMP
    )';
    DBMS_OUTPUT.PUT_LINE('Test results table created successfully');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN  -- Table already exists
            DBMS_OUTPUT.PUT_LINE('Test results table already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Test Runner Procedure
CREATE OR REPLACE PROCEDURE run_test(
    p_category VARCHAR2,
    p_test_name VARCHAR2,
    p_test_sql VARCHAR2,
    p_expected_result VARCHAR2 DEFAULT NULL
) AS
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration_ms NUMBER;
    v_actual_result VARCHAR2(4000);
    v_status VARCHAR2(20);
    v_error VARCHAR2(4000);
BEGIN
    v_start_time := SYSTIMESTAMP;

    BEGIN
        -- Execute test SQL
        EXECUTE IMMEDIATE p_test_sql INTO v_actual_result;

        -- Check result if expected result provided
        IF p_expected_result IS NOT NULL THEN
            IF v_actual_result = p_expected_result THEN
                v_status := 'PASSED';
            ELSE
                v_status := 'FAILED';
                v_error := 'Expected: ' || p_expected_result || ', Got: ' || v_actual_result;
            END IF;
        ELSE
            v_status := 'PASSED';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            v_status := 'FAILED';
            v_error := SQLERRM;
    END;

    v_end_time := SYSTIMESTAMP;
    v_duration_ms := EXTRACT(SECOND FROM (v_end_time - v_start_time)) * 1000;

    -- Log result
    INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
    VALUES (p_category, p_test_name, v_status, v_error, v_duration_ms);

    COMMIT;

    -- Output result
    DBMS_OUTPUT.PUT_LINE('  [' || v_status || '] ' || p_test_name ||
                        ' (' || ROUND(v_duration_ms, 2) || 'ms)');
    IF v_status = 'FAILED' THEN
        DBMS_OUTPUT.PUT_LINE('    Error: ' || v_error);
    END IF;

END;
/

-- Main Test Suite Runner
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration_sec NUMBER;
    v_passed NUMBER := 0;
    v_failed NUMBER := 0;
    v_total NUMBER := 0;
BEGIN
    v_start_time := SYSTIMESTAMP;

    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('ILM FRAMEWORK TEST SUITE');
    DBMS_OUTPUT.PUT_LINE('Started: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Setup Test Data
    DBMS_OUTPUT.PUT_LINE('Setting up test data...');
    @test_data_setup.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Unit Tests - Policy Validation
    DBMS_OUTPUT.PUT_LINE('--- UNIT TESTS: Policy Validation ---');
    @unit_tests_policy_validation.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Unit Tests - Partition Tracking
    DBMS_OUTPUT.PUT_LINE('--- UNIT TESTS: Partition Tracking ---');
    @unit_tests_partition_tracking.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Unit Tests - Policy Evaluation
    DBMS_OUTPUT.PUT_LINE('--- UNIT TESTS: Policy Evaluation ---');
    @unit_tests_policy_evaluation.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Unit Tests - Threshold Profiles
    DBMS_OUTPUT.PUT_LINE('--- UNIT TESTS: Threshold Profiles ---');
    @unit_tests_threshold_profiles.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Integration Tests
    DBMS_OUTPUT.PUT_LINE('--- INTEGRATION TESTS ---');
    @integration_tests_ilm_workflow.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Run Regression Tests
    DBMS_OUTPUT.PUT_LINE('--- REGRESSION TESTS ---');
    @regression_tests.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Cleanup Test Data
    DBMS_OUTPUT.PUT_LINE('Cleaning up test data...');
    @test_cleanup.sql
    DBMS_OUTPUT.PUT_LINE('');

    -- Calculate Summary
    SELECT COUNT(*) INTO v_total
    FROM cmr.dwh_test_results
    WHERE test_date > v_start_time;

    SELECT COUNT(*) INTO v_passed
    FROM cmr.dwh_test_results
    WHERE test_date > v_start_time
    AND test_status = 'PASSED';

    v_failed := v_total - v_passed;

    v_end_time := SYSTIMESTAMP;
    v_duration_sec := EXTRACT(SECOND FROM (v_end_time - v_start_time)) +
                     (EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60);

    -- Print Summary
    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('TEST SUITE SUMMARY');
    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('Total Tests:   ' || v_total);
    DBMS_OUTPUT.PUT_LINE('Passed:        ' || v_passed || ' (' || ROUND((v_passed/v_total)*100, 1) || '%)');
    DBMS_OUTPUT.PUT_LINE('Failed:        ' || v_failed || ' (' || ROUND((v_failed/v_total)*100, 1) || '%)');
    DBMS_OUTPUT.PUT_LINE('Duration:      ' || ROUND(v_duration_sec, 2) || ' seconds');
    DBMS_OUTPUT.PUT_LINE('=============================================================================');

    IF v_failed = 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ ALL TESTS PASSED!');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ ' || v_failed || ' TEST(S) FAILED');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Failed Tests:');
        FOR rec IN (
            SELECT test_category, test_name, error_message
            FROM cmr.dwh_test_results
            WHERE test_date > v_start_time
            AND test_status = 'FAILED'
            ORDER BY test_category, test_name
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  - [' || rec.test_category || '] ' || rec.test_name);
            DBMS_OUTPUT.PUT_LINE('    ' || rec.error_message);
        END LOOP;
    END IF;

    DBMS_OUTPUT.PUT_LINE('=============================================================================');

END;
/

-- Cleanup test runner procedure
DROP PROCEDURE run_test;
