-- =============================================================================
-- Unit Tests: Partition Tracking
-- =============================================================================

DECLARE
    v_tracking_count NUMBER;
    v_temperature VARCHAR2(20);
BEGIN

    -- Test 1: Partition tracking refresh should populate data
    BEGIN
        dwh_refresh_partition_access_tracking('CMR', 'TEST_SALES_FACT');

        SELECT COUNT(*) INTO v_tracking_count
        FROM cmr.dwh_ilm_partition_access
        WHERE table_owner = 'CMR'
        AND table_name = 'TEST_SALES_FACT';

        IF v_tracking_count >= 12 THEN  -- Should have 12 partitions
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Tracking refresh populates data', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Tracking refresh populates data (' || v_tracking_count || ' partitions)');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Tracking refresh populates data', 'FAILED', 'Expected >=12 partitions, got ' || v_tracking_count, 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Tracking refresh: expected >=12, got ' || v_tracking_count);
        END IF;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Tracking refresh populates data', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Tracking refresh: ' || SQLERRM);
    END;

    -- Test 2: Temperature calculation should be valid
    BEGIN
        SELECT DISTINCT temperature INTO v_temperature
        FROM cmr.dwh_ilm_partition_access
        WHERE table_owner = 'CMR'
        AND table_name = 'TEST_SALES_FACT'
        AND ROWNUM = 1;

        IF v_temperature IN ('HOT', 'WARM', 'COLD') THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Temperature calculation valid', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Temperature calculation valid');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Temperature calculation valid', 'FAILED', 'Invalid temperature: ' || v_temperature, 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Invalid temperature: ' || v_temperature);
        END IF;
        COMMIT;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Temperature calculation valid', 'FAILED', 'No tracking data found', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] No tracking data found');
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Temperature calculation valid', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Temperature calc: ' || SQLERRM);
    END;

    -- Test 3: Size tracking should capture partition sizes
    BEGIN
        SELECT COUNT(*) INTO v_tracking_count
        FROM cmr.dwh_ilm_partition_access
        WHERE table_owner = 'CMR'
        AND table_name = 'TEST_SALES_FACT'
        AND size_mb > 0;

        IF v_tracking_count > 0 THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Size tracking captures data', 'PASSED', 0);
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Size tracking captures data');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Size tracking captures data', 'FAILED', 'No partitions with size > 0', 0);
            DBMS_OUTPUT.PUT_LINE('  [FAILED] No partitions with size > 0');
        END IF;
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('PARTITION_TRACKING', 'Size tracking captures data', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Size tracking: ' || SQLERRM);
    END;

END;
/
