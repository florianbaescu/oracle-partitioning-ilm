-- =============================================================================
-- Unit Tests: Threshold Profiles
-- =============================================================================
-- Description: Tests for ILM threshold profiles feature
-- =============================================================================

DECLARE
    v_test_passed BOOLEAN;
    v_error_msg VARCHAR2(4000);
    v_profile_id NUMBER;
    v_policy_id NUMBER;
    v_threshold NUMBER;
    v_count NUMBER;
    v_profile_name VARCHAR2(100);
BEGIN

    DBMS_OUTPUT.PUT_LINE('--- UNIT TESTS: Threshold Profiles ---');

    -- Test 1: Create custom threshold profile
    BEGIN
        v_test_passed := FALSE;
        INSERT INTO cmr.dwh_ilm_threshold_profiles (
            profile_name, description,
            hot_threshold_days, warm_threshold_days, cold_threshold_days
        ) VALUES (
            'TEST_CUSTOM_PROFILE', 'Test profile for unit tests',
            45, 180, 540
        ) RETURNING profile_id INTO v_profile_id;

        v_test_passed := TRUE;
        DELETE FROM cmr.dwh_ilm_threshold_profiles WHERE profile_id = v_profile_id;
        COMMIT;

        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
        VALUES ('THRESHOLD_PROFILES', 'Create custom threshold profile', 'PASSED', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [PASSED] Create custom threshold profile');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Create custom threshold profile', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Create custom threshold profile: ' || SQLERRM);
    END;

    -- Test 2: Policy with NULL profile uses global config
    BEGIN
        v_test_passed := FALSE;

        -- Create policy with NULL profile
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled,
            threshold_profile_id
        ) VALUES (
            'TEST_POLICY_NULL_PROFILE', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N',
            NULL  -- No profile, should use global config
        ) RETURNING policy_id INTO v_policy_id;

        -- Verify get_policy_thresholds returns global config values
        v_threshold := get_policy_thresholds(v_policy_id, 'HOT');

        IF v_threshold = 90 THEN  -- Global config default
            v_test_passed := TRUE;
        END IF;

        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        IF v_test_passed THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'NULL profile uses global config', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] NULL profile uses global config');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'NULL profile uses global config', 'FAILED',
                   'Expected HOT threshold 90, got ' || v_threshold, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] NULL profile uses global config');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'NULL profile uses global config', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] NULL profile uses global config: ' || SQLERRM);
    END;

    -- Test 3: Policy with profile uses profile values
    BEGIN
        v_test_passed := FALSE;

        -- Get FAST_AGING profile ID
        SELECT profile_id INTO v_profile_id
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name = 'FAST_AGING';

        -- Create policy with FAST_AGING profile
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled,
            threshold_profile_id
        ) VALUES (
            'TEST_POLICY_WITH_PROFILE', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N',
            v_profile_id
        ) RETURNING policy_id INTO v_policy_id;

        -- Verify get_policy_thresholds returns FAST_AGING values (30/90/180)
        v_threshold := get_policy_thresholds(v_policy_id, 'HOT');

        IF v_threshold = 30 THEN  -- FAST_AGING HOT threshold
            v_test_passed := TRUE;
        END IF;

        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        IF v_test_passed THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Policy with profile uses profile values', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Policy with profile uses profile values');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Policy with profile uses profile values', 'FAILED',
                   'Expected HOT threshold 30 (FAST_AGING), got ' || v_threshold, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Policy with profile uses profile values');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Policy with profile uses profile values', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Policy with profile uses profile values: ' || SQLERRM);
    END;

    -- Test 4: Profile constraint validation (hot < warm < cold)
    BEGIN
        v_test_passed := FALSE;

        -- Try to insert profile with invalid thresholds (warm < hot)
        INSERT INTO cmr.dwh_ilm_threshold_profiles (
            profile_name, description,
            hot_threshold_days, warm_threshold_days, cold_threshold_days
        ) VALUES (
            'TEST_INVALID_THRESHOLDS', 'Should fail constraint',
            90, 30, 180  -- INVALID: warm (30) < hot (90)
        );

        -- Should not reach here
        ROLLBACK;
        INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
        VALUES ('THRESHOLD_PROFILES', 'Profile constraint validation', 'FAILED',
               'Invalid thresholds were accepted', 0);
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  [FAILED] Profile constraint validation');
    EXCEPTION
        WHEN OTHERS THEN
            -- Expected to fail
            ROLLBACK;
            IF SQLERRM LIKE '%chk_profile_thresholds%' OR SQLERRM LIKE '%check constraint%' THEN
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
                VALUES ('THRESHOLD_PROFILES', 'Profile constraint validation', 'PASSED', 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [PASSED] Profile constraint validation');
            ELSE
                INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
                VALUES ('THRESHOLD_PROFILES', 'Profile constraint validation', 'FAILED',
                       'Unexpected error: ' || SQLERRM, 0);
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('  [FAILED] Profile constraint validation: ' || SQLERRM);
            END IF;
    END;

    -- Test 5: get_policy_thresholds function returns all threshold types
    BEGIN
        v_test_passed := FALSE;

        -- Get SLOW_AGING profile ID
        SELECT profile_id INTO v_profile_id
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name = 'SLOW_AGING';

        -- Create policy with SLOW_AGING profile
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled,
            threshold_profile_id
        ) VALUES (
            'TEST_POLICY_THRESHOLDS', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N',
            v_profile_id
        ) RETURNING policy_id INTO v_policy_id;

        -- Verify all three threshold types (SLOW_AGING: 180/730/1825)
        IF get_policy_thresholds(v_policy_id, 'HOT') = 180 AND
           get_policy_thresholds(v_policy_id, 'WARM') = 730 AND
           get_policy_thresholds(v_policy_id, 'COLD') = 1825 THEN
            v_test_passed := TRUE;
        END IF;

        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        IF v_test_passed THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'get_policy_thresholds returns all types', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] get_policy_thresholds returns all types');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'get_policy_thresholds returns all types', 'FAILED',
                   'Thresholds do not match SLOW_AGING profile', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] get_policy_thresholds returns all types');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'get_policy_thresholds returns all types', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] get_policy_thresholds returns all types: ' || SQLERRM);
    END;

    -- Test 6: View shows effective thresholds correctly
    BEGIN
        v_test_passed := FALSE;

        -- Get AGGRESSIVE_ARCHIVE profile ID
        SELECT profile_id INTO v_profile_id
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name = 'AGGRESSIVE_ARCHIVE';

        -- Create policy with AGGRESSIVE_ARCHIVE profile
        INSERT INTO cmr.dwh_ilm_policies (
            policy_name, table_owner, table_name,
            policy_type, action_type, age_days,
            compression_type, priority, enabled,
            threshold_profile_id
        ) VALUES (
            'TEST_POLICY_VIEW', 'CMR', 'TEST_SALES_FACT',
            'COMPRESSION', 'COMPRESS', 90,
            'QUERY LOW', 100, 'N',
            v_profile_id
        ) RETURNING policy_id INTO v_policy_id;

        -- Query view and verify effective thresholds
        SELECT profile_name, effective_hot_threshold_days, threshold_source
        INTO v_profile_name, v_threshold, v_error_msg
        FROM cmr.dwh_v_ilm_policy_thresholds
        WHERE policy_id = v_policy_id;

        IF v_profile_name = 'AGGRESSIVE_ARCHIVE' AND
           v_threshold = 14 AND
           v_error_msg = 'CUSTOM' THEN
            v_test_passed := TRUE;
        END IF;

        DELETE FROM cmr.dwh_ilm_policies WHERE policy_id = v_policy_id;
        COMMIT;

        IF v_test_passed THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'View shows effective thresholds', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] View shows effective thresholds');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'View shows effective thresholds', 'FAILED',
                   'View data incorrect: profile=' || v_profile_name || ', threshold=' || v_threshold || ', source=' || v_error_msg, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] View shows effective thresholds');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'View shows effective thresholds', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] View shows effective thresholds: ' || SQLERRM);
    END;

    -- Test 7: Default profiles exist and have correct values
    BEGIN
        v_test_passed := FALSE;

        -- Count default profiles
        SELECT COUNT(*) INTO v_count
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name IN ('DEFAULT', 'FAST_AGING', 'SLOW_AGING', 'AGGRESSIVE_ARCHIVE');

        IF v_count = 4 THEN
            -- Verify DEFAULT profile values (90/365/1095)
            SELECT COUNT(*) INTO v_count
            FROM cmr.dwh_ilm_threshold_profiles
            WHERE profile_name = 'DEFAULT'
            AND hot_threshold_days = 90
            AND warm_threshold_days = 365
            AND cold_threshold_days = 1095;

            IF v_count = 1 THEN
                v_test_passed := TRUE;
            END IF;
        END IF;

        IF v_test_passed THEN
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Default profiles exist with correct values', 'PASSED', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [PASSED] Default profiles exist with correct values');
        ELSE
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Default profiles exist with correct values', 'FAILED',
                   'Missing or incorrect default profiles', 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Default profiles exist with correct values');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
            VALUES ('THRESHOLD_PROFILES', 'Default profiles exist with correct values', 'FAILED', SQLERRM, 0);
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  [FAILED] Default profiles exist with correct values: ' || SQLERRM);
    END;

END;
/
