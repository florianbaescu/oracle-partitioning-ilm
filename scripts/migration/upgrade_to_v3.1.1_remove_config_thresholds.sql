-- =============================================================================
-- Migration Script: Upgrade to v3.1.1 - Remove Config Thresholds
-- =============================================================================
-- Removes redundant HOT/WARM/COLD thresholds from dwh_ilm_config
-- Uses DEFAULT threshold profile as single source of truth
-- Safe to run multiple times (idempotent)
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Upgrading to v3.1.1: Remove Config Thresholds
PROMPT ========================================
PROMPT

-- Step 1: Verify DEFAULT profile exists
PROMPT Step 1: Verifying DEFAULT threshold profile exists...

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'DEFAULT';

    IF v_count = 1 THEN
        DBMS_OUTPUT.PUT_LINE('✓ DEFAULT profile exists');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ DEFAULT profile not found - please install threshold profiles first');
        DBMS_OUTPUT.PUT_LINE('  Run: @scripts/migration/upgrade_to_v3.1_threshold_profiles.sql');
        RAISE_APPLICATION_ERROR(-20001, 'DEFAULT threshold profile required');
    END IF;
END;
/

-- Step 2: Remove redundant config entries
PROMPT Step 2: Removing redundant threshold config entries...

BEGIN
    DELETE FROM cmr.dwh_ilm_config
    WHERE config_key IN ('HOT_THRESHOLD_DAYS', 'WARM_THRESHOLD_DAYS', 'COLD_THRESHOLD_DAYS');

    DBMS_OUTPUT.PUT_LINE('✓ Removed ' || SQL%ROWCOUNT || ' config entries');
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/

-- Step 3: Recreate get_policy_thresholds function
PROMPT Step 3: Updating get_policy_thresholds function...

CREATE OR REPLACE FUNCTION get_policy_thresholds(
    p_policy_id NUMBER,
    p_threshold_type VARCHAR2 -- 'HOT', 'WARM', or 'COLD'
) RETURN NUMBER
AS
    v_profile_id NUMBER;
    v_threshold NUMBER;
BEGIN
    -- Get profile_id for this policy
    SELECT threshold_profile_id INTO v_profile_id
    FROM cmr.dwh_ilm_policies
    WHERE policy_id = p_policy_id;

    IF v_profile_id IS NOT NULL THEN
        -- Use specific profile thresholds
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_id = v_profile_id;
    ELSE
        -- Use DEFAULT profile as global fallback
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name = 'DEFAULT';
    END IF;

    RETURN v_threshold;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Fallback to DEFAULT profile if policy not found
        BEGIN
            SELECT
                CASE p_threshold_type
                    WHEN 'HOT' THEN hot_threshold_days
                    WHEN 'WARM' THEN warm_threshold_days
                    WHEN 'COLD' THEN cold_threshold_days
                END INTO v_threshold
            FROM cmr.dwh_ilm_threshold_profiles
            WHERE profile_name = 'DEFAULT';
            RETURN v_threshold;
        EXCEPTION
            WHEN OTHERS THEN
                -- Hardcoded fallback if DEFAULT profile missing
                RETURN CASE p_threshold_type
                    WHEN 'HOT' THEN 90
                    WHEN 'WARM' THEN 365
                    WHEN 'COLD' THEN 1095
                END;
        END;
    WHEN OTHERS THEN
        -- Hardcoded fallback for any other errors
        RETURN CASE p_threshold_type
            WHEN 'HOT' THEN 90
            WHEN 'WARM' THEN 365
            WHEN 'COLD' THEN 1095
        END;
END get_policy_thresholds;
/

DBMS_OUTPUT.PUT_LINE('✓ Updated function get_policy_thresholds');

-- Step 4: Recreate affected views
PROMPT Step 4: Updating views to use DEFAULT profile...

-- Note: Full view recreation is included in main setup script
-- This migration just ensures the function is updated
-- Views will be automatically updated on next setup script run

DBMS_OUTPUT.PUT_LINE('✓ Views will use DEFAULT profile through updated function');

PROMPT
PROMPT ========================================
PROMPT Migration to v3.1.1 Complete!
PROMPT ========================================
PROMPT
PROMPT Summary:
PROMPT   - Removed HOT/WARM/COLD_THRESHOLD_DAYS from dwh_ilm_config
PROMPT   - Updated get_policy_thresholds() to use DEFAULT profile
PROMPT   - DEFAULT profile is now single source of truth for thresholds
PROMPT
PROMPT Benefits:
PROMPT   - Eliminated redundancy (single source of truth)
PROMPT   - Users can modify DEFAULT profile to change global thresholds
PROMPT   - Cleaner architecture
PROMPT
PROMPT Threshold Management:
PROMPT   View current DEFAULT thresholds:
PROMPT     SELECT * FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT';
PROMPT
PROMPT   Modify DEFAULT thresholds:
PROMPT     UPDATE cmr.dwh_ilm_threshold_profiles
PROMPT     SET hot_threshold_days = X, warm_threshold_days = Y, cold_threshold_days = Z
PROMPT     WHERE profile_name = 'DEFAULT';
PROMPT
