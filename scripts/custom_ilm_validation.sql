-- =============================================================================
-- ILM Policy Validation Utilities
-- =============================================================================
-- Description: Optional validation and testing procedures for ILM policies
-- Prerequisites:
--   - custom_ilm_setup.sql must be run first
--   - custom_ilm_policy_engine.sql must be run first
-- Usage: @custom_ilm_validation.sql
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

-- -----------------------------------------------------------------------------
-- Procedure: Validate ILM Policy Configuration
-- -----------------------------------------------------------------------------
-- Description: Standalone procedure to validate an existing ILM policy
-- Tests policy configuration and eligibility evaluation
-- Usage: EXEC dwh_validate_ilm_policy(policy_id);
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_validate_ilm_policy(
    p_policy_id NUMBER
) AS
    v_policy_name VARCHAR2(100);
    v_table_owner VARCHAR2(128);
    v_table_name VARCHAR2(128);
    v_enabled CHAR(1);
    v_table_exists NUMBER;
    v_eligible_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('ILM Policy Validation');
    DBMS_OUTPUT.PUT_LINE('===========================================');

    -- Get policy details
    SELECT policy_name, table_owner, table_name, enabled
    INTO v_policy_name, v_table_owner, v_table_name, v_enabled
    FROM cmr.dwh_ilm_policies
    WHERE policy_id = p_policy_id;

    DBMS_OUTPUT.PUT_LINE('Policy ID: ' || p_policy_id);
    DBMS_OUTPUT.PUT_LINE('Policy Name: ' || v_policy_name);
    DBMS_OUTPUT.PUT_LINE('Table: ' || v_table_owner || '.' || v_table_name);
    DBMS_OUTPUT.PUT_LINE('Enabled: ' || v_enabled);
    DBMS_OUTPUT.PUT_LINE('');

    -- Check 1: Verify table exists
    BEGIN
        SELECT COUNT(*)
        INTO v_table_exists
        FROM all_tables
        WHERE owner = v_table_owner
        AND table_name = v_table_name;

        IF v_table_exists = 1 THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] Table exists');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[FAIL] Table does not exist');
            RETURN;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[FAIL] Error checking table: ' || SQLERRM);
            RETURN;
    END;

    -- Check 2: Test eligibility evaluation
    BEGIN
        pck_dwh_ilm_policy_engine.evaluate_policy(p_policy_id);

        SELECT COUNT(*) INTO v_eligible_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = p_policy_id
        AND eligible = 'Y'
        AND evaluation_date > SYSTIMESTAMP - INTERVAL '1' MINUTE;

        DBMS_OUTPUT.PUT_LINE('[PASS] Policy evaluation successful');
        DBMS_OUTPUT.PUT_LINE('      Eligible partitions found: ' || v_eligible_count);

        -- Clean up test evaluation
        DELETE FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = p_policy_id
        AND evaluation_date > SYSTIMESTAMP - INTERVAL '1' MINUTE;
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('[FAIL] Policy evaluation error: ' || SQLERRM);
            ROLLBACK;
            RETURN;
    END;

    -- Check 3: Verify policy configuration
    DECLARE
        v_age_days NUMBER;
        v_age_months NUMBER;
        v_compression_type VARCHAR2(30);
        v_action_type VARCHAR2(20);
    BEGIN
        SELECT age_days, age_months, compression_type, action_type
        INTO v_age_days, v_age_months, v_compression_type, v_action_type
        FROM cmr.dwh_ilm_policies
        WHERE policy_id = p_policy_id;

        IF v_age_days IS NULL AND v_age_months IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] No age criteria specified (age_days and age_months are NULL)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('[PASS] Age criteria defined');
        END IF;

        IF v_action_type = 'COMPRESS' AND v_compression_type IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('[WARN] Action is COMPRESS but compression_type is NULL');
        ELSIF v_action_type = 'COMPRESS' THEN
            DBMS_OUTPUT.PUT_LINE('[PASS] Compression type specified: ' || v_compression_type);
        END IF;
    END;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Validation Complete');
    DBMS_OUTPUT.PUT_LINE('===========================================');

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Policy ID ' || p_policy_id || ' not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
END dwh_validate_ilm_policy;
/

-- Note: Procedure validates an existing ILM policy by testing evaluation and checking configuration

SHOW ERRORS

PROMPT
PROMPT ========================================
PROMPT ILM Validation Utilities Installed
PROMPT ========================================
PROMPT
PROMPT Available Procedures:
PROMPT   - dwh_validate_ilm_policy(policy_id) : Validate policy configuration and eligibility
PROMPT
PROMPT Example Usage:
PROMPT   EXEC dwh_validate_ilm_policy(1);
PROMPT
