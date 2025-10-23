-- =============================================================================
-- Migration Script: Upgrade to v3.1 - Threshold Profiles
-- =============================================================================
-- Adds threshold profiles feature to existing ILM installation
-- Safe to run multiple times (idempotent)
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Upgrading to v3.1: Threshold Profiles
PROMPT ========================================
PROMPT

-- Step 1: Create threshold profiles table
PROMPT Step 1: Creating dwh_ilm_threshold_profiles table...

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_ilm_threshold_profiles (
            profile_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            profile_name        VARCHAR2(100) NOT NULL UNIQUE,
            description         VARCHAR2(500),
            hot_threshold_days  NUMBER NOT NULL,
            warm_threshold_days NUMBER NOT NULL,
            cold_threshold_days NUMBER NOT NULL,
            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
            modified_by         VARCHAR2(50),
            modified_date       TIMESTAMP,
            CONSTRAINT chk_profile_thresholds CHECK (
                hot_threshold_days < warm_threshold_days
                AND warm_threshold_days < cold_threshold_days
            )
        )';
    DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_ilm_threshold_profiles');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Table dwh_ilm_threshold_profiles already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 2: Create index
PROMPT Step 2: Creating indexes...

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_profiles_name ON cmr.dwh_ilm_threshold_profiles(profile_name)';
    DBMS_OUTPUT.PUT_LINE('✓ Created index idx_ilm_profiles_name');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Index idx_ilm_profiles_name already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 3: Insert default profiles
PROMPT Step 3: Inserting default threshold profiles...

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'DEFAULT' AS profile_name,
              'Standard aging profile (matches global config)' AS description,
              90 AS hot_threshold_days,
              365 AS warm_threshold_days,
              1095 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'FAST_AGING' AS profile_name,
              'Fast aging for transactional data (sales, orders)' AS description,
              30 AS hot_threshold_days,
              90 AS warm_threshold_days,
              180 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'SLOW_AGING' AS profile_name,
              'Slow aging for reference/master data' AS description,
              180 AS hot_threshold_days,
              730 AS warm_threshold_days,
              1825 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'AGGRESSIVE_ARCHIVE' AS profile_name,
              'Aggressive archival for high-volume data' AS description,
              14 AS hot_threshold_days,
              30 AS warm_threshold_days,
              90 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Inserted 4 default profiles');

-- Step 4: Alter policies table
PROMPT Step 4: Adding threshold_profile_id column to dwh_ilm_policies...

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM user_tab_columns
    WHERE table_name = 'DWH_ILM_POLICIES'
    AND column_name = 'THRESHOLD_PROFILE_ID';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_policies ADD (threshold_profile_id NUMBER)';
        DBMS_OUTPUT.PUT_LINE('✓ Added column threshold_profile_id');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Column threshold_profile_id already exists');
    END IF;
END;
/

-- Step 5: Create foreign key
PROMPT Step 5: Creating foreign key constraint...

BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_policies
                       ADD CONSTRAINT fk_ilm_policy_profile
                       FOREIGN KEY (threshold_profile_id)
                       REFERENCES cmr.dwh_ilm_threshold_profiles(profile_id)';
    DBMS_OUTPUT.PUT_LINE('✓ Created foreign key fk_ilm_policy_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -2275 THEN
            DBMS_OUTPUT.PUT_LINE('  Foreign key fk_ilm_policy_profile already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 6: Create index on FK
PROMPT Step 6: Creating index on foreign key...

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_policies_profile ON cmr.dwh_ilm_policies(threshold_profile_id)';
    DBMS_OUTPUT.PUT_LINE('✓ Created index idx_ilm_policies_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Index idx_ilm_policies_profile already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 7: Create helper function
PROMPT Step 7: Creating get_policy_thresholds function...

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
        -- Use profile thresholds
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_id = v_profile_id;
    ELSE
        -- Use global config
        SELECT TO_NUMBER(config_value) INTO v_threshold
        FROM cmr.dwh_ilm_config
        WHERE config_key = p_threshold_type || '_THRESHOLD_DAYS';
    END IF;

    RETURN v_threshold;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Fallback to global config
        SELECT TO_NUMBER(config_value) INTO v_threshold
        FROM cmr.dwh_ilm_config
        WHERE config_key = p_threshold_type || '_THRESHOLD_DAYS';
        RETURN v_threshold;
    WHEN OTHERS THEN
        -- Default fallback values
        RETURN CASE p_threshold_type
            WHEN 'HOT' THEN 90
            WHEN 'WARM' THEN 365
            WHEN 'COLD' THEN 1095
        END;
END get_policy_thresholds;
/

DBMS_OUTPUT.PUT_LINE('✓ Created function get_policy_thresholds');

-- Step 8: Create new view
PROMPT Step 8: Creating dwh_v_ilm_policy_thresholds view...

CREATE OR REPLACE VIEW dwh_v_ilm_policy_thresholds AS
SELECT
    p.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    p.threshold_profile_id,
    prof.profile_name,
    -- Show effective thresholds
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.hot_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS')
    END AS effective_hot_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.warm_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS')
    END AS effective_warm_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.cold_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'COLD_THRESHOLD_DAYS')
    END AS effective_cold_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN 'CUSTOM'
        ELSE 'GLOBAL'
    END AS threshold_source
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_threshold_profiles prof
    ON prof.profile_id = p.threshold_profile_id;

COMMENT ON TABLE cmr.dwh_v_ilm_policy_thresholds IS
    'Shows effective threshold values for each ILM policy';

DBMS_OUTPUT.PUT_LINE('✓ Created view dwh_v_ilm_policy_thresholds');

-- Step 9: Update packages
PROMPT Step 9: Recompiling ILM packages with threshold profile support...

@@../custom_ilm_policy_engine.sql

DBMS_OUTPUT.PUT_LINE('✓ Recompiled pck_dwh_ilm_policy_engine');

PROMPT
PROMPT ========================================
PROMPT Migration to v3.1 Complete!
PROMPT ========================================
PROMPT
PROMPT Summary:
PROMPT   - Added dwh_ilm_threshold_profiles table
PROMPT   - Added 4 default profiles (DEFAULT, FAST_AGING, SLOW_AGING, AGGRESSIVE_ARCHIVE)
PROMPT   - Extended dwh_ilm_policies with threshold_profile_id
PROMPT   - Created get_policy_thresholds() function
PROMPT   - Created dwh_v_ilm_policy_thresholds view
PROMPT   - Updated policy evaluation logic
PROMPT
PROMPT Next Steps:
PROMPT   1. Review default profiles: SELECT * FROM cmr.dwh_ilm_threshold_profiles;
PROMPT   2. View effective thresholds: SELECT * FROM cmr.dwh_v_ilm_policy_thresholds;
PROMPT   3. Optionally assign profiles to existing policies:
PROMPT      UPDATE cmr.dwh_ilm_policies SET threshold_profile_id = X WHERE ...;
PROMPT
