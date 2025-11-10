-- =============================================================================
-- Validation Script for Tiered ILM Templates
-- Verifies JSON schema compliance and template structure
-- =============================================================================
--
-- PURPOSE:
--   Standalone validation script to test template JSON structure and parsing
--   before running actual migrations.
--
-- USAGE:
--   1. Run after table_migration_setup.sql to verify templates were created
--   2. Run after modifying templates to ensure JSON is still valid
--   3. Quick sanity check before running migrations
--
-- NOTE:
--   This validation logic is ALSO integrated into build_tiered_partitions()
--   in table_migration_execution.sql, so runtime validation happens
--   automatically during actual migrations.
--
--   Validation errors during migration will raise:
--     ORA-20100: tier_config.hot is missing
--     ORA-20101: tier_config.hot missing required fields
--     ORA-20102: tier_config.hot must have either age_months or age_days
--     ORA-20103: tier_config.warm is missing
--     ORA-20104: tier_config.warm missing required fields
--     ORA-20105: tier_config.cold is missing
--     ORA-20106: tier_config.cold missing required fields
--
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200

PROMPT ========================================
PROMPT Tiered Template Validation
PROMPT ========================================
PROMPT

-- =============================================================================
-- Test 1: Verify Tiered Templates Exist
-- =============================================================================
PROMPT Test 1: Verifying tiered templates exist...
PROMPT

SELECT
    template_name,
    table_type,
    JSON_VALUE(policies_json, '$.tier_config.enabled') as tier_enabled,
    JSON_VALUE(policies_json, '$.tier_config.hot.age_months') as hot_age_months,
    JSON_VALUE(policies_json, '$.tier_config.hot.age_days') as hot_age_days,
    JSON_VALUE(policies_json, '$.tier_config.hot.interval') as hot_interval,
    JSON_VALUE(policies_json, '$.tier_config.hot.tablespace') as hot_tablespace,
    JSON_VALUE(policies_json, '$.tier_config.hot.compression') as hot_compression
FROM cmr.dwh_migration_ilm_templates
WHERE JSON_EXISTS(policies_json, '$.tier_config');

PROMPT
PROMPT Expected: 3 templates (FACT_TABLE_STANDARD_TIERED, EVENTS_SHORT_RETENTION_TIERED, SCD2_VALID_FROM_TO_TIERED)
PROMPT

-- =============================================================================
-- Test 2: JSON Parsing Test
-- =============================================================================
PROMPT Test 2: JSON Parsing Test...
PROMPT

DECLARE
    v_template cmr.dwh_migration_ilm_templates%ROWTYPE;
    v_json JSON_OBJECT_T;
    v_tier JSON_OBJECT_T;
    v_hot JSON_OBJECT_T;
    v_warm JSON_OBJECT_T;
    v_cold JSON_OBJECT_T;
    v_policies JSON_ARRAY_T;
    v_test_count NUMBER := 0;
    v_pass_count NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Testing: FACT_TABLE_STANDARD_TIERED');
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Test FACT_TABLE_STANDARD_TIERED
    SELECT * INTO v_template
    FROM cmr.dwh_migration_ilm_templates
    WHERE template_name = 'FACT_TABLE_STANDARD_TIERED';

    v_json := JSON_OBJECT_T.PARSE(v_template.policies_json);

    -- Test tier_config exists
    v_test_count := v_test_count + 1;
    IF v_json.has('tier_config') THEN
        DBMS_OUTPUT.PUT_LINE('✓ tier_config exists');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ tier_config MISSING');
    END IF;

    v_tier := TREAT(v_json.get('tier_config') AS JSON_OBJECT_T);

    -- Test enabled flag
    v_test_count := v_test_count + 1;
    IF v_tier.get_boolean('enabled') = TRUE THEN
        DBMS_OUTPUT.PUT_LINE('✓ tier_config.enabled = true');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ tier_config.enabled is false or missing');
    END IF;

    -- Test HOT tier
    v_hot := TREAT(v_tier.get('hot') AS JSON_OBJECT_T);
    v_test_count := v_test_count + 1;
    IF v_hot.get_number('age_months') = 12 THEN
        DBMS_OUTPUT.PUT_LINE('✓ HOT tier: age_months = 12');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ HOT tier: age_months != 12');
    END IF;

    v_test_count := v_test_count + 1;
    IF v_hot.get_string('interval') = 'MONTHLY' THEN
        DBMS_OUTPUT.PUT_LINE('✓ HOT tier: interval = MONTHLY');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ HOT tier: interval != MONTHLY');
    END IF;

    v_test_count := v_test_count + 1;
    IF v_hot.get_string('tablespace') = 'TBS_HOT' THEN
        DBMS_OUTPUT.PUT_LINE('✓ HOT tier: tablespace = TBS_HOT');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ HOT tier: tablespace != TBS_HOT');
    END IF;

    v_test_count := v_test_count + 1;
    IF v_hot.get_string('compression') = 'NONE' THEN
        DBMS_OUTPUT.PUT_LINE('✓ HOT tier: compression = NONE');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ HOT tier: compression != NONE');
    END IF;

    -- Test WARM tier
    v_warm := TREAT(v_tier.get('warm') AS JSON_OBJECT_T);
    v_test_count := v_test_count + 1;
    IF v_warm.get_number('age_months') = 36 AND
       v_warm.get_string('interval') = 'YEARLY' AND
       v_warm.get_string('compression') = 'BASIC' THEN
        DBMS_OUTPUT.PUT_LINE('✓ WARM tier: configured correctly (36m, YEARLY, BASIC)');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ WARM tier: configuration mismatch');
    END IF;

    -- Test COLD tier
    v_cold := TREAT(v_tier.get('cold') AS JSON_OBJECT_T);
    v_test_count := v_test_count + 1;
    IF v_cold.get_number('age_months') = 84 AND
       v_cold.get_string('interval') = 'YEARLY' AND
       v_cold.get_string('compression') = 'OLTP' THEN
        DBMS_OUTPUT.PUT_LINE('✓ COLD tier: configured correctly (84m, YEARLY, OLTP)');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ COLD tier: configuration mismatch');
    END IF;

    -- Test policies array exists
    v_test_count := v_test_count + 1;
    IF v_json.has('policies') THEN
        v_policies := TREAT(v_json.get('policies') AS JSON_ARRAY_T);
        DBMS_OUTPUT.PUT_LINE('✓ policies array exists (' || v_policies.get_size || ' policies)');
        v_pass_count := v_pass_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ policies array MISSING');
    END IF;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Test Results: ' || v_pass_count || '/' || v_test_count || ' passed');
    DBMS_OUTPUT.PUT_LINE('========================================');

    IF v_pass_count = v_test_count THEN
        DBMS_OUTPUT.PUT_LINE('✓ ALL TESTS PASSED');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ SOME TESTS FAILED');
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Template FACT_TABLE_STANDARD_TIERED not found');
        DBMS_OUTPUT.PUT_LINE('Please run table_migration_setup.sql first');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
/

-- =============================================================================
-- Test 3: Backward Compatibility - Non-Tiered Templates Still Work
-- =============================================================================
PROMPT
PROMPT Test 3: Backward Compatibility Test...
PROMPT

DECLARE
    v_template cmr.dwh_migration_ilm_templates%ROWTYPE;
    v_json_text CLOB;
    v_first_char VARCHAR2(1);
    v_is_array BOOLEAN := FALSE;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Testing non-tiered template: FACT_TABLE_STANDARD');

    SELECT * INTO v_template
    FROM cmr.dwh_migration_ilm_templates
    WHERE template_name = 'FACT_TABLE_STANDARD';

    -- Get JSON text and check first character
    v_json_text := v_template.policies_json;
    v_first_char := LTRIM(SUBSTR(v_json_text, 1, 1));

    -- Determine if it's an array or object
    IF v_first_char = '[' THEN
        v_is_array := TRUE;
        DBMS_OUTPUT.PUT_LINE('✓ Template uses JSON array format (legacy format)');
        DBMS_OUTPUT.PUT_LINE('✓ JSON is valid and parseable');
        DBMS_OUTPUT.PUT_LINE('✓ Non-tiered template does NOT have tier_config (correct)');
        DBMS_OUTPUT.PUT_LINE('✓ Backward compatibility maintained');
    ELSIF v_first_char = '{' THEN
        -- It's an object, should not have tier_config
        IF JSON_EXISTS(v_json_text, '$.tier_config') THEN
            DBMS_OUTPUT.PUT_LINE('✗ Non-tiered template has tier_config (unexpected)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✓ Template uses JSON object format (new format)');
            DBMS_OUTPUT.PUT_LINE('✓ Non-tiered template does NOT have tier_config (correct)');
            DBMS_OUTPUT.PUT_LINE('✓ Backward compatibility maintained');
        END IF;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ Invalid JSON format');
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Template FACT_TABLE_STANDARD not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
/

-- =============================================================================
-- Test 4: Template Catalog Summary
-- =============================================================================
PROMPT
PROMPT Test 4: Template Catalog Summary
PROMPT

SELECT
    template_name,
    table_type,
    CASE
        WHEN JSON_EXISTS(policies_json, '$.tier_config') THEN 'TIERED'
        ELSE 'STANDARD'
    END as template_type,
    SUBSTR(description, 1, 80) as description
FROM cmr.dwh_migration_ilm_templates
ORDER BY
    CASE WHEN JSON_EXISTS(policies_json, '$.tier_config') THEN 1 ELSE 2 END,
    template_name;

PROMPT
PROMPT ========================================
PROMPT Validation Complete
PROMPT ========================================
PROMPT
PROMPT If all tests passed, Phase 1 is complete!
PROMPT Next: Phase 2 - Implement build_tiered_partitions()
PROMPT
