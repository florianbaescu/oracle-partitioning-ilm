-- =============================================================================
-- Custom ILM Framework - Usage Examples
-- Practical examples of defining and using custom ILM policies
-- =============================================================================

-- =============================================================================
-- SECTION 1: BASIC POLICY DEFINITIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Compress partitions older than 90 days
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_days,
    compression_type,
    priority,
    enabled
) VALUES (
    'COMPRESS_SALES_90D',
    USER,
    'SALES_FACT',
    'COMPRESSION',
    'COMPRESS',
    90,
    'QUERY HIGH',
    100,
    'Y'
);

COMMIT;


-- -----------------------------------------------------------------------------
-- Example 2: Move partitions to warm storage after 6 months
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_months,
    target_tablespace,
    compression_type,
    priority,
    enabled
) VALUES (
    'TIER_SALES_WARM_6M',
    USER,
    'SALES_FACT',
    'TIERING',
    'MOVE',
    6,
    'TBS_WARM',
    'QUERY HIGH',
    200,
    'Y'
);

COMMIT;


-- -----------------------------------------------------------------------------
-- Example 3: Move to cold storage and make read-only after 2 years
-- -----------------------------------------------------------------------------

-- Policy 1: Move to cold storage
INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_months,
    target_tablespace,
    compression_type,
    priority,
    enabled
) VALUES (
    'TIER_SALES_COLD_24M',
    USER,
    'SALES_FACT',
    'TIERING',
    'MOVE',
    24,
    'TBS_COLD',
    'ARCHIVE HIGH',
    300,
    'Y'
);

-- Policy 2: Make read-only (runs after move)
INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_months,
    priority,
    enabled
) VALUES (
    'READONLY_SALES_24M',
    USER,
    'SALES_FACT',
    'ARCHIVAL',
    'READ_ONLY',
    24,
    301,  -- Higher priority number = runs after move
    'Y'
);

COMMIT;


-- -----------------------------------------------------------------------------
-- Example 4: Purge partitions older than 7 years
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_months,
    priority,
    enabled
) VALUES (
    'PURGE_SALES_84M',
    USER,
    'SALES_FACT',
    'PURGE',
    'DROP',
    84,  -- 7 years
    900,  -- Run last
    'Y'
);

COMMIT;


-- =============================================================================
-- SECTION 2: ADVANCED POLICY DEFINITIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 5: Compress based on temperature (access pattern)
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    access_pattern,
    compression_type,
    priority,
    enabled
) VALUES (
    'COMPRESS_COLD_PARTITIONS',
    USER,
    'ORDER_FACT',
    'COMPRESSION',
    'COMPRESS',
    'COLD',
    'ARCHIVE HIGH',
    100,
    'Y'
);

COMMIT;


-- -----------------------------------------------------------------------------
-- Example 6: Conditional compression with size threshold
-- Only compress if partition is larger than 100 MB
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_days,
    size_threshold_mb,
    compression_type,
    priority,
    enabled
) VALUES (
    'COMPRESS_LARGE_OLD_PARTITIONS',
    USER,
    'WEB_EVENTS_FACT',
    'COMPRESSION',
    'COMPRESS',
    30,
    100,  -- Only if > 100 MB
    'QUERY HIGH',
    100,
    'Y'
);

COMMIT;


-- -----------------------------------------------------------------------------
-- Example 7: Custom condition - compress partitions with specific criteria
-- -----------------------------------------------------------------------------

INSERT INTO ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_days,
    custom_condition,
    compression_type,
    priority,
    enabled
) VALUES (
    'COMPRESS_COMPLETED_ORDERS',
    USER,
    'ORDER_FACT',
    'COMPRESSION',
    'COMPRESS',
    60,
    'order_status = ''COMPLETED''',  -- Custom WHERE clause
    'QUERY HIGH',
    100,
    'Y'
);

COMMIT;


-- =============================================================================
-- SECTION 3: COMPLETE LIFECYCLE EXAMPLE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Complete lifecycle for FINANCIAL_TRANSACTIONS table
-- -----------------------------------------------------------------------------

-- Stage 1: Active (0-3 months) - No action
-- Data stays in TBS_HOT uncompressed

-- Stage 2: Warm (3-12 months) - Compress
INSERT INTO ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, compression_type, priority, enabled
) VALUES (
    'FIN_COMPRESS_3M', USER, 'FINANCIAL_TRANSACTIONS', 'COMPRESSION', 'COMPRESS',
    3, 'QUERY HIGH', 100, 'Y'
);

-- Stage 3: Cool (12-36 months) - Move to warm storage
INSERT INTO ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, target_tablespace, compression_type, priority, enabled
) VALUES (
    'FIN_TIER_WARM_12M', USER, 'FINANCIAL_TRANSACTIONS', 'TIERING', 'MOVE',
    12, 'TBS_WARM', 'QUERY HIGH', 200, 'Y'
);

-- Stage 4: Cold (36-84 months) - Move to cold storage with archive compression
INSERT INTO ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, target_tablespace, compression_type, priority, enabled
) VALUES (
    'FIN_TIER_COLD_36M', USER, 'FINANCIAL_TRANSACTIONS', 'TIERING', 'MOVE',
    36, 'TBS_COLD', 'ARCHIVE HIGH', 300, 'Y'
);

-- Stage 5: Archive (84+ months) - Make read-only
INSERT INTO ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'FIN_READONLY_84M', USER, 'FINANCIAL_TRANSACTIONS', 'ARCHIVAL', 'READ_ONLY',
    84, 400, 'Y'
);

-- Stage 6: Purge (120+ months / 10 years) - Drop partition
INSERT INTO ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'FIN_PURGE_120M', USER, 'FINANCIAL_TRANSACTIONS', 'PURGE', 'DROP',
    120, 900, 'Y'
);

COMMIT;


-- =============================================================================
-- SECTION 4: RUNNING THE ILM FRAMEWORK
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Option 1: Automatic (via scheduler)
-- -----------------------------------------------------------------------------

-- Check scheduler status
SELECT * FROM v_ilm_scheduler_status;

-- Enable automatic execution
UPDATE ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Start all scheduler jobs
EXEC dwh_start_ilm_jobs();


-- -----------------------------------------------------------------------------
-- Option 2: Manual execution
-- -----------------------------------------------------------------------------

-- Run complete ILM cycle
EXEC dwh_run_ilm_cycle();

-- Or run steps individually:

-- Step 1: Refresh access tracking
EXEC dwh_refresh_partition_access_tracking();

-- Step 2: Evaluate policies
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();

-- Step 3: Review what will be executed
SELECT
    q.queue_id,
    p.policy_name,
    q.table_name,
    q.partition_name,
    p.action_type,
    p.compression_type,
    p.target_tablespace,
    q.reason
FROM ilm_evaluation_queue q
JOIN ilm_policies p ON p.policy_id = q.policy_id
WHERE q.execution_status = 'PENDING'
AND q.eligible = 'Y'
ORDER BY p.priority, q.partition_name;

-- Step 4: Execute (limit to 5 operations for safety)
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(p_max_operations => 5);


-- -----------------------------------------------------------------------------
-- Option 3: Execute specific policy
-- -----------------------------------------------------------------------------

-- Evaluate single policy
EXEC pck_dwh_ilm_policy_engine.evaluate_policy(1);  -- policy_id

-- Execute single policy
EXEC pck_dwh_ilm_execution_engine.execute_policy(1, p_max_operations => 10);


-- -----------------------------------------------------------------------------
-- Option 4: Direct execution (bypass queue)
-- -----------------------------------------------------------------------------

-- Compress specific partition
EXEC pck_dwh_ilm_execution_engine.compress_partition(
    p_table_owner => USER,
    p_table_name => 'SALES_FACT',
    p_partition_name => 'P_2023_01',
    p_compression_type => 'ARCHIVE HIGH',
    p_rebuild_indexes => TRUE,
    p_gather_stats => TRUE
);

-- Move partition to different tablespace
EXEC pck_dwh_ilm_execution_engine.move_partition(
    p_table_owner => USER,
    p_table_name => 'SALES_FACT',
    p_partition_name => 'P_2023_01',
    p_target_tablespace => 'TBS_COLD',
    p_compression_type => 'ARCHIVE HIGH',
    p_rebuild_indexes => TRUE,
    p_gather_stats => TRUE
);


-- =============================================================================
-- SECTION 5: MONITORING AND REPORTING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View active policies
-- -----------------------------------------------------------------------------

SELECT * FROM v_ilm_active_policies
ORDER BY priority, policy_name;


-- -----------------------------------------------------------------------------
-- View execution statistics
-- -----------------------------------------------------------------------------

SELECT * FROM v_ilm_execution_stats
ORDER BY total_space_saved_mb DESC;


-- -----------------------------------------------------------------------------
-- View partition temperatures
-- -----------------------------------------------------------------------------

SELECT * FROM v_ilm_partition_temperature
WHERE table_name = 'SALES_FACT'
ORDER BY days_since_write DESC;


-- -----------------------------------------------------------------------------
-- Execution history for specific table
-- -----------------------------------------------------------------------------

SELECT
    execution_id,
    policy_name,
    partition_name,
    action_type,
    execution_start,
    duration_seconds,
    status,
    size_before_mb,
    size_after_mb,
    space_saved_mb,
    ROUND(compression_ratio, 2) AS compression_ratio
FROM ilm_execution_log
WHERE table_name = 'SALES_FACT'
ORDER BY execution_start DESC
FETCH FIRST 20 ROWS ONLY;


-- -----------------------------------------------------------------------------
-- Space savings summary
-- -----------------------------------------------------------------------------

SELECT
    table_name,
    COUNT(DISTINCT partition_name) AS partitions_processed,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(space_saved_mb), 2) AS total_space_saved_mb,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM ilm_execution_log
WHERE action_type = 'COMPRESS'
GROUP BY table_name
ORDER BY total_space_saved_mb DESC;


-- -----------------------------------------------------------------------------
-- Policy effectiveness report
-- -----------------------------------------------------------------------------

SELECT
    p.policy_name,
    p.policy_type,
    p.action_type,
    COUNT(e.execution_id) AS executions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    ROUND(SUM(e.space_saved_mb), 2) AS total_space_saved_mb,
    ROUND(AVG(e.duration_seconds), 2) AS avg_duration_sec,
    MAX(e.execution_end) AS last_execution
FROM ilm_policies p
LEFT JOIN ilm_execution_log e ON e.policy_id = p.policy_id
WHERE p.enabled = 'Y'
GROUP BY p.policy_name, p.policy_type, p.action_type
ORDER BY total_space_saved_mb DESC NULLS LAST;


-- =============================================================================
-- SECTION 6: MAINTENANCE OPERATIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Disable policy temporarily
-- -----------------------------------------------------------------------------

UPDATE ilm_policies
SET enabled = 'N', modified_date = SYSTIMESTAMP, modified_by = USER
WHERE policy_name = 'COMPRESS_SALES_90D';
COMMIT;


-- -----------------------------------------------------------------------------
-- Update policy parameters
-- -----------------------------------------------------------------------------

UPDATE ilm_policies
SET age_days = 120,  -- Change from 90 to 120 days
    modified_date = SYSTIMESTAMP,
    modified_by = USER
WHERE policy_name = 'COMPRESS_SALES_90D';
COMMIT;


-- -----------------------------------------------------------------------------
-- Clear evaluation queue
-- -----------------------------------------------------------------------------

EXEC pck_dwh_ilm_policy_engine.clear_queue();  -- All policies
EXEC pck_dwh_ilm_policy_engine.clear_queue(p_policy_id => 1);  -- Specific policy


-- -----------------------------------------------------------------------------
-- Refresh evaluation queue
-- -----------------------------------------------------------------------------

EXEC pck_dwh_ilm_policy_engine.refresh_queue();


-- -----------------------------------------------------------------------------
-- Update configuration
-- -----------------------------------------------------------------------------

-- Change execution window
UPDATE ilm_config
SET config_value = '23:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE ilm_config
SET config_value = '05:00'
WHERE config_key = 'EXECUTION_WINDOW_END';

COMMIT;


-- -----------------------------------------------------------------------------
-- Cleanup old logs
-- -----------------------------------------------------------------------------

EXEC cleanup_execution_logs();


-- =============================================================================
-- SECTION 6B: POLICY VALIDATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Successful Policy Creation with Validation
-- -----------------------------------------------------------------------------

-- This will succeed - table exists and is partitioned
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_SALES_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90,
    'QUERY HIGH', 100, 'Y'
);
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Policy created successfully with automatic validation');


-- -----------------------------------------------------------------------------
-- Example 2: Validation Failure - Non-Existent Table
-- -----------------------------------------------------------------------------

-- This will FAIL - table doesn't exist
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        compression_type, priority, enabled
    ) VALUES (
        'COMPRESS_NONEXISTENT', USER, 'TABLE_DOES_NOT_EXIST',
        'COMPRESSION', 'COMPRESS', 90,
        'QUERY HIGH', 100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20001: Table CMR.TABLE_DOES_NOT_EXIST does not exist
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 3: Validation Failure - Non-Partitioned Table
-- -----------------------------------------------------------------------------

-- This will FAIL - table exists but is not partitioned
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        compression_type, priority, enabled
    ) VALUES (
        'COMPRESS_REGULAR_TABLE', USER, 'SOME_NON_PARTITIONED_TABLE',
        'COMPRESSION', 'COMPRESS', 90,
        'QUERY HIGH', 100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20001: Table exists but is not partitioned. ILM policies require partitioned tables.
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 4: Validation Failure - Missing Compression Type
-- -----------------------------------------------------------------------------

-- This will FAIL - COMPRESS action requires compression_type
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        priority, enabled
    ) VALUES (
        'COMPRESS_NO_TYPE', USER, 'SALES_FACT',
        'COMPRESSION', 'COMPRESS', 90,
        100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20004: COMPRESS action requires compression_type to be specified
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 5: Validation Failure - Invalid Compression Type
-- -----------------------------------------------------------------------------

-- This will FAIL - invalid compression type
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        compression_type, priority, enabled
    ) VALUES (
        'COMPRESS_INVALID_TYPE', USER, 'SALES_FACT',
        'COMPRESSION', 'COMPRESS', 90,
        'SUPER DUPER COMPRESSION', 100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20003: Invalid compression type
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 6: Validation Failure - Missing Target Tablespace for MOVE
-- -----------------------------------------------------------------------------

-- This will FAIL - MOVE action requires target_tablespace
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        priority, enabled
    ) VALUES (
        'MOVE_NO_TARGET', USER, 'SALES_FACT',
        'TIERING', 'MOVE', 365,
        100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20005: MOVE action requires target_tablespace to be specified
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 7: Validation Failure - No Condition Specified
-- -----------------------------------------------------------------------------

-- This will FAIL - policy must have at least one condition
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type,
        compression_type, priority, enabled
    ) VALUES (
        'COMPRESS_NO_CONDITION', USER, 'SALES_FACT',
        'COMPRESSION', 'COMPRESS',
        'QUERY HIGH', 100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20007: Policy must have at least one condition
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 8: Validation Failure - Policy/Action Type Mismatch
-- -----------------------------------------------------------------------------

-- This will FAIL - COMPRESSION policy type requires COMPRESS action
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        target_tablespace, priority, enabled
    ) VALUES (
        'COMPRESSION_POLICY_MOVE_ACTION', USER, 'SALES_FACT',
        'COMPRESSION', 'MOVE', 90,
        'TBS_COLD', 100, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20008: COMPRESSION policy type requires COMPRESS action
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 9: Validation Failure - Priority Out of Range
-- -----------------------------------------------------------------------------

-- This will FAIL - priority must be between 1 and 999
BEGIN
    INSERT INTO cmr.dwh_ilm_policies (
        policy_name, table_owner, table_name,
        policy_type, action_type, age_days,
        compression_type, priority, enabled
    ) VALUES (
        'COMPRESS_INVALID_PRIORITY', USER, 'SALES_FACT',
        'COMPRESSION', 'COMPRESS', 90,
        'QUERY HIGH', 9999, 'Y'
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Validation Error: ' || SQLERRM);
        -- Expected: ORA-20010: Priority must be between 1 and 999
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Example 10: Manual Policy Validation
-- -----------------------------------------------------------------------------

-- Validate an existing policy (requires policy_id from previous INSERT)
SET SERVEROUTPUT ON
EXEC dwh_validate_ilm_policy(1);

-- Expected output:
-- ========================================
-- Validating Policy: COMPRESS_SALES_90D
-- ========================================
-- Target table: CMR.SALES_FACT
-- Partition count: 24
-- Eligible partitions: 8
-- ========================================
-- VALIDATION PASSED
-- ========================================


-- -----------------------------------------------------------------------------
-- Example 11: Validate All Policies
-- -----------------------------------------------------------------------------

-- Validate all policies in the system
DECLARE
    v_policy_count NUMBER := 0;
    v_passed NUMBER := 0;
    v_failed NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Validating All ILM Policies');
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('');

    FOR pol IN (SELECT policy_id, policy_name, enabled FROM cmr.dwh_ilm_policies ORDER BY policy_id) LOOP
        v_policy_count := v_policy_count + 1;

        BEGIN
            dwh_validate_ilm_policy(pol.policy_id);
            v_passed := v_passed + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Policy ' || pol.policy_name || ' FAILED validation: ' || SQLERRM);
                v_failed := v_failed + 1;
        END;

        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Validation Summary');
    DBMS_OUTPUT.PUT_LINE('===========================================');
    DBMS_OUTPUT.PUT_LINE('Total policies: ' || v_policy_count);
    DBMS_OUTPUT.PUT_LINE('Passed: ' || v_passed);
    DBMS_OUTPUT.PUT_LINE('Failed: ' || v_failed);
    DBMS_OUTPUT.PUT_LINE('===========================================');
END;
/


-- =============================================================================
-- SECTION 7: TROUBLESHOOTING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Find failed executions
-- -----------------------------------------------------------------------------

SELECT
    execution_id,
    policy_name,
    table_name,
    partition_name,
    action_type,
    execution_start,
    error_code,
    error_message
FROM ilm_execution_log
WHERE status = 'FAILED'
ORDER BY execution_start DESC;


-- -----------------------------------------------------------------------------
-- Check why partition not eligible
-- -----------------------------------------------------------------------------

DECLARE
    v_eligible BOOLEAN;
    v_reason VARCHAR2(500);
BEGIN
    v_eligible := pck_dwh_ilm_policy_engine.is_partition_eligible(
        p_policy_id => 1,
        p_table_owner => USER,
        p_table_name => 'SALES_FACT',
        p_partition_name => 'P_2024_01',
        p_reason => v_reason
    );

    DBMS_OUTPUT.PUT_LINE('Eligible: ' || CASE WHEN v_eligible THEN 'YES' ELSE 'NO' END);
    DBMS_OUTPUT.PUT_LINE('Reason: ' || v_reason);
END;
/


-- -----------------------------------------------------------------------------
-- Check scheduler job failures
-- -----------------------------------------------------------------------------

SELECT * FROM v_ilm_job_history
WHERE status = 'FAILED'
ORDER BY log_date DESC;


-- =============================================================================
-- SECTION 7B: ILM TEMPLATE APPLICATION (MIGRATION INTEGRATION)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Manual Template Assignment
-- -----------------------------------------------------------------------------

-- When creating a migration task, specify ILM template manually
INSERT INTO cmr.dwh_migration_tasks (
    task_name,
    source_table,
    partition_type,
    partition_key,
    migration_method,
    apply_ilm_policies,
    ilm_policy_template  -- Explicitly specify template
) VALUES (
    'Migrate Customer SCD2',
    'CUSTOMER_SCD2',
    'RANGE',
    'EFFECTIVE_DATE',
    'CTAS',
    'Y',
    'SCD2_EFFECTIVE_DATE'  -- Use SCD2 template
);


-- -----------------------------------------------------------------------------
-- Example 2: Auto-Detection - SCD2 Tables
-- -----------------------------------------------------------------------------

-- Migration framework will auto-detect template based on naming patterns
INSERT INTO cmr.dwh_migration_tasks (
    task_name,
    source_table,
    partition_type,
    partition_key,
    migration_method,
    apply_ilm_policies
    -- ilm_policy_template NOT specified - will auto-detect
) VALUES (
    'Migrate Product History',
    'PRODUCT_HIST',  -- Contains _HIST suffix
    'RANGE',
    'VALID_FROM_DTTM',
    'CTAS',
    'Y'  -- Auto-detection enabled
);

-- Expected: Will detect 'SCD2_VALID_FROM_TO' template based on:
--   1. Table name contains '_HIST'
--   2. Has columns VALID_FROM_DTTM and VALID_TO_DTTM


-- -----------------------------------------------------------------------------
-- Example 3: Auto-Detection - Events Tables
-- -----------------------------------------------------------------------------

INSERT INTO cmr.dwh_migration_tasks (
    task_name,
    source_table,
    partition_type,
    partition_key,
    migration_method,
    apply_ilm_policies
) VALUES (
    'Migrate Audit Events',
    'SYSTEM_AUDIT_LOG',  -- Contains _LOG suffix
    'RANGE',
    'EVENT_DATE',
    'CTAS',
    'Y'
);

-- Expected: Will detect 'EVENTS_SHORT_RETENTION' template
--   Based on: Table name contains '_LOG'


-- -----------------------------------------------------------------------------
-- Example 4: Auto-Detection - Staging Tables
-- -----------------------------------------------------------------------------

INSERT INTO cmr.dwh_migration_tasks (
    task_name,
    source_table,
    partition_type,
    partition_key,
    migration_method,
    apply_ilm_policies
) VALUES (
    'Migrate Staging CDC',
    'STG_CUSTOMER_CDC',  -- Starts with STG_ and contains CDC
    'RANGE',
    'LOAD_DATE',
    'CTAS',
    'Y'
);

-- Expected: Will detect 'STAGING_CDC' template
--   Based on: Table name starts with 'STG_' and contains 'CDC'


-- -----------------------------------------------------------------------------
-- Example 5: View Available Templates
-- -----------------------------------------------------------------------------

-- See all available ILM templates
SELECT
    template_name,
    description,
    retention_days,
    retention_months
FROM cmr.dwh_migration_ilm_templates
ORDER BY template_name;

-- Output:
-- TEMPLATE_NAME              | DESCRIPTION                                    | RETENTION_DAYS | RETENTION_MONTHS
-- -------------------------- | ---------------------------------------------- | -------------- | ----------------
-- EVENTS_COMPLIANCE          | Events requiring long compliance retention     | NULL           | 84
-- EVENTS_SHORT_RETENTION     | High-volume events with short retention        | 90             | NULL
-- HIST_TABLE                 | Generic historical tables                      | 1095           | NULL
-- SCD2_EFFECTIVE_DATE        | SCD2 with EFFECTIVE_DATE column                | 1095           | NULL
-- SCD2_VALID_FROM_TO         | SCD2 with VALID_FROM/VALID_TO columns          | 1095           | NULL
-- STAGING_7DAY               | Transactional staging (7-day retention)        | 7              | NULL
-- STAGING_CDC                | CDC staging (30-day retention)                 | 30             | NULL
-- STAGING_ERROR_QUARANTINE   | Error quarantine (90-day retention)            | 90             | NULL


-- -----------------------------------------------------------------------------
-- Example 6: View Template Policies (JSON)
-- -----------------------------------------------------------------------------

-- See the policies that will be created from a template
SELECT
    template_name,
    policies_json
FROM cmr.dwh_migration_ilm_templates
WHERE template_name = 'SCD2_EFFECTIVE_DATE';

-- The JSON defines policies like:
-- [
--   {
--     "policy_name": "{TABLE}_COMPRESS_90D",
--     "action": "COMPRESS",
--     "age_days": 90,
--     "compression": "QUERY HIGH",
--     "priority": 100
--   },
--   {
--     "policy_name": "{TABLE}_COMPRESS_1Y",
--     "action": "COMPRESS",
--     "age_days": 365,
--     "compression": "ARCHIVE HIGH",
--     "priority": 200
--   }
-- ]


-- -----------------------------------------------------------------------------
-- Example 7: Execute Migration with ILM Template
-- -----------------------------------------------------------------------------

-- Analyze table
EXEC pck_dwh_table_migration_analyzer.analyze_table(1);

-- Execute migration (will automatically apply ILM template)
EXEC pck_dwh_table_migration_executor.execute_migration(1);

-- Output will show:
-- ========================================
-- Applying ILM Policies
-- ========================================
-- Template: SCD2_EFFECTIVE_DATE (auto-detected)
-- Table: CMR.CUSTOMER_SCD2
--
-- ✓ Created policy: CUSTOMER_SCD2_COMPRESS_90D
--   Action: COMPRESS after 90 days
--   Compression: QUERY HIGH
--
-- ✓ Created policy: CUSTOMER_SCD2_COMPRESS_1Y
--   Action: COMPRESS after 365 days
--   Compression: ARCHIVE HIGH
--
-- ========================================
-- ILM Policy Application Summary
-- ========================================
-- Policies created: 2
-- Policies skipped: 0


-- -----------------------------------------------------------------------------
-- Example 8: Verify ILM Policies Created
-- -----------------------------------------------------------------------------

-- Check policies created by migration
SELECT
    policy_name,
    policy_type,
    action_type,
    age_days,
    compression_type,
    enabled,
    created_by
FROM cmr.dwh_ilm_policies
WHERE table_name = 'CUSTOMER_SCD2'
ORDER BY priority;

-- Output:
-- POLICY_NAME                 | POLICY_TYPE  | ACTION_TYPE | AGE_DAYS | COMPRESSION_TYPE | ENABLED | CREATED_BY
-- --------------------------- | ------------ | ----------- | -------- | ---------------- | ------- | --------------------------
-- CUSTOMER_SCD2_COMPRESS_90D  | COMPRESSION  | COMPRESS    | 90       | QUERY HIGH       | Y       | CMR (Migration Task 1)
-- CUSTOMER_SCD2_COMPRESS_1Y   | COMPRESSION  | COMPRESS    | 365      | ARCHIVE HIGH     | Y       | CMR (Migration Task 1)


-- -----------------------------------------------------------------------------
-- Example 9: Check Partition Access Tracking Initialized
-- -----------------------------------------------------------------------------

-- Verify partition tracking was initialized
SELECT
    table_name,
    partition_name,
    temperature,
    days_since_write,
    size_mb,
    last_updated
FROM cmr.dwh_ilm_partition_access
WHERE table_name = 'CUSTOMER_SCD2'
ORDER BY days_since_write DESC
FETCH FIRST 5 ROWS ONLY;

-- Shows temperature classification for each partition


-- -----------------------------------------------------------------------------
-- Example 10: Test ILM Policy Evaluation
-- -----------------------------------------------------------------------------

-- Evaluate policies on the migrated table
EXEC pck_dwh_ilm_policy_engine.evaluate_table('CMR', 'CUSTOMER_SCD2');

-- Check eligible partitions
SELECT
    p.policy_name,
    q.partition_name,
    q.eligible,
    q.reason,
    a.days_since_write,
    a.temperature
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON q.policy_id = p.policy_id
JOIN cmr.dwh_ilm_partition_access a
    ON q.table_owner = a.table_owner
    AND q.table_name = a.table_name
    AND q.partition_name = a.partition_name
WHERE q.table_name = 'CUSTOMER_SCD2'
AND q.eligible = 'Y'
ORDER BY a.days_since_write DESC;


-- -----------------------------------------------------------------------------
-- Example 11: Update Template After Migration
-- -----------------------------------------------------------------------------

-- If template wasn't auto-detected or wrong template was used,
-- you can manually update and re-apply

-- Update task with different template
UPDATE cmr.dwh_migration_tasks
SET ilm_policy_template = 'EVENTS_COMPLIANCE'
WHERE task_id = 1;
COMMIT;

-- Re-apply policies (will skip duplicates)
EXEC pck_dwh_table_migration_executor.apply_ilm_policies(1);


-- -----------------------------------------------------------------------------
-- Example 12: Disable ILM for Specific Migration
-- -----------------------------------------------------------------------------

-- If you don't want ILM policies applied
UPDATE cmr.dwh_migration_tasks
SET apply_ilm_policies = 'N'
WHERE task_id = 2;
COMMIT;

-- Now execute migration (no ILM policies will be created)
EXEC pck_dwh_table_migration_executor.execute_migration(2);


-- =============================================================================
-- SECTION 8: EMAIL NOTIFICATIONS FOR ILM FAILURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Configure Email Notifications
-- -----------------------------------------------------------------------------

-- Step 1: Update email configuration
UPDATE cmr.dwh_ilm_config
SET config_value = 'dba@company.com,datawarehouse-team@company.com'
WHERE config_key = 'ALERT_EMAIL_RECIPIENTS';

UPDATE cmr.dwh_ilm_config
SET config_value = 'oracle-ilm@company.com'
WHERE config_key = 'ALERT_EMAIL_SENDER';

UPDATE cmr.dwh_ilm_config
SET config_value = 'smtp.company.com'
WHERE config_key = 'SMTP_SERVER';

COMMIT;

-- Step 2: Configure Oracle to send emails via UTL_MAIL
-- Note: This requires database administrator privileges
-- Run as SYSDBA:
/*
ALTER SYSTEM SET smtp_out_server='smtp.company.com:25' SCOPE=BOTH;
GRANT EXECUTE ON UTL_MAIL TO cmr;
*/


-- -----------------------------------------------------------------------------
-- Example 2: Enable Email Notifications
-- -----------------------------------------------------------------------------

-- Enable email alerts
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;

-- Verify configuration
SELECT
    config_key,
    config_value,
    description
FROM cmr.dwh_ilm_config
WHERE config_key IN (
    'ENABLE_EMAIL_NOTIFICATIONS',
    'ALERT_EMAIL_RECIPIENTS',
    'ALERT_EMAIL_SENDER',
    'SMTP_SERVER',
    'ALERT_FAILURE_THRESHOLD',
    'ALERT_INTERVAL_HOURS'
)
ORDER BY config_key;

-- Output:
-- CONFIG_KEY                  | CONFIG_VALUE                      | DESCRIPTION
-- --------------------------- | --------------------------------- | ---------------------------------------------
-- ALERT_EMAIL_RECIPIENTS      | dba@company.com,...               | Comma-separated list of email recipients
-- ALERT_EMAIL_SENDER          | oracle-ilm@company.com            | Email address for alert sender
-- ALERT_FAILURE_THRESHOLD     | 3                                 | Number of failures before sending alert
-- ALERT_INTERVAL_HOURS        | 4                                 | Minimum hours between alert emails
-- ENABLE_EMAIL_NOTIFICATIONS  | Y                                 | Enable email notifications (Y/N)
-- SMTP_SERVER                 | smtp.company.com                  | SMTP server for sending emails


-- -----------------------------------------------------------------------------
-- Example 3: Test Email Alert System
-- -----------------------------------------------------------------------------

-- Send a test alert manually
BEGIN
    dwh_send_ilm_alert(
        p_alert_type => 'TEST',
        p_subject => 'ILM Email Notification Test',
        p_message => 'This is a test email from the ILM framework.' || CHR(10) ||
                     'If you receive this, email notifications are working correctly.' || CHR(10) ||
                     CHR(10) ||
                     'Test Date: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) ||
                     'Database: ' || SYS_CONTEXT('USERENV', 'DB_NAME')
    );
    DBMS_OUTPUT.PUT_LINE('Test alert sent successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error sending test alert: ' || SQLERRM);
END;
/


-- -----------------------------------------------------------------------------
-- Example 4: Check for Recent Failures (Manual Monitoring)
-- -----------------------------------------------------------------------------

-- Run failure check manually (normally runs via scheduler every 4 hours)
SET SERVEROUTPUT ON
EXEC dwh_check_ilm_failures(p_hours_back => 24);

-- Output will show:
-- If failures >= threshold:
--   Email alert sent with consolidated failure details
-- If failures < threshold:
--   No alert sent (below threshold)
-- If alert sent recently:
--   Skipped to prevent spam


-- -----------------------------------------------------------------------------
-- Example 5: View Recent Failures
-- -----------------------------------------------------------------------------

-- Check for failed ILM executions in last 24 hours
SELECT
    execution_id,
    policy_name,
    table_name,
    partition_name,
    action_type,
    execution_start,
    duration_seconds,
    error_code,
    SUBSTR(error_message, 1, 100) AS error_message_short,
    executed_by
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSTIMESTAMP - INTERVAL '24' HOUR
ORDER BY execution_start DESC;


-- -----------------------------------------------------------------------------
-- Example 6: Adjust Alert Thresholds
-- -----------------------------------------------------------------------------

-- Reduce failure threshold to 2 (more sensitive)
UPDATE cmr.dwh_ilm_config
SET config_value = '2'
WHERE config_key = 'ALERT_FAILURE_THRESHOLD';

-- Increase alert interval to 8 hours (less frequent alerts)
UPDATE cmr.dwh_ilm_config
SET config_value = '8'
WHERE config_key = 'ALERT_INTERVAL_HOURS';

COMMIT;

-- Use these settings for:
-- - More sensitive alerting (threshold = 2): Production critical tables
-- - Less frequent alerts (interval = 8): Development or non-critical environments


-- -----------------------------------------------------------------------------
-- Example 7: Monitor Alert History
-- -----------------------------------------------------------------------------

-- Track when alerts were sent (stored in execution log)
-- Note: Alert sending creates a log entry with special markers

SELECT
    execution_start,
    COUNT(*) AS failure_count,
    LISTAGG(DISTINCT table_name, ', ') WITHIN GROUP (ORDER BY table_name) AS affected_tables,
    LISTAGG(DISTINCT action_type, ', ') WITHIN GROUP (ORDER BY action_type) AS failed_actions
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
GROUP BY TRUNC(execution_start, 'HH')
HAVING COUNT(*) >= (
    SELECT TO_NUMBER(config_value)
    FROM cmr.dwh_ilm_config
    WHERE config_key = 'ALERT_FAILURE_THRESHOLD'
)
ORDER BY execution_start DESC
FETCH FIRST 10 ROWS ONLY;

-- Shows hourly periods where alert threshold was exceeded


-- -----------------------------------------------------------------------------
-- Example 8: Enable/Start Failure Monitoring Scheduler Job
-- -----------------------------------------------------------------------------

-- The failure monitoring job is created but disabled by default
-- Enable it after email configuration is complete

BEGIN
    DBMS_SCHEDULER.ENABLE('ILM_JOB_MONITOR_FAILURES');
    DBMS_OUTPUT.PUT_LINE('Failure monitoring job enabled');
END;
/

-- Verify job is running
SELECT
    job_name,
    enabled,
    state,
    repeat_interval,
    last_start_date,
    next_run_date,
    run_count,
    failure_count
FROM user_scheduler_jobs
WHERE job_name = 'ILM_JOB_MONITOR_FAILURES';

-- Output:
-- JOB_NAME                   | ENABLED | STATE     | REPEAT_INTERVAL            | LAST_START_DATE | NEXT_RUN_DATE | RUN_COUNT | FAILURE_COUNT
-- -------------------------- | ------- | --------- | -------------------------- | --------------- | ------------- | --------- | -------------
-- ILM_JOB_MONITOR_FAILURES   | TRUE    | SCHEDULED | FREQ=HOURLY; INTERVAL=4    | 2024-10-23 ...  | 2024-10-23... | 12        | 0


-- -----------------------------------------------------------------------------
-- Example 9: Run Failure Check Manually
-- -----------------------------------------------------------------------------

-- Check failures in last 48 hours (wider window)
SET SERVEROUTPUT ON
EXEC dwh_check_ilm_failures(p_hours_back => 48);

-- Or check just last 4 hours (shorter window)
EXEC dwh_check_ilm_failures(p_hours_back => 4);


-- -----------------------------------------------------------------------------
-- Example 10: Disable Email Notifications Temporarily
-- -----------------------------------------------------------------------------

-- Disable during maintenance window
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;

-- Perform maintenance...
-- (ILM operations continue, but no alert emails will be sent)

-- Re-enable after maintenance
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;


-- -----------------------------------------------------------------------------
-- Example 11: Simulate Alert for Testing
-- -----------------------------------------------------------------------------

-- Create some test failures (for testing alert system in non-production)
-- Note: Only use this in test/dev environments!

/*
-- Temporarily create a policy that will fail
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'TEST_FAILURE_ALERT', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 0,  -- age_days=0 will try to compress all partitions
    'INVALID_TYPE', 100, 'Y'  -- Invalid compression type will cause failure
);

-- Run evaluation and execution (will fail)
EXEC pck_dwh_ilm_policy_engine.evaluate_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_FAILURE_ALERT')
);

EXEC pck_dwh_ilm_execution_engine.execute_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_FAILURE_ALERT'),
    p_max_operations => 3
);

-- Now check for failures (should trigger alert if threshold met)
EXEC dwh_check_ilm_failures(p_hours_back => 1);

-- Clean up test policy
DELETE FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_FAILURE_ALERT';
COMMIT;
*/


-- -----------------------------------------------------------------------------
-- Example 12: Custom Alert Logic Integration
-- -----------------------------------------------------------------------------

-- You can call dwh_send_ilm_alert from your own procedures
-- Example: Send alert when specific condition is met

CREATE OR REPLACE PROCEDURE check_ilm_health AS
    v_stuck_operations NUMBER;
    v_alert_message CLOB;
BEGIN
    -- Check for operations stuck in RUNNING state > 4 hours
    SELECT COUNT(*) INTO v_stuck_operations
    FROM cmr.dwh_ilm_execution_log
    WHERE status = 'RUNNING'
    AND execution_start < SYSTIMESTAMP - INTERVAL '4' HOUR;

    IF v_stuck_operations > 0 THEN
        v_alert_message := 'WARNING: ' || v_stuck_operations ||
                          ' ILM operations stuck in RUNNING state for > 4 hours.' || CHR(10) ||
                          CHR(10) ||
                          'Please investigate and consider stopping/restarting these operations.' || CHR(10) ||
                          CHR(10) ||
                          'Run this query to see details:' || CHR(10) ||
                          'SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = ''RUNNING'' ' ||
                          'AND execution_start < SYSTIMESTAMP - INTERVAL ''4'' HOUR;';

        dwh_send_ilm_alert(
            p_alert_type => 'WARNING',
            p_subject => v_stuck_operations || ' stuck ILM operations detected',
            p_message => v_alert_message
        );
    END IF;
END;
/


-- =============================================================================
-- SECTION 10: ENHANCED MONITORING VIEWS AND DASHBOARDS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 10.1: Performance Dashboard - Real-Time System Overview
-- -----------------------------------------------------------------------------

-- Get complete system performance snapshot
SELECT * FROM dwh_v_ilm_performance_dashboard;

/*
Sample Output:
ACTIVE_POLICIES_COUNT: 12
PENDING_ACTIONS_COUNT: 45
RUNNING_ACTIONS_COUNT: 2
ACTIONS_TODAY_SUCCESS: 38
ACTIONS_TODAY_FAILED: 1
SPACE_SAVED_TODAY_GB: 125.50
AVG_DURATION_TODAY_MIN: 12.3
ACTIONS_7DAYS_SUCCESS: 285
ACTIONS_7DAYS_FAILED: 8
SPACE_SAVED_7DAYS_GB: 1024.75
AVG_COMPRESSION_RATIO_7DAYS: 3.2
...
*/


-- -----------------------------------------------------------------------------
-- Example 10.2: Alerting Metrics - Health Status Monitoring
-- -----------------------------------------------------------------------------

-- Check overall ILM health status with threshold-based alerts
SELECT * FROM dwh_v_ilm_alerting_metrics;

/*
Sample Output:
FAILURE_RATE_24H_PCT: 2.5
FAILURE_RATE_STATUS: OK
FAILURES_COUNT_24H: 1
AVG_DURATION_24H_MIN: 15.2
DURATION_STATUS: OK
PENDING_QUEUE_COUNT: 45
QUEUE_STATUS: OK
AVG_COMPRESSION_RATIO_7D: 3.15
COMPRESSION_STATUS: OK
OVERDUE_ACTIONS_COUNT: 5
EXECUTION_LAG_STATUS: OK
STALE_PARTITIONS_COUNT: 12
STALE_TRACKING_STATUS: OK
OVERALL_HEALTH_STATUS: OK
*/

-- Filter for issues requiring attention
SELECT
    'Failure Rate' AS metric,
    failure_rate_24h_pct AS value,
    failure_rate_status AS status
FROM dwh_v_ilm_alerting_metrics
WHERE failure_rate_status IN ('WARNING', 'CRITICAL')
UNION ALL
SELECT
    'Queue Backlog' AS metric,
    pending_queue_count AS value,
    queue_status AS status
FROM dwh_v_ilm_alerting_metrics
WHERE queue_status IN ('WARNING', 'CRITICAL')
UNION ALL
SELECT
    'Compression Ratio' AS metric,
    avg_compression_ratio_7d AS value,
    compression_status AS status
FROM dwh_v_ilm_alerting_metrics
WHERE compression_status IN ('WARNING', 'CRITICAL')
UNION ALL
SELECT
    'Execution Lag' AS metric,
    overdue_actions_count AS value,
    execution_lag_status AS status
FROM dwh_v_ilm_alerting_metrics
WHERE execution_lag_status IN ('WARNING', 'CRITICAL');


-- -----------------------------------------------------------------------------
-- Example 10.3: Policy Effectiveness - ROI Analysis
-- -----------------------------------------------------------------------------

-- Identify most effective policies (highest space savings per hour)
SELECT
    policy_name,
    table_name,
    total_executions,
    success_rate_pct,
    total_space_saved_gb,
    avg_execution_time_min,
    space_saved_per_hour_gb,
    effectiveness_rating
FROM dwh_v_ilm_policy_effectiveness
WHERE total_executions > 0
ORDER BY space_saved_per_hour_gb DESC NULLS LAST
FETCH FIRST 10 ROWS ONLY;

-- Find policies that need attention
SELECT
    policy_name,
    table_name,
    total_executions,
    success_rate_pct,
    failed_executions,
    effectiveness_rating,
    days_since_last_execution
FROM dwh_v_ilm_policy_effectiveness
WHERE effectiveness_rating IN ('POOR', 'FAIR', 'NOT_EXECUTED', 'SLOW')
OR days_since_last_execution > 7
ORDER BY
    CASE effectiveness_rating
        WHEN 'NOT_EXECUTED' THEN 1
        WHEN 'POOR' THEN 2
        WHEN 'FAIR' THEN 3
        WHEN 'SLOW' THEN 4
        ELSE 5
    END,
    days_since_last_execution DESC NULLS LAST;

-- Calculate total ROI across all policies
SELECT
    COUNT(DISTINCT policy_id) AS total_policies,
    SUM(total_executions) AS total_operations,
    ROUND(SUM(total_space_saved_gb), 2) AS total_space_saved_gb,
    ROUND(AVG(success_rate_pct), 2) AS avg_success_rate_pct,
    ROUND(AVG(space_saved_per_hour_gb), 2) AS avg_roi_gb_per_hour,
    SUM(CASE WHEN effectiveness_rating = 'EXCELLENT' THEN 1 ELSE 0 END) AS excellent_policies,
    SUM(CASE WHEN effectiveness_rating = 'GOOD' THEN 1 ELSE 0 END) AS good_policies,
    SUM(CASE WHEN effectiveness_rating IN ('POOR', 'FAIR', 'SLOW') THEN 1 ELSE 0 END) AS problem_policies
FROM dwh_v_ilm_policy_effectiveness;


-- -----------------------------------------------------------------------------
-- Example 10.4: Resource Utilization Trends - Capacity Planning
-- -----------------------------------------------------------------------------

-- Analyze daily trends for last 30 days
SELECT
    execution_date,
    total_actions,
    successful_actions,
    failed_actions,
    failure_rate_pct,
    space_saved_gb,
    total_execution_hours,
    avg_duration_minutes,
    gb_saved_per_hour,
    compress_actions,
    move_actions,
    drop_actions
FROM dwh_v_ilm_resource_trends
WHERE execution_date > SYSDATE - 30
ORDER BY execution_date DESC;

-- Weekly aggregates for trend analysis
SELECT
    year_week,
    SUM(total_actions) AS weekly_actions,
    SUM(space_saved_gb) AS weekly_space_saved_gb,
    ROUND(AVG(failure_rate_pct), 2) AS avg_failure_rate_pct,
    ROUND(AVG(avg_compression_ratio), 2) AS avg_compression_ratio,
    SUM(total_execution_hours) AS weekly_execution_hours,
    ROUND(AVG(gb_saved_per_hour), 2) AS avg_efficiency_gb_per_hour
FROM dwh_v_ilm_resource_trends
WHERE execution_date > SYSDATE - 90
GROUP BY year_week
ORDER BY year_week DESC;

-- Monthly summary for capacity planning
SELECT
    year_month,
    SUM(total_actions) AS monthly_actions,
    SUM(successful_actions) AS monthly_successes,
    SUM(failed_actions) AS monthly_failures,
    ROUND(AVG(failure_rate_pct), 2) AS avg_failure_rate_pct,
    ROUND(SUM(space_saved_gb), 2) AS monthly_space_saved_gb,
    ROUND(SUM(total_execution_hours), 2) AS monthly_execution_hours,
    ROUND(AVG(avg_compression_ratio), 2) AS avg_compression_ratio,
    SUM(compress_actions) AS compress_operations,
    SUM(move_actions) AS move_operations,
    SUM(drop_actions) AS drop_operations,
    SUM(custom_actions) AS custom_operations
FROM dwh_v_ilm_resource_trends
GROUP BY year_month
ORDER BY year_month DESC
FETCH FIRST 12 ROWS ONLY;

-- Predict next month's space savings (based on last 3 months average)
SELECT
    ROUND(AVG(monthly_space_saved), 2) AS avg_monthly_space_saved_gb,
    ROUND(AVG(monthly_space_saved) * 1.1, 2) AS predicted_next_month_gb,
    ROUND(AVG(monthly_space_saved) * 12, 2) AS projected_annual_gb
FROM (
    SELECT
        year_month,
        SUM(space_saved_gb) AS monthly_space_saved
    FROM dwh_v_ilm_resource_trends
    WHERE execution_date > ADD_MONTHS(SYSDATE, -3)
    GROUP BY year_month
);


-- -----------------------------------------------------------------------------
-- Example 10.5: Failure Analysis - Root Cause Investigation
-- -----------------------------------------------------------------------------

-- Categorized failure summary
SELECT
    error_category,
    COUNT(DISTINCT table_name) AS affected_tables,
    SUM(failure_count) AS total_failures,
    MIN(first_failure) AS earliest_failure,
    MAX(last_failure) AS most_recent_failure,
    ROUND(AVG(avg_duration_before_failure_sec) / 60, 2) AS avg_time_to_failure_min,
    MIN(recommended_action) AS action_to_take
FROM dwh_v_ilm_failure_analysis
GROUP BY error_category
ORDER BY total_failures DESC;

-- Detailed failure investigation for specific error category
SELECT
    table_owner,
    table_name,
    policy_name,
    action_type,
    error_code,
    failure_count,
    last_failure,
    sample_error_message,
    recommended_action
FROM dwh_v_ilm_failure_analysis
WHERE error_category = 'Resource Busy (Lock Conflict)'
ORDER BY failure_count DESC, last_failure DESC;

-- Identify tables with persistent failures
SELECT
    table_owner,
    table_name,
    COUNT(DISTINCT error_category) AS different_error_types,
    SUM(failure_count) AS total_failures,
    MAX(last_failure) AS most_recent_failure,
    LISTAGG(DISTINCT error_category, ', ') WITHIN GROUP (ORDER BY error_category) AS error_categories
FROM dwh_v_ilm_failure_analysis
GROUP BY table_owner, table_name
HAVING SUM(failure_count) > 5
ORDER BY total_failures DESC;

-- Generate remediation script for common issues
SELECT
    'Table: ' || table_owner || '.' || table_name AS affected_object,
    'Error: ' || error_category AS issue,
    'Action: ' || recommended_action AS fix
FROM dwh_v_ilm_failure_analysis
WHERE error_category IN ('Insufficient Privileges', 'Tablespace Full', 'Index Not Partitioned')
ORDER BY failure_count DESC;


-- -----------------------------------------------------------------------------
-- Example 10.6: Table Lifecycle Overview - Comprehensive Status
-- -----------------------------------------------------------------------------

-- Get overview of all partitioned tables with ILM status
SELECT
    table_owner,
    table_name,
    partition_count,
    total_table_size_gb,
    hot_partitions,
    warm_partitions,
    cold_partitions,
    active_policies_count,
    total_ilm_executions,
    total_space_saved_gb,
    pending_actions,
    lifecycle_status,
    recommendation
FROM dwh_v_ilm_table_overview
ORDER BY
    CASE lifecycle_status
        WHEN 'NO_ILM_POLICIES' THEN 1
        WHEN 'NEVER_EXECUTED' THEN 2
        WHEN 'HIGH_FAILURE_RATE' THEN 3
        WHEN 'STALE' THEN 4
        WHEN 'HIGH_BACKLOG' THEN 5
        WHEN 'ACTIVE' THEN 6
    END,
    total_table_size_gb DESC;

-- Identify tables needing ILM setup
SELECT
    table_name,
    partition_count,
    total_table_size_gb,
    hot_partitions,
    warm_partitions,
    cold_partitions,
    recommendation
FROM dwh_v_ilm_table_overview
WHERE lifecycle_status = 'NO_ILM_POLICIES'
AND total_table_size_gb > 10  -- Focus on tables larger than 10 GB
ORDER BY total_table_size_gb DESC;

-- Find tables with many COLD partitions (archival candidates)
SELECT
    table_name,
    partition_count,
    total_table_size_gb,
    hot_partitions,
    warm_partitions,
    cold_partitions,
    ROUND((cold_partitions / NULLIF(partition_count, 0)) * 100, 1) AS cold_pct,
    active_policies_count,
    recommendation
FROM dwh_v_ilm_table_overview
WHERE cold_partitions > partition_count * 0.3  -- >30% COLD
ORDER BY cold_pct DESC;

-- Summary statistics across all partitioned tables
SELECT
    COUNT(*) AS total_tables,
    SUM(partition_count) AS total_partitions,
    ROUND(SUM(total_table_size_gb), 2) AS total_size_gb,
    SUM(hot_partitions) AS all_hot_partitions,
    SUM(warm_partitions) AS all_warm_partitions,
    SUM(cold_partitions) AS all_cold_partitions,
    SUM(CASE WHEN active_policies_count > 0 THEN 1 ELSE 0 END) AS tables_with_policies,
    SUM(CASE WHEN active_policies_count = 0 THEN 1 ELSE 0 END) AS tables_without_policies,
    ROUND(SUM(total_space_saved_gb), 2) AS cumulative_space_saved_gb,
    SUM(pending_actions) AS total_pending_actions,
    SUM(CASE WHEN lifecycle_status = 'ACTIVE' THEN 1 ELSE 0 END) AS healthy_tables,
    SUM(CASE WHEN lifecycle_status IN ('NO_ILM_POLICIES', 'NEVER_EXECUTED', 'STALE', 'HIGH_FAILURE_RATE') THEN 1 ELSE 0 END) AS problematic_tables
FROM dwh_v_ilm_table_overview;


-- -----------------------------------------------------------------------------
-- Example 10.7: Combined Dashboard Query - Executive Summary
-- -----------------------------------------------------------------------------

-- Single query for executive dashboard (high-level KPIs)
SELECT
    -- System Health
    (SELECT overall_health_status FROM dwh_v_ilm_alerting_metrics) AS system_health,
    -- Active Policies
    (SELECT active_policies_count FROM dwh_v_ilm_performance_dashboard) AS active_policies,
    -- Today's Activity
    (SELECT actions_today_success FROM dwh_v_ilm_performance_dashboard) AS actions_today,
    (SELECT space_saved_today_gb FROM dwh_v_ilm_performance_dashboard) AS space_saved_today_gb,
    -- Last 7 Days
    (SELECT actions_7days_success FROM dwh_v_ilm_performance_dashboard) AS actions_7days,
    (SELECT space_saved_7days_gb FROM dwh_v_ilm_performance_dashboard) AS space_saved_7days_gb,
    (SELECT ROUND(avg_compression_ratio_7days, 2) FROM dwh_v_ilm_performance_dashboard) AS avg_compression_7days,
    -- Last 30 Days
    (SELECT space_saved_30days_gb FROM dwh_v_ilm_performance_dashboard) AS space_saved_30days_gb,
    -- Pending Work
    (SELECT pending_actions_count FROM dwh_v_ilm_performance_dashboard) AS pending_actions,
    (SELECT running_actions_count FROM dwh_v_ilm_performance_dashboard) AS running_actions,
    -- Failures
    (SELECT failures_count_24h FROM dwh_v_ilm_alerting_metrics) AS failures_24h,
    (SELECT failure_rate_24h_pct FROM dwh_v_ilm_alerting_metrics) AS failure_rate_pct,
    -- Temperature Distribution
    (SELECT partitions_hot FROM dwh_v_ilm_performance_dashboard) AS partitions_hot,
    (SELECT partitions_warm FROM dwh_v_ilm_performance_dashboard) AS partitions_warm,
    (SELECT partitions_cold FROM dwh_v_ilm_performance_dashboard) AS partitions_cold,
    -- Tables
    (SELECT COUNT(*) FROM dwh_v_ilm_table_overview WHERE lifecycle_status = 'ACTIVE') AS healthy_tables,
    (SELECT COUNT(*) FROM dwh_v_ilm_table_overview WHERE lifecycle_status != 'ACTIVE') AS problematic_tables,
    -- Timestamp
    SYSTIMESTAMP AS report_timestamp
FROM DUAL;


-- -----------------------------------------------------------------------------
-- Example 10.8: Create Custom Monitoring Procedure (Optional)
-- -----------------------------------------------------------------------------

/*
-- Create a custom procedure that checks all metrics and raises alerts

CREATE OR REPLACE PROCEDURE check_ilm_health AS
    v_health VARCHAR2(20);
    v_failures NUMBER;
    v_queue NUMBER;
    v_stale NUMBER;
    v_alert_needed BOOLEAN := FALSE;
    v_alert_message VARCHAR2(4000);
BEGIN
    -- Get current health metrics
    SELECT overall_health_status, failures_count_24h, pending_queue_count, stale_partitions_count
    INTO v_health, v_failures, v_queue, v_stale
    FROM dwh_v_ilm_alerting_metrics;

    -- Build alert message if issues found
    IF v_health IN ('WARNING', 'CRITICAL') THEN
        v_alert_needed := TRUE;
        v_alert_message := 'ILM Health Status: ' || v_health || CHR(10);

        IF v_failures > 0 THEN
            v_alert_message := v_alert_message || '- Failures in last 24h: ' || v_failures || CHR(10);
        END IF;

        IF v_queue > 100 THEN
            v_alert_message := v_alert_message || '- Queue backlog: ' || v_queue || ' pending actions' || CHR(10);
        END IF;

        IF v_stale > 100 THEN
            v_alert_message := v_alert_message || '- Stale partitions: ' || v_stale || CHR(10);
        END IF;
    END IF;

    -- Log or send alert
    IF v_alert_needed THEN
        DBMS_OUTPUT.PUT_LINE(v_alert_message);
        -- Optionally call dwh_send_ilm_alert() here
    ELSE
        DBMS_OUTPUT.PUT_LINE('ILM Framework: All systems healthy');
    END IF;
END;
/

-- Run health check
EXEC check_ilm_health();
*/


-- -----------------------------------------------------------------------------
-- Example 10.9: Automated Reporting Query - Daily Operations Report
-- -----------------------------------------------------------------------------

-- Generate daily operations report (suitable for email or dashboard)
SELECT
    'ILM DAILY OPERATIONS REPORT' AS report_title,
    TO_CHAR(SYSDATE, 'YYYY-MM-DD') AS report_date,
    '=========================' AS separator
FROM DUAL
UNION ALL
SELECT
    'System Health: ' || overall_health_status,
    NULL,
    NULL
FROM dwh_v_ilm_alerting_metrics
UNION ALL
SELECT
    '=========================',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    'TODAY''S ACTIVITY:',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    '  Successful Actions: ' || actions_today_success,
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Failed Actions: ' || actions_today_failed,
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Space Saved: ' || space_saved_today_gb || ' GB',
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Avg Duration: ' || avg_duration_today_min || ' min',
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '=========================',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    'LAST 7 DAYS:',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    '  Total Actions: ' || actions_7days_success,
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Space Saved: ' || space_saved_7days_gb || ' GB',
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Avg Compression: ' || avg_compression_ratio_7days || 'x',
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '=========================',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    'PENDING WORK:',
    NULL,
    NULL
FROM DUAL
UNION ALL
SELECT
    '  Pending Actions: ' || pending_actions_count,
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard
UNION ALL
SELECT
    '  Running Actions: ' || running_actions_count,
    NULL,
    NULL
FROM dwh_v_ilm_performance_dashboard;


-- -----------------------------------------------------------------------------
-- Example 10.10: Performance Tuning Query - Identify Slow Policies
-- -----------------------------------------------------------------------------

-- Find policies that are taking longer than expected
SELECT
    p.policy_name,
    p.table_name,
    p.action_type,
    p.total_executions,
    p.avg_execution_time_min,
    p.max_execution_time_sec / 60 AS max_execution_time_min,
    p.total_space_saved_gb,
    p.space_saved_per_hour_gb,
    p.effectiveness_rating,
    -- Suggested optimization
    CASE
        WHEN p.avg_execution_time_min > 60 THEN 'Consider increasing parallel_degree or splitting large partitions'
        WHEN p.avg_execution_time_min > 30 AND p.action_type = 'COMPRESS' THEN 'Review compression type - may be too aggressive'
        WHEN p.max_execution_time_sec > p.avg_execution_time_sec * 3 THEN 'Investigate outlier executions'
        ELSE 'Performance acceptable'
    END AS optimization_suggestion
FROM dwh_v_ilm_policy_effectiveness p
WHERE p.total_executions > 0
AND p.avg_execution_time_min > 15  -- Longer than 15 minutes average
ORDER BY p.avg_execution_time_min DESC;


-- =============================================================================
-- SECTION 9: CLEANUP / REMOVAL
-- =============================================================================

-- To completely remove the ILM framework (use with caution!):

/*
-- Stop all jobs
EXEC dwh_stop_ilm_jobs();

-- Drop scheduler jobs
BEGIN
    FOR job IN (
        SELECT job_name FROM user_scheduler_jobs
        WHERE job_name LIKE 'ILM_JOB_%'
    ) LOOP
        DBMS_SCHEDULER.DROP_JOB(job.job_name, TRUE);
    END LOOP;
END;
/

-- Drop scheduler programs
BEGIN
    FOR prog IN (
        SELECT program_name FROM user_scheduler_programs
        WHERE program_name LIKE 'ILM_%'
    ) LOOP
        DBMS_SCHEDULER.DROP_PROGRAM(prog.program_name, TRUE);
    END LOOP;
END;
/

-- Drop packages
DROP PACKAGE ilm_execution_engine;
DROP PACKAGE ilm_policy_engine;

-- Drop procedures/functions
DROP PROCEDURE refresh_partition_access_tracking;
DROP PROCEDURE cleanup_execution_logs;
DROP PROCEDURE stop_ilm_jobs;
DROP PROCEDURE start_ilm_jobs;
DROP PROCEDURE run_ilm_job_now;
DROP PROCEDURE run_ilm_cycle;
DROP FUNCTION is_execution_window_open;
DROP FUNCTION get_ilm_config;

-- Drop views
DROP VIEW v_ilm_active_policies;
DROP VIEW v_ilm_execution_stats;
DROP VIEW v_ilm_partition_temperature;
DROP VIEW v_ilm_scheduler_status;
DROP VIEW v_ilm_job_history;

-- Drop tables (data loss!)
DROP TABLE ilm_evaluation_queue;
DROP TABLE ilm_execution_log;
DROP TABLE ilm_partition_access;
DROP TABLE ilm_policies;
DROP TABLE ilm_config;
*/
