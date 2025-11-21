-- =============================================================================
-- Partition Utilities - Usage Examples
-- Demonstrates validation, logging, and utility operations
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

-- =============================================================================
-- SECTION 1: SETUP AND INSTALLATION
-- =============================================================================

-- Install in this order:
-- 1. @partition_utilities_setup.sql         -- Create tables and views
-- 2. @pck_dwh_partition_utils_helper.sql    -- Create helper package
-- 3. @pck_dwh_partition_utilities_v2.sql    -- Create main utilities package

-- =============================================================================
-- SECTION 2: PARTITION NAME VALIDATION
-- =============================================================================

-- Check if a table's partitions follow framework naming
DECLARE
    v_result VARCHAR2(20);
BEGIN
    v_result := pck_dwh_partition_utils_helper.validate_partition_naming(
        p_table_owner => 'DWH',
        p_table_name => 'SALES_FACT_PART',
        p_sample_size => 10
    );

    DBMS_OUTPUT.PUT_LINE('Naming compliance: ' || v_result);
    -- Returns: COMPLIANT, NON_COMPLIANT, MIXED, or UNKNOWN
END;
/

-- Check multiple tables
SELECT
    table_owner,
    table_name,
    pck_dwh_partition_utils_helper.validate_partition_naming(
        table_owner, table_name, 10
    ) as naming_compliance
FROM all_tables
WHERE table_owner = 'DWH'
AND partitioned = 'YES'
ORDER BY table_name;

-- =============================================================================
-- SECTION 3: CONFIGURATION DETECTION
-- =============================================================================

-- Detect partition configuration for a table
DECLARE
    v_interval VARCHAR2(20);
    v_tablespace VARCHAR2(30);
    v_compression VARCHAR2(50);
    v_config_source VARCHAR2(50);
BEGIN
    pck_dwh_partition_utils_helper.detect_partition_config(
        p_table_owner => 'DWH',
        p_table_name => 'SALES_FACT_PART',
        p_interval => v_interval,
        p_tablespace => v_tablespace,
        p_compression => v_compression,
        p_config_source => v_config_source
    );

    DBMS_OUTPUT.PUT_LINE('Configuration source: ' || v_config_source);
    DBMS_OUTPUT.PUT_LINE('Interval: ' || v_interval);
    DBMS_OUTPUT.PUT_LINE('Tablespace: ' || v_tablespace);
    DBMS_OUTPUT.PUT_LINE('Compression: ' || NVL(v_compression, 'NONE'));
END;
/

-- =============================================================================
-- SECTION 4: MANUAL CONFIGURATION OVERRIDE
-- =============================================================================

-- Add manual configuration for a non-framework table
INSERT INTO cmr.dwh_partition_precreation_config (
    table_owner,
    table_name,
    partition_interval,
    tablespace,
    compression,
    pctfree,
    enabled,
    notes
) VALUES (
    'DWH',
    'CUSTOM_PARTITIONED_TABLE',
    'MONTHLY',
    'TBS_HOT',
    'QUERY HIGH',
    10,
    'Y',
    'External table with custom partition names - manual config required'
);

COMMIT;

-- View all manual configurations
SELECT * FROM cmr.dwh_partition_precreation_config;

-- =============================================================================
-- SECTION 5: PRE-CREATE HOT TIER PARTITIONS
-- =============================================================================

-- Preview what would be created (no actual creation)
EXEC pck_dwh_partition_utilities.preview_hot_partitions('DWH', 'SALES_FACT_PART');

-- Pre-create partitions for a specific table
EXEC pck_dwh_partition_utilities.precreate_hot_partitions('DWH', 'SALES_FACT_PART');

-- Batch pre-create for all ILM-managed tables
EXEC pck_dwh_partition_utilities.precreate_all_hot_partitions;

-- =============================================================================
-- SECTION 6: EXAMPLE - NON-COMPLIANT TABLE
-- =============================================================================

-- Demonstrate what happens with non-compliant partition names

-- Create test table with custom partition names
CREATE TABLE dwh.test_custom_partitions (
    sale_date DATE NOT NULL,
    amount NUMBER
)
PARTITION BY RANGE (sale_date) (
    PARTITION part_jan2024 VALUES LESS THAN (TO_DATE('2024-02-01', 'YYYY-MM-DD')),
    PARTITION part_feb2024 VALUES LESS THAN (TO_DATE('2024-03-01', 'YYYY-MM-DD')),
    PARTITION part_mar2024 VALUES LESS THAN (TO_DATE('2024-04-01', 'YYYY-MM-DD'))
);

-- Try to pre-create partitions (will be skipped with warning)
EXEC pck_dwh_partition_utilities.precreate_hot_partitions('DWH', 'TEST_CUSTOM_PARTITIONS');

-- Expected output:
-- =========================================
-- Pre-creating HOT tier partitions
-- Table: DWH.TEST_CUSTOM_PARTITIONS
-- =========================================
-- Partition naming compliance: NON_COMPLIANT
--
-- ⚠️  WARNING: Table partitions do NOT follow framework naming patterns.
-- Pre-creation may fail or create inconsistent partition names.
-- Framework patterns: P_YYYY, P_YYYY_MM, P_YYYY_MM_DD, P_IYYY_IW
--
-- Operation SKIPPED due to non-compliant partition naming.
-- =========================================

-- Cleanup
DROP TABLE dwh.test_custom_partitions PURGE;

-- =============================================================================
-- SECTION 7: MONITORING AND LOGGING
-- =============================================================================

-- View recent operations
SELECT * FROM cmr.dwh_v_partition_utilities_recent;

-- View operation summary
SELECT * FROM cmr.dwh_v_partition_utilities_summary;

-- View errors and warnings
SELECT * FROM cmr.dwh_v_partition_utilities_errors;

-- Detailed log for specific table
SELECT
    log_id,
    operation_name,
    start_time,
    duration_seconds,
    status,
    partitions_created,
    partitions_skipped,
    config_source,
    interval_type,
    message
FROM cmr.dwh_partition_utilities_log
WHERE table_owner = 'DWH'
AND table_name = 'SALES_FACT_PART'
ORDER BY start_time DESC;

-- Operations by status
SELECT
    status,
    COUNT(*) as count,
    ROUND(AVG(duration_seconds), 2) as avg_duration_sec
FROM cmr.dwh_partition_utilities_log
GROUP BY status
ORDER BY count DESC;

-- =============================================================================
-- SECTION 8: SCHEDULED MONTHLY PRE-CREATION
-- =============================================================================

-- Create scheduler job to run at end of each month
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
        job_name => 'CMR.JOB_PRECREATE_HOT_PARTITIONS',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN
                         pck_dwh_partition_utilities.precreate_all_hot_partitions;
                       END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=-1',  -- Last day of month
        enabled => TRUE,
        comments => 'Pre-create next month HOT tier partitions for all ILM-managed tables'
    );

    DBMS_OUTPUT.PUT_LINE('Scheduler job created: CMR.JOB_PRECREATE_HOT_PARTITIONS');
END;
/

-- View scheduler job status
SELECT
    job_name,
    enabled,
    state,
    last_start_date,
    next_run_date,
    run_count,
    failure_count
FROM all_scheduler_jobs
WHERE job_name = 'JOB_PRECREATE_HOT_PARTITIONS';

-- View job run history
SELECT
    log_date,
    status,
    error#,
    SUBSTR(additional_info, 1, 100) as info
FROM all_scheduler_job_run_details
WHERE job_name = 'JOB_PRECREATE_HOT_PARTITIONS'
ORDER BY log_date DESC;

-- Disable job if needed
-- EXEC DBMS_SCHEDULER.DISABLE('CMR.JOB_PRECREATE_HOT_PARTITIONS');

-- Drop job if needed
-- EXEC DBMS_SCHEDULER.DROP_JOB('CMR.JOB_PRECREATE_HOT_PARTITIONS');

-- =============================================================================
-- SECTION 9: TROUBLESHOOTING
-- =============================================================================

-- Check tables without ILM policies (won't be included in batch)
SELECT
    t.owner,
    t.table_name,
    t.partitioned
FROM all_tables t
WHERE t.owner = 'DWH'
AND t.partitioned = 'YES'
AND NOT EXISTS (
    SELECT 1
    FROM cmr.dwh_ilm_policies p
    WHERE p.table_owner = t.owner
    AND p.table_name = t.table_name
    AND p.status = 'ACTIVE'
);

-- Check for tables with mixed naming
SELECT
    table_owner,
    table_name,
    pck_dwh_partition_utils_helper.validate_partition_naming(
        table_owner, table_name, 20
    ) as naming_status
FROM (
    SELECT DISTINCT table_owner, table_name
    FROM cmr.dwh_ilm_policies
    WHERE status = 'ACTIVE'
)
WHERE pck_dwh_partition_utils_helper.validate_partition_naming(
    table_owner, table_name, 20
) IN ('MIXED', 'NON_COMPLIANT');

-- Check for detection failures
SELECT
    table_owner,
    table_name,
    config_source,
    interval_type,
    message
FROM cmr.dwh_partition_utilities_log
WHERE config_source = 'NOT_DETECTED'
OR interval_type = 'UNKNOWN'
ORDER BY start_time DESC;

-- =============================================================================
-- SECTION 10: EXAMPLE WORKFLOW
-- =============================================================================

-- Complete workflow for adding a new table to automated pre-creation

-- Step 1: Check if table has ILM policy
SELECT * FROM cmr.dwh_ilm_policies
WHERE table_owner = 'DWH'
AND table_name = 'MY_NEW_TABLE';

-- Step 2: Validate partition naming
EXEC DBMS_OUTPUT.PUT_LINE(
    pck_dwh_partition_utils_helper.validate_partition_naming('DWH', 'MY_NEW_TABLE')
);

-- Step 3: Preview what would be created
EXEC pck_dwh_partition_utilities.preview_hot_partitions('DWH', 'MY_NEW_TABLE');

-- Step 4: If preview looks good, run actual pre-creation
EXEC pck_dwh_partition_utilities.precreate_hot_partitions('DWH', 'MY_NEW_TABLE');

-- Step 5: Verify in log
SELECT * FROM cmr.dwh_v_partition_utilities_recent
WHERE table_full_name = 'DWH.MY_NEW_TABLE';

-- Step 6: Table will now be included in monthly batch job automatically

-- =============================================================================
-- SECTION 11: QUERY EXAMPLES
-- =============================================================================

-- Tables successfully processed today
SELECT
    table_owner || '.' || table_name as table_name,
    partitions_created,
    config_source,
    interval_type,
    TO_CHAR(start_time, 'HH24:MI:SS') as time
FROM cmr.dwh_partition_utilities_log
WHERE TRUNC(start_time) = TRUNC(SYSDATE)
AND status = 'SUCCESS'
AND operation_name = 'precreate_hot_partitions'
ORDER BY start_time DESC;

-- Total partitions created per table (lifetime)
SELECT
    table_owner || '.' || table_name as table_name,
    SUM(partitions_created) as total_created,
    COUNT(*) as execution_count,
    MAX(start_time) as last_execution
FROM cmr.dwh_partition_utilities_log
WHERE operation_name = 'precreate_hot_partitions'
AND status = 'SUCCESS'
GROUP BY table_owner, table_name
ORDER BY total_created DESC;

-- Average execution time by interval type
SELECT
    interval_type,
    COUNT(*) as executions,
    ROUND(AVG(duration_seconds), 2) as avg_seconds,
    ROUND(AVG(partitions_created), 1) as avg_partitions_created
FROM cmr.dwh_partition_utilities_log
WHERE operation_name = 'precreate_hot_partitions'
AND status = 'SUCCESS'
GROUP BY interval_type
ORDER BY avg_seconds DESC;
