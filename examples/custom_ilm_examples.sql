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
EXEC start_ilm_jobs();


-- -----------------------------------------------------------------------------
-- Option 2: Manual execution
-- -----------------------------------------------------------------------------

-- Run complete ILM cycle
EXEC run_ilm_cycle();

-- Or run steps individually:

-- Step 1: Refresh access tracking
EXEC refresh_partition_access_tracking();

-- Step 2: Evaluate policies
EXEC ilm_policy_engine.evaluate_all_policies();

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
EXEC ilm_execution_engine.execute_pending_actions(p_max_operations => 5);


-- -----------------------------------------------------------------------------
-- Option 3: Execute specific policy
-- -----------------------------------------------------------------------------

-- Evaluate single policy
EXEC ilm_policy_engine.evaluate_policy(1);  -- policy_id

-- Execute single policy
EXEC ilm_execution_engine.execute_policy(1, p_max_operations => 10);


-- -----------------------------------------------------------------------------
-- Option 4: Direct execution (bypass queue)
-- -----------------------------------------------------------------------------

-- Compress specific partition
EXEC ilm_execution_engine.compress_partition(
    p_table_owner => USER,
    p_table_name => 'SALES_FACT',
    p_partition_name => 'P_2023_01',
    p_compression_type => 'ARCHIVE HIGH',
    p_rebuild_indexes => TRUE,
    p_gather_stats => TRUE
);

-- Move partition to different tablespace
EXEC ilm_execution_engine.move_partition(
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

EXEC ilm_policy_engine.clear_queue();  -- All policies
EXEC ilm_policy_engine.clear_queue(p_policy_id => 1);  -- Specific policy


-- -----------------------------------------------------------------------------
-- Refresh evaluation queue
-- -----------------------------------------------------------------------------

EXEC ilm_policy_engine.refresh_queue();


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
    v_eligible := ilm_policy_engine.is_partition_eligible(
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
-- SECTION 8: CLEANUP / REMOVAL
-- =============================================================================

-- To completely remove the ILM framework (use with caution!):

/*
-- Stop all jobs
EXEC stop_ilm_jobs();

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
