-- =============================================================================
-- ILM Movement Test Suite
-- Tests partition movement and compression based on ILM policies
-- =============================================================================
-- For tables created by test_tiered_partitioning.sql:
--   - TEST_SALES_3Y (FACT_TABLE_STANDARD_TIERED)
--   - TEST_SALES_12Y (FACT_TABLE_STANDARD_TIERED)
--   - TEST_EVENTS_90D (EVENTS_SHORT_RETENTION_TIERED)
--
-- This script:
--   1. Adds test data to partitions
--   2. Simulates partition aging by updating access tracking
--   3. Evaluates ILM policies
--   4. Executes partition movements (SIMULATE then REAL)
--   5. Verifies results
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000

PROMPT ========================================
PROMPT ILM Movement Test Suite
PROMPT ========================================
PROMPT

-- =============================================================================
-- SECTION 1: Initial State
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 1: Initial Partition State
PROMPT ========================================
PROMPT

PROMPT --- TEST_SALES_3Y ---
SELECT
    'TEST_SALES_3Y' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_3Y'
AND num_rows > 0
ORDER BY partition_position;

PROMPT
PROMPT --- TEST_SALES_12Y ---
SELECT
    'TEST_SALES_12Y' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_12Y'
AND num_rows > 0
ORDER BY partition_position;

PROMPT
PROMPT --- TEST_EVENTS_90D ---
SELECT
    'TEST_EVENTS_90D' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_EVENTS_90D'
AND num_rows > 0
ORDER BY partition_position;

-- =============================================================================
-- SECTION 2: Add Test Data to Different Partitions
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 2: Adding Test Data
PROMPT ========================================
PROMPT

-- Add data to TEST_SALES_3Y (various years)
PROMPT Adding data to TEST_SALES_3Y...

INSERT INTO cmr.TEST_SALES_3Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 1000000,
    DATE '2021-06-15' + TRUNC(DBMS_RANDOM.VALUE(0, 180)),  -- Old data (2021)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 3000;

INSERT INTO cmr.TEST_SALES_3Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 2000000,
    DATE '2022-03-15' + TRUNC(DBMS_RANDOM.VALUE(0, 180)),  -- Medium age (2022)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 5000;

INSERT INTO cmr.TEST_SALES_3Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 3000000,
    DATE '2024-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 180)),  -- Recent (2024)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 7000;

COMMIT;
DBMS_OUTPUT.PUT_LINE('Added 15,000 rows to TEST_SALES_3Y (2021, 2022, 2024)');

-- Add data to TEST_SALES_12Y (various years)
PROMPT Adding data to TEST_SALES_12Y...

INSERT INTO cmr.TEST_SALES_12Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 10000000,
    DATE '2018-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 365)),  -- Very old (2018)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 2000;

INSERT INTO cmr.TEST_SALES_12Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 11000000,
    DATE '2020-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 365)),  -- Old (2020)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 3000;

INSERT INTO cmr.TEST_SALES_12Y (sale_id, sale_date, product_name, amount)
SELECT
    ROWNUM + 12000000,
    DATE '2023-01-01' + TRUNC(DBMS_RANDOM.VALUE(0, 365)),  -- Medium (2023)
    'PRODUCT_' || MOD(ROWNUM, 100),
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2)
FROM dual
CONNECT BY LEVEL <= 5000;

COMMIT;
DBMS_OUTPUT.PUT_LINE('Added 10,000 rows to TEST_SALES_12Y (2018, 2020, 2023)');

-- Add data to TEST_EVENTS_90D
PROMPT Adding data to TEST_EVENTS_90D...

INSERT INTO cmr.TEST_EVENTS_90D (event_id, event_timestamp, event_type, user_id)
SELECT
    ROWNUM + 5000000,
    TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(DBMS_RANDOM.VALUE(0, 90), 'DAY'),
    'TYPE_' || MOD(ROWNUM, 10),
    'USER_' || MOD(ROWNUM, 1000)
FROM dual
CONNECT BY LEVEL <= 5000;

COMMIT;
DBMS_OUTPUT.PUT_LINE('Added 5,000 rows to TEST_EVENTS_90D');

-- Gather statistics
PROMPT
PROMPT Gathering table statistics...

BEGIN
    DBMS_STATS.GATHER_TABLE_STATS('CMR', 'TEST_SALES_3Y', granularity => 'ALL');
    DBMS_STATS.GATHER_TABLE_STATS('CMR', 'TEST_SALES_12Y', granularity => 'ALL');
    DBMS_STATS.GATHER_TABLE_STATS('CMR', 'TEST_EVENTS_90D', granularity => 'ALL');
    DBMS_OUTPUT.PUT_LINE('Statistics gathered successfully');
END;
/

-- =============================================================================
-- SECTION 3: Simulate Partition Aging
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 3: Simulating Partition Aging
PROMPT ========================================
PROMPT

-- Update access tracking for TEST_SALES_3Y
-- Make 2021 partitions appear 61 months old (> 5 years) → should move to COLD
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = ADD_MONTHS(SYSDATE, -61),
    last_read_time = ADD_MONTHS(SYSDATE, -61)
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_3Y'
AND partition_name LIKE 'P_2021%';

DBMS_OUTPUT.PUT_LINE('Updated TEST_SALES_3Y 2021 partitions: 61 months old (COLD tier)');

-- Make 2022 partitions appear 25 months old (> 2 years) → should move to WARM
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = ADD_MONTHS(SYSDATE, -25),
    last_read_time = ADD_MONTHS(SYSDATE, -25)
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_3Y'
AND partition_name LIKE 'P_2022%';

DBMS_OUTPUT.PUT_LINE('Updated TEST_SALES_3Y 2022 partitions: 25 months old (WARM tier)');

-- Update access tracking for TEST_SALES_12Y
-- Make 2018 partitions appear 72 months old (6 years) → COLD
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = ADD_MONTHS(SYSDATE, -72),
    last_read_time = ADD_MONTHS(SYSDATE, -72)
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_12Y'
AND partition_name LIKE 'P_2018%';

DBMS_OUTPUT.PUT_LINE('Updated TEST_SALES_12Y 2018 partitions: 72 months old (COLD tier)');

-- Make 2020 partitions appear 48 months old (4 years) → WARM
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = ADD_MONTHS(SYSDATE, -48),
    last_read_time = ADD_MONTHS(SYSDATE, -48)
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_12Y'
AND partition_name LIKE 'P_2020%';

DBMS_OUTPUT.PUT_LINE('Updated TEST_SALES_12Y 2020 partitions: 48 months old (WARM tier)');

-- Update access tracking for TEST_EVENTS_90D
-- Make some partitions appear 31+ days old → WARM tier (age_days: 30)
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = SYSDATE - 35,
    last_read_time = SYSDATE - 35
WHERE table_owner = 'CMR'
AND table_name = 'TEST_EVENTS_90D'
AND partition_name <= 'P_20240215';  -- Earlier partitions

DBMS_OUTPUT.PUT_LINE('Updated TEST_EVENTS_90D early partitions: 35 days old (WARM tier)');

-- Make some partitions appear 91+ days old → COLD tier / DROP (age_days: 90)
UPDATE cmr.dwh_ilm_partition_access
SET last_write_time = SYSDATE - 95,
    last_read_time = SYSDATE - 95
WHERE table_owner = 'CMR'
AND table_name = 'TEST_EVENTS_90D'
AND partition_name <= 'P_20240110';  -- Very old partitions

DBMS_OUTPUT.PUT_LINE('Updated TEST_EVENTS_90D oldest partitions: 95 days old (COLD/DROP)');

COMMIT;

-- Verify partition ages
PROMPT
PROMPT Partition Ages After Simulation:

SELECT
    table_name,
    partition_name,
    temperature,
    TRUNC(MONTHS_BETWEEN(SYSDATE, last_write_time)) AS age_months,
    TRUNC(SYSDATE - last_write_time) AS age_days,
    last_write_time
FROM cmr.dwh_ilm_partition_access
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
ORDER BY table_name, partition_name;

-- =============================================================================
-- SECTION 4: Evaluate ILM Policies
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 4: Evaluating ILM Policies
PROMPT ========================================
PROMPT

-- Evaluate TEST_SALES_3Y
PROMPT Evaluating TEST_SALES_3Y...
BEGIN
    cmr.pck_dwh_ilm_policy_engine.evaluate_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_3Y'
    );
END;
/

-- Evaluate TEST_SALES_12Y
PROMPT Evaluating TEST_SALES_12Y...
BEGIN
    cmr.pck_dwh_ilm_policy_engine.evaluate_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_12Y'
    );
END;
/

-- Evaluate TEST_EVENTS_90D
PROMPT Evaluating TEST_EVENTS_90D...
BEGIN
    cmr.pck_dwh_ilm_policy_engine.evaluate_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_EVENTS_90D'
    );
END;
/

-- Show evaluation results
PROMPT
PROMPT Recommended Actions:

SELECT
    table_name,
    partition_name,
    recommended_action,
    action_reason,
    target_tablespace,
    target_compression,
    priority,
    status
FROM cmr.dwh_ilm_evaluation_queue
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
ORDER BY table_name, priority, partition_name;

-- =============================================================================
-- SECTION 5: Execute ILM Policies (SIMULATE)
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 5: Executing ILM (SIMULATE MODE)
PROMPT ========================================
PROMPT

-- Execute TEST_SALES_3Y (simulate)
PROMPT Simulating TEST_SALES_3Y movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_3Y',
        p_max_actions => 20,
        p_simulate => TRUE
    );
END;
/

-- Execute TEST_SALES_12Y (simulate)
PROMPT Simulating TEST_SALES_12Y movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_12Y',
        p_max_actions => 20,
        p_simulate => TRUE
    );
END;
/

-- Execute TEST_EVENTS_90D (simulate)
PROMPT Simulating TEST_EVENTS_90D movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_EVENTS_90D',
        p_max_actions => 20,
        p_simulate => TRUE
    );
END;
/

-- Show simulation results
PROMPT
PROMPT Simulation Results:

SELECT
    table_name,
    partition_name,
    action_type,
    status,
    SUBSTR(sql_statement, 1, 100) AS sql_preview
FROM cmr.dwh_ilm_execution_log
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
ORDER BY execution_id DESC
FETCH FIRST 50 ROWS ONLY;

-- =============================================================================
-- SECTION 6: Execute ILM Policies (REAL)
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 6: Executing ILM (REAL MODE)
PROMPT ========================================
PROMPT
PROMPT WARNING: This will actually move partitions!
PROMPT Review simulation results above before proceeding.
PROMPT
PROMPT Press Enter to continue or Ctrl+C to cancel...
PAUSE

-- Execute TEST_SALES_3Y (REAL)
PROMPT Executing TEST_SALES_3Y movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_3Y',
        p_max_actions => 20,
        p_simulate => FALSE
    );
END;
/

-- Execute TEST_SALES_12Y (REAL)
PROMPT Executing TEST_SALES_12Y movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_SALES_12Y',
        p_max_actions => 20,
        p_simulate => FALSE
    );
END;
/

-- Execute TEST_EVENTS_90D (REAL)
PROMPT Executing TEST_EVENTS_90D movements...
BEGIN
    cmr.pck_dwh_ilm_execution_engine.execute_policies(
        p_table_owner => 'CMR',
        p_table_name => 'TEST_EVENTS_90D',
        p_max_actions => 20,
        p_simulate => FALSE
    );
END;
/

-- =============================================================================
-- SECTION 7: Verify Results
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 7: Verification
PROMPT ========================================
PROMPT

-- Show execution results
PROMPT Execution Log:

SELECT
    table_name,
    partition_name,
    action_type,
    status,
    start_time,
    end_time,
    ROUND((end_time - start_time) * 24 * 60, 2) AS duration_min,
    error_message
FROM cmr.dwh_ilm_execution_log
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
AND start_time > SYSDATE - 1/24  -- Last hour
ORDER BY execution_id DESC;

-- Show final partition state
PROMPT
PROMPT Final Partition State:

PROMPT
PROMPT --- TEST_SALES_3Y ---
SELECT
    'TEST_SALES_3Y' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows,
    ROUND(blocks * 8 / 1024, 2) AS size_mb
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_3Y'
AND num_rows > 0
ORDER BY partition_position;

PROMPT
PROMPT --- TEST_SALES_12Y ---
SELECT
    'TEST_SALES_12Y' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows,
    ROUND(blocks * 8 / 1024, 2) AS size_mb
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_SALES_12Y'
AND num_rows > 0
ORDER BY partition_position;

PROMPT
PROMPT --- TEST_EVENTS_90D ---
SELECT
    'TEST_EVENTS_90D' AS table_name,
    partition_name,
    tablespace_name,
    compression,
    num_rows,
    ROUND(blocks * 8 / 1024, 2) AS size_mb
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name = 'TEST_EVENTS_90D'
AND num_rows > 0
ORDER BY partition_position;

-- =============================================================================
-- SECTION 8: Summary Report
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT SECTION 8: Summary Report
PROMPT ========================================
PROMPT

-- Summary by table and tier
SELECT
    table_name,
    tablespace_name,
    compression,
    COUNT(*) AS partition_count,
    SUM(num_rows) AS total_rows,
    ROUND(SUM(blocks * 8 / 1024), 2) AS total_mb
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
AND num_rows > 0
GROUP BY table_name, tablespace_name, compression
ORDER BY table_name, tablespace_name;

-- Actions executed summary
PROMPT
PROMPT Actions Executed:

SELECT
    table_name,
    action_type,
    COUNT(*) AS action_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
FROM cmr.dwh_ilm_execution_log
WHERE table_owner = 'CMR'
AND table_name IN ('TEST_SALES_3Y', 'TEST_SALES_12Y', 'TEST_EVENTS_90D')
AND start_time > SYSDATE - 1/24
GROUP BY table_name, action_type
ORDER BY table_name, action_type;

PROMPT
PROMPT ========================================
PROMPT Test Complete
PROMPT ========================================
PROMPT
PROMPT Expected Results:
PROMPT - TEST_SALES_3Y:
PROMPT   * 2021 partitions in TBS_COLD with OLTP compression
PROMPT   * 2022 partitions in TBS_WARM with BASIC compression
PROMPT   * 2024 partitions in TBS_HOT with no compression
PROMPT
PROMPT - TEST_SALES_12Y:
PROMPT   * 2018 partitions in TBS_COLD with OLTP compression
PROMPT   * 2020 partitions in TBS_WARM with BASIC compression
PROMPT   * 2023+ partitions in TBS_HOT with no compression
PROMPT
PROMPT - TEST_EVENTS_90D:
PROMPT   * Old partitions (90+ days) may be dropped
PROMPT   * Medium partitions (30+ days) in TBS_WARM with QUERY HIGH compression
PROMPT   * Recent partitions in TBS_HOT
PROMPT
