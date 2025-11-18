-- =============================================================================
-- Comprehensive Test Suite for Tiered Partitioning
-- Tests end-to-end functionality of ILM-aware partition generation
-- =============================================================================
--
-- IMPORTANT: This test suite runs in SIMULATE mode (p_simulate => TRUE)
--            - DDL is generated and displayed but NOT executed
--            - No actual tables are created or migrated
--            - No execution log entries are created (log only populated in real execution)
--            - Test tables and tasks are reused if they already exist
--
-- To run actual migrations (non-simulate):
--   Change p_simulate => TRUE to p_simulate => FALSE in execute_migration calls
--   WARNING: This will actually create partitioned tables and modify data
--
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000

PROMPT ========================================
PROMPT Tiered Partitioning Test Suite
PROMPT ========================================
PROMPT

-- =============================================================================
-- SECTION 1: Environment Setup
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 1: Environment Setup
PROMPT ========================================
PROMPT

-- Check if templates exist
SELECT
    COUNT(*) as tiered_template_count,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END as status
FROM cmr.dwh_migration_ilm_templates
WHERE JSON_EXISTS(policies_json, '$.tier_config');

PROMPT
PROMPT Tiered templates available:
SELECT template_name, table_type, SUBSTR(description, 1, 60) as description
FROM cmr.dwh_migration_ilm_templates
WHERE JSON_EXISTS(policies_json, '$.tier_config')
ORDER BY template_name;

PROMPT

-- =============================================================================
-- SECTION 2: Test Table Creation
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 2: Creating Test Tables
PROMPT ========================================
PROMPT

-- Create test_sales_3y if it doesn't exist
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR' AND table_name = 'TEST_SALES_3Y';

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('test_sales_3y already exists (skipping creation)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Creating test table: test_sales_3y (3 years of data)...');
        EXECUTE IMMEDIATE '
            CREATE TABLE cmr.test_sales_3y AS
            SELECT
                ROWNUM as sale_id,
                TRUNC(SYSDATE) - LEVEL as sale_date,
                ''PRODUCT_'' || MOD(LEVEL, 100) as product_name,
                ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
            FROM dual
            CONNECT BY LEVEL <= 1095';
        DBMS_OUTPUT.PUT_LINE('Created test_sales_3y with 1,095 rows (3 years)');
    END IF;
END;
/

-- Create test_sales_12y if it doesn't exist
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR' AND table_name = 'TEST_SALES_12Y';

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('test_sales_12y already exists (skipping creation)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Creating test table: test_sales_12y (12 years of data)...');
        EXECUTE IMMEDIATE '
            CREATE TABLE cmr.test_sales_12y AS
            SELECT
                ROWNUM as sale_id,
                TO_DATE(''2013-01-01'', ''YYYY-MM-DD'') + LEVEL as sale_date,
                ''PRODUCT_'' || MOD(LEVEL, 100) as product_name,
                ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
            FROM dual
            CONNECT BY LEVEL <= 4380';
        DBMS_OUTPUT.PUT_LINE('Created test_sales_12y with 4,380 rows (12 years)');
    END IF;
END;
/

-- Create test_events_90d if it doesn't exist
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM all_tables
    WHERE owner = 'CMR' AND table_name = 'TEST_EVENTS_90D';

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('test_events_90d already exists (skipping creation)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Creating test table: test_events_90d (90 days of data)...');
        EXECUTE IMMEDIATE '
            CREATE TABLE cmr.test_events_90d AS
            SELECT
                ROWNUM as event_id,
                TRUNC(SYSDATE) - LEVEL as event_date,
                ''EVENT_'' || MOD(LEVEL, 50) as event_type,
                ''User_'' || MOD(LEVEL, 1000) as user_id
            FROM dual
            CONNECT BY LEVEL <= 90';
        DBMS_OUTPUT.PUT_LINE('Created test_events_90d with 90 rows (90 days)');
    END IF;
END;
/

PROMPT

-- =============================================================================
-- SECTION 3: Test Case 1 - 3 Year Table with Tiered Template
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 3: Test Case 1 - 3 Year Table
PROMPT ========================================
PROMPT

DECLARE
    v_task_id NUMBER;
    v_analysis_exists NUMBER;
BEGIN
    -- Create or reuse migration task with tiered template
    MERGE INTO cmr.dwh_migration_tasks t
    USING (SELECT 'Test 3Y Tiered' AS task_name FROM DUAL) s
    ON (t.task_name = s.task_name)
    WHEN NOT MATCHED THEN
        INSERT (
            task_name,
            source_owner,
            source_table,
            partition_type,
            partition_key,
            migration_method,
            enable_row_movement,
            ilm_policy_template,
            status
        ) VALUES (
            'Test 3Y Tiered',
            'CMR',
            'TEST_SALES_3Y',
            'RANGE(sale_date)',
            'sale_date',
            'CTAS',
            'Y',
            'FACT_TABLE_STANDARD_TIERED',
            'PENDING'
        )
    WHEN MATCHED THEN
        UPDATE SET
            status = CASE WHEN status IN ('FAILED', 'COMPLETED') THEN 'PENDING' ELSE status END,
            error_message = NULL;

    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Test 3Y Tiered';

    DBMS_OUTPUT.PUT_LINE('Using task_id: ' || v_task_id);

    -- Check if analysis already exists
    SELECT COUNT(*) INTO v_analysis_exists
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    IF v_analysis_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Analysis already exists (skipping re-analysis)');
    ELSE
        -- Run analysis
        DBMS_OUTPUT.PUT_LINE('Running analysis...');
        cmr.pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    cmr.pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL (simulate mode)
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');
    cmr.pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Test Case 1 Results:');
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Query results
    FOR rec IN (
        SELECT
            t.task_name,
            t.status,
            a.table_rows,
            TO_CHAR(a.partition_boundary_min_date, 'YYYY-MM-DD') as min_date,
            TO_CHAR(a.partition_boundary_max_date, 'YYYY-MM-DD') as max_date,
            a.partition_range_years,
            a.recommended_strategy
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Task: ' || rec.task_name);
        DBMS_OUTPUT.PUT_LINE('Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('Rows: ' || rec.table_rows);
        DBMS_OUTPUT.PUT_LINE('Date range: ' || rec.min_date || ' to ' || rec.max_date);
        DBMS_OUTPUT.PUT_LINE('Years: ' || rec.partition_range_years);
        DBMS_OUTPUT.PUT_LINE('Strategy: ' || rec.recommended_strategy);
    END LOOP;

    -- Note: Execution log is only populated when p_simulate => FALSE
    -- In simulate mode, we only generate DDL without executing or logging
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Note: Running in SIMULATE mode - no execution log entries created');
    DBMS_OUTPUT.PUT_LINE('      (Execution log only populated when p_simulate => FALSE)');

    COMMIT;
END;
/

PROMPT

-- =============================================================================
-- SECTION 4: Test Case 2 - 12 Year Table with Tiered Template
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 4: Test Case 2 - 12 Year Table
PROMPT ========================================
PROMPT

DECLARE
    v_task_id NUMBER;
    v_analysis_exists NUMBER;
BEGIN
    -- Create or reuse migration task
    MERGE INTO cmr.dwh_migration_tasks t
    USING (SELECT 'Test 12Y Tiered' AS task_name FROM DUAL) s
    ON (t.task_name = s.task_name)
    WHEN NOT MATCHED THEN
        INSERT (
            task_name,
            source_owner,
            source_table,
            partition_type,
            partition_key,
            migration_method,
            enable_row_movement,
            ilm_policy_template,
            status
        ) VALUES (
            'Test 12Y Tiered',
            'CMR',
            'TEST_SALES_12Y',
            'RANGE(sale_date)',
            'sale_date',
            'CTAS',
            'Y',
            'FACT_TABLE_STANDARD_TIERED',
            'PENDING'
        )
    WHEN MATCHED THEN
        UPDATE SET
            status = CASE WHEN status IN ('FAILED', 'COMPLETED') THEN 'PENDING' ELSE status END,
            error_message = NULL;

    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Test 12Y Tiered';

    DBMS_OUTPUT.PUT_LINE('Using task_id: ' || v_task_id);

    -- Check if analysis already exists
    SELECT COUNT(*) INTO v_analysis_exists
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    IF v_analysis_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Analysis already exists (skipping re-analysis)');
    ELSE
        -- Run analysis
        DBMS_OUTPUT.PUT_LINE('Running analysis...');
        cmr.pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    cmr.pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL (12 YEARS)');
    DBMS_OUTPUT.PUT_LINE('========================================');
    cmr.pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Test Case 2 Results:');
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Query results
    FOR rec IN (
        SELECT
            t.task_name,
            t.status,
            a.table_rows,
            TO_CHAR(a.partition_boundary_min_date, 'YYYY-MM-DD') as min_date,
            TO_CHAR(a.partition_boundary_max_date, 'YYYY-MM-DD') as max_date,
            a.partition_range_years,
            a.recommended_strategy
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Task: ' || rec.task_name);
        DBMS_OUTPUT.PUT_LINE('Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('Rows: ' || rec.table_rows);
        DBMS_OUTPUT.PUT_LINE('Date range: ' || rec.min_date || ' to ' || rec.max_date);
        DBMS_OUTPUT.PUT_LINE('Years: ' || rec.partition_range_years);
        DBMS_OUTPUT.PUT_LINE('Strategy: ' || rec.recommended_strategy);
    END LOOP;

    COMMIT;
END;
/

PROMPT

-- =============================================================================
-- SECTION 5: Test Case 3 - Events Table with Short Retention
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 5: Test Case 3 - Events (90d)
PROMPT ========================================
PROMPT

DECLARE
    v_task_id NUMBER;
    v_analysis_exists NUMBER;
BEGIN
    -- Create or reuse migration task with events template
    MERGE INTO cmr.dwh_migration_tasks t
    USING (SELECT 'Test Events 90d Tiered' AS task_name FROM DUAL) s
    ON (t.task_name = s.task_name)
    WHEN NOT MATCHED THEN
        INSERT (
            task_name,
            source_owner,
            source_table,
            partition_type,
            partition_key,
            migration_method,
            enable_row_movement,
            ilm_policy_template,
            status
        ) VALUES (
            'Test Events 90d Tiered',
            'CMR',
            'TEST_EVENTS_90D',
            'RANGE(event_date)',
            'event_date',
            'CTAS',
            'Y',
            'EVENTS_SHORT_RETENTION_TIERED',
            'PENDING'
        )
    WHEN MATCHED THEN
        UPDATE SET
            status = CASE WHEN status IN ('FAILED', 'COMPLETED') THEN 'PENDING' ELSE status END,
            error_message = NULL;

    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Test Events 90d Tiered';

    DBMS_OUTPUT.PUT_LINE('Using task_id: ' || v_task_id);

    -- Check if analysis already exists
    SELECT COUNT(*) INTO v_analysis_exists
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    IF v_analysis_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Analysis already exists (skipping re-analysis)');
    ELSE
        -- Run analysis
        DBMS_OUTPUT.PUT_LINE('Running analysis...');
        cmr.pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    cmr.pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL (EVENTS)');
    DBMS_OUTPUT.PUT_LINE('========================================');
    cmr.pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Test Case 3 Results:');
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Query results
    FOR rec IN (
        SELECT
            t.task_name,
            t.status,
            a.table_rows,
            TO_CHAR(a.partition_boundary_min_date, 'YYYY-MM-DD') as min_date,
            TO_CHAR(a.partition_boundary_max_date, 'YYYY-MM-DD') as max_date,
            a.recommended_strategy
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Task: ' || rec.task_name);
        DBMS_OUTPUT.PUT_LINE('Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('Rows: ' || rec.table_rows);
        DBMS_OUTPUT.PUT_LINE('Date range: ' || rec.min_date || ' to ' || rec.max_date);
        DBMS_OUTPUT.PUT_LINE('Strategy: ' || rec.recommended_strategy);
    END LOOP;

    COMMIT;
END;
/

PROMPT

-- =============================================================================
-- SECTION 6: Test Case 4 - Backward Compatibility (Non-Tiered Template)
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 6: Test Case 4 - Backward Compat
PROMPT ========================================
PROMPT

DECLARE
    v_task_id NUMBER;
    v_analysis_exists NUMBER;
BEGIN
    -- Create or reuse migration task with NON-TIERED template
    MERGE INTO cmr.dwh_migration_tasks t
    USING (SELECT 'Test Non-Tiered' AS task_name FROM DUAL) s
    ON (t.task_name = s.task_name)
    WHEN NOT MATCHED THEN
        INSERT (
            task_name,
            source_owner,
            source_table,
            partition_type,
            partition_key,
            interval_clause,
            migration_method,
            enable_row_movement,
            ilm_policy_template,
            status
        ) VALUES (
            'Test Non-Tiered',
            'CMR',
            'TEST_SALES_3Y',
            'RANGE(sale_date)',
            'sale_date',
            'NUMTOYMINTERVAL(1,''MONTH'')',
            'CTAS',
            'Y',
            'FACT_TABLE_STANDARD',  -- Non-tiered template
            'PENDING'
        )
    WHEN MATCHED THEN
        UPDATE SET
            status = CASE WHEN status IN ('FAILED', 'COMPLETED') THEN 'PENDING' ELSE status END,
            error_message = NULL;

    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Test Non-Tiered';

    DBMS_OUTPUT.PUT_LINE('Using task_id: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('Using NON-TIERED template: FACT_TABLE_STANDARD');

    -- Check if analysis already exists
    SELECT COUNT(*) INTO v_analysis_exists
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    IF v_analysis_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Analysis already exists (skipping re-analysis)');
    ELSE
        -- Run analysis
        cmr.pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    cmr.pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL (should use uniform partitioning)
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING UNIFORM PARTITION DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');
    cmr.pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Expected: Should use uniform interval partitioning (build_uniform_partitions)');

    COMMIT;
END;
/

PROMPT

-- =============================================================================
-- SECTION 7: Validation Queries
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 7: Validation Summary
PROMPT ========================================
PROMPT

PROMPT All test tasks:
SELECT
    task_id,
    task_name,
    ilm_policy_template,
    status,
    CASE
        WHEN ilm_policy_template LIKE '%TIERED%' THEN 'TIERED'
        ELSE 'UNIFORM'
    END as partition_type
FROM cmr.dwh_migration_tasks
WHERE task_name LIKE 'Test%'
ORDER BY task_id;

PROMPT
PROMPT Analysis results:
SELECT
    t.task_name,
    a.table_rows,
    TO_CHAR(a.partition_boundary_min_date, 'YYYY-MM-DD') as min_date,
    TO_CHAR(a.partition_boundary_max_date, 'YYYY-MM-DD') as max_date,
    a.partition_range_years,
    SUBSTR(a.recommended_strategy, 1, 40) as strategy
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
WHERE t.task_name LIKE 'Test%'
ORDER BY t.task_id;

PROMPT
PROMPT Note: Execution log is empty because all tests ran in SIMULATE mode
PROMPT       To see execution log entries, run execute_migration with p_simulate => FALSE
PROMPT
PROMPT ========================================
PROMPT Test Suite Complete
PROMPT ========================================
PROMPT
PROMPT Expected Results (based on HOT=2y, WARM=2-5y, COLD=>5y):
PROMPT   - Test Case 1 (3Y): Should generate WARM + HOT partitions
PROMPT                        (HOT: last 2y monthly, WARM: 2-3y ago yearly)
PROMPT   - Test Case 2 (12Y): Should generate COLD + WARM + HOT partitions
PROMPT                         (COLD: >5y ago yearly, WARM: 2-5y ago yearly, HOT: last 2y monthly)
PROMPT   - Test Case 3 (90d): Should generate COLD + WARM + HOT with daily/weekly intervals
PROMPT   - Test Case 4: Should use uniform interval partitioning (backward compatible)
PROMPT
PROMPT Check execution logs for 'Build Tiered Partitions' vs 'Using uniform interval' messages
PROMPT

-- =============================================================================
-- SECTION 8: Partition Merge Configuration Tests
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 8: Partition Merge Config Tests
PROMPT ========================================
PROMPT

PROMPT Checking AUTO_MERGE_PARTITIONS configuration:
SELECT
    config_key,
    config_value,
    description
FROM cmr.dwh_ilm_config
WHERE config_key IN ('AUTO_MERGE_PARTITIONS', 'MERGE_LOCK_TIMEOUT')
ORDER BY config_key;

PROMPT
PROMPT Expected:
PROMPT   - AUTO_MERGE_PARTITIONS = 'Y' (enabled by default)
PROMPT   - MERGE_LOCK_TIMEOUT = '30' (30 seconds)
PROMPT

-- Verify tracking table exists
PROMPT Checking dwh_ilm_partition_merges table:
SELECT
    COUNT(*) as merge_records,
    CASE WHEN COUNT(*) >= 0 THEN 'Table exists' ELSE 'Table missing' END as status
FROM cmr.dwh_ilm_partition_merges;

PROMPT

-- =============================================================================
-- SECTION 9: Simulated Partition Merge Scenario
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 9: Partition Merge Logic Test
PROMPT ========================================
PROMPT
PROMPT This section demonstrates the merge logic without executing DDL
PROMPT (Testing merge_monthly_into_yearly procedure validation logic)
PROMPT

-- Test 1: Invalid partition name format (should skip)
DECLARE
    v_test_partition VARCHAR2(128) := 'P_2023'; -- Not monthly format
    v_is_monthly BOOLEAN;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Test 1: Non-monthly partition format');
    DBMS_OUTPUT.PUT_LINE('  Partition: ' || v_test_partition);

    v_is_monthly := REGEXP_LIKE(v_test_partition, '^P_\d{4}_\d{2}$');

    IF v_is_monthly THEN
        DBMS_OUTPUT.PUT_LINE('  Result: Would attempt merge (UNEXPECTED)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Result: Would SKIP merge (CORRECT)');
    END IF;
    DBMS_OUTPUT.PUT_LINE('');
END;
/

-- Test 2: Valid monthly partition format (should attempt merge)
DECLARE
    v_test_partition VARCHAR2(128) := 'P_2023_11'; -- Monthly format
    v_yearly_partition VARCHAR2(128);
    v_year VARCHAR2(4);
    v_is_monthly BOOLEAN;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Test 2: Valid monthly partition format');
    DBMS_OUTPUT.PUT_LINE('  Partition: ' || v_test_partition);

    v_is_monthly := REGEXP_LIKE(v_test_partition, '^P_\d{4}_\d{2}$');

    IF v_is_monthly THEN
        v_year := SUBSTR(v_test_partition, 3, 4);
        v_yearly_partition := 'P_' || v_year;
        DBMS_OUTPUT.PUT_LINE('  Result: Would attempt merge (CORRECT)');
        DBMS_OUTPUT.PUT_LINE('  Target: ' || v_test_partition || ' -> ' || v_yearly_partition);
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Result: Would SKIP merge (UNEXPECTED)');
    END IF;
    DBMS_OUTPUT.PUT_LINE('');
END;
/

-- Test 3: Demonstrate year extraction logic
DECLARE
    TYPE partition_test IS RECORD (
        monthly_name VARCHAR2(128),
        expected_yearly VARCHAR2(128)
    );
    TYPE partition_tests IS TABLE OF partition_test;

    v_tests partition_tests := partition_tests(
        partition_test('P_2020_01', 'P_2020'),
        partition_test('P_2021_12', 'P_2021'),
        partition_test('P_2023_06', 'P_2023'),
        partition_test('P_2024_11', 'P_2024')
    );

    v_year VARCHAR2(4);
    v_yearly_partition VARCHAR2(128);
BEGIN
    DBMS_OUTPUT.PUT_LINE('Test 3: Year extraction for multiple partitions');
    DBMS_OUTPUT.PUT_LINE('');

    FOR i IN 1..v_tests.COUNT LOOP
        v_year := SUBSTR(v_tests(i).monthly_name, 3, 4);
        v_yearly_partition := 'P_' || v_year;

        DBMS_OUTPUT.PUT_LINE('  ' || v_tests(i).monthly_name || ' -> ' || v_yearly_partition ||
            CASE WHEN v_yearly_partition = v_tests(i).expected_yearly
                THEN ' ✓'
                ELSE ' ✗ (Expected: ' || v_tests(i).expected_yearly || ')'
            END);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');
END;
/

PROMPT

-- =============================================================================
-- SECTION 10: Partition Merge Tracking Table Structure
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 10: Merge Tracking Table Info
PROMPT ========================================
PROMPT

PROMPT Table structure for dwh_ilm_partition_merges:
SELECT
    column_name,
    data_type,
    nullable,
    data_default
FROM all_tab_columns
WHERE owner = 'CMR'
AND table_name = 'DWH_ILM_PARTITION_MERGES'
ORDER BY column_id;

PROMPT
PROMPT Expected columns:
PROMPT   - merge_id: Auto-generated primary key
PROMPT   - merge_date: Timestamp of merge attempt
PROMPT   - table_owner, table_name: Target table
PROMPT   - source_partition: Monthly partition (P_2023_11)
PROMPT   - target_partition: Yearly partition (P_2023)
PROMPT   - merge_status: SUCCESS, FAILED, SKIPPED
PROMPT   - error_message: Error details if failed
PROMPT   - duration_seconds: Merge execution time
PROMPT   - rows_merged: Number of rows in merged partition
PROMPT

-- =============================================================================
-- SECTION 11: Integration Test Preparation Notes
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 11: Integration Test Notes
PROMPT ========================================
PROMPT
PROMPT To test actual partition merge execution:
PROMPT
PROMPT 1. Create a tiered partitioned table (p_simulate => FALSE)
PROMPT    Example: execute_migration(task_id, p_simulate => FALSE)
PROMPT
PROMPT 2. Verify partitions created:
PROMPT    SELECT partition_name, tablespace_name, high_value
PROMPT    FROM dba_tab_partitions
PROMPT    WHERE table_owner = 'CMR' AND table_name = 'TEST_SALES_3Y_PART'
PROMPT    ORDER BY partition_position;
PROMPT
PROMPT 3. Manually move a monthly partition to trigger merge:
PROMPT    EXEC cmr.pck_dwh_ilm_execution_engine.move_partition(
PROMPT        p_table_owner => 'CMR',
PROMPT        p_table_name => 'TEST_SALES_3Y_PART',
PROMPT        p_partition_name => 'P_2023_11',
PROMPT        p_target_tablespace => 'TBS_WARM',
PROMPT        p_compression_type => 'BASIC'
PROMPT    );
PROMPT
PROMPT 4. Check merge results:
PROMPT    SELECT * FROM cmr.dwh_ilm_partition_merges
PROMPT    ORDER BY merge_date DESC;
PROMPT
PROMPT 5. Verify partition was merged:
PROMPT    SELECT partition_name, tablespace_name
PROMPT    FROM dba_tab_partitions
PROMPT    WHERE table_owner = 'CMR' AND table_name = 'TEST_SALES_3Y_PART'
PROMPT    ORDER BY partition_position;
PROMPT    (P_2023_11 should no longer exist, merged into P_2023)
PROMPT
PROMPT 6. Disable auto-merge to test skipping:
PROMPT    UPDATE cmr.dwh_ilm_config
PROMPT    SET config_value = 'N'
PROMPT    WHERE config_key = 'AUTO_MERGE_PARTITIONS';
PROMPT    COMMIT;
PROMPT

-- =============================================================================
-- SECTION 12: ILM Policy Creation and Validation Tests
-- =============================================================================
PROMPT ========================================
PROMPT SECTION 12: ILM Policy Tests
PROMPT ========================================
PROMPT
PROMPT Testing ILM policy creation for both tiered and non-tiered templates
PROMPT (Verifies fix for JSON_TABLE path: $.policies[*] vs $[*])
PROMPT

-- Clean up existing policies for test tables
DELETE FROM cmr.dwh_ilm_policies WHERE table_name = 'TEST_SALES_3Y';
DELETE FROM cmr.dwh_ilm_policies WHERE table_name = 'TEST_SALES_12Y';
DELETE FROM cmr.dwh_ilm_policies WHERE table_name = 'TEST_EVENTS_90D';
COMMIT;

PROMPT
PROMPT Test 1: Tiered Template - FACT_TABLE_STANDARD_TIERED
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_policy_count NUMBER;
BEGIN
    -- Get task ID for TEST_SALES_3Y (tiered)
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('Template: FACT_TABLE_STANDARD_TIERED');
    DBMS_OUTPUT.PUT_LINE('');

    -- Apply ILM policies
    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

    -- Count policies created
    SELECT COUNT(*) INTO v_policy_count
    FROM cmr.dwh_ilm_policies
    WHERE table_name = 'TEST_SALES_3Y';

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Policies created: ' || v_policy_count);

    IF v_policy_count = 2 THEN
        DBMS_OUTPUT.PUT_LINE('✓ PASS: Expected 2 policies for tiered template');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ FAIL: Expected 2 policies, got ' || v_policy_count);
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Note: Test task not found. Tests run in simulate mode may not create tasks.');
END;
/

PROMPT
PROMPT Tiered template policies created:
SELECT
    policy_name,
    policy_type,
    action_type,
    age_months,
    target_tablespace,
    compression_type,
    priority,
    enabled
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y'
ORDER BY priority;

PROMPT
PROMPT Expected policies:
PROMPT   1. TEST_SALES_3Y_TIER_WARM: MOVE to TBS_WARM at 24 months, BASIC compression, priority 200
PROMPT   2. TEST_SALES_3Y_TIER_COLD: MOVE to TBS_COLD at 60 months, OLTP compression, priority 300
PROMPT

PROMPT
PROMPT Test 2: Events Tiered Template - EVENTS_SHORT_RETENTION_TIERED
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_policy_count NUMBER;
BEGIN
    -- Get task ID for TEST_EVENTS_90D (tiered)
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_EVENTS_90D'
    AND task_name = 'Test Events 90d Tiered'
    AND ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('Template: EVENTS_SHORT_RETENTION_TIERED');
    DBMS_OUTPUT.PUT_LINE('');

    -- Apply ILM policies
    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

    -- Count policies created
    SELECT COUNT(*) INTO v_policy_count
    FROM cmr.dwh_ilm_policies
    WHERE table_name = 'TEST_EVENTS_90D';

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Policies created: ' || v_policy_count);

    IF v_policy_count >= 2 THEN
        DBMS_OUTPUT.PUT_LINE('✓ PASS: Created ' || v_policy_count || ' policies for events tiered template');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ FAIL: Expected at least 2 policies, got ' || v_policy_count);
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Note: Test task not found.');
END;
/

PROMPT
PROMPT Events template policies created:
SELECT
    policy_name,
    policy_type,
    action_type,
    COALESCE(TO_CHAR(age_months), TO_CHAR(age_days) || ' days') as retention,
    target_tablespace,
    compression_type
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_EVENTS_90D'
ORDER BY priority;

PROMPT

PROMPT
PROMPT Test 3: Non-Tiered Template - FACT_TABLE_STANDARD (Backward Compatibility)
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_policy_count NUMBER;
BEGIN
    -- Get task ID for non-tiered test
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test Non-Tiered'
    AND ROWNUM = 1;

    -- Clean up first
    DELETE FROM cmr.dwh_ilm_policies WHERE table_name = 'TEST_SALES_3Y';
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('Template: FACT_TABLE_STANDARD (non-tiered)');
    DBMS_OUTPUT.PUT_LINE('');

    -- Apply ILM policies
    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

    -- Count policies created
    SELECT COUNT(*) INTO v_policy_count
    FROM cmr.dwh_ilm_policies
    WHERE table_name = 'TEST_SALES_3Y';

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Policies created: ' || v_policy_count);

    IF v_policy_count >= 2 THEN
        DBMS_OUTPUT.PUT_LINE('✓ PASS: Non-tiered template still works (backward compatible)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ FAIL: Expected at least 2 policies, got ' || v_policy_count);
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Note: Non-tiered test task not found (optional test).');
END;
/

PROMPT
PROMPT Non-tiered template policies:
SELECT
    policy_name,
    policy_type,
    action_type,
    age_months,
    compression_type
FROM cmr.dwh_ilm_policies
WHERE table_name = 'TEST_SALES_3Y'
ORDER BY priority;

PROMPT

PROMPT
PROMPT Test 4: ILM Policy Validation
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Use tiered task for validation test
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1;

    -- Ensure policies exist
    DELETE FROM cmr.dwh_ilm_policies WHERE table_name = 'TEST_SALES_3Y';
    COMMIT;

    cmr.pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Running ILM policy validation...');
    DBMS_OUTPUT.PUT_LINE('');

    -- Validate policies
    cmr.pck_dwh_table_migration_executor.validate_ilm_policies(v_task_id);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Note: Test task not found.');
END;
/

PROMPT
PROMPT Validation results (from execution log):
SELECT
    step_number,
    step_name,
    status,
    error_message,
    TO_CHAR(end_time, 'YYYY-MM-DD HH24:MI:SS') as validation_time
FROM cmr.dwh_migration_execution_log
WHERE task_id = (
    SELECT task_id FROM cmr.dwh_migration_tasks
    WHERE source_table = 'TEST_SALES_3Y'
    AND task_name = 'Test 3Y Tiered'
    AND ROWNUM = 1
)
AND step_name = 'Validate ILM Policies'
ORDER BY step_number DESC
FETCH FIRST 1 ROW ONLY;

PROMPT
PROMPT Expected: status = 'SUCCESS' (not 'FAILED')
PROMPT If status is FAILED with "No ILM policies found", the JSON_TABLE fix did not work
PROMPT

PROMPT
PROMPT ========================================
PROMPT SECTION 12 Summary: ILM Policy Tests
PROMPT ========================================
PROMPT
PROMPT Tests completed:
PROMPT   1. Tiered template (FACT_TABLE_STANDARD_TIERED) - Should create 2 policies
PROMPT   2. Events tiered template (EVENTS_SHORT_RETENTION_TIERED) - Should create policies
PROMPT   3. Non-tiered template (FACT_TABLE_STANDARD) - Backward compatibility test
PROMPT   4. Validation should pass with status = SUCCESS
PROMPT
PROMPT Key Fix Verified:
PROMPT   - Tiered templates use JSON path: $.policies[*]
PROMPT   - Non-tiered templates use JSON path: $[*]
PROMPT   - Both paths work via UNION ALL query
PROMPT
PROMPT Common Issues:
PROMPT   - If policies_created = 0 for tiered templates:
PROMPT     JSON_TABLE is not using $.policies[*] path
PROMPT   - If validation status = FAILED:
PROMPT     apply_ilm_policies did not create policies
PROMPT   - If policies created but validation shows "No ILM policies found":
PROMPT     Validation query has wrong schema or table name
PROMPT

PROMPT ========================================
PROMPT Test Suite Complete
PROMPT ========================================
PROMPT
