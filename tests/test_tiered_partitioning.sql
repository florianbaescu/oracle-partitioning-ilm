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
        pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL (simulate mode)
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

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
        pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL (12 YEARS)');
    DBMS_OUTPUT.PUT_LINE('========================================');
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

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
        pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    DBMS_OUTPUT.PUT_LINE('Applying recommendations...');
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING TIERED PARTITION DDL (EVENTS)');
    DBMS_OUTPUT.PUT_LINE('========================================');
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

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
        pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    END IF;

    -- Apply recommendations
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL (should use uniform partitioning)
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('GENERATING UNIFORM PARTITION DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

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
