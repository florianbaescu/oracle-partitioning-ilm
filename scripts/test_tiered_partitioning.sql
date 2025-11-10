-- =============================================================================
-- Comprehensive Test Suite for Tiered Partitioning
-- Tests end-to-end functionality of ILM-aware partition generation
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

-- Drop test tables if they exist
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.test_sales_3y PURGE';
    DBMS_OUTPUT.PUT_LINE('Dropped existing test_sales_3y');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('test_sales_3y does not exist');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.test_sales_12y PURGE';
    DBMS_OUTPUT.PUT_LINE('Dropped existing test_sales_12y');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('test_sales_12y does not exist');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.test_events_90d PURGE';
    DBMS_OUTPUT.PUT_LINE('Dropped existing test_events_90d');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('test_events_90d does not exist');
        ELSE
            RAISE;
        END IF;
END;
/

PROMPT
PROMPT Creating test table: test_sales_3y (3 years of data)...

CREATE TABLE cmr.test_sales_3y AS
SELECT
    ROWNUM as sale_id,
    TRUNC(SYSDATE) - LEVEL as sale_date,
    'PRODUCT_' || MOD(LEVEL, 100) as product_name,
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 1095;

PROMPT Created test_sales_3y with 1,095 rows (3 years)
PROMPT

PROMPT Creating test table: test_sales_12y (12 years of data)...

CREATE TABLE cmr.test_sales_12y AS
SELECT
    ROWNUM as sale_id,
    TO_DATE('2013-01-01', 'YYYY-MM-DD') + LEVEL as sale_date,
    'PRODUCT_' || MOD(LEVEL, 100) as product_name,
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 4380;

PROMPT Created test_sales_12y with 4,380 rows (12 years)
PROMPT

PROMPT Creating test table: test_events_90d (90 days of data)...

CREATE TABLE cmr.test_events_90d AS
SELECT
    ROWNUM as event_id,
    TRUNC(SYSDATE) - LEVEL as event_date,
    'EVENT_' || MOD(LEVEL, 50) as event_type,
    'User_' || MOD(LEVEL, 1000) as user_id
FROM dual
CONNECT BY LEVEL <= 90;

PROMPT Created test_events_90d with 90 rows (90 days)
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
BEGIN
    -- Create migration task with tiered template
    INSERT INTO cmr.dwh_migration_tasks (
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
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task_id: ' || v_task_id);

    -- Run analysis
    DBMS_OUTPUT.PUT_LINE('Running analysis...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

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

    -- Query execution log
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Execution Log:');
    FOR log_rec IN (
        SELECT step_name, status, duration_seconds
        FROM cmr.dwh_migration_execution_log
        WHERE task_id = v_task_id
        ORDER BY step_number
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || log_rec.step_name || ': ' || log_rec.status ||
                           ' (' || ROUND(log_rec.duration_seconds, 2) || 's)');
    END LOOP;

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
BEGIN
    -- Create migration task
    INSERT INTO cmr.dwh_migration_tasks (
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
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task_id: ' || v_task_id);

    -- Run analysis
    DBMS_OUTPUT.PUT_LINE('Running analysis...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

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
BEGIN
    -- Create migration task with events template
    INSERT INTO cmr.dwh_migration_tasks (
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
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task_id: ' || v_task_id);

    -- Run analysis
    DBMS_OUTPUT.PUT_LINE('Running analysis...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

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
BEGIN
    -- Create migration task with NON-TIERED template
    INSERT INTO cmr.dwh_migration_tasks (
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
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task_id: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('Using NON-TIERED template: FACT_TABLE_STANDARD');

    -- Run analysis
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

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
PROMPT Execution log summary:
SELECT
    t.task_name,
    l.step_name,
    l.status,
    ROUND(l.duration_seconds, 2) as duration_sec
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE t.task_name LIKE 'Test%'
  AND l.step_name LIKE '%Partition%'
ORDER BY t.task_id, l.step_number;

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
