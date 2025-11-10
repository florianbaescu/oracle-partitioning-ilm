-- =============================================================================
-- Table Migration Framework - Usage Examples
-- Complete examples of migrating non-partitioned tables to partitioned
-- =============================================================================

-- =============================================================================
-- SECTION 1: VIEWING CANDIDATES FOR MIGRATION
-- =============================================================================

-- View all non-partitioned tables that are good candidates
SELECT * FROM cmr.v_dwh_migration_candidates
WHERE migration_priority IN ('HIGH PRIORITY', 'MEDIUM PRIORITY')
ORDER BY size_mb DESC;

-- View specific table details
SELECT
    table_name,
    num_rows,
    size_mb,
    num_indexes,
    num_constraints,
    migration_priority,
    recommendation_reason
FROM cmr.v_dwh_migration_candidates
WHERE table_name = 'SALES_FACT_STAGING';


-- =============================================================================
-- SECTION 2: CREATING A MIGRATION PROJECT
-- =============================================================================

-- Example 1: Create a migration project
INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
VALUES (
    'Q1_2024_TABLE_PARTITIONING',
    'Migrate key fact tables to partitioned structures for Q1 2024',
    'PLANNING'
);
COMMIT;

-- Get the project ID
DECLARE
    v_project_id NUMBER;
BEGIN
    SELECT project_id INTO v_project_id
    FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

    DBMS_OUTPUT.PUT_LINE('Project ID: ' || v_project_id);
END;
/


-- =============================================================================
-- SECTION 3: CREATING MIGRATION TASKS
-- =============================================================================

-- Example 2: Create migration task with automatic strategy recommendation
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    migration_method,
    use_compression,
    compression_type,
    apply_ilm_policies,
    ilm_policy_template,
    status
)
SELECT
    project_id,
    'Migrate SALES_FACT',
    USER,
    'SALES_FACT',
    'CTAS',
    'Y',
    'QUERY HIGH',
    'Y',
    'FACT_TABLE_STANDARD',
    'PENDING'
FROM cmr.dwh_migration_projects
WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

COMMIT;


-- Example 3: Create migration task with explicit partition strategy
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    interval_clause,
    migration_method,
    use_compression,
    compression_type,
    target_tablespace,
    parallel_degree,
    apply_ilm_policies,
    ilm_policy_template,
    status
)
SELECT
    project_id,
    'Migrate ORDER_FACT',
    USER,
    'ORDER_FACT',
    'RANGE(order_date)',  -- Explicit partition strategy
    'order_date',
    'NUMTOYMINTERVAL(1,''MONTH'')',  -- Monthly interval
    'CTAS',
    'Y',
    'QUERY HIGH',
    'TBS_HOT',
    8,
    'Y',
    'FACT_TABLE_STANDARD',
    'PENDING'
FROM cmr.dwh_migration_projects
WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

COMMIT;


-- Example 4: Composite partitioning (Range-Hash)
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    subpartition_type,
    subpartition_key,
    interval_clause,
    migration_method,
    use_compression,
    compression_type,
    status
)
VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate WEB_EVENTS',
    USER,
    'WEB_EVENTS',
    'RANGE(event_date)',
    'event_date',
    'HASH(session_id) SUBPARTITIONS 8',
    'session_id',
    'NUMTODSINTERVAL(1,''DAY'')',
    'CTAS',
    'Y',
    'QUERY HIGH',
    'PENDING'
);

COMMIT;


-- Example 5: Large dimension table with hash partitioning
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    migration_method,
    use_compression,
    compression_type,
    apply_ilm_policies,
    ilm_policy_template,
    status
)
VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate CUSTOMER_DIM',
    USER,
    'CUSTOMER_DIM',
    'HASH(customer_id) PARTITIONS 16',
    'customer_id',
    'CTAS',
    'Y',
    'QUERY HIGH',
    'Y',
    'DIMENSION_LARGE',
    'PENDING'
);

COMMIT;


-- =============================================================================
-- SECTION 4: ANALYZING TABLES
-- =============================================================================

-- Example 6: Analyze a single task
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
END;
/


-- Example 7: Analyze all pending tasks in a project
DECLARE
    v_project_id NUMBER;
BEGIN
    SELECT project_id INTO v_project_id
    FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

    pck_dwh_table_migration_analyzer.analyze_all_pending_tasks(v_project_id);
END;
/


-- Example 8: Review analysis results
SELECT
    t.task_name,
    t.source_table,
    a.recommended_strategy,
    a.recommendation_reason,
    a.table_rows,
    ROUND(a.table_size_mb, 2) AS table_size_mb,
    a.estimated_partitions,
    ROUND(a.avg_partition_size_mb, 2) AS avg_partition_size_mb,
    a.estimated_compression_ratio,
    ROUND(a.estimated_space_savings_mb, 2) AS estimated_savings_mb,
    a.complexity_score,
    a.estimated_downtime_minutes
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
WHERE t.project_id = (
    SELECT project_id FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'
)
ORDER BY t.task_id;


-- Example 9: Check for blocking issues
SELECT
    t.task_name,
    t.source_table,
    t.validation_status,
    a.blocking_issues
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
WHERE a.blocking_issues IS NOT NULL
AND DBMS_LOB.GETLENGTH(a.blocking_issues) > 2;


-- =============================================================================
-- SECTION 4A: APPLYING ANALYSIS RECOMMENDATIONS (NEW!)
-- =============================================================================
-- After analyzing tables, use apply_recommendations() to copy the recommended
-- partition strategy from dwh_migration_analysis to dwh_migration_tasks.
-- This is the MISSING LINK between analysis and execution!

-- Example 9A: Apply recommendations to a single task
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    -- This copies recommended_strategy from analysis to partition_type in task
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Task status is now 'READY' and can be executed
END;
/


-- Example 9B: Apply recommendations to all analyzed tasks in a project
DECLARE
    v_project_id NUMBER;
BEGIN
    SELECT project_id INTO v_project_id
    FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

    -- Apply recommendations to all analyzed tasks
    FOR rec IN (
        SELECT task_id, task_name
        FROM cmr.dwh_migration_tasks
        WHERE project_id = v_project_id
        AND status = 'ANALYZED'
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Applying recommendations for: ' || rec.task_name);
        pck_dwh_table_migration_executor.apply_recommendations(rec.task_id);
    END LOOP;
END;
/


-- Example 9C: Review tasks after applying recommendations
SELECT
    task_id,
    task_name,
    source_table,
    partition_type,          -- NOW POPULATED!
    partition_key,           -- NOW POPULATED!
    interval_clause,         -- NOW POPULATED!
    migration_method,        -- NOW POPULATED!
    status,                  -- Should be 'READY'
    validation_status        -- Should be 'READY'
FROM cmr.dwh_migration_tasks
WHERE project_id = (
    SELECT project_id FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'
)
AND status = 'READY'
ORDER BY task_id;


-- =============================================================================
-- SECTION 5: EXECUTING MIGRATIONS
-- =============================================================================

-- Example 10: Execute a single migration
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    pck_dwh_table_migration_executor.execute_migration(v_task_id);
END;
/


-- Example 11: Execute all ready tasks in a project (one at a time for safety)
DECLARE
    v_project_id NUMBER;
BEGIN
    SELECT project_id INTO v_project_id
    FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

    pck_dwh_table_migration_executor.execute_all_ready_tasks(
        p_project_id => v_project_id,
        p_max_tasks => 1  -- Limit to 1 for safety
    );
END;
/


-- Example 12: Execute with manual control
-- Step 1: Review what will be migrated
SELECT
    task_id,
    task_name,
    source_table,
    partition_type,
    migration_method,
    status,
    validation_status
FROM cmr.dwh_migration_tasks
WHERE project_id = (
    SELECT project_id FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'
)
AND status IN ('READY', 'ANALYZED')
AND validation_status = 'READY'
ORDER BY task_id;

-- Step 2: Execute specific task
EXEC pck_dwh_table_migration_executor.execute_migration(1);  -- Replace 1 with actual task_id


-- =============================================================================
-- SECTION 6: MONITORING MIGRATIONS
-- =============================================================================

-- Example 13: View project dashboard
SELECT * FROM cmr.v_dwh_migration_dashboard
ORDER BY created_date DESC;


-- Example 14: View task status
SELECT * FROM cmr.v_dwh_migration_task_status
WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'
ORDER BY task_id;


-- Example 15: View execution logs for a task
SELECT
    step_number,
    step_name,
    step_type,
    status,
    duration_seconds,
    error_message
FROM cmr.dwh_migration_execution_log
WHERE task_id = 1  -- Replace with actual task_id
ORDER BY step_number;


-- Example 16: Monitor running migration
SELECT
    t.task_id,
    t.task_name,
    t.source_table,
    t.status,
    TO_CHAR(t.execution_start, 'YYYY-MM-DD HH24:MI:SS') AS started,
    ROUND((SYSTIMESTAMP - t.execution_start) * 24 * 60, 1) AS running_minutes,
    l.step_number,
    l.step_name,
    l.status AS step_status
FROM cmr.dwh_migration_tasks t
LEFT JOIN (
    SELECT task_id, MAX(step_number) AS step_number
    FROM cmr.dwh_migration_execution_log
    GROUP BY task_id
) ls ON ls.task_id = t.task_id
LEFT JOIN cmr.dwh_migration_execution_log l
    ON l.task_id = t.task_id
    AND l.step_number = ls.step_number
WHERE t.status = 'RUNNING';


-- =============================================================================
-- SECTION 7: POST-MIGRATION VALIDATION
-- =============================================================================

-- Example 17: Verify partition structure
SELECT
    table_name,
    partition_name,
    partition_position,
    high_value,
    num_rows,
    compression,
    tablespace_name
FROM user_tab_partitions
WHERE table_name = 'SALES_FACT'
ORDER BY partition_position DESC
FETCH FIRST 10 ROWS ONLY;


-- Example 18: Compare sizes before and after
SELECT
    task_name,
    source_table,
    ROUND(source_size_mb, 2) AS source_mb,
    ROUND(target_size_mb, 2) AS target_mb,
    ROUND(space_saved_mb, 2) AS saved_mb,
    ROUND((space_saved_mb / NULLIF(source_size_mb, 0)) * 100, 1) AS savings_pct,
    ROUND(duration_seconds / 60, 1) AS duration_min
FROM cmr.dwh_migration_tasks
WHERE status = 'COMPLETED'
ORDER BY space_saved_mb DESC;


-- Example 19: Validate row counts
DECLARE
    v_task_id NUMBER := 1;  -- Replace with actual task_id
BEGIN
    pck_dwh_table_migration_executor.validate_migration(v_task_id);
END;
/


-- =============================================================================
-- SECTION 8: ILM POLICY APPLICATION
-- =============================================================================

-- Example 20: Check ILM policies created for migrated table
SELECT * FROM cmr.dwh_ilm_policies
WHERE table_name = 'SALES_FACT'
ORDER BY priority;


-- Example 21: Manually apply ILM policies after migration
DECLARE
    v_task_id NUMBER := 1;  -- Replace with actual task_id
BEGIN
    pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);
END;
/


-- =============================================================================
-- SECTION 9: ROLLBACK
-- =============================================================================

-- Example 22: Check if rollback is possible
SELECT
    task_id,
    task_name,
    source_table,
    backup_table_name,
    can_rollback,
    status
FROM cmr.dwh_migration_tasks
WHERE can_rollback = 'Y';


-- Example 23: Rollback a migration
DECLARE
    v_task_id NUMBER := 1;  -- Replace with actual task_id
BEGIN
    -- WARNING: This will revert to the old non-partitioned table
    pck_dwh_table_migration_executor.rollback_migration(v_task_id);
END;
/


-- Example 24: Clean up old backup tables (after verifying migration)
DECLARE
    v_sql VARCHAR2(1000);
BEGIN
    FOR rec IN (
        SELECT source_owner, backup_table_name
        FROM cmr.dwh_migration_tasks
        WHERE status = 'COMPLETED'
        AND backup_table_name IS NOT NULL
        AND execution_end < SYSDATE - 30  -- Older than 30 days
    ) LOOP
        v_sql := 'DROP TABLE ' || rec.source_owner || '.' || rec.backup_table_name || ' PURGE';
        DBMS_OUTPUT.PUT_LINE('Dropping: ' || rec.backup_table_name);
        EXECUTE IMMEDIATE v_sql;

        UPDATE cmr.dwh_migration_tasks
        SET can_rollback = 'N', backup_table_name = NULL
        WHERE backup_table_name = rec.backup_table_name;
    END LOOP;

    COMMIT;
END;
/


-- =============================================================================
-- SECTION 10: COMPLETE WORKFLOW EXAMPLE
-- =============================================================================

-- Example 25: Complete migration workflow for a single table
DECLARE
    v_project_id NUMBER;
    v_task_id NUMBER;
BEGIN
    -- Step 1: Create project
    INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
    VALUES ('SINGLE_TABLE_MIGRATION', 'Migrate SALES_FACT_2023', 'PLANNING')
    RETURNING project_id INTO v_project_id;

    -- Step 2: Create task
    INSERT INTO cmr.dwh_migration_tasks (
        project_id, task_name, source_owner, source_table,
        partition_type, partition_key, interval_clause,
        migration_method, use_compression, compression_type,
        apply_ilm_policies, ilm_policy_template, status
    ) VALUES (
        v_project_id, 'Migrate SALES_FACT_2023', USER, 'SALES_FACT_2023',
        'RANGE(sale_date)', 'sale_date', 'NUMTOYMINTERVAL(1,''MONTH'')',
        'CTAS', 'Y', 'QUERY HIGH',
        'Y', 'FACT_TABLE_STANDARD', 'PENDING'
    ) RETURNING task_id INTO v_task_id;

    COMMIT;

    -- Step 3: Analyze
    DBMS_OUTPUT.PUT_LINE('Analyzing table...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    -- Step 4: Review analysis
    FOR rec IN (
        SELECT recommended_strategy, complexity_score, estimated_downtime_minutes
        FROM cmr.dwh_migration_analysis WHERE task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Strategy: ' || rec.recommended_strategy);
        DBMS_OUTPUT.PUT_LINE('Complexity: ' || rec.complexity_score || '/10');
        DBMS_OUTPUT.PUT_LINE('Est. downtime: ' || rec.estimated_downtime_minutes || ' min');
    END LOOP;

    -- Step 5: Execute (comment out until ready)
    -- DBMS_OUTPUT.PUT_LINE('Executing migration...');
    -- pck_dwh_table_migration_executor.execute_migration(v_task_id);

    -- Step 6: Validate (after execution)
    -- pck_dwh_table_migration_executor.validate_migration(v_task_id);

    DBMS_OUTPUT.PUT_LINE('Workflow complete - review and execute when ready');
END;
/


-- =============================================================================
-- SECTION 11: BATCH MIGRATION WORKFLOW
-- =============================================================================

-- Example 26: Migrate multiple tables in sequence
DECLARE
    v_project_id NUMBER;
    v_tables SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST(
        'SALES_FACT', 'ORDER_FACT', 'INVENTORY_FACT'
    );
    v_task_id NUMBER;
BEGIN
    -- Create project
    INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
    VALUES ('BATCH_FACT_MIGRATION', 'Migrate all fact tables', 'PLANNING')
    RETURNING project_id INTO v_project_id;

    -- Create tasks for each table
    FOR i IN 1..v_tables.COUNT LOOP
        INSERT INTO cmr.dwh_migration_tasks (
            project_id, task_name, source_owner, source_table,
            migration_method, use_compression, compression_type,
            apply_ilm_policies, ilm_policy_template, status
        ) VALUES (
            v_project_id, 'Migrate ' || v_tables(i), USER, v_tables(i),
            'CTAS', 'Y', 'QUERY HIGH',
            'Y', 'FACT_TABLE_STANDARD', 'PENDING'
        );
    END LOOP;

    COMMIT;

    -- Analyze all
    DBMS_OUTPUT.PUT_LINE('Analyzing tables...');
    pck_dwh_table_migration_analyzer.analyze_all_pending_tasks(v_project_id);

    -- Review results
    FOR rec IN (
        SELECT t.task_name, t.validation_status, a.complexity_score
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.project_id = v_project_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(rec.task_name || ' - ' ||
                           rec.validation_status || ' (complexity: ' ||
                           rec.complexity_score || ')');
    END LOOP;

    -- Execute all ready tasks (one at a time)
    -- pck_dwh_table_migration_executor.execute_all_ready_tasks(v_project_id, 3);

END;
/


-- =============================================================================
-- SECTION 12: COMPREHENSIVE DATE COLUMN ANALYSIS
-- =============================================================================

-- Example 27: View all date columns analyzed for a table
SELECT * FROM cmr.v_dwh_date_column_analysis
WHERE source_table = 'SALES_FACT';

-- Example 28: Parse JSON to see all date columns for a specific task
SELECT
    task_name,
    source_table,
    primary_date_column,
    date_format_detected,
    JSON_QUERY(all_date_columns_analysis, '$[*]' WITH WRAPPER) AS all_columns_json
FROM cmr.v_dwh_date_column_analysis
WHERE task_id = 1;

-- Example 29: Extract detailed date column info from JSON
-- Shows all date columns with their ranges and characteristics
SELECT
    a.task_name,
    a.source_table,
    jt.*
FROM cmr.v_dwh_date_column_analysis a,
JSON_TABLE(
    a.all_date_columns_analysis, '$[*]'
    COLUMNS (
        column_name VARCHAR2(128) PATH '$.column_name',
        data_type VARCHAR2(30) PATH '$.data_type',
        min_date VARCHAR2(20) PATH '$.min_date',
        max_date VARCHAR2(20) PATH '$.max_date',
        range_days NUMBER PATH '$.range_days',
        range_years NUMBER PATH '$.range_years',
        is_primary VARCHAR2(5) PATH '$.is_primary'
    )
) jt
WHERE a.task_id = 1
ORDER BY jt.range_days DESC;

-- Example 30: Find tables with multiple date columns
SELECT
    task_name,
    source_table,
    primary_date_column,
    JSON_VALUE(all_date_columns_analysis, '$.size()') AS num_date_columns,
    all_date_columns_analysis
FROM cmr.v_dwh_date_column_analysis
WHERE JSON_VALUE(all_date_columns_analysis, '$.size()') > 1
ORDER BY num_date_columns DESC;

-- Example 31: Analyze a table with non-standard date columns
-- This example shows how the analyzer handles tables without stereotype patterns
DECLARE
    v_task_id NUMBER;
BEGIN
    -- Create task for a table with custom date columns
    INSERT INTO cmr.dwh_migration_tasks (
        task_name, source_owner, source_table, status
    ) VALUES (
        'Analyze Custom Date Table', USER, 'MY_CUSTOM_TABLE', 'PENDING'
    ) RETURNING task_id INTO v_task_id;

    -- Run analysis - it will analyze ALL date columns
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    -- View comprehensive analysis
    DBMS_OUTPUT.PUT_LINE('=== Date Column Analysis Results ===');

    FOR col IN (
        SELECT jt.*
        FROM cmr.dwh_migration_analysis a,
        JSON_TABLE(
            a.all_date_columns_analysis, '$[*]'
            COLUMNS (
                column_name VARCHAR2(128) PATH '$.column_name',
                min_date VARCHAR2(20) PATH '$.min_date',
                max_date VARCHAR2(20) PATH '$.max_date',
                range_days NUMBER PATH '$.range_days',
                is_primary VARCHAR2(5) PATH '$.is_primary'
            )
        ) jt
        WHERE a.task_id = v_task_id
        ORDER BY jt.range_days DESC
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(
            col.column_name || ': ' ||
            col.min_date || ' to ' || col.max_date ||
            ' (' || col.range_days || ' days)' ||
            CASE WHEN col.is_primary = 'true' THEN ' [PRIMARY]' ELSE '' END
        );
    END LOOP;

    COMMIT;
END;
/


-- =============================================================================
-- SECTION 13: TROUBLESHOOTING
-- =============================================================================

-- Example 32: Find failed migrations
SELECT
    task_id,
    task_name,
    source_table,
    status,
    error_message,
    execution_start,
    execution_end
FROM cmr.dwh_migration_tasks
WHERE status = 'FAILED'
ORDER BY execution_start DESC;


-- Example 33: Find failed steps in migration
SELECT
    l.task_id,
    t.task_name,
    l.step_number,
    l.step_name,
    l.error_code,
    l.error_message
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE l.status = 'FAILED'
ORDER BY l.start_time DESC;


-- Example 34: Retry failed migration (after fixing issues)
DECLARE
    v_task_id NUMBER := 1;  -- Replace with failed task_id
BEGIN
    -- Reset task status
    UPDATE cmr.dwh_migration_tasks
    SET status = 'READY',
        error_message = NULL,
        execution_start = NULL,
        execution_end = NULL
    WHERE task_id = v_task_id;

    COMMIT;

    -- Retry
    pck_dwh_table_migration_executor.execute_migration(v_task_id);
END;
/


-- =============================================================================
-- SECTION 14: AUTOMATIC LIST PARTITIONING (Oracle 12.2+)
-- =============================================================================
-- AUTOMATIC LIST partitioning automatically creates new partitions when new
-- distinct values are inserted. Ideal for multi-tenant, regional, or
-- status-based partitioning scenarios.
-- =============================================================================

-- Example 1: Single column AUTOMATIC LIST (multi-tenant)
-- Framework will automatically determine default value based on column type
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate TENANT_DATA with AUTOMATIC LIST',
    USER,
    'TENANT_DATA',
    'LIST(tenant_id)',
    'tenant_id',
    'Y',  -- Enable AUTOMATIC LIST
    'CTAS',
    'Y',
    'Y',  -- Required for AUTOMATIC LIST
    'PENDING'
);

-- Resulting DDL (if tenant_id is VARCHAR2):
-- CREATE TABLE tenant_data_part (...)
-- PARTITION BY LIST (tenant_id) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES ('NAV')
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Example 2: Multi-column AUTOMATIC LIST (region + country)
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate SALES_REGIONAL with multi-column AUTOMATIC LIST',
    USER,
    'SALES_REGIONAL',
    'LIST(region, country)',
    'region, country',
    'Y',
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);

-- Resulting DDL:
-- CREATE TABLE sales_regional_part (...)
-- PARTITION BY LIST (region, country) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES ('NAV')
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Example 3: Custom default values for P_XDEF partition
-- Override framework-generated defaults with your own values
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    list_default_values,  -- Custom defaults
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate ORDERS with custom default values',
    USER,
    'ORDERS',
    'LIST(order_status)',
    'order_status',
    'Y',
    '''UNKNOWN'',''NOT_SET''',  -- Custom default values (must be properly quoted strings)
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);

-- Resulting DDL:
-- CREATE TABLE orders_part (...)
-- PARTITION BY LIST (order_status) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES ('UNKNOWN','NOT_SET')
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Example 4: Number-based LIST partitioning
-- Framework will use -1 as default for numeric columns
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate PRODUCT_CATEGORIES',
    USER,
    'PRODUCT_CATEGORIES',
    'LIST(category_id)',
    'category_id',
    'Y',
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);

-- Resulting DDL (if category_id is NUMBER):
-- CREATE TABLE product_categories_part (...)
-- PARTITION BY LIST (category_id) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES (-1)
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Execute AUTOMATIC LIST migration example
DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get task ID
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate TENANT_DATA with AUTOMATIC LIST';

    -- Analyze table (optional but recommended)
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    -- Execute migration
    pck_dwh_table_migration_executor.execute_migration(v_task_id);

    -- After migration, Oracle will automatically create partitions for each new distinct value
    -- Example: INSERT INTO tenant_data VALUES (1, 'TENANT_001', ...)
    --          → Oracle creates partition for 'TENANT_001' automatically
END;
/


-- Query partition information after AUTOMATIC LIST migration
SELECT partition_name, high_value, num_rows, blocks
FROM dba_tab_partitions
WHERE table_owner = USER
AND table_name = 'TENANT_DATA'
ORDER BY partition_position;

-- Example output:
-- PARTITION_NAME  HIGH_VALUE             NUM_ROWS  BLOCKS
-- P_XDEF          'NAV'                  0         0
-- P_TENANT_001    'TENANT_001'          1250      15
-- P_TENANT_002    'TENANT_002'          890       10
-- P_TENANT_003    'TENANT_003'          1560      18


-- Drop specific partition (e.g., after tenant removal)
-- ALTER TABLE tenant_data DROP PARTITION p_tenant_001;

-- Truncate specific partition (clear tenant data)
-- ALTER TABLE tenant_data TRUNCATE PARTITION p_tenant_002;


-- =============================================================================
-- CLEANUP EXAMPLES
-- =============================================================================

-- Remove all tasks from a project
DELETE FROM cmr.dwh_migration_tasks
WHERE project_id = (
    SELECT project_id FROM cmr.dwh_migration_projects
    WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'
);

-- Remove a project
DELETE FROM cmr.dwh_migration_projects
WHERE project_name = 'Q1_2024_TABLE_PARTITIONING';

COMMIT;
