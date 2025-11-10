-- =============================================================================
-- Complete Migration Workflow with apply_recommendations()
-- Demonstrates the proper end-to-end workflow for table migration
-- =============================================================================

-- =============================================================================
-- RECOMMENDED WORKFLOW: Let analysis recommend the strategy
-- =============================================================================

PROMPT ========================================
PROMPT Example 1: Complete Automated Workflow
PROMPT ========================================

DECLARE
    v_project_id NUMBER;
    v_task_id NUMBER;
BEGIN
    -- Step 1: Create migration project
    INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
    VALUES ('AUTOMATED_MIGRATION_2024', 'Let analysis recommend partitioning strategy', 'PLANNING')
    RETURNING project_id INTO v_project_id;

    DBMS_OUTPUT.PUT_LINE('Created project ID: ' || v_project_id);

    -- Step 2: Create minimal task (no partition strategy specified)
    INSERT INTO cmr.dwh_migration_tasks (
        project_id,
        task_name,
        source_owner,
        source_table,
        -- DO NOT specify partition_type, partition_key, interval_clause
        -- Let the analyzer recommend them!
        use_compression,
        compression_type,
        apply_ilm_policies,
        ilm_policy_template,
        status
    ) VALUES (
        v_project_id,
        'Migrate SALES_FACT',
        USER,
        'SALES_FACT',
        'Y',
        'QUERY HIGH',
        'Y',
        'FACT_TABLE_STANDARD',
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 3: Analyze table (discovers date columns, recommends strategy)
    DBMS_OUTPUT.PUT_LINE('Step 3: Running analysis...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 4: Review analysis results
    DBMS_OUTPUT.PUT_LINE('Step 4: Reviewing analysis results...');
    FOR rec IN (
        SELECT
            a.recommended_strategy,
            a.recommendation_reason,
            a.date_column_name,
            a.requires_conversion,
            a.complexity_score,
            a.estimated_downtime_minutes,
            a.recommended_method
        FROM cmr.dwh_migration_analysis a
        WHERE a.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  Recommended strategy: ' || rec.recommended_strategy);
        DBMS_OUTPUT.PUT_LINE('  Reason: ' || rec.recommendation_reason);
        DBMS_OUTPUT.PUT_LINE('  Date column: ' || rec.date_column_name);
        DBMS_OUTPUT.PUT_LINE('  Requires conversion: ' || rec.requires_conversion);
        DBMS_OUTPUT.PUT_LINE('  Complexity score: ' || rec.complexity_score || '/10');
        DBMS_OUTPUT.PUT_LINE('  Estimated downtime: ' || rec.estimated_downtime_minutes || ' min');
        DBMS_OUTPUT.PUT_LINE('  Recommended method: ' || rec.recommended_method);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 5: Apply recommendations (THIS IS THE KEY STEP!)
    DBMS_OUTPUT.PUT_LINE('Step 5: Applying recommendations to task...');
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 6: Verify task is ready
    DBMS_OUTPUT.PUT_LINE('Step 6: Verifying task configuration...');
    FOR rec IN (
        SELECT
            task_id,
            partition_type,
            partition_key,
            interval_clause,
            migration_method,
            status,
            validation_status
        FROM cmr.dwh_migration_tasks
        WHERE task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  Task ID: ' || rec.task_id);
        DBMS_OUTPUT.PUT_LINE('  Partition type: ' || rec.partition_type);
        DBMS_OUTPUT.PUT_LINE('  Partition key: ' || rec.partition_key);
        DBMS_OUTPUT.PUT_LINE('  Interval clause: ' || rec.interval_clause);
        DBMS_OUTPUT.PUT_LINE('  Migration method: ' || rec.migration_method);
        DBMS_OUTPUT.PUT_LINE('  Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('  Validation status: ' || rec.validation_status);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 7: Simulate migration (preview without executing)
    DBMS_OUTPUT.PUT_LINE('Step 7: Simulating migration...');
    pck_dwh_table_migration_executor.execute_migration(
        p_task_id => v_task_id,
        p_simulate => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 8: Execute migration (uncomment when ready)
    -- DBMS_OUTPUT.PUT_LINE('Step 8: Executing migration...');
    -- pck_dwh_table_migration_executor.execute_migration(v_task_id);

    COMMIT;
END;
/


-- =============================================================================
-- Example 2: Batch Migration with Recommendations
-- =============================================================================

PROMPT ========================================
PROMPT Example 2: Batch Migration Workflow
PROMPT ========================================

DECLARE
    v_project_id NUMBER;
    v_task_id NUMBER;
    TYPE t_table_list IS TABLE OF VARCHAR2(128);
    v_tables t_table_list := t_table_list('SALES_FACT', 'ORDER_FACT', 'INVENTORY_FACT');
BEGIN
    -- Create project
    INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
    VALUES ('BATCH_MIGRATION_2024', 'Batch migrate fact tables', 'PLANNING')
    RETURNING project_id INTO v_project_id;

    DBMS_OUTPUT.PUT_LINE('Created project ID: ' || v_project_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Create tasks for all tables
    FOR i IN 1..v_tables.COUNT LOOP
        INSERT INTO cmr.dwh_migration_tasks (
            project_id,
            task_name,
            source_owner,
            source_table,
            use_compression,
            compression_type,
            apply_ilm_policies,
            ilm_policy_template,
            status
        ) VALUES (
            v_project_id,
            'Migrate ' || v_tables(i),
            USER,
            v_tables(i),
            'Y',
            'QUERY HIGH',
            'Y',
            'FACT_TABLE_STANDARD',
            'PENDING'
        ) RETURNING task_id INTO v_task_id;

        DBMS_OUTPUT.PUT_LINE('Created task ' || v_task_id || ' for table ' || v_tables(i));
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Analyze all tasks
    DBMS_OUTPUT.PUT_LINE('Analyzing all tables...');
    pck_dwh_table_migration_analyzer.analyze_all_pending_tasks(v_project_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Apply recommendations to all analyzed tasks
    DBMS_OUTPUT.PUT_LINE('Applying recommendations to all tasks...');
    FOR rec IN (
        SELECT task_id, task_name
        FROM cmr.dwh_migration_tasks
        WHERE project_id = v_project_id
        AND status = 'ANALYZED'
        ORDER BY task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Applying recommendations for: ' || rec.task_name);
        pck_dwh_table_migration_executor.apply_recommendations(rec.task_id);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Review all tasks
    DBMS_OUTPUT.PUT_LINE('=== Migration Tasks Summary ===');
    FOR rec IN (
        SELECT
            t.task_id,
            t.task_name,
            t.partition_type,
            t.migration_method,
            t.status,
            a.complexity_score
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.project_id = v_project_id
        ORDER BY t.task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(rec.task_id || ': ' || rec.task_name);
        DBMS_OUTPUT.PUT_LINE('  Partition: ' || rec.partition_type);
        DBMS_OUTPUT.PUT_LINE('  Method: ' || rec.migration_method);
        DBMS_OUTPUT.PUT_LINE('  Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('  Complexity: ' || rec.complexity_score || '/10');
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    -- Execute all ready tasks (uncomment when ready)
    -- pck_dwh_table_migration_executor.execute_all_ready_tasks(
    --     p_project_id => v_project_id,
    --     p_max_tasks => 3
    -- );

    COMMIT;
END;
/


-- =============================================================================
-- Example 3: Override Analysis Recommendations (Advanced)
-- =============================================================================

PROMPT ========================================
PROMPT Example 3: Override Recommendations
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Create task with explicit partition strategy (override analyzer)
    INSERT INTO cmr.dwh_migration_tasks (
        task_name,
        source_owner,
        source_table,
        partition_type,          -- Explicitly specify
        partition_key,           -- Explicitly specify
        interval_clause,         -- Explicitly specify
        migration_method,        -- Explicitly specify
        use_compression,
        compression_type,
        status
    ) VALUES (
        'Custom Partition Strategy',
        USER,
        'CUSTOM_TABLE',
        'RANGE(event_date)',
        'event_date',
        'NUMTODSINTERVAL(1,''DAY'')',  -- Daily partitions
        'ONLINE',                       -- Force online redefinition
        'Y',
        'QUERY HIGH',
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task with explicit strategy: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Analyze (will validate your strategy)
    DBMS_OUTPUT.PUT_LINE('Running analysis to validate strategy...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Review validation
    FOR rec IN (
        SELECT validation_status, recommended_strategy, recommendation_reason
        FROM cmr.dwh_migration_tasks t
        JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Validation status: ' || rec.validation_status);
        DBMS_OUTPUT.PUT_LINE('Analyzer would recommend: ' || rec.recommended_strategy);
        DBMS_OUTPUT.PUT_LINE('Reason: ' || rec.recommendation_reason);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- If validated, ready to execute (no need to call apply_recommendations)
    -- pck_dwh_table_migration_executor.execute_migration(v_task_id);

    COMMIT;
END;
/


-- =============================================================================
-- Example 4: Handling Tables with Multiple Date Columns
-- =============================================================================

PROMPT ========================================
PROMPT Example 4: Multiple Date Columns
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_all_date_columns CLOB;
BEGIN
    -- Create minimal task
    INSERT INTO cmr.dwh_migration_tasks (
        task_name,
        source_owner,
        source_table,
        status
    ) VALUES (
        'Analyze Multi-Date Table',
        USER,
        'TABLE_WITH_MANY_DATES',
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    -- Analyze table
    DBMS_OUTPUT.PUT_LINE('Analyzing table with multiple date columns...');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Review ALL date columns analyzed
    SELECT all_date_columns_analysis
    INTO v_all_date_columns
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    DBMS_OUTPUT.PUT_LINE('All date columns analyzed:');
    DBMS_OUTPUT.PUT_LINE(v_all_date_columns);
    DBMS_OUTPUT.PUT_LINE('');

    -- Parse and display date columns from JSON
    FOR col IN (
        SELECT jt.*
        FROM cmr.dwh_migration_analysis a,
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
        WHERE a.task_id = v_task_id
        ORDER BY jt.range_days DESC
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(col.column_name || ' (' || col.data_type || ')');
        DBMS_OUTPUT.PUT_LINE('  Range: ' || col.min_date || ' to ' || col.max_date);
        DBMS_OUTPUT.PUT_LINE('  Days: ' || col.range_days || ' (' || ROUND(col.range_years, 1) || ' years)');
        IF col.is_primary = 'true' THEN
            DBMS_OUTPUT.PUT_LINE('  *** SELECTED FOR PARTITIONING ***');
        END IF;
        DBMS_OUTPUT.PUT_LINE('');
    END LOOP;

    -- Apply recommendations (will use primary date column)
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    COMMIT;
END;
/


-- =============================================================================
-- Example 5: Error Handling and Blocking Issues
-- =============================================================================

PROMPT ========================================
PROMPT Example 5: Handling Blocking Issues
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_blocking_issues CLOB;
BEGIN
    -- Create task
    INSERT INTO cmr.dwh_migration_tasks (
        task_name,
        source_owner,
        source_table,
        status
    ) VALUES (
        'Table with Issues',
        USER,
        'PROBLEMATIC_TABLE',
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    -- Analyze
    BEGIN
        pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Analysis completed with warnings');
    END;

    -- Check for blocking issues
    SELECT blocking_issues
    INTO v_blocking_issues
    FROM cmr.dwh_migration_analysis
    WHERE task_id = v_task_id;

    IF v_blocking_issues IS NOT NULL AND DBMS_LOB.GETLENGTH(v_blocking_issues) > 2 THEN
        DBMS_OUTPUT.PUT_LINE('=== BLOCKING ISSUES FOUND ===');
        DBMS_OUTPUT.PUT_LINE(v_blocking_issues);
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Resolve these issues before applying recommendations.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('No blocking issues found. Ready to apply recommendations.');

        -- Try to apply recommendations
        BEGIN
            pck_dwh_table_migration_executor.apply_recommendations(v_task_id);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        END;
    END IF;

    COMMIT;
END;
/


-- =============================================================================
-- Cleanup Example
-- =============================================================================

PROMPT ========================================
PROMPT Cleanup: Remove Test Data
PROMPT ========================================

-- Remove test projects and tasks
/*
DELETE FROM cmr.dwh_migration_tasks
WHERE project_id IN (
    SELECT project_id FROM cmr.dwh_migration_projects
    WHERE project_name LIKE '%2024'
);

DELETE FROM cmr.dwh_migration_projects
WHERE project_name LIKE '%2024';

COMMIT;
*/

-- =============================================================================
-- Example 6: Complete Migration with ILM Integration (NEW!)
-- =============================================================================

PROMPT ========================================
PROMPT Example 6: Migration with ILM Policies
PROMPT ========================================

DECLARE
    v_project_id NUMBER;
    v_task_id NUMBER;
BEGIN
    -- Create project
    INSERT INTO cmr.dwh_migration_projects (project_name, description, status)
    VALUES ('ILM_MIGRATION_2024', 'Migration with automatic ILM policy application', 'PLANNING')
    RETURNING project_id INTO v_project_id;

    DBMS_OUTPUT.PUT_LINE('Created project ID: ' || v_project_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Create task with ILM policies enabled
    INSERT INTO cmr.dwh_migration_tasks (
        project_id,
        task_name,
        source_owner,
        source_table,
        use_compression,
        compression_type,
        apply_ilm_policies,        -- Enable ILM
        ilm_policy_template,       -- Choose template
        status
    ) VALUES (
        v_project_id,
        'Migrate with ILM',
        USER,
        'SALES_FACT',
        'Y',
        'QUERY HIGH',
        'Y',                       -- Apply ILM policies
        'FACT_TABLE_STANDARD',     -- Template: 90-day compression, tiering, archival
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task ID: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('ILM policies will be automatically applied after migration');
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 1: Analyze table
    DBMS_OUTPUT.PUT_LINE('=== Step 1: Analyzing table ===');
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 2: Apply recommendations
    DBMS_OUTPUT.PUT_LINE('=== Step 2: Applying recommendations ===');
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 3: Simulate migration
    DBMS_OUTPUT.PUT_LINE('=== Step 3: Simulating migration ===');
    pck_dwh_table_migration_executor.execute_migration(
        p_task_id => v_task_id,
        p_simulate => TRUE
    );
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 4: Execute migration (uncomment to run)
    -- DBMS_OUTPUT.PUT_LINE('=== Step 4: Executing migration with ILM ===');
    -- pck_dwh_table_migration_executor.execute_migration(v_task_id);
    --
    -- After migration completes, the following happens automatically:
    --   1. ILM policies are created from template
    --   2. Policies are validated
    --   3. Partition access tracking is initialized
    --   4. Partitions are classified by temperature (HOT/WARM/COLD)

    COMMIT;
END;
/


-- =============================================================================
-- Example 7: Monitor ILM Policies After Migration
-- =============================================================================

PROMPT ========================================
PROMPT Example 7: ILM Monitoring Dashboard
PROMPT ========================================

PROMPT Query 1: Summary of ILM policies per table
SELECT * FROM v_dwh_ilm_policy_summary ORDER BY pending_actions DESC;

PROMPT Query 2: Upcoming ILM actions in next 30 days
SELECT * FROM v_dwh_ilm_upcoming_actions WHERE urgency IN ('Overdue', 'Today', 'This Week');

PROMPT Query 3: Partition lifecycle status
SELECT
    table_name,
    partition_name,
    lifecycle_stage,
    recommended_action,
    partition_age_days_from_highvalue,
    size_mb,
    compression
FROM v_dwh_ilm_partition_lifecycle
WHERE table_name = 'SALES_FACT'
ORDER BY partition_position;

PROMPT Query 4: Space savings history
SELECT
    table_name,
    action_type,
    execution_date,
    partitions_processed,
    total_space_saved_mb,
    avg_compression_ratio
FROM v_dwh_ilm_space_savings
WHERE table_name = 'SALES_FACT'
AND execution_date > SYSDATE - 30
ORDER BY execution_date DESC;

PROMPT Query 5: Recent ILM execution history
SELECT
    policy_name,
    partition_name,
    action_type,
    status,
    execution_start,
    duration_minutes,
    space_saved_mb,
    performance_category
FROM v_dwh_ilm_execution_history
WHERE table_name = 'SALES_FACT'
ORDER BY execution_start DESC;


-- =============================================================================
-- Example 8: Manual ILM Policy Evaluation
-- =============================================================================

PROMPT ========================================
PROMPT Example 8: Evaluate ILM Policies
PROMPT ========================================

-- Evaluate all policies for a specific table
EXEC pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT');

-- Check evaluation queue for eligible partitions
SELECT
    policy_name,
    partition_name,
    evaluation_date,
    reason,
    execution_status
FROM cmr.dwh_ilm_evaluation_queue
WHERE table_name = 'SALES_FACT'
AND eligible = 'Y'
ORDER BY evaluation_date DESC;


-- =============================================================================
-- Example 9: Record Manual Partition Access
-- =============================================================================

PROMPT ========================================
PROMPT Example 9: Track Partition Access
PROMPT ========================================

-- Record read access to a partition
EXEC dwh_record_partition_access(USER, 'SALES_FACT', 'P_2024_01', 'READ');

-- Record write access to a partition
EXEC dwh_record_partition_access(USER, 'SALES_FACT', 'P_2024_12', 'WRITE');

-- Refresh partition access tracking for all tables
EXEC dwh_refresh_partition_access_tracking(USER, NULL);

-- View updated temperature classification
SELECT
    partition_name,
    temperature,
    days_since_read,
    days_since_write,
    read_count,
    write_count,
    last_read_time,
    last_write_time
FROM cmr.dwh_ilm_partition_access
WHERE table_name = 'SALES_FACT'
ORDER BY partition_name;


-- =============================================================================
-- Example 10: Complete End-to-End ILM Workflow
-- =============================================================================

PROMPT ========================================
PROMPT Example 10: Complete ILM Lifecycle
PROMPT ========================================

DECLARE
    v_task_id NUMBER := 1; -- Your migration task ID
BEGIN
    DBMS_OUTPUT.PUT_LINE('=== Complete ILM Lifecycle Demo ===');
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 1: Migrate table (with ILM enabled)
    DBMS_OUTPUT.PUT_LINE('Step 1: Table migrated with apply_ilm_policies = Y');
    DBMS_OUTPUT.PUT_LINE('  - Partitioned table created');
    DBMS_OUTPUT.PUT_LINE('  - ILM policies applied from template');
    DBMS_OUTPUT.PUT_LINE('  - Partition access tracking initialized');
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 2: Verify ILM policies were created
    DBMS_OUTPUT.PUT_LINE('Step 2: Verify ILM policies');
    FOR pol IN (
        SELECT policy_name, policy_type, action_type, age_days
        FROM cmr.dwh_ilm_policies
        WHERE table_owner = USER
        AND table_name = 'SALES_FACT'
        ORDER BY policy_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  Policy: ' || pol.policy_name);
        DBMS_OUTPUT.PUT_LINE('    Type: ' || pol.policy_type);
        DBMS_OUTPUT.PUT_LINE('    Action: ' || pol.action_type);
        DBMS_OUTPUT.PUT_LINE('    Age: ' || pol.age_days || ' days');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 3: Check partition temperatures
    DBMS_OUTPUT.PUT_LINE('Step 3: Partition temperature classification');
    FOR part IN (
        SELECT partition_name, temperature, days_since_write, size_mb
        FROM cmr.dwh_ilm_partition_access
        WHERE table_name = 'SALES_FACT'
        ORDER BY days_since_write DESC
        FETCH FIRST 5 ROWS ONLY
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || part.partition_name || ': ' || part.temperature);
        DBMS_OUTPUT.PUT_LINE('    Age: ' || part.days_since_write || ' days');
        DBMS_OUTPUT.PUT_LINE('    Size: ' || part.size_mb || ' MB');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 4: Evaluate policies (find eligible partitions)
    DBMS_OUTPUT.PUT_LINE('Step 4: Evaluate ILM policies');
    pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT');
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 5: Review pending actions
    DBMS_OUTPUT.PUT_LINE('Step 5: Review pending ILM actions');
    FOR action IN (
        SELECT policy_name, partition_name, reason
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE table_name = 'SALES_FACT'
        AND eligible = 'Y'
        AND execution_status = 'PENDING'
        FETCH FIRST 5 ROWS ONLY
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || action.partition_name);
        DBMS_OUTPUT.PUT_LINE('    Policy: ' || action.policy_name);
        DBMS_OUTPUT.PUT_LINE('    Reason: ' || action.reason);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('');

    -- Step 6: Execute ILM actions (commented - requires execution engine)
    DBMS_OUTPUT.PUT_LINE('Step 6: Execute ILM actions (manual step)');
    DBMS_OUTPUT.PUT_LINE('  Run: pck_dwh_ilm_execution_engine.execute_pending_actions()');
    DBMS_OUTPUT.PUT_LINE('  Or enable automatic execution via scheduler');
    DBMS_OUTPUT.PUT_LINE('');

    DBMS_OUTPUT.PUT_LINE('=== ILM Lifecycle Complete ===');
END;
/


PROMPT ========================================
PROMPT Complete Workflow Examples Loaded
PROMPT ========================================
PROMPT
PROMPT Run the examples above to see the complete workflow:
PROMPT 1. Create minimal task (no partition strategy)
PROMPT 2. Analyze table (get recommendations)
PROMPT 3. Apply recommendations (KEY STEP!)
PROMPT 4. Execute migration
PROMPT 5. ILM policies applied automatically
PROMPT 6. Monitor via dashboard views
PROMPT
PROMPT NEW: ILM Integration (Examples 6-10)
PROMPT - Automatic ILM policy application
PROMPT - Partition access tracking
PROMPT - Temperature classification
PROMPT - Policy evaluation and monitoring
PROMPT - Complete lifecycle management
PROMPT
PROMPT ========================================
