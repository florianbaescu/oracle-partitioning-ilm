-- =============================================================================
-- Diagnostic script to check why recommended_method is not being populated
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Diagnostic Check 1: Package Status
PROMPT ========================================

SELECT
    object_name,
    object_type,
    status,
    TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_compiled
FROM dba_objects
WHERE owner = 'CMR'
AND object_name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
ORDER BY object_type;

PROMPT
PROMPT ========================================
PROMPT Diagnostic Check 2: Column Existence
PROMPT ========================================

SELECT
    column_name,
    data_type,
    nullable
FROM dba_tab_columns
WHERE owner = 'CMR'
AND table_name = 'DWH_MIGRATION_ANALYSIS'
AND column_name IN ('SUPPORTS_ONLINE_REDEF', 'ONLINE_REDEF_METHOD', 'RECOMMENDED_METHOD')
ORDER BY column_name;

PROMPT
PROMPT ========================================
PROMPT Diagnostic Check 3: Recent Analysis Results
PROMPT ========================================

SELECT
    a.task_id,
    t.task_name,
    t.source_table,
    t.status,
    a.supports_online_redef,
    a.online_redef_method,
    a.recommended_method,
    a.recommended_strategy,
    TO_CHAR(a.analysis_date, 'YYYY-MM-DD HH24:MI:SS') AS analysis_date
FROM cmr.dwh_migration_analysis a
JOIN cmr.dwh_migration_tasks t ON t.task_id = a.task_id
ORDER BY a.analysis_date DESC
FETCH FIRST 5 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Diagnostic Check 4: Task Status
PROMPT ========================================

SELECT
    task_id,
    task_name,
    source_table,
    status,
    validation_status,
    error_message
FROM cmr.dwh_migration_tasks
ORDER BY created_date DESC
FETCH FIRST 5 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Diagnostic Check 5: Package Source Check
PROMPT ========================================
PROMPT Checking if package body contains the recommended_method logic...

SELECT COUNT(*) AS lines_found
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%';

PROMPT
PROMPT If lines_found = 0, package body is not compiled with latest code!
PROMPT

PROMPT ========================================
PROMPT Diagnostic Check 6: Specific Source Lines
PROMPT ========================================

SELECT
    line,
    text
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND (
    UPPER(text) LIKE '%V_RECOMMENDED_METHOD :=%'
    OR UPPER(text) LIKE '%RECOMMENDED_METHOD = V_RECOMMENDED_METHOD%'
)
ORDER BY line;

PROMPT
PROMPT ========================================
PROMPT Test: Run Analysis with Debug
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get a task to test
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Testing analysis on task_id: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Run analysis
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Analysis completed. Checking results...');

    -- Check results
    FOR rec IN (
        SELECT
            supports_online_redef,
            online_redef_method,
            recommended_method,
            recommended_strategy
        FROM cmr.dwh_migration_analysis
        WHERE task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  supports_online_redef: ' || NVL(rec.supports_online_redef, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  online_redef_method: ' || NVL(rec.online_redef_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_method: ' || NVL(rec.recommended_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_strategy: ' || NVL(rec.recommended_strategy, 'NULL'));
    END LOOP;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: No tasks found in dwh_migration_tasks');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
END;
/

PROMPT
PROMPT ========================================
PROMPT Diagnostic Complete
PROMPT ========================================
PROMPT
PROMPT ACTION ITEMS:
PROMPT 1. Check package status (should be VALID)
PROMPT 2. Verify columns exist (should show 3 rows)
PROMPT 3. Check if recommended_method is populated in analysis results
PROMPT 4. Verify package source contains v_recommended_method logic
PROMPT 5. If source lines found = 0, recompile package:
PROMPT    @scripts/table_migration_analysis.sql
PROMPT
