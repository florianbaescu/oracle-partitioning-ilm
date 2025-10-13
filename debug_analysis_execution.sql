-- =============================================================================
-- Debug Analysis Execution - Trace what's happening
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET TIMING ON

PROMPT ========================================
PROMPT Step 1: Check Package Compilation
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
PROMPT Checking for compilation errors...

SELECT
    line,
    position,
    text AS error_text
FROM dba_errors
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
ORDER BY sequence;

PROMPT
PROMPT ========================================
PROMPT Step 2: Verify Columns Exist
PROMPT ========================================

SELECT
    column_name,
    data_type,
    data_length,
    nullable,
    data_default
FROM dba_tab_columns
WHERE owner = 'CMR'
AND table_name = 'DWH_MIGRATION_ANALYSIS'
AND column_name IN ('SUPPORTS_ONLINE_REDEF', 'ONLINE_REDEF_METHOD', 'RECOMMENDED_METHOD')
ORDER BY column_name;

PROMPT
PROMPT ========================================
PROMPT Step 3: Check Current Analysis Data
PROMPT ========================================

SELECT
    task_id,
    supports_online_redef,
    online_redef_method,
    recommended_method,
    recommended_strategy,
    TO_CHAR(analysis_date, 'YYYY-MM-DD HH24:MI:SS') AS analysis_date
FROM cmr.dwh_migration_analysis
ORDER BY analysis_date DESC
FETCH FIRST 3 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Step 4: Manual Test with Inline Code
PROMPT ========================================

DECLARE
    v_task_id NUMBER := 1; -- Change to your task_id
    v_owner VARCHAR2(30);
    v_table_name VARCHAR2(128);
    v_supports_online_redef CHAR(1) := 'N';
    v_online_redef_method VARCHAR2(30) := NULL;
    v_recommended_method VARCHAR2(30) := 'CTAS';
    v_table_size NUMBER := 0;
    v_requires_conversion CHAR(1) := 'N';
BEGIN
    -- Get task details
    SELECT source_owner, source_table
    INTO v_owner, v_table_name
    FROM cmr.dwh_migration_tasks
    WHERE task_id = v_task_id;

    DBMS_OUTPUT.PUT_LINE('Testing table: ' || v_owner || '.' || v_table_name);
    DBMS_OUTPUT.PUT_LINE('');

    -- Test online redefinition capability
    DBMS_OUTPUT.PUT_LINE('Testing DBMS_REDEFINITION.CAN_REDEF_TABLE...');
    BEGIN
        DBMS_REDEFINITION.CAN_REDEF_TABLE(
            uname => v_owner,
            tname => v_table_name,
            options_flag => DBMS_REDEFINITION.CONS_USE_PK
        );
        v_supports_online_redef := 'Y';
        v_online_redef_method := 'CONS_USE_PK';
        DBMS_OUTPUT.PUT_LINE('✓ Supports ONLINE (CONS_USE_PK)');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ CONS_USE_PK failed: ' || SQLERRM);
            -- Try ROWID
            BEGIN
                DBMS_REDEFINITION.CAN_REDEF_TABLE(
                    uname => v_owner,
                    tname => v_table_name,
                    options_flag => DBMS_REDEFINITION.CONS_USE_ROWID
                );
                v_supports_online_redef := 'Y';
                v_online_redef_method := 'CONS_USE_ROWID';
                DBMS_OUTPUT.PUT_LINE('✓ Supports ONLINE (CONS_USE_ROWID)');
            EXCEPTION
                WHEN OTHERS THEN
                    v_supports_online_redef := 'N';
                    v_online_redef_method := NULL;
                    DBMS_OUTPUT.PUT_LINE('✗ CONS_USE_ROWID failed: ' || SQLERRM);
            END;
    END;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Results from manual test:');
    DBMS_OUTPUT.PUT_LINE('  supports_online_redef: ' || v_supports_online_redef);
    DBMS_OUTPUT.PUT_LINE('  online_redef_method: ' || NVL(v_online_redef_method, 'NULL'));
    DBMS_OUTPUT.PUT_LINE('  recommended_method: ' || v_recommended_method);
    DBMS_OUTPUT.PUT_LINE('');

    -- Now test UPDATE directly
    DBMS_OUTPUT.PUT_LINE('Testing direct UPDATE on dwh_migration_analysis...');
    UPDATE cmr.dwh_migration_analysis
    SET supports_online_redef = v_supports_online_redef,
        online_redef_method = v_online_redef_method,
        recommended_method = v_recommended_method
    WHERE task_id = v_task_id;

    IF SQL%ROWCOUNT > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ UPDATE successful (' || SQL%ROWCOUNT || ' row)');
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ UPDATE failed - no rows found for task_id ' || v_task_id);
    END IF;

    -- Verify
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Verifying data after UPDATE:');
    FOR rec IN (
        SELECT supports_online_redef, online_redef_method, recommended_method
        FROM cmr.dwh_migration_analysis
        WHERE task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  supports_online_redef: ' || NVL(rec.supports_online_redef, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  online_redef_method: ' || NVL(rec.online_redef_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_method: ' || NVL(rec.recommended_method, 'NULL'));
    END LOOP;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Task not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        ROLLBACK;
END;
/

PROMPT
PROMPT ========================================
PROMPT Step 5: Run Actual Analysis
PROMPT ========================================
PROMPT Now running pck_dwh_table_migration_analyzer.analyze_table...
PROMPT Watch for output about "Checking online redefinition capability"
PROMPT

DECLARE
    v_task_id NUMBER := 1; -- Change to your task_id
BEGIN
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
END;
/

PROMPT
PROMPT ========================================
PROMPT Step 6: Check Results After Analysis
PROMPT ========================================

SELECT
    a.task_id,
    t.source_table,
    t.status,
    t.error_message,
    a.supports_online_redef,
    a.online_redef_method,
    a.recommended_method,
    a.recommended_strategy,
    TO_CHAR(a.analysis_date, 'YYYY-MM-DD HH24:MI:SS') AS analysis_date
FROM cmr.dwh_migration_analysis a
JOIN cmr.dwh_migration_tasks t ON t.task_id = a.task_id
WHERE a.task_id = 1 -- Change to your task_id
ORDER BY a.analysis_date DESC;

PROMPT
PROMPT ========================================
PROMPT Step 7: Check Package Source
PROMPT ========================================

SELECT COUNT(*) AS count_v_recommended_method
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%';

PROMPT
PROMPT If count = 0, the package body doesn't have the variable!
PROMPT Recompile: @scripts/table_migration_analysis.sql
PROMPT

PROMPT ========================================
PROMPT Diagnostic Complete
PROMPT ========================================
PROMPT
PROMPT INTERPRETATION:
PROMPT - Step 1: Status should be VALID, no errors
PROMPT - Step 2: Should show 3 columns
PROMPT - Step 4: Manual test should UPDATE successfully
PROMPT - Step 5: Look for "Checking online redefinition capability"
PROMPT - Step 6: Columns should be populated
PROMPT - Step 7: Count should be > 0
PROMPT
