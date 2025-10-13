-- =============================================================================
-- Complete Fix for recommended_method Issue
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Step 1: Add Missing Columns (if needed)
PROMPT ========================================

DECLARE
    v_column_count NUMBER;
BEGIN
    -- Check if columns already exist
    SELECT COUNT(*) INTO v_column_count
    FROM dba_tab_columns
    WHERE owner = 'CMR'
    AND table_name = 'DWH_MIGRATION_ANALYSIS'
    AND column_name IN ('SUPPORTS_ONLINE_REDEF', 'ONLINE_REDEF_METHOD', 'RECOMMENDED_METHOD');

    IF v_column_count = 3 THEN
        DBMS_OUTPUT.PUT_LINE('✓ All 3 columns already exist. Skipping ALTER TABLE.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Adding missing columns...');

        -- Add columns if missing
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_migration_analysis ADD (
                supports_online_redef CHAR(1) DEFAULT ''N'',
                online_redef_method VARCHAR2(30),
                recommended_method VARCHAR2(30)
            )';
            DBMS_OUTPUT.PUT_LINE('✓ Columns added successfully.');
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -1430 THEN -- Column already exists
                    DBMS_OUTPUT.PUT_LINE('✓ Columns already exist.');
                ELSE
                    DBMS_OUTPUT.PUT_LINE('✗ Error adding columns: ' || SQLERRM);
                    RAISE;
                END IF;
        END;
    END IF;
END;
/

PROMPT
PROMPT ========================================
PROMPT Step 2: Grant EXECUTE on DBMS_REDEFINITION
PROMPT ========================================

DECLARE
    v_grant_exists NUMBER;
BEGIN
    -- Check if grant already exists
    SELECT COUNT(*) INTO v_grant_exists
    FROM dba_tab_privs
    WHERE grantee = 'CMR'
    AND table_name = 'DBMS_REDEFINITION'
    AND privilege = 'EXECUTE';

    IF v_grant_exists > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ EXECUTE privilege on DBMS_REDEFINITION already granted to CMR.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠ EXECUTE privilege on DBMS_REDEFINITION NOT found.');
        DBMS_OUTPUT.PUT_LINE('  Run as SYSDBA:');
        DBMS_OUTPUT.PUT_LINE('    GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Note: Without this grant, all tables will use CTAS method only.');
        DBMS_OUTPUT.PUT_LINE('        Analysis will still work, but ONLINE method won''t be available.');
    END IF;
END;
/

PROMPT
PROMPT ========================================
PROMPT Step 3: Check Package Compilation
PROMPT ========================================

SELECT
    object_type,
    status,
    TO_CHAR(last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_compiled
FROM dba_objects
WHERE owner = 'CMR'
AND object_name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
ORDER BY object_type;

PROMPT
PROMPT Checking if package contains required logic...

DECLARE
    v_has_logic NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_has_logic
    FROM dba_source
    WHERE owner = 'CMR'
    AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
    AND type = 'PACKAGE BODY'
    AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%';

    IF v_has_logic = 0 THEN
        DBMS_OUTPUT.PUT_LINE('✗ Package does NOT contain v_recommended_method logic!');
        DBMS_OUTPUT.PUT_LINE('  ACTION: Recompile package:');
        DBMS_OUTPUT.PUT_LINE('    @scripts/table_migration_analysis.sql');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ Package contains v_recommended_method logic (' || v_has_logic || ' occurrences)');
    END IF;
END;
/

PROMPT
PROMPT ========================================
PROMPT Step 4: Reset Failed Tasks to PENDING
PROMPT ========================================

UPDATE cmr.dwh_migration_tasks
SET status = 'PENDING',
    error_message = NULL
WHERE status IN ('ANALYZED', 'FAILED');

PROMPT Updated rows:

SELECT SQL%ROWCOUNT FROM DUAL;

COMMIT;

PROMPT
PROMPT ========================================
PROMPT Step 5: Test Analysis on First Task
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
BEGIN
    -- Get first available task
    SELECT MIN(task_id) INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE status = 'PENDING';

    IF v_task_id IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('No PENDING tasks found. Create a task first.');
        RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('Running analysis on task_id: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Run analysis
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Analysis complete. Checking results...');
    DBMS_OUTPUT.PUT_LINE('');

    -- Show results
    FOR rec IN (
        SELECT
            a.task_id,
            t.source_table,
            t.status,
            a.supports_online_redef,
            a.online_redef_method,
            a.recommended_method,
            a.recommended_strategy
        FROM cmr.dwh_migration_analysis a
        JOIN cmr.dwh_migration_tasks t ON t.task_id = a.task_id
        WHERE a.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Results for task ' || rec.task_id || ' (' || rec.source_table || ')');
        DBMS_OUTPUT.PUT_LINE('  Task status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('  supports_online_redef: ' || NVL(rec.supports_online_redef, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  online_redef_method: ' || NVL(rec.online_redef_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_method: ' || NVL(rec.recommended_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_strategy: ' || NVL(rec.recommended_strategy, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('');

        IF rec.recommended_method IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('❌ STILL NULL! Package not properly compiled.');
            DBMS_OUTPUT.PUT_LINE('   Run: @scripts/table_migration_analysis.sql');
        ELSIF rec.recommended_method = 'CTAS' AND rec.supports_online_redef = 'N' THEN
            DBMS_OUTPUT.PUT_LINE('⚠ Result: CTAS (no EXECUTE grant on DBMS_REDEFINITION)');
            DBMS_OUTPUT.PUT_LINE('  To enable ONLINE method, grant as SYSDBA:');
            DBMS_OUTPUT.PUT_LINE('    GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✓ SUCCESS! Columns populated correctly.');
        END IF;
    END LOOP;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('No analysis results found. Check if analysis ran successfully.');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
END;
/

PROMPT
PROMPT ========================================
PROMPT Fix Complete
PROMPT ========================================
