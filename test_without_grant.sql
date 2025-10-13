-- =============================================================================
-- Test Analysis Without DBMS_REDEFINITION Grant
-- This verifies the package compiles and works without the grant
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Test 1: Check Grant Status
PROMPT ========================================

SELECT
    CASE
        WHEN COUNT(*) > 0 THEN '✓ Grant exists - ONLINE method available'
        ELSE '✗ No grant - CTAS method only (this is OK!)'
    END AS grant_status
FROM dba_tab_privs
WHERE grantee = 'CMR'
AND table_name = 'DBMS_REDEFINITION'
AND privilege = 'EXECUTE';

PROMPT
PROMPT ========================================
PROMPT Test 2: Recompile Package
PROMPT ========================================

@scripts/table_migration_analysis.sql

PROMPT
PROMPT ========================================
PROMPT Test 3: Check Package Status
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
PROMPT Expected: Both PACKAGE and PACKAGE BODY should be VALID
PROMPT Even without EXECUTE grant on DBMS_REDEFINITION!
PROMPT

PROMPT ========================================
PROMPT Test 4: Run Analysis
PROMPT ========================================

DECLARE
    v_task_id NUMBER;
    v_found NUMBER;
BEGIN
    -- Check if we have any tasks
    SELECT COUNT(*) INTO v_found
    FROM cmr.dwh_migration_tasks
    WHERE status = 'PENDING';

    IF v_found = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Creating test task...');
        INSERT INTO cmr.dwh_migration_tasks (
            task_name, source_owner, source_table, status
        ) VALUES (
            'Test Without Grant', USER, USER_TABLES.TABLE_NAME, 'PENDING'
        ) RETURNING task_id INTO v_task_id;
        COMMIT;
    ELSE
        SELECT MIN(task_id) INTO v_task_id
        FROM cmr.dwh_migration_tasks
        WHERE status = 'PENDING';
    END IF;

    DBMS_OUTPUT.PUT_LINE('Running analysis on task_id: ' || v_task_id);
    DBMS_OUTPUT.PUT_LINE('');

    -- Run analysis
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Test 5: Verify Results');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Show results
    FOR rec IN (
        SELECT
            t.task_id,
            t.task_name,
            t.source_table,
            t.status,
            t.validation_status,
            a.supports_online_redef,
            a.online_redef_method,
            a.recommended_method,
            a.recommended_strategy
        FROM cmr.dwh_migration_tasks t
        LEFT JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
        WHERE t.task_id = v_task_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Task: ' || rec.task_name);
        DBMS_OUTPUT.PUT_LINE('Table: ' || rec.source_table);
        DBMS_OUTPUT.PUT_LINE('Status: ' || rec.status);
        DBMS_OUTPUT.PUT_LINE('Validation: ' || rec.validation_status);
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Online Redefinition Check:');
        DBMS_OUTPUT.PUT_LINE('  supports_online_redef: ' || NVL(rec.supports_online_redef, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  online_redef_method: ' || NVL(rec.online_redef_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_method: ' || NVL(rec.recommended_method, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('  recommended_strategy: ' || NVL(rec.recommended_strategy, 'NULL'));
        DBMS_OUTPUT.PUT_LINE('');

        -- Interpret results
        IF rec.recommended_method IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('❌ FAILED: recommended_method is NULL');
            DBMS_OUTPUT.PUT_LINE('   Check compilation errors above.');
        ELSIF rec.supports_online_redef = 'N' AND rec.recommended_method = 'CTAS' THEN
            DBMS_OUTPUT.PUT_LINE('✓ SUCCESS (No Grant Mode):');
            DBMS_OUTPUT.PUT_LINE('   - Package compiled successfully without DBMS_REDEFINITION grant');
            DBMS_OUTPUT.PUT_LINE('   - Analysis completed successfully');
            DBMS_OUTPUT.PUT_LINE('   - All migrations will use CTAS method (requires downtime)');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('   To enable ONLINE method (near-zero downtime), grant as SYSDBA:');
            DBMS_OUTPUT.PUT_LINE('   GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
        ELSIF rec.supports_online_redef = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('✓ SUCCESS (Full Capability):');
            DBMS_OUTPUT.PUT_LINE('   - Package has EXECUTE grant on DBMS_REDEFINITION');
            DBMS_OUTPUT.PUT_LINE('   - Can recommend ONLINE method for large tables');
            DBMS_OUTPUT.PUT_LINE('   - Using method: ' || rec.online_redef_method);
            DBMS_OUTPUT.PUT_LINE('   - Recommended: ' || rec.recommended_method);
        ELSE
            DBMS_OUTPUT.PUT_LINE('⚠ UNEXPECTED STATE:');
            DBMS_OUTPUT.PUT_LINE('   supports_online_redef: ' || NVL(rec.supports_online_redef, 'NULL'));
            DBMS_OUTPUT.PUT_LINE('   recommended_method: ' || NVL(rec.recommended_method, 'NULL'));
        END IF;
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('❌ ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('   SQLCODE: ' || SQLCODE);
END;
/

PROMPT
PROMPT ========================================
PROMPT Summary
PROMPT ========================================
PROMPT
PROMPT This test verifies that the migration framework works
PROMPT with or without EXECUTE grant on DBMS_REDEFINITION.
PROMPT
PROMPT WITHOUT Grant:
PROMPT   - Package compiles successfully (using EXECUTE IMMEDIATE)
PROMPT   - Analysis runs without errors
PROMPT   - recommended_method = CTAS (always)
PROMPT   - All migrations use CTAS (requires downtime)
PROMPT
PROMPT WITH Grant:
PROMPT   - All of the above, PLUS:
PROMPT   - Can detect online redefinition capability
PROMPT   - Can recommend ONLINE method for large tables
PROMPT   - Near-zero downtime migrations available
PROMPT
PROMPT ========================================
