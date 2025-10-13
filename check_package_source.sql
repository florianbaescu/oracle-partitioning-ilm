-- =============================================================================
-- Check if compiled package matches source file
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Checking Package Source in Database
PROMPT ========================================

-- Check if v_recommended_method variable exists in compiled package
SELECT
    'v_recommended_method declaration' AS check_item,
    COUNT(*) AS found_count,
    CASE WHEN COUNT(*) > 0 THEN '✓ FOUND' ELSE '✗ MISSING' END AS status
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%VARCHAR2%'
UNION ALL
SELECT
    'v_supports_online_redef declaration',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✓ FOUND' ELSE '✗ MISSING' END
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_SUPPORTS_ONLINE_REDEF%CHAR%'
UNION ALL
SELECT
    'DBMS_REDEFINITION.CAN_REDEF_TABLE call',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✓ FOUND' ELSE '✗ MISSING' END
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%DBMS_REDEFINITION.CAN_REDEF_TABLE%'
UNION ALL
SELECT
    'recommended_method in MERGE UPDATE',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✓ FOUND' ELSE '✗ MISSING' END
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%RECOMMENDED_METHOD = V_RECOMMENDED_METHOD%'
UNION ALL
SELECT
    'recommended_method in MERGE INSERT',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✓ FOUND' ELSE '✗ MISSING' END
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD,%';

PROMPT
PROMPT ========================================
PROMPT Package Compilation Status
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
PROMPT ========================================
PROMPT Check for Compilation Errors
PROMPT ========================================

SELECT
    line,
    position,
    text
FROM dba_errors
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
ORDER BY sequence;

PROMPT
PROMPT ========================================
PROMPT Show Relevant Source Lines
PROMPT ========================================
PROMPT Looking for v_recommended_method initialization...

SELECT line, text
FROM dba_source
WHERE owner = 'CMR'
AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
AND type = 'PACKAGE BODY'
AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%'
ORDER BY line
FETCH FIRST 10 ROWS ONLY;

PROMPT
PROMPT ========================================
PROMPT Recommendation
PROMPT ========================================

DECLARE
    v_missing_count NUMBER := 0;
BEGIN
    SELECT COUNT(*) INTO v_missing_count
    FROM (
        SELECT COUNT(*) AS cnt
        FROM dba_source
        WHERE owner = 'CMR'
        AND name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER'
        AND type = 'PACKAGE BODY'
        AND UPPER(text) LIKE '%V_RECOMMENDED_METHOD%VARCHAR2%'
    )
    WHERE cnt = 0;

    IF v_missing_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('❌ PROBLEM FOUND:');
        DBMS_OUTPUT.PUT_LINE('   The compiled package body does NOT contain v_recommended_method logic.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('ACTION REQUIRED:');
        DBMS_OUTPUT.PUT_LINE('   1. Recompile the package from source:');
        DBMS_OUTPUT.PUT_LINE('      @scripts/table_migration_analysis.sql');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('   2. Grant EXECUTE on DBMS_REDEFINITION (optional but recommended):');
        DBMS_OUTPUT.PUT_LINE('      GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ Package source looks correct.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('GRANT STATUS CHECK:');
        DBMS_OUTPUT.PUT_LINE('  Check if CMR has EXECUTE on DBMS_REDEFINITION:');
        DBMS_OUTPUT.PUT_LINE('  SELECT * FROM dba_tab_privs');
        DBMS_OUTPUT.PUT_LINE('  WHERE grantee = ''CMR''');
        DBMS_OUTPUT.PUT_LINE('  AND table_name = ''DBMS_REDEFINITION'';');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  If no rows, grant it:');
        DBMS_OUTPUT.PUT_LINE('  GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
    END IF;
END;
/
