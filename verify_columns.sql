-- =============================================================================
-- Verify if the three columns exist in dwh_migration_analysis table
-- =============================================================================

SET SERVEROUTPUT ON

DECLARE
    v_column_exists NUMBER;
    v_columns_missing NUMBER := 0;
    TYPE t_column_list IS TABLE OF VARCHAR2(30);
    v_check_columns t_column_list := t_column_list(
        'SUPPORTS_ONLINE_REDEF',
        'ONLINE_REDEF_METHOD',
        'RECOMMENDED_METHOD'
    );
BEGIN
    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('Checking for missing columns in cmr.dwh_migration_analysis');
    DBMS_OUTPUT.PUT_LINE('=============================================================================');
    DBMS_OUTPUT.PUT_LINE('');

    FOR i IN 1..v_check_columns.COUNT LOOP
        SELECT COUNT(*)
        INTO v_column_exists
        FROM dba_tab_columns
        WHERE owner = 'CMR'
        AND table_name = 'DWH_MIGRATION_ANALYSIS'
        AND column_name = v_check_columns(i);

        IF v_column_exists > 0 THEN
            DBMS_OUTPUT.PUT_LINE('✓ ' || v_check_columns(i) || ' - EXISTS');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ ' || v_check_columns(i) || ' - MISSING');
            v_columns_missing := v_columns_missing + 1;
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=============================================================================');

    IF v_columns_missing = 0 THEN
        DBMS_OUTPUT.PUT_LINE('RESULT: All columns exist. No action needed.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('These columns are automatically populated by analyze_table():');
        DBMS_OUTPUT.PUT_LINE('  - supports_online_redef: Y/N (can table use DBMS_REDEFINITION?)');
        DBMS_OUTPUT.PUT_LINE('  - online_redef_method: CONS_USE_PK/CONS_USE_ROWID/NULL');
        DBMS_OUTPUT.PUT_LINE('  - recommended_method: CTAS/ONLINE/EXCHANGE');
    ELSE
        DBMS_OUTPUT.PUT_LINE('RESULT: ' || v_columns_missing || ' column(s) missing.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('ACTION REQUIRED: Run the following to add missing columns:');
        DBMS_OUTPUT.PUT_LINE('');

        FOR i IN 1..v_check_columns.COUNT LOOP
            SELECT COUNT(*)
            INTO v_column_exists
            FROM dba_tab_columns
            WHERE owner = 'CMR'
            AND table_name = 'DWH_MIGRATION_ANALYSIS'
            AND column_name = v_check_columns(i);

            IF v_column_exists = 0 THEN
                CASE v_check_columns(i)
                    WHEN 'SUPPORTS_ONLINE_REDEF' THEN
                        DBMS_OUTPUT.PUT_LINE('ALTER TABLE cmr.dwh_migration_analysis ADD supports_online_redef CHAR(1) DEFAULT ''N'';');
                    WHEN 'ONLINE_REDEF_METHOD' THEN
                        DBMS_OUTPUT.PUT_LINE('ALTER TABLE cmr.dwh_migration_analysis ADD online_redef_method VARCHAR2(30);');
                    WHEN 'RECOMMENDED_METHOD' THEN
                        DBMS_OUTPUT.PUT_LINE('ALTER TABLE cmr.dwh_migration_analysis ADD recommended_method VARCHAR2(30);');
                END CASE;
            END IF;
        END LOOP;
    END IF;

    DBMS_OUTPUT.PUT_LINE('=============================================================================');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR checking columns: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Make sure you have access to DBA_TAB_COLUMNS view.');
END;
/

-- Also show the actual column list from the table
PROMPT
PROMPT Current columns in cmr.dwh_migration_analysis:
PROMPT ============================================

SELECT
    column_name,
    data_type,
    data_length,
    nullable
FROM dba_tab_columns
WHERE owner = 'CMR'
AND table_name = 'DWH_MIGRATION_ANALYSIS'
ORDER BY column_id;
