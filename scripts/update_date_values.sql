-- =============================================================================
-- Update Date Values from 5999-12-31 to 5999-01-01
-- =============================================================================
-- This script updates existing data and objects that used the old date value
-- Run this AFTER deploying the constants package and updated code
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK ON
SET VERIFY OFF

PROMPT
PROMPT ========================================
PROMPT Updating Date Values: 5999-12-31 to 5999-01-01
PROMPT ========================================
PROMPT

-- =============================================================================
-- 1. Update Configuration Values
-- =============================================================================
PROMPT 1. Updating dwh_ilm_config table...

UPDATE cmr.dwh_ilm_config
SET config_value = '5999-01-01'
WHERE config_key = 'NULL_DEFAULT_DATE'
AND config_value = '5999-12-31';

PROMPT   Rows updated: 
SELECT SQL%ROWCOUNT FROM dual;

COMMIT;

-- =============================================================================
-- 2. Update Migration Tasks (list_default_values column)
-- =============================================================================
PROMPT
PROMPT 2. Checking dwh_migration_tasks for old date values...

DECLARE
    v_count NUMBER;
BEGIN
    -- Check if any tasks have the old date in list_default_values
    SELECT COUNT(*) INTO v_count
    FROM cmr.dwh_migration_tasks
    WHERE list_default_values LIKE '%5999-12-31%';
    
    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('   Found ' || v_count || ' tasks with old date values');
        
        -- Update DATE references
        UPDATE cmr.dwh_migration_tasks
        SET list_default_values = REPLACE(list_default_values, 'DATE ''5999-12-31''', 'pck_dwh_constants.c_maxvalue_date')
        WHERE list_default_values LIKE '%DATE ''5999-12-31''%';
        
        DBMS_OUTPUT.PUT_LINE('   Updated DATE references: ' || SQL%ROWCOUNT || ' rows');
        
        -- Update TO_DATE references
        UPDATE cmr.dwh_migration_tasks
        SET list_default_values = REPLACE(list_default_values, 'TO_DATE(''5999-12-31'', ''YYYY-MM-DD'')', 'pck_dwh_constants.c_maxvalue_date')
        WHERE list_default_values LIKE '%TO_DATE(''5999-12-31''%';
        
        DBMS_OUTPUT.PUT_LINE('   Updated TO_DATE references: ' || SQL%ROWCOUNT || ' rows');
        
        -- Update TIMESTAMP references
        UPDATE cmr.dwh_migration_tasks
        SET list_default_values = REPLACE(list_default_values, 'TO_TIMESTAMP(''5999-12-31 23:59:59'', ''YYYY-MM-DD HH24:MI:SS'')', 'pck_dwh_constants.c_maxvalue_timestamp')
        WHERE list_default_values LIKE '%TO_TIMESTAMP(''5999-12-31%';
        
        DBMS_OUTPUT.PUT_LINE('   Updated TO_TIMESTAMP references: ' || SQL%ROWCOUNT || ' rows');
        
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('   No tasks found with old date values');
    END IF;
END;
/

-- =============================================================================
-- 3. Check for MAXVALUE Partitions with Old High Value
-- =============================================================================
PROMPT
PROMPT 3. Checking for partitions with old MAXVALUE date (5999-12-31)...
PROMPT    Note: These cannot be automatically updated - would require partition rebuild
PROMPT

SELECT 
    table_owner,
    table_name,
    partition_name,
    high_value,
    tablespace_name
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND high_value LIKE '%5999-12-31%'
ORDER BY table_owner, table_name, partition_position;

PROMPT
PROMPT    If partitions found above, they were created with old date.
PROMPT    Impact: Minimal - 5999-12-31 and 5999-01-01 both work as MAXVALUE
PROMPT    Action: No immediate action needed. Will use new date for future partitions.
PROMPT

-- =============================================================================
-- 4. Check Partition Access Tracking for Placeholder Dates
-- =============================================================================
PROMPT
PROMPT 4. Checking partition access tracking for old date values...

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM cmr.dwh_ilm_partition_access
    WHERE last_write_time = TO_DATE('5999-12-31', 'YYYY-MM-DD')
    OR last_read_time = TO_DATE('5999-12-31', 'YYYY-MM-DD');
    
    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('   Found ' || v_count || ' partition access records with old date');
        
        -- Update last_write_time
        UPDATE cmr.dwh_ilm_partition_access
        SET last_write_time = TO_DATE('5999-01-01', 'YYYY-MM-DD')
        WHERE last_write_time = TO_DATE('5999-12-31', 'YYYY-MM-DD');
        
        DBMS_OUTPUT.PUT_LINE('   Updated last_write_time: ' || SQL%ROWCOUNT || ' rows');
        
        -- Update last_read_time
        UPDATE cmr.dwh_ilm_partition_access
        SET last_read_time = TO_DATE('5999-01-01', 'YYYY-MM-DD')
        WHERE last_read_time = TO_DATE('5999-12-31', 'YYYY-MM-DD');
        
        DBMS_OUTPUT.PUT_LINE('   Updated last_read_time: ' || SQL%ROWCOUNT || ' rows');
        
        COMMIT;
    ELSE
        DBMS_OUTPUT.PUT_LINE('   No records found with old date values');
    END IF;
END;
/

-- =============================================================================
-- 5. Summary
-- =============================================================================
PROMPT
PROMPT ========================================
PROMPT Summary
PROMPT ========================================
PROMPT
PROMPT Configuration Updates:
PROMPT   - dwh_ilm_config.NULL_DEFAULT_DATE updated
PROMPT
PROMPT Migration Tasks:
PROMPT   - list_default_values column checked and updated
PROMPT
PROMPT Partition Access Tracking:
PROMPT   - last_write_time and last_read_time checked and updated
PROMPT
PROMPT Existing Partitions:
PROMPT   - Listed above (if any exist with old date)
PROMPT   - No action needed - both dates work as MAXVALUE
PROMPT   - New partitions will use 5999-01-01 going forward
PROMPT
PROMPT Next Steps:
PROMPT   1. Review any existing partitions listed above
PROMPT   2. Verify dwh_ilm_config shows: NULL_DEFAULT_DATE = '5999-01-01'
PROMPT   3. Future operations will use pck_dwh_constants.c_maxvalue_date
PROMPT
PROMPT ========================================
PROMPT Update Complete
PROMPT ========================================
PROMPT
