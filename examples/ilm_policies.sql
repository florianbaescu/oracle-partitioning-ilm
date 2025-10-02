-- =============================================================================
-- ILM (Information Lifecycle Management) Policy Examples
-- Automated Data Optimization for Oracle Data Warehouse
-- =============================================================================

-- =============================================================================
-- SECTION 1: ENABLE HEAT MAP AND ADO
-- =============================================================================

-- Enable Heat Map at database level (requires restart or SCOPE=BOTH)
ALTER SYSTEM SET HEAT_MAP = ON SCOPE=BOTH;

-- Verify Heat Map is enabled
SELECT PARAMETER, VALUE FROM V$OPTION WHERE PARAMETER = 'Heat Map';

-- Enable Automatic Data Optimization
ALTER SYSTEM SET "_heat_map_enabled" = TRUE;

-- Enable auto space advisor task
BEGIN
    DBMS_AUTO_TASK_ADMIN.ENABLE(
        client_name => 'auto space advisor',
        operation => NULL,
        window_name => NULL
    );
END;
/


-- =============================================================================
-- SECTION 2: BASIC ILM POLICIES - COMPRESSION ONLY
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Compress segments after 90 days of no modification
-- -----------------------------------------------------------------------------

ALTER TABLE sales_fact ILM ADD POLICY
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 90 DAYS OF NO MODIFICATION;

-- Verify policy
SELECT * FROM USER_ILMPOLICIES WHERE OBJECT_NAME = 'SALES_FACT';


-- -----------------------------------------------------------------------------
-- Example 2: Compress segments after 6 months using archive compression
-- -----------------------------------------------------------------------------

ALTER TABLE order_fact ILM ADD POLICY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 6 MONTHS OF NO MODIFICATION;


-- -----------------------------------------------------------------------------
-- Example 3: Row-level compression (compress inactive rows within partition)
-- -----------------------------------------------------------------------------

ALTER TABLE web_events_fact ILM ADD POLICY
    ROW STORE COMPRESS ADVANCED
    ROW
    AFTER 30 DAYS OF NO MODIFICATION;


-- =============================================================================
-- SECTION 3: STORAGE TIERING POLICIES
-- =============================================================================

-- Create tablespaces for different storage tiers
-- (Assumes storage is properly configured with different performance characteristics)

CREATE TABLESPACE tbs_hot
    DATAFILE '/u01/oradata/hot/hot01.dbf' SIZE 10G AUTOEXTEND ON NEXT 1G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

CREATE TABLESPACE tbs_warm
    DATAFILE '/u02/oradata/warm/warm01.dbf' SIZE 50G AUTOEXTEND ON NEXT 5G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

CREATE TABLESPACE tbs_cold
    DATAFILE '/u03/oradata/cold/cold01.dbf' SIZE 100G AUTOEXTEND ON NEXT 10G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

CREATE TABLESPACE tbs_archive
    DATAFILE '/u04/oradata/archive/archive01.dbf' SIZE 500G AUTOEXTEND ON NEXT 50G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

-- -----------------------------------------------------------------------------
-- Example 4: Multi-tier storage policy for sales fact table
-- -----------------------------------------------------------------------------

-- Tier 1: Move to warm storage after 3 months, compress
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tbs_warm
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 90 DAYS OF NO MODIFICATION;

-- Tier 2: Move to cold storage after 1 year, archive compression
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tbs_cold
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 12 MONTHS OF NO MODIFICATION;

-- Tier 3: Move to archive after 3 years, read-only
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tbs_archive
    READ ONLY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 36 MONTHS OF NO MODIFICATION;


-- -----------------------------------------------------------------------------
-- Example 5: Financial transactions - compliance-driven tiering
-- -----------------------------------------------------------------------------

-- Keep active for 6 months
ALTER TABLE financial_transactions ILM ADD POLICY
    TIER TO tbs_warm
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 6 MONTHS OF NO MODIFICATION;

-- Archive after 2 years but keep queryable
ALTER TABLE financial_transactions ILM ADD POLICY
    TIER TO tbs_cold
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 24 MONTHS OF NO MODIFICATION;

-- Make read-only after 7 years (compliance requirement)
ALTER TABLE financial_transactions ILM ADD POLICY
    TIER TO tbs_archive
    READ ONLY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 84 MONTHS OF NO MODIFICATION;


-- =============================================================================
-- SECTION 4: PARTITION-SPECIFIC ILM POLICIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 6: Apply policy to specific partition
-- -----------------------------------------------------------------------------

ALTER TABLE sales_fact
    MODIFY PARTITION p_2023_01
    ILM ADD POLICY
        COMPRESS FOR ARCHIVE HIGH
        SEGMENT;

-- Mark old partition as read-only
ALTER TABLE sales_fact
    MODIFY PARTITION p_2023_01
    READ ONLY;


-- -----------------------------------------------------------------------------
-- Example 7: Different policies for different partition ranges
-- -----------------------------------------------------------------------------

-- Recent partitions: light compression after 60 days
ALTER TABLE order_fact ILM ADD POLICY
    COMPRESS FOR QUERY LOW
    SEGMENT
    AFTER 60 DAYS OF NO MODIFICATION
    PRIORITY 1;

-- Older partitions: heavy compression after 1 year
ALTER TABLE order_fact ILM ADD POLICY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 365 DAYS OF NO MODIFICATION
    PRIORITY 2;


-- =============================================================================
-- SECTION 5: CUSTOM FUNCTION-BASED POLICIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 8: Custom policy function based on size and age
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ilm_archive_eligible(
    p_segment_name VARCHAR2,
    p_tablespace_name VARCHAR2,
    p_last_modification_time TIMESTAMP
) RETURN NUMBER IS
    v_days_old NUMBER;
    v_segment_size_mb NUMBER;
BEGIN
    -- Calculate age in days
    v_days_old := TRUNC(SYSDATE - CAST(p_last_modification_time AS DATE));

    -- Get segment size
    SELECT SUM(bytes)/1024/1024 INTO v_segment_size_mb
    FROM user_segments
    WHERE segment_name = p_segment_name
    AND tablespace_name = p_tablespace_name;

    -- Archive if:
    -- 1. Older than 180 days AND larger than 1GB, OR
    -- 2. Older than 365 days regardless of size
    IF (v_days_old > 180 AND v_segment_size_mb > 1024) OR
       (v_days_old > 365) THEN
        RETURN 1; -- Eligible
    END IF;

    RETURN 0; -- Not eligible
END;
/

-- Apply custom function-based policy
ALTER TABLE sales_fact ILM ADD POLICY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    ON ilm_archive_eligible(segment_name, tablespace_name, last_modification_time);


-- =============================================================================
-- SECTION 6: GROUP-LEVEL POLICIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 9: Create and apply policy to table group
-- -----------------------------------------------------------------------------

-- Create ILM policy
BEGIN
    DBMS_ILM_ADMIN.CUSTOMIZE_ILM(
        policy_name => 'COMPRESS_WARM_DATA',
        action_type => DBMS_ILM.COMPRESS_FOR,
        compression_level => DBMS_ILM.QUERY_HIGH,
        condition => 'AFTER 90 DAYS OF NO MODIFICATION',
        scope => DBMS_ILM.SCOPE_SEGMENT
    );
END;
/

-- Apply to multiple tables
BEGIN
    FOR rec IN (
        SELECT table_name
        FROM user_tables
        WHERE table_name LIKE '%_FACT'
    ) LOOP
        EXECUTE IMMEDIATE
            'ALTER TABLE ' || rec.table_name ||
            ' ILM ADD POLICY COMPRESS_WARM_DATA';
    END LOOP;
END;
/


-- =============================================================================
-- SECTION 7: MONITORING AND EXECUTION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check Heat Map statistics
-- -----------------------------------------------------------------------------

-- View segment-level heat map data
SELECT
    object_name,
    subobject_name AS partition_name,
    segment_write_time,
    segment_read_time,
    full_scan,
    lookup_scan
FROM user_heat_map_segment
WHERE object_name = 'SALES_FACT'
ORDER BY segment_write_time DESC;

-- View all ILM policies
SELECT
    policy_name,
    object_name,
    subobject_name,
    action_type,
    compression_level,
    condition_type,
    condition_days,
    enabled
FROM user_ilmpolicies
ORDER BY object_name, policy_name;

-- Check segments eligible for ILM policies
SELECT
    object_name,
    subobject_name,
    policy_name,
    execution_mode,
    evaluation_time,
    execution_time
FROM user_ilmevaluationdetails
WHERE object_name = 'SALES_FACT'
ORDER BY evaluation_time DESC;


-- -----------------------------------------------------------------------------
-- Execute ILM policies manually
-- -----------------------------------------------------------------------------

-- Execute all pending ILM tasks immediately
EXEC DBMS_ILM.FLUSH_ALL_SEGMENTS;

-- Execute ILM for specific table
DECLARE
    v_task_id NUMBER;
BEGIN
    v_task_id := DBMS_ILM.EXECUTE_ILM(
        owner => USER,
        object_name => 'SALES_FACT',
        task_id => NULL,
        policy_name => NULL,
        execution_mode => DBMS_ILM.EXECUTE_OFFLINE
    );

    DBMS_OUTPUT.PUT_LINE('Task ID: ' || v_task_id);
END;
/

-- Check task status
SELECT
    task_id,
    object_name,
    subobject_name,
    policy_name,
    state,
    start_time,
    completion_time
FROM user_ilmtasks
WHERE object_name = 'SALES_FACT'
ORDER BY start_time DESC;


-- -----------------------------------------------------------------------------
-- Review ILM results and space savings
-- -----------------------------------------------------------------------------

-- Check compression results
SELECT
    object_name,
    subobject_name,
    policy_name,
    action_type,
    completion_time,
    initial_size_mb,
    final_size_mb,
    space_saved_mb,
    ROUND((space_saved_mb / NULLIF(initial_size_mb, 0)) * 100, 2) AS pct_saved
FROM (
    SELECT
        object_name,
        subobject_name,
        policy_name,
        action_type,
        completion_time,
        ROUND(bytes_before/1024/1024, 2) AS initial_size_mb,
        ROUND(bytes_after/1024/1024, 2) AS final_size_mb,
        ROUND((bytes_before - bytes_after)/1024/1024, 2) AS space_saved_mb
    FROM user_ilmresults
    WHERE object_name = 'SALES_FACT'
)
ORDER BY completion_time DESC;


-- =============================================================================
-- SECTION 8: DISABLE AND REMOVE POLICIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Disable policy without removing it
-- -----------------------------------------------------------------------------

ALTER TABLE sales_fact ILM DISABLE POLICY policy_name;

-- Re-enable policy
ALTER TABLE sales_fact ILM ENABLE POLICY policy_name;


-- -----------------------------------------------------------------------------
-- Remove specific policy
-- -----------------------------------------------------------------------------

ALTER TABLE sales_fact ILM DELETE POLICY policy_name;

-- Remove all policies from table
ALTER TABLE sales_fact ILM DELETE_ALL;


-- =============================================================================
-- SECTION 9: AUTOMATED PARTITION MANAGEMENT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Stored procedure: Automatic old partition archival and purge
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE manage_partition_lifecycle(
    p_table_name VARCHAR2,
    p_schema VARCHAR2 DEFAULT USER,
    p_archive_months NUMBER DEFAULT 36,
    p_purge_months NUMBER DEFAULT 84
) AS
    v_partition_name VARCHAR2(128);
    v_high_value LONG;
    v_partition_date DATE;
    v_archive_cutoff DATE := ADD_MONTHS(SYSDATE, -p_archive_months);
    v_purge_cutoff DATE := ADD_MONTHS(SYSDATE, -p_purge_months);
    v_sql VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('Processing table: ' || p_table_name);
    DBMS_OUTPUT.PUT_LINE('Archive cutoff: ' || TO_CHAR(v_archive_cutoff, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('Purge cutoff: ' || TO_CHAR(v_purge_cutoff, 'YYYY-MM-DD'));

    FOR rec IN (
        SELECT partition_name, high_value, partition_position
        FROM all_tab_partitions
        WHERE table_owner = p_schema
        AND table_name = UPPER(p_table_name)
        ORDER BY partition_position
    ) LOOP
        BEGIN
            -- Extract partition date from high_value
            v_sql := 'SELECT ' || rec.high_value || ' FROM DUAL';
            EXECUTE IMMEDIATE v_sql INTO v_partition_date;

            -- Check if eligible for purge
            IF v_partition_date < v_purge_cutoff THEN
                DBMS_OUTPUT.PUT_LINE('Purging partition: ' || rec.partition_name ||
                                   ' (Date: ' || TO_CHAR(v_partition_date, 'YYYY-MM-DD') || ')');

                -- Optional: Export partition before dropping
                -- export_partition_to_external(p_table_name, rec.partition_name);

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_schema || '.' || p_table_name ||
                    ' DROP PARTITION ' || rec.partition_name;

            -- Check if eligible for archive
            ELSIF v_partition_date < v_archive_cutoff THEN
                DBMS_OUTPUT.PUT_LINE('Archiving partition: ' || rec.partition_name ||
                                   ' (Date: ' || TO_CHAR(v_partition_date, 'YYYY-MM-DD') || ')');

                -- Move to archive tablespace and compress
                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_schema || '.' || p_table_name ||
                    ' MOVE PARTITION ' || rec.partition_name ||
                    ' TABLESPACE tbs_archive COMPRESS FOR ARCHIVE HIGH';

                -- Make read-only
                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_schema || '.' || p_table_name ||
                    ' MODIFY PARTITION ' || rec.partition_name || ' READ ONLY';

                -- Rebuild indexes
                FOR idx IN (
                    SELECT index_name, partition_name
                    FROM all_ind_partitions
                    WHERE index_owner = p_schema
                    AND index_name IN (
                        SELECT index_name FROM all_indexes
                        WHERE table_owner = p_schema
                        AND table_name = UPPER(p_table_name)
                    )
                    AND partition_name = rec.partition_name
                ) LOOP
                    EXECUTE IMMEDIATE
                        'ALTER INDEX ' || p_schema || '.' || idx.index_name ||
                        ' REBUILD PARTITION ' || idx.partition_name ||
                        ' TABLESPACE tbs_archive COMPRESS';
                END LOOP;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error processing partition ' || rec.partition_name || ': ' || SQLERRM);
        END;
    END LOOP;

    -- Gather statistics after modifications
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => p_schema,
        tabname => p_table_name,
        cascade => TRUE,
        degree => 4
    );

    DBMS_OUTPUT.PUT_LINE('Partition lifecycle management completed for ' || p_table_name);
END;
/


-- -----------------------------------------------------------------------------
-- Schedule partition lifecycle job
-- -----------------------------------------------------------------------------

BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'PARTITION_LIFECYCLE_JOB',
        job_type => 'PLSQL_BLOCK',
        job_action => '
            BEGIN
                manage_partition_lifecycle(''SALES_FACT'', p_archive_months => 36, p_purge_months => 84);
                manage_partition_lifecycle(''ORDER_FACT'', p_archive_months => 24, p_purge_months => 60);
                manage_partition_lifecycle(''WEB_EVENTS_FACT'', p_archive_months => 12, p_purge_months => 36);
            END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=2',
        enabled => TRUE,
        comments => 'Monthly partition lifecycle management for data warehouse tables'
    );
END;
/


-- =============================================================================
-- SECTION 10: REPORTING AND VALIDATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Comprehensive ILM status report
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_status_report AS
SELECT
    t.table_name,
    t.num_rows,
    ROUND(SUM(s.bytes)/1024/1024/1024, 2) AS total_size_gb,
    COUNT(DISTINCT tp.partition_name) AS partition_count,
    COUNT(DISTINCT CASE WHEN tp.compression = 'DISABLED' THEN tp.partition_name END) AS uncompressed_parts,
    COUNT(DISTINCT CASE WHEN tp.compression = 'ENABLED' AND tp.compress_for = 'QUERY HIGH' THEN tp.partition_name END) AS query_compressed_parts,
    COUNT(DISTINCT CASE WHEN tp.compression = 'ENABLED' AND tp.compress_for LIKE 'ARCHIVE%' THEN tp.partition_name END) AS archive_compressed_parts,
    COUNT(DISTINCT CASE WHEN tp.read_only = 'YES' THEN tp.partition_name END) AS readonly_parts,
    COUNT(DISTINCT p.policy_name) AS ilm_policies,
    MAX(h.segment_write_time) AS last_write_time,
    TRUNC(SYSDATE - MAX(h.segment_write_time)) AS days_since_last_write
FROM user_tables t
LEFT JOIN user_segments s ON s.segment_name = t.table_name
LEFT JOIN user_tab_partitions tp ON tp.table_name = t.table_name
LEFT JOIN user_ilmpolicies p ON p.object_name = t.table_name
LEFT JOIN user_heat_map_segment h ON h.object_name = t.table_name
WHERE t.partitioned = 'YES'
GROUP BY t.table_name, t.num_rows
ORDER BY total_size_gb DESC;

-- Query the report
SELECT * FROM v_ilm_status_report;


-- Partition-level detail report
SELECT
    table_name,
    partition_name,
    partition_position,
    high_value,
    num_rows,
    ROUND(bytes/1024/1024, 2) AS size_mb,
    compression,
    compress_for,
    tablespace_name,
    read_only
FROM (
    SELECT
        tp.table_name,
        tp.partition_name,
        tp.partition_position,
        tp.high_value,
        tp.num_rows,
        s.bytes,
        tp.compression,
        tp.compress_for,
        s.tablespace_name,
        tp.read_only
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name = 'SALES_FACT'
)
ORDER BY partition_position DESC;
