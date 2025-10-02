-- =============================================================================
-- Monitoring and Diagnostics Scripts
-- For Oracle Partitioned Tables and ILM
-- =============================================================================

-- =============================================================================
-- SECTION 1: PARTITION DISTRIBUTION AND SKEW ANALYSIS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Analyze partition distribution and identify skew
-- -----------------------------------------------------------------------------

SELECT
    table_name,
    partition_name,
    partition_position,
    num_rows,
    ROUND(num_rows * 100.0 / SUM(num_rows) OVER (PARTITION BY table_name), 2) AS pct_of_total,
    ROUND(bytes/1024/1024, 2) AS size_mb,
    ROUND(bytes/1024/1024 * 100.0 / SUM(bytes) OVER (PARTITION BY table_name), 2) AS pct_size,
    compression,
    compress_for,
    CASE
        WHEN num_rows > AVG(num_rows) OVER (PARTITION BY table_name) * 1.5 THEN 'OVERSIZED'
        WHEN num_rows < AVG(num_rows) OVER (PARTITION BY table_name) * 0.5 THEN 'UNDERSIZED'
        ELSE 'NORMAL'
    END AS size_category
FROM (
    SELECT
        tp.table_name,
        tp.partition_name,
        tp.partition_position,
        tp.num_rows,
        s.bytes,
        tp.compression,
        tp.compress_for
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name IN ('SALES_FACT', 'ORDER_FACT', 'WEB_EVENTS_FACT')
)
ORDER BY table_name, partition_position DESC;


-- =============================================================================
-- SECTION 2: PARTITION PRUNING ANALYSIS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check if queries are using partition pruning
-- Enable this before running queries to analyze
-- -----------------------------------------------------------------------------

ALTER SESSION SET STATISTICS_LEVEL = ALL;

-- Run your query here, then check execution plan
-- Example:
-- SELECT * FROM sales_fact WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-01-31';

-- View execution plan with partition pruning details
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST PARTITION'));

-- Reset statistics level
ALTER SESSION SET STATISTICS_LEVEL = TYPICAL;


-- -----------------------------------------------------------------------------
-- Analyze partition access patterns from AWR
-- -----------------------------------------------------------------------------

SELECT
    obj.object_name,
    obj.subobject_name AS partition_name,
    SUM(s.logical_reads_total) AS logical_reads,
    SUM(s.physical_reads_total) AS physical_reads,
    SUM(s.executions_total) AS executions,
    ROUND(SUM(s.elapsed_time_total)/1000000, 2) AS elapsed_time_sec
FROM dba_hist_seg_stat s
JOIN dba_hist_seg_stat_obj obj ON s.obj# = obj.obj#
WHERE obj.object_name IN ('SALES_FACT', 'ORDER_FACT')
AND s.snap_id BETWEEN (SELECT MAX(snap_id) - 7 FROM dba_hist_snapshot)
              AND (SELECT MAX(snap_id) FROM dba_hist_snapshot)
GROUP BY obj.object_name, obj.subobject_name
ORDER BY logical_reads DESC
FETCH FIRST 20 ROWS ONLY;


-- =============================================================================
-- SECTION 3: COMPRESSION EFFECTIVENESS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Analyze compression ratios and savings
-- -----------------------------------------------------------------------------

SELECT
    table_name,
    COUNT(DISTINCT partition_name) AS total_partitions,
    COUNT(DISTINCT CASE WHEN compression = 'DISABLED' THEN partition_name END) AS uncompressed,
    COUNT(DISTINCT CASE WHEN compress_for LIKE '%QUERY%' THEN partition_name END) AS query_compressed,
    COUNT(DISTINCT CASE WHEN compress_for LIKE '%ARCHIVE%' THEN partition_name END) AS archive_compressed,
    ROUND(SUM(CASE WHEN compression = 'DISABLED' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS uncompressed_gb,
    ROUND(SUM(CASE WHEN compress_for LIKE '%QUERY%' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS query_compressed_gb,
    ROUND(SUM(CASE WHEN compress_for LIKE '%ARCHIVE%' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS archive_compressed_gb,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS total_size_gb
FROM (
    SELECT
        tp.table_name,
        tp.partition_name,
        tp.compression,
        tp.compress_for,
        NVL(s.bytes, 0) AS bytes
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name LIKE '%_FACT'
)
GROUP BY table_name
ORDER BY total_size_gb DESC;


-- -----------------------------------------------------------------------------
-- Estimate potential compression savings
-- -----------------------------------------------------------------------------

WITH partition_sizes AS (
    SELECT
        tp.table_name,
        tp.partition_name,
        tp.compression,
        tp.compress_for,
        NVL(s.bytes, 0)/1024/1024/1024 AS size_gb
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name LIKE '%_FACT'
    AND tp.compression = 'DISABLED'
)
SELECT
    table_name,
    COUNT(*) AS uncompressed_partitions,
    ROUND(SUM(size_gb), 2) AS current_size_gb,
    ROUND(SUM(size_gb) * 0.25, 2) AS estimated_query_high_gb,
    ROUND(SUM(size_gb) * 0.10, 2) AS estimated_archive_high_gb,
    ROUND(SUM(size_gb) * 0.75, 2) AS estimated_savings_query_gb,
    ROUND(SUM(size_gb) * 0.90, 2) AS estimated_savings_archive_gb
FROM partition_sizes
GROUP BY table_name
ORDER BY current_size_gb DESC;


-- =============================================================================
-- SECTION 4: ILM POLICY MONITORING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- View all active ILM policies
-- -----------------------------------------------------------------------------

SELECT
    p.policy_name,
    p.object_owner,
    p.object_name,
    p.subobject_name,
    p.action_type,
    p.scope,
    p.compression_level,
    p.condition_type,
    p.condition_days,
    p.enabled,
    p.policy_subtype,
    COUNT(e.policy_name) AS pending_evaluations
FROM dba_ilmpolicies p
LEFT JOIN dba_ilmevaluationdetails e
    ON e.policy_name = p.policy_name
    AND e.object_name = p.object_name
WHERE p.object_owner = USER
GROUP BY
    p.policy_name, p.object_owner, p.object_name, p.subobject_name,
    p.action_type, p.scope, p.compression_level, p.condition_type,
    p.condition_days, p.enabled, p.policy_subtype
ORDER BY p.object_name, p.policy_name;


-- -----------------------------------------------------------------------------
-- Check ILM policy execution history
-- -----------------------------------------------------------------------------

SELECT
    task_id,
    object_owner,
    object_name,
    subobject_name,
    policy_name,
    state,
    start_time,
    completion_time,
    ROUND((completion_time - start_time) * 24 * 60, 2) AS duration_minutes,
    error_message
FROM dba_ilmtasks
WHERE object_owner = USER
ORDER BY start_time DESC
FETCH FIRST 50 ROWS ONLY;


-- -----------------------------------------------------------------------------
-- View ILM execution results and space savings
-- -----------------------------------------------------------------------------

SELECT
    r.object_name,
    r.subobject_name,
    r.policy_name,
    r.action_type,
    r.execution_time,
    ROUND(r.bytes_before/1024/1024/1024, 2) AS size_before_gb,
    ROUND(r.bytes_after/1024/1024/1024, 2) AS size_after_gb,
    ROUND((r.bytes_before - r.bytes_after)/1024/1024/1024, 2) AS space_saved_gb,
    ROUND(((r.bytes_before - r.bytes_after) / NULLIF(r.bytes_before, 0)) * 100, 2) AS pct_saved
FROM dba_ilmresults r
WHERE r.object_owner = USER
ORDER BY r.execution_time DESC
FETCH FIRST 20 ROWS ONLY;


-- -----------------------------------------------------------------------------
-- Check segments eligible for ILM policies
-- -----------------------------------------------------------------------------

SELECT
    object_name,
    subobject_name,
    policy_name,
    execution_mode,
    evaluation_time,
    TRUNC(SYSDATE - evaluation_time) AS days_since_eval,
    is_current
FROM user_ilmevaluationdetails
WHERE object_name LIKE '%_FACT'
ORDER BY evaluation_time DESC;


-- =============================================================================
-- SECTION 5: HEAT MAP ANALYSIS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check if Heat Map is enabled
-- -----------------------------------------------------------------------------

SELECT parameter, value
FROM v$option
WHERE parameter = 'Heat Map';


-- -----------------------------------------------------------------------------
-- Analyze segment access patterns
-- -----------------------------------------------------------------------------

SELECT
    object_name,
    subobject_name AS partition_name,
    segment_write_time,
    segment_read_time,
    TRUNC(SYSDATE - segment_write_time) AS days_since_write,
    TRUNC(SYSDATE - segment_read_time) AS days_since_read,
    full_scan,
    lookup_scan,
    CASE
        WHEN segment_write_time > SYSDATE - 90 THEN 'HOT'
        WHEN segment_write_time > SYSDATE - 365 THEN 'WARM'
        WHEN segment_write_time > SYSDATE - 1095 THEN 'COOL'
        ELSE 'COLD'
    END AS data_temperature
FROM user_heat_map_segment
WHERE object_name LIKE '%_FACT'
ORDER BY segment_write_time DESC NULLS LAST;


-- -----------------------------------------------------------------------------
-- Identify cold data candidates for archival
-- -----------------------------------------------------------------------------

SELECT
    h.object_name,
    h.subobject_name AS partition_name,
    h.segment_write_time,
    TRUNC(SYSDATE - h.segment_write_time) AS days_inactive,
    tp.compression,
    tp.compress_for,
    s.tablespace_name,
    ROUND(s.bytes/1024/1024, 2) AS size_mb,
    CASE
        WHEN TRUNC(SYSDATE - h.segment_write_time) > 1095 THEN 'PURGE CANDIDATE'
        WHEN TRUNC(SYSDATE - h.segment_write_time) > 730 THEN 'ARCHIVE CANDIDATE'
        WHEN TRUNC(SYSDATE - h.segment_write_time) > 365 THEN 'COMPRESS CANDIDATE'
        ELSE 'ACTIVE'
    END AS recommendation
FROM user_heat_map_segment h
JOIN user_tab_partitions tp
    ON tp.table_name = h.object_name
    AND tp.partition_name = h.subobject_name
LEFT JOIN user_segments s
    ON s.segment_name = h.object_name
    AND s.partition_name = h.subobject_name
WHERE h.object_name LIKE '%_FACT'
AND TRUNC(SYSDATE - h.segment_write_time) > 90
ORDER BY days_inactive DESC;


-- =============================================================================
-- SECTION 6: INDEX HEALTH AND MAINTENANCE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check for unusable index partitions
-- -----------------------------------------------------------------------------

SELECT
    i.table_name,
    ip.index_name,
    ip.partition_name,
    ip.status,
    ip.tablespace_name,
    ROUND(s.bytes/1024/1024, 2) AS size_mb
FROM user_ind_partitions ip
JOIN user_indexes i ON i.index_name = ip.index_name
LEFT JOIN user_segments s
    ON s.segment_name = ip.index_name
    AND s.partition_name = ip.partition_name
WHERE i.table_name LIKE '%_FACT'
AND ip.status != 'USABLE'
ORDER BY i.table_name, ip.index_name, ip.partition_position;


-- -----------------------------------------------------------------------------
-- Identify index partitions needing rebuild
-- -----------------------------------------------------------------------------

SELECT
    i.table_name,
    ip.index_name,
    ip.partition_name,
    ip.blevel,
    ip.leaf_blocks,
    ip.distinct_keys,
    ip.clustering_factor,
    ROUND(s.bytes/1024/1024, 2) AS size_mb,
    CASE
        WHEN ip.blevel > 4 THEN 'REBUILD - High B-Level'
        WHEN ip.clustering_factor > ip.num_rows * 2 THEN 'REBUILD - Poor Clustering'
        ELSE 'OK'
    END AS recommendation
FROM user_ind_partitions ip
JOIN user_indexes i ON i.index_name = ip.index_name
LEFT JOIN user_segments s
    ON s.segment_name = ip.index_name
    AND s.partition_name = ip.partition_name
WHERE i.table_name LIKE '%_FACT'
AND ip.status = 'USABLE'
AND (ip.blevel > 4 OR ip.clustering_factor > ip.num_rows * 2)
ORDER BY i.table_name, ip.index_name;


-- =============================================================================
-- SECTION 7: STATISTICS HEALTH
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Identify partitions with missing or stale statistics
-- -----------------------------------------------------------------------------

SELECT
    table_name,
    partition_name,
    partition_position,
    num_rows,
    last_analyzed,
    TRUNC(SYSDATE - last_analyzed) AS days_since_analyzed,
    stale_stats,
    CASE
        WHEN last_analyzed IS NULL THEN 'MISSING STATS'
        WHEN stale_stats = 'YES' THEN 'STALE'
        WHEN TRUNC(SYSDATE - last_analyzed) > 30 THEN 'OLD STATS'
        ELSE 'OK'
    END AS stats_status
FROM user_tab_statistics
WHERE object_type = 'PARTITION'
AND table_name LIKE '%_FACT'
AND (last_analyzed IS NULL OR stale_stats = 'YES' OR last_analyzed < SYSDATE - 30)
ORDER BY table_name, partition_position DESC;


-- -----------------------------------------------------------------------------
-- Check incremental statistics configuration
-- -----------------------------------------------------------------------------

SELECT
    table_name,
    DBMS_STATS.GET_PREFS('INCREMENTAL', USER, table_name) AS incremental,
    DBMS_STATS.GET_PREFS('INCREMENTAL_STALENESS', USER, table_name) AS incremental_staleness,
    DBMS_STATS.GET_PREFS('INCREMENTAL_LEVEL', USER, table_name) AS incremental_level,
    DBMS_STATS.GET_PREFS('PUBLISH', USER, table_name) AS publish,
    DBMS_STATS.GET_PREFS('STALE_PERCENT', USER, table_name) AS stale_percent
FROM user_tables
WHERE partitioned = 'YES'
AND table_name LIKE '%_FACT'
ORDER BY table_name;


-- =============================================================================
-- SECTION 8: STORAGE AND SPACE UTILIZATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Tablespace usage by data tier
-- -----------------------------------------------------------------------------

SELECT
    tablespace_name,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS used_gb,
    ROUND(SUM(CASE WHEN segment_type LIKE '%PARTITION%' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS partition_data_gb,
    COUNT(DISTINCT segment_name) AS segment_count,
    COUNT(DISTINCT partition_name) AS partition_count,
    CASE
        WHEN tablespace_name LIKE '%HOT%' THEN 'HOT TIER'
        WHEN tablespace_name LIKE '%WARM%' THEN 'WARM TIER'
        WHEN tablespace_name LIKE '%COLD%' THEN 'COLD TIER'
        WHEN tablespace_name LIKE '%ARCHIVE%' THEN 'ARCHIVE TIER'
        ELSE 'UNCLASSIFIED'
    END AS tier
FROM user_segments
WHERE segment_name LIKE '%_FACT'
GROUP BY tablespace_name
ORDER BY used_gb DESC;


-- -----------------------------------------------------------------------------
-- Space savings summary across all fact tables
-- -----------------------------------------------------------------------------

WITH space_analysis AS (
    SELECT
        tp.table_name,
        COUNT(DISTINCT tp.partition_name) AS total_partitions,
        ROUND(SUM(NVL(s.bytes, 0))/1024/1024/1024, 2) AS total_size_gb,
        ROUND(SUM(CASE WHEN tp.compression = 'DISABLED' THEN NVL(s.bytes, 0) ELSE 0 END)/1024/1024/1024, 2) AS uncompressed_gb,
        ROUND(SUM(CASE WHEN tp.compress_for LIKE '%QUERY%' THEN NVL(s.bytes, 0) ELSE 0 END)/1024/1024/1024, 2) AS query_compressed_gb,
        ROUND(SUM(CASE WHEN tp.compress_for LIKE '%ARCHIVE%' THEN NVL(s.bytes, 0) ELSE 0 END)/1024/1024/1024, 2) AS archive_compressed_gb
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name LIKE '%_FACT'
    GROUP BY tp.table_name
)
SELECT
    table_name,
    total_partitions,
    total_size_gb,
    uncompressed_gb,
    query_compressed_gb,
    archive_compressed_gb,
    ROUND(uncompressed_gb * 0.75 + query_compressed_gb * 0.60, 2) AS potential_savings_gb,
    ROUND((uncompressed_gb * 0.75 + query_compressed_gb * 0.60) / NULLIF(total_size_gb, 0) * 100, 2) AS potential_savings_pct
FROM space_analysis
ORDER BY total_size_gb DESC;


-- =============================================================================
-- SECTION 9: PERFORMANCE MONITORING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Query performance by partition
-- Requires AWR license
-- -----------------------------------------------------------------------------

SELECT
    sql_id,
    plan_hash_value,
    executions_total,
    ROUND(elapsed_time_total/1000000, 2) AS elapsed_sec,
    ROUND(cpu_time_total/1000000, 2) AS cpu_sec,
    ROUND(iowait_total/1000000, 2) AS io_wait_sec,
    buffer_gets_total,
    disk_reads_total,
    ROUND(elapsed_time_total/NULLIF(executions_total, 0)/1000000, 4) AS avg_elapsed_sec
FROM dba_hist_sqlstat
WHERE sql_id IN (
    SELECT DISTINCT sql_id
    FROM dba_hist_sql_plan
    WHERE object_name IN ('SALES_FACT', 'ORDER_FACT')
    AND operation = 'PARTITION RANGE'
)
AND snap_id BETWEEN (SELECT MAX(snap_id) - 7 FROM dba_hist_snapshot)
              AND (SELECT MAX(snap_id) FROM dba_hist_snapshot)
ORDER BY elapsed_time_total DESC
FETCH FIRST 20 ROWS ONLY;


-- =============================================================================
-- SECTION 10: ALERTING AND THRESHOLDS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Generate alerts for partition maintenance issues
-- -----------------------------------------------------------------------------

WITH alerts AS (
    -- Alert: Unusable indexes
    SELECT
        'CRITICAL' AS severity,
        'UNUSABLE_INDEX' AS alert_type,
        i.table_name AS object_name,
        ip.index_name || '.' || ip.partition_name AS detail,
        'Rebuild required' AS recommendation
    FROM user_ind_partitions ip
    JOIN user_indexes i ON i.index_name = ip.index_name
    WHERE ip.status != 'USABLE'

    UNION ALL

    -- Alert: Missing statistics
    SELECT
        'WARNING' AS severity,
        'MISSING_STATS' AS alert_type,
        table_name AS object_name,
        partition_name AS detail,
        'Gather statistics' AS recommendation
    FROM user_tab_statistics
    WHERE object_type = 'PARTITION'
    AND (last_analyzed IS NULL OR last_analyzed < SYSDATE - 30)

    UNION ALL

    -- Alert: Uncompressed old partitions
    SELECT
        'INFO' AS severity,
        'COMPRESSION_CANDIDATE' AS alert_type,
        tp.table_name AS object_name,
        tp.partition_name AS detail,
        'Apply compression' AS recommendation
    FROM user_tab_partitions tp
    JOIN user_heat_map_segment h
        ON h.object_name = tp.table_name
        AND h.subobject_name = tp.partition_name
    WHERE tp.compression = 'DISABLED'
    AND TRUNC(SYSDATE - h.segment_write_time) > 90

    UNION ALL

    -- Alert: Partition size skew
    SELECT
        'WARNING' AS severity,
        'SIZE_SKEW' AS alert_type,
        table_name AS object_name,
        partition_name AS detail,
        'Review partitioning strategy' AS recommendation
    FROM (
        SELECT
            tp.table_name,
            tp.partition_name,
            tp.num_rows,
            AVG(tp.num_rows) OVER (PARTITION BY tp.table_name) AS avg_rows
        FROM user_tab_partitions tp
        WHERE tp.table_name LIKE '%_FACT'
    )
    WHERE num_rows > avg_rows * 2
)
SELECT
    severity,
    alert_type,
    object_name,
    detail,
    recommendation,
    SYSTIMESTAMP AS alert_timestamp
FROM alerts
ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
        WHEN 'INFO' THEN 3
        ELSE 4
    END,
    object_name;
