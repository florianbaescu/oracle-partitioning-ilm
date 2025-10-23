# ILM Framework Performance Guide

## Table of Contents

1. [Overview](#overview)
2. [Benchmarking Methodology](#benchmarking-methodology)
3. [Compression Performance Testing](#compression-performance-testing)
4. [Migration Performance Testing](#migration-performance-testing)
5. [Performance Monitoring](#performance-monitoring)
6. [Tuning Recommendations](#tuning-recommendations)
7. [Baseline Establishment](#baseline-establishment)
8. [Performance Regression Detection](#performance-regression-detection)
9. [Troubleshooting Performance Issues](#troubleshooting-performance-issues)

---

## Overview

This guide provides methodologies, scripts, and best practices for benchmarking and optimizing the performance of the Oracle Custom ILM Framework.

### Performance Objectives

- **Compression**: Achieve 2-4x compression ratios within acceptable time windows
- **Space Savings**: Reduce storage by 50-75% for older partitions
- **Minimal Impact**: <5% query performance degradation on compressed partitions
- **Scalability**: Handle 1000+ partitions efficiently
- **Throughput**: Process 10-50 partitions per hour (depending on size)

### Key Performance Metrics

| Metric | Target | Critical Threshold |
|--------|--------|-------------------|
| Compression Ratio | ≥ 2.0x | < 1.5x |
| Compression Time | < 30 min/10GB | > 60 min/10GB |
| Space Savings | ≥ 50% | < 30% |
| Query Overhead | < 5% slower | > 15% slower |
| Failure Rate | < 2% | > 10% |
| Queue Processing | < 2 hours backlog | > 8 hours |

---

## Benchmarking Methodology

### Test Environment Setup

```sql
-- Create benchmark configuration table
CREATE TABLE cmr.dwh_benchmark_results (
    benchmark_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    benchmark_date TIMESTAMP DEFAULT SYSTIMESTAMP,
    test_category VARCHAR2(50),
    test_name VARCHAR2(200),
    partition_name VARCHAR2(128),
    partition_size_mb NUMBER,
    compression_type VARCHAR2(30),
    execution_time_seconds NUMBER,
    size_before_mb NUMBER,
    size_after_mb NUMBER,
    compression_ratio NUMBER,
    query_time_before_ms NUMBER,
    query_time_after_ms NUMBER,
    query_overhead_pct NUMBER,
    parallel_degree NUMBER,
    notes CLOB
);

-- Create benchmark reporting view
CREATE OR REPLACE VIEW cmr.dwh_v_benchmark_summary AS
SELECT
    test_category,
    compression_type,
    COUNT(*) AS test_count,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio,
    ROUND(AVG(execution_time_seconds), 2) AS avg_execution_time_sec,
    ROUND(AVG(query_overhead_pct), 2) AS avg_query_overhead_pct,
    ROUND(AVG(partition_size_mb), 2) AS avg_partition_size_mb,
    MIN(benchmark_date) AS first_test,
    MAX(benchmark_date) AS last_test
FROM cmr.dwh_benchmark_results
GROUP BY test_category, compression_type
ORDER BY test_category, compression_type;
```

### Benchmarking Procedure

1. **Establish Baseline**: Capture current state before any changes
2. **Controlled Testing**: Test one variable at a time
3. **Multiple Runs**: Run each test 3-5 times for statistical significance
4. **Varied Conditions**: Test different partition sizes, data patterns, and loads
5. **Document Results**: Record all metrics and environmental factors

---

## Compression Performance Testing

### Test 1: Compression Ratio by Type

Tests different compression types to find optimal balance.

```sql
-- Benchmark compression types on sample partition
DECLARE
    v_test_partition VARCHAR2(128) := 'P_2024_01';
    v_test_table VARCHAR2(128) := 'SALES_FACT';
    v_compression_types DBMS_SQL.VARCHAR2_TABLE;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_size_before NUMBER;
    v_size_after NUMBER;
    v_query_time_before NUMBER;
    v_query_time_after NUMBER;
    v_sql VARCHAR2(4000);
BEGIN
    -- Define compression types to test
    v_compression_types(1) := 'NOCOMPRESS';
    v_compression_types(2) := 'QUERY LOW';
    v_compression_types(3) := 'QUERY HIGH';
    v_compression_types(4) := 'ARCHIVE LOW';
    v_compression_types(5) := 'ARCHIVE HIGH';

    -- Get baseline size
    SELECT bytes/1024/1024 INTO v_size_before
    FROM user_segments
    WHERE segment_name = v_test_table
    AND partition_name = v_test_partition;

    -- Measure baseline query performance
    v_start_time := SYSTIMESTAMP;
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_test_table ||
                     ' PARTITION (' || v_test_partition || ')';
    v_query_time_before := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;

    -- Test each compression type
    FOR i IN 1..v_compression_types.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Testing compression: ' || v_compression_types(i));

        -- Compress partition
        v_start_time := SYSTIMESTAMP;
        v_sql := 'ALTER TABLE ' || v_test_table ||
                ' MOVE PARTITION ' || v_test_partition ||
                ' COMPRESS FOR ' || v_compression_types(i);
        EXECUTE IMMEDIATE v_sql;
        v_end_time := SYSTIMESTAMP;

        -- Get compressed size
        SELECT bytes/1024/1024 INTO v_size_after
        FROM user_segments
        WHERE segment_name = v_test_table
        AND partition_name = v_test_partition;

        -- Measure query performance after compression
        EXECUTE IMMEDIATE 'ALTER SYSTEM FLUSH BUFFER_CACHE';  -- Requires SYSDBA
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_test_table ||
                         ' PARTITION (' || v_test_partition || ')';
        v_query_time_after := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)) * 1000;

        -- Record results
        INSERT INTO cmr.dwh_benchmark_results (
            test_category, test_name, partition_name, partition_size_mb,
            compression_type, execution_time_seconds,
            size_before_mb, size_after_mb,
            compression_ratio, query_time_before_ms, query_time_after_ms,
            query_overhead_pct
        ) VALUES (
            'COMPRESSION_RATIO',
            'Compression Type Comparison',
            v_test_partition,
            v_size_before,
            v_compression_types(i),
            EXTRACT(SECOND FROM (v_end_time - v_start_time)),
            v_size_before,
            v_size_after,
            v_size_before / NULLIF(v_size_after, 0),
            v_query_time_before,
            v_query_time_after,
            ((v_query_time_after - v_query_time_before) / NULLIF(v_query_time_before, 0)) * 100
        );
        COMMIT;

        -- Restore to uncompressed for next iteration
        IF i < v_compression_types.COUNT THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || v_test_table ||
                            ' MOVE PARTITION ' || v_test_partition || ' NOCOMPRESS';
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Compression benchmarking completed');
END;
/

-- View results
SELECT
    compression_type,
    ROUND(AVG(compression_ratio), 2) AS avg_ratio,
    ROUND(AVG(execution_time_seconds), 2) AS avg_time_sec,
    ROUND(AVG(query_overhead_pct), 2) AS avg_query_overhead_pct,
    ROUND(AVG(size_before_mb), 2) AS avg_size_before_mb,
    ROUND(AVG(size_after_mb), 2) AS avg_size_after_mb
FROM cmr.dwh_benchmark_results
WHERE test_category = 'COMPRESSION_RATIO'
AND benchmark_date > SYSDATE - 1
GROUP BY compression_type
ORDER BY avg_ratio DESC;
```

**Expected Results:**

| Compression Type | Avg Ratio | Avg Time (sec) | Query Overhead % | Space Saved % |
|-----------------|-----------|----------------|------------------|---------------|
| ARCHIVE HIGH    | 4.5x      | 450            | 8%               | 78%           |
| ARCHIVE LOW     | 3.8x      | 320            | 5%               | 74%           |
| QUERY HIGH      | 3.2x      | 180            | 3%               | 69%           |
| QUERY LOW       | 2.4x      | 90             | 1%               | 58%           |
| NOCOMPRESS      | 1.0x      | 30             | 0%               | 0%            |

### Test 2: Parallel Degree Impact

Tests how parallel execution affects compression performance.

```sql
-- Test parallel degrees: 2, 4, 8, 16
DECLARE
    v_parallel_degrees DBMS_SQL.NUMBER_TABLE;
    v_test_partition VARCHAR2(128) := 'P_2024_02';
    v_test_table VARCHAR2(128) := 'SALES_FACT';
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_size_before NUMBER;
    v_size_after NUMBER;
BEGIN
    v_parallel_degrees(1) := 1;   -- Serial
    v_parallel_degrees(2) := 2;
    v_parallel_degrees(3) := 4;
    v_parallel_degrees(4) := 8;
    v_parallel_degrees(5) := 16;

    -- Get baseline size
    SELECT bytes/1024/1024 INTO v_size_before
    FROM user_segments
    WHERE segment_name = v_test_table
    AND partition_name = v_test_partition;

    FOR i IN 1..v_parallel_degrees.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('Testing parallel degree: ' || v_parallel_degrees(i));

        -- Compress with specified parallel degree
        v_start_time := SYSTIMESTAMP;
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
        EXECUTE IMMEDIATE 'ALTER TABLE ' || v_test_table ||
                        ' MOVE PARTITION ' || v_test_partition ||
                        ' COMPRESS FOR QUERY LOW PARALLEL ' || v_parallel_degrees(i);
        v_end_time := SYSTIMESTAMP;

        -- Get compressed size
        SELECT bytes/1024/1024 INTO v_size_after
        FROM user_segments
        WHERE segment_name = v_test_table
        AND partition_name = v_test_partition;

        -- Record results
        INSERT INTO cmr.dwh_benchmark_results (
            test_category, test_name, partition_name,
            compression_type, execution_time_seconds,
            size_before_mb, size_after_mb, compression_ratio,
            parallel_degree
        ) VALUES (
            'PARALLEL_PERFORMANCE',
            'Parallel Degree Impact',
            v_test_partition,
            'QUERY LOW',
            EXTRACT(SECOND FROM (v_end_time - v_start_time)),
            v_size_before,
            v_size_after,
            v_size_before / NULLIF(v_size_after, 0),
            v_parallel_degrees(i)
        );
        COMMIT;

        -- Restore for next test
        IF i < v_parallel_degrees.COUNT THEN
            EXECUTE IMMEDIATE 'ALTER TABLE ' || v_test_table ||
                            ' MOVE PARTITION ' || v_test_partition || ' NOCOMPRESS';
        END IF;
    END LOOP;
END;
/

-- View results
SELECT
    parallel_degree,
    ROUND(AVG(execution_time_seconds), 2) AS avg_time_sec,
    ROUND(AVG(compression_ratio), 2) AS avg_ratio,
    ROUND(MIN(execution_time_seconds) / AVG(execution_time_seconds), 2) AS speedup_vs_best
FROM cmr.dwh_benchmark_results
WHERE test_category = 'PARALLEL_PERFORMANCE'
AND benchmark_date > SYSDATE - 1
GROUP BY parallel_degree
ORDER BY parallel_degree;
```

**Expected Results:**

| Parallel Degree | Avg Time (sec) | Speedup vs Best |
|----------------|----------------|-----------------|
| 1 (Serial)     | 180            | 0.25x           |
| 2              | 95             | 0.47x           |
| 4              | 50             | 0.90x           |
| 8              | 45             | 1.00x (best)    |
| 16             | 48             | 0.94x           |

**Analysis**: Diminishing returns after 8 parallel workers due to overhead.

### Test 3: Partition Size Impact

Tests how partition size affects compression performance.

```sql
-- Compare small, medium, large, extra-large partitions
SELECT
    CASE
        WHEN partition_size_mb < 100 THEN 'Small (<100MB)'
        WHEN partition_size_mb BETWEEN 100 AND 1000 THEN 'Medium (100MB-1GB)'
        WHEN partition_size_mb BETWEEN 1000 AND 10000 THEN 'Large (1-10GB)'
        ELSE 'Extra Large (>10GB)'
    END AS size_category,
    COUNT(*) AS partition_count,
    ROUND(AVG(execution_time_seconds), 2) AS avg_time_sec,
    ROUND(AVG(execution_time_seconds / NULLIF(partition_size_mb, 0) * 1000), 2) AS avg_sec_per_gb,
    ROUND(AVG(compression_ratio), 2) AS avg_ratio
FROM cmr.dwh_ilm_execution_log
WHERE action_type = 'COMPRESS'
AND status = 'SUCCESS'
AND execution_end > SYSDATE - 30
GROUP BY
    CASE
        WHEN size_before_mb < 100 THEN 'Small (<100MB)'
        WHEN size_before_mb BETWEEN 100 AND 1000 THEN 'Medium (100MB-1GB)'
        WHEN size_before_mb BETWEEN 1000 AND 10000 THEN 'Large (1-10GB)'
        ELSE 'Extra Large (>10GB)'
    END
ORDER BY
    MIN(size_before_mb);
```

---

## Migration Performance Testing

### Test 4: Table Migration Duration

Benchmark complete table migration from non-partitioned to partitioned.

```sql
-- Benchmark table migration
DECLARE
    v_task_id NUMBER;
    v_start_time TIMESTAMP;
    v_analyze_time TIMESTAMP;
    v_execute_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_row_count NUMBER;
BEGIN
    -- Create test project and task
    INSERT INTO cmr.dwh_migration_projects (project_name, description)
    VALUES ('BENCHMARK_PROJECT', 'Performance benchmarking')
    RETURNING project_id INTO v_project_id;

    INSERT INTO cmr.dwh_migration_tasks (
        project_id, table_owner, table_name, analysis_status
    ) VALUES (
        v_project_id, 'DWH', 'BENCHMARK_TABLE', 'PENDING'
    ) RETURNING task_id INTO v_task_id;
    COMMIT;

    -- Get row count
    SELECT COUNT(*) INTO v_row_count FROM dwh.benchmark_table;

    -- Step 1: Analyze
    v_start_time := SYSTIMESTAMP;
    dwh_analyze_table(v_task_id);
    v_analyze_time := SYSTIMESTAMP;

    -- Step 2: Execute migration
    dwh_execute_migration(v_task_id);
    v_execute_time := SYSTIMESTAMP;

    -- Record benchmark
    INSERT INTO cmr.dwh_benchmark_results (
        test_category, test_name,
        notes
    ) VALUES (
        'TABLE_MIGRATION',
        'Complete Migration Duration',
        JSON_OBJECT(
            'task_id' VALUE v_task_id,
            'row_count' VALUE v_row_count,
            'analyze_time_sec' VALUE EXTRACT(SECOND FROM (v_analyze_time - v_start_time)),
            'execute_time_sec' VALUE EXTRACT(SECOND FROM (v_execute_time - v_analyze_time)),
            'total_time_sec' VALUE EXTRACT(SECOND FROM (v_execute_time - v_start_time))
        )
    );
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Migration benchmark completed');
    DBMS_OUTPUT.PUT_LINE('Rows migrated: ' || v_row_count);
    DBMS_OUTPUT.PUT_LINE('Analysis time: ' || EXTRACT(SECOND FROM (v_analyze_time - v_start_time)) || 's');
    DBMS_OUTPUT.PUT_LINE('Execution time: ' || EXTRACT(SECOND FROM (v_execute_time - v_analyze_time)) || 's');
END;
/
```

---

## Performance Monitoring

### Real-Time Performance Dashboard

```sql
-- Create performance monitoring view
CREATE OR REPLACE VIEW cmr.dwh_v_performance_realtime AS
SELECT
    -- Current Operations
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log
     WHERE status = 'IN_PROGRESS') AS operations_running,

    -- Hourly Throughput
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log
     WHERE execution_end > SYSDATE - 1/24
     AND status = 'SUCCESS') AS partitions_last_hour,

    (SELECT ROUND(AVG(duration_seconds), 2)
     FROM cmr.dwh_ilm_execution_log
     WHERE execution_end > SYSDATE - 1/24
     AND status = 'SUCCESS') AS avg_duration_last_hour_sec,

    -- Current Efficiency
    (SELECT ROUND(AVG(compression_ratio), 2)
     FROM cmr.dwh_ilm_execution_log
     WHERE execution_end > SYSDATE - 1/24
     AND action_type = 'COMPRESS'
     AND status = 'SUCCESS') AS avg_compression_last_hour,

    (SELECT ROUND(SUM(space_saved_mb)/1024, 2)
     FROM cmr.dwh_ilm_execution_log
     WHERE execution_end > SYSDATE - 1/24
     AND status = 'SUCCESS') AS space_saved_last_hour_gb,

    -- Queue Status
    (SELECT COUNT(*) FROM cmr.dwh_ilm_evaluation_queue
     WHERE execution_status = 'PENDING'
     AND eligible = 'Y') AS pending_queue_size,

    (SELECT ROUND(AVG(EXTRACT(HOUR FROM (SYSTIMESTAMP - evaluation_date)) * 60 +
                      EXTRACT(MINUTE FROM (SYSTIMESTAMP - evaluation_date))), 2)
     FROM cmr.dwh_ilm_evaluation_queue
     WHERE execution_status = 'PENDING'
     AND eligible = 'Y') AS avg_queue_wait_minutes,

    SYSTIMESTAMP AS snapshot_time
FROM DUAL;

-- Monitor in real-time
SELECT * FROM cmr.dwh_v_performance_realtime;
```

### Historical Performance Trends

```sql
-- Performance trends over time
SELECT
    TRUNC(execution_end, 'HH24') AS hour,
    COUNT(*) AS operations,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    ROUND(AVG(compression_ratio), 2) AS avg_compression,
    ROUND(SUM(space_saved_mb)/1024, 2) AS space_saved_gb,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures
FROM cmr.dwh_ilm_execution_log
WHERE execution_end > SYSDATE - 7
GROUP BY TRUNC(execution_end, 'HH24')
ORDER BY hour DESC;
```

---

## Tuning Recommendations

### Compression Type Selection

| Data Age | Access Pattern | Recommended Compression | Reason |
|----------|---------------|------------------------|--------|
| < 3 months | High | QUERY LOW | Minimal query impact |
| 3-12 months | Medium | QUERY HIGH | Good balance |
| 12-36 months | Low | ARCHIVE LOW | High compression, acceptable overhead |
| 36+ months | Rare | ARCHIVE HIGH | Maximum compression |

### Parallel Degree Guidelines

```sql
-- Calculate optimal parallel degree based on partition size
CREATE OR REPLACE FUNCTION calculate_optimal_parallel_degree(
    p_partition_size_mb NUMBER
) RETURN NUMBER AS
    v_parallel_degree NUMBER;
BEGIN
    v_parallel_degree := CASE
        WHEN p_partition_size_mb < 100 THEN 1      -- Serial for small partitions
        WHEN p_partition_size_mb < 1000 THEN 2     -- 2 workers for <1GB
        WHEN p_partition_size_mb < 5000 THEN 4     -- 4 workers for 1-5GB
        WHEN p_partition_size_mb < 20000 THEN 8    -- 8 workers for 5-20GB
        ELSE 16                                     -- 16 workers for >20GB
    END;

    RETURN v_parallel_degree;
END;
/
```

### Execution Window Optimization

```sql
-- Analyze execution patterns to optimize window
SELECT
    TO_CHAR(execution_start, 'HH24') AS hour_of_day,
    COUNT(*) AS operations,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    ROUND(AVG(duration_seconds) / 60, 2) AS avg_duration_min
FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 30
GROUP BY TO_CHAR(execution_start, 'HH24')
ORDER BY hour_of_day;

-- Recommended execution window: Hours with lowest avg duration and failure rate
```

### Memory Configuration

```sql
-- Check PGA/SGA allocation for compression operations
SELECT
    name,
    value/1024/1024 AS value_mb
FROM v$parameter
WHERE name IN ('pga_aggregate_target', 'sga_target', 'memory_target')
ORDER BY name;

-- Recommendations:
-- PGA: At least 2GB per parallel worker
-- SGA: Buffer cache sized for frequently accessed partitions
```

---

## Baseline Establishment

### Initial Baseline Capture

```sql
-- Capture baseline metrics before ILM implementation
CREATE TABLE cmr.dwh_performance_baseline AS
SELECT
    table_owner,
    table_name,
    SUM(bytes)/1024/1024/1024 AS current_size_gb,
    COUNT(DISTINCT partition_name) AS partition_count,
    MAX(last_analyzed) AS last_stats_date,
    SYSTIMESTAMP AS baseline_date
FROM all_tab_partitions tp
JOIN all_segments s
    ON s.owner = tp.table_owner
    AND s.segment_name = tp.table_name
    AND s.partition_name = tp.partition_name
WHERE tp.table_owner = 'DWH'
GROUP BY tp.table_owner, tp.table_name;

-- Baseline query performance
CREATE TABLE cmr.dwh_query_performance_baseline AS
SELECT
    sql_id,
    parsing_schema_name,
    sql_text,
    executions,
    elapsed_time/1000000 AS elapsed_time_sec,
    cpu_time/1000000 AS cpu_time_sec,
    buffer_gets,
    disk_reads,
    SYSTIMESTAMP AS baseline_date
FROM v$sql
WHERE parsing_schema_name = 'DWH'
AND executions > 10;
```

### Baseline Comparison

```sql
-- Compare current state to baseline
SELECT
    b.table_name,
    b.current_size_gb AS baseline_size_gb,
    ROUND(SUM(s.bytes)/1024/1024/1024, 2) AS current_size_gb,
    ROUND((b.current_size_gb - SUM(s.bytes)/1024/1024/1024), 2) AS space_saved_gb,
    ROUND((b.current_size_gb - SUM(s.bytes)/1024/1024/1024) / b.current_size_gb * 100, 1) AS space_saved_pct
FROM cmr.dwh_performance_baseline b
JOIN all_segments s
    ON s.owner = b.table_owner
    AND s.segment_name = b.table_name
GROUP BY b.table_name, b.current_size_gb
ORDER BY space_saved_gb DESC;
```

---

## Performance Regression Detection

### Automated Regression Detection

```sql
-- Create regression detection procedure
CREATE OR REPLACE PROCEDURE detect_performance_regression AS
    v_current_avg_time NUMBER;
    v_baseline_avg_time NUMBER;
    v_regression_pct NUMBER;
    v_alert_message VARCHAR2(4000);
BEGIN
    -- Compare last 7 days to previous 30 days
    SELECT AVG(duration_seconds) INTO v_current_avg_time
    FROM cmr.dwh_ilm_execution_log
    WHERE execution_end > SYSDATE - 7
    AND status = 'SUCCESS';

    SELECT AVG(duration_seconds) INTO v_baseline_avg_time
    FROM cmr.dwh_ilm_execution_log
    WHERE execution_end BETWEEN SYSDATE - 37 AND SYSDATE - 7
    AND status = 'SUCCESS';

    v_regression_pct := ((v_current_avg_time - v_baseline_avg_time) / v_baseline_avg_time) * 100;

    IF v_regression_pct > 20 THEN  -- 20% regression threshold
        v_alert_message := 'PERFORMANCE REGRESSION DETECTED: ' ||
                          'Average execution time increased by ' || ROUND(v_regression_pct, 1) || '%' || CHR(10) ||
                          'Baseline: ' || ROUND(v_baseline_avg_time, 2) || 's' || CHR(10) ||
                          'Current: ' || ROUND(v_current_avg_time, 2) || 's';

        DBMS_OUTPUT.PUT_LINE(v_alert_message);

        -- Optionally send email alert via dwh_send_ilm_alert()
    END IF;
END;
/

-- Schedule regression detection
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ILM_PERFORMANCE_REGRESSION_CHECK',
        job_type => 'STORED_PROCEDURE',
        job_action => 'detect_performance_regression',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=6',  -- Daily at 6 AM
        enabled => TRUE,
        comments => 'Detect ILM performance regressions'
    );
END;
/
```

---

## Troubleshooting Performance Issues

### Slow Compression Operations

**Symptoms**: Compression taking > 60 minutes per 10GB

**Diagnosis**:
```sql
-- Check for resource contention
SELECT
    table_name,
    partition_name,
    action_type,
    duration_seconds,
    size_before_mb,
    ROUND(duration_seconds / (size_before_mb/1024), 2) AS seconds_per_gb
FROM cmr.dwh_ilm_execution_log
WHERE action_type = 'COMPRESS'
AND duration_seconds > 3600  -- > 1 hour
AND execution_end > SYSDATE - 7
ORDER BY seconds_per_gb DESC;

-- Check system load during slow operations
SELECT
    TO_CHAR(sample_time, 'YYYY-MM-DD HH24:MI') AS sample_minute,
    session_state,
    COUNT(*) AS session_count
FROM v$active_session_history
WHERE sample_time BETWEEN TO_TIMESTAMP('2025-10-23 14:00:00', 'YYYY-MM-DD HH24:MI:SS')
                     AND TO_TIMESTAMP('2025-10-23 15:00:00', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY TO_CHAR(sample_time, 'YYYY-MM-DD HH24:MI'), session_state
ORDER BY sample_minute, session_state;
```

**Solutions**:
1. Increase parallel degree for large partitions
2. Run during off-peak hours to avoid contention
3. Check for inadequate PGA/temp space
4. Review tablespace fragmentation

### High Queue Backlog

**Symptoms**: > 100 partitions pending in evaluation queue

**Diagnosis**:
```sql
-- Analyze queue backlog
SELECT
    table_name,
    policy_name,
    COUNT(*) AS pending_count,
    MIN(evaluation_date) AS oldest_evaluation,
    ROUND(AVG(EXTRACT(DAY FROM (SYSTIMESTAMP - evaluation_date)) * 24 +
              EXTRACT(HOUR FROM (SYSTIMESTAMP - evaluation_date))), 2) AS avg_wait_hours
FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'PENDING'
AND eligible = 'Y'
GROUP BY table_name, policy_name
ORDER BY pending_count DESC;
```

**Solutions**:
1. Increase execution frequency (reduce scheduler interval)
2. Increase parallel execution (process multiple partitions concurrently)
3. Adjust policy criteria to reduce eligible partition count
4. Add more execution windows throughout the day

### Query Performance Degradation

**Symptoms**: Queries on compressed partitions > 15% slower

**Diagnosis**:
```sql
-- Compare query performance before/after compression
SELECT
    sql_id,
    sql_text,
    executions_before,
    executions_after,
    ROUND(avg_elapsed_before_sec, 2) AS avg_elapsed_before_sec,
    ROUND(avg_elapsed_after_sec, 2) AS avg_elapsed_after_sec,
    ROUND(((avg_elapsed_after_sec - avg_elapsed_before_sec) / avg_elapsed_before_sec) * 100, 1) AS degradation_pct
FROM (
    -- Queries before compression
    SELECT sql_id, sql_text, COUNT(*) AS executions_before,
           AVG(elapsed_time)/1000000 AS avg_elapsed_before_sec
    FROM v$sql
    WHERE parsing_schema_name = 'DWH'
    AND last_active_time BETWEEN SYSDATE - 60 AND SYSDATE - 30
    GROUP BY sql_id, sql_text
) b
JOIN (
    -- Queries after compression
    SELECT sql_id, COUNT(*) AS executions_after,
           AVG(elapsed_time)/1000000 AS avg_elapsed_after_sec
    FROM v$sql
    WHERE parsing_schema_name = 'DWH'
    AND last_active_time > SYSDATE - 30
    GROUP BY sql_id
) a USING (sql_id)
WHERE ((avg_elapsed_after_sec - avg_elapsed_before_sec) / avg_elapsed_before_sec) > 0.15
ORDER BY degradation_pct DESC;
```

**Solutions**:
1. Use less aggressive compression (QUERY LOW instead of ARCHIVE HIGH)
2. Gather statistics after compression
3. Review and optimize SQL execution plans
4. Consider materialized views for heavily queried old data

---

## Best Practices Summary

### DO

✅ Establish baselines before implementing ILM
✅ Test compression types on sample data first
✅ Monitor performance metrics continuously
✅ Use appropriate parallel degree for partition size
✅ Run compression during off-peak hours
✅ Gather statistics after compression
✅ Document performance benchmarks
✅ Set up automated regression detection

### DON'T

❌ Compress HOT data (< 90 days old)
❌ Use ARCHIVE HIGH on frequently queried partitions
❌ Set parallel degree too high (> 16 typically)
❌ Compress during ETL load windows
❌ Ignore query performance after compression
❌ Skip performance testing before production
❌ Forget to update cost-based optimizer statistics

---

## Performance Metrics Reference Card

### Target Metrics

| Operation | Metric | Target | Good | Poor |
|-----------|--------|--------|------|------|
| Compression (QUERY LOW) | Time per GB | 90s | < 120s | > 180s |
| Compression (ARCHIVE HIGH) | Time per GB | 450s | < 600s | > 900s |
| Compression Ratio | Ratio | 2.5x | > 2.0x | < 1.5x |
| Query Overhead | % Slower | 3% | < 5% | > 15% |
| Space Savings | % Reduced | 60% | > 50% | < 30% |
| Queue Processing | Hours backlog | 1 hour | < 2 hours | > 8 hours |
| Throughput | Partitions/hour | 30 | > 20 | < 10 |

---

**Version**: 1.0
**Last Updated**: 2025-10-23
**Maintained By**: Data Warehouse Team
