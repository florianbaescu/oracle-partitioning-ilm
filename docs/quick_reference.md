# Quick Reference Guide - Oracle Partitioning & ILM

## Essential Commands

### Enable Heat Map
```sql
ALTER SYSTEM SET HEAT_MAP = ON SCOPE=BOTH;
```

### Create Interval Partitioned Table
```sql
CREATE TABLE sales_fact (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_initial VALUES LESS THAN (DATE '2024-01-01')
)
COMPRESS FOR QUERY HIGH;
```

### Add ILM Compression Policy
```sql
ALTER TABLE sales_fact ILM ADD POLICY
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 90 DAYS OF NO MODIFICATION;
```

### Add Multi-Tier Storage Policy
```sql
-- Warm tier
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tbs_warm COMPRESS FOR QUERY HIGH
    SEGMENT AFTER 3 MONTHS OF NO MODIFICATION;

-- Cold tier
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tbs_cold COMPRESS FOR ARCHIVE HIGH
    SEGMENT AFTER 12 MONTHS OF NO MODIFICATION;
```

## Partition Operations

### Add Partition
```sql
ALTER TABLE sales_fact
    ADD PARTITION p_2024_11
    VALUES LESS THAN (DATE '2024-12-01');
```

### Drop Partition
```sql
ALTER TABLE sales_fact DROP PARTITION p_2020_01;
```

### Truncate Partition
```sql
ALTER TABLE sales_fact TRUNCATE PARTITION p_2024_01 UPDATE INDEXES;
```

### Split Partition
```sql
ALTER TABLE sales_fact
    SPLIT PARTITION p_2024_q1
    AT (DATE '2024-02-01')
    INTO (PARTITION p_2024_01, PARTITION p_2024_02_03);
```

### Merge Partitions
```sql
ALTER TABLE sales_fact
    MERGE PARTITIONS p_2024_01, p_2024_02
    INTO PARTITION p_2024_q1;
```

### Exchange Partition
```sql
ALTER TABLE sales_fact
    EXCHANGE PARTITION p_2024_01
    WITH TABLE sales_staging
    INCLUDING INDEXES
    WITHOUT VALIDATION;
```

### Compress Partition
```sql
ALTER TABLE sales_fact
    MOVE PARTITION p_2024_01
    COMPRESS FOR ARCHIVE HIGH;
```

### Move Partition to Tablespace
```sql
ALTER TABLE sales_fact
    MOVE PARTITION p_2024_01
    TABLESPACE tbs_cold
    COMPRESS FOR ARCHIVE HIGH;
```

### Make Partition Read-Only
```sql
ALTER TABLE sales_fact MODIFY PARTITION p_2020_01 READ ONLY;
```

### Make Partition Read-Write
```sql
ALTER TABLE sales_fact MODIFY PARTITION p_2024_01 READ WRITE;
```

## Index Operations

### Create Local Index
```sql
CREATE INDEX idx_sales_customer
    ON sales_fact(customer_id, sale_date)
    LOCAL;
```

### Rebuild Index Partition
```sql
ALTER INDEX idx_sales_customer
    REBUILD PARTITION p_2024_01;
```

### Rebuild Unusable Indexes
```sql
-- All unusable partitions for a table
BEGIN
    FOR idx IN (
        SELECT index_name, partition_name
        FROM user_ind_partitions
        WHERE status = 'UNUSABLE'
        AND index_name IN (
            SELECT index_name FROM user_indexes
            WHERE table_name = 'SALES_FACT'
        )
    ) LOOP
        EXECUTE IMMEDIATE
            'ALTER INDEX ' || idx.index_name ||
            ' REBUILD PARTITION ' || idx.partition_name;
    END LOOP;
END;
/
```

## Statistics

### Gather Table Statistics
```sql
EXEC DBMS_STATS.GATHER_TABLE_STATS('SALES_FACT', CASCADE => TRUE);
```

### Gather Partition Statistics
```sql
EXEC DBMS_STATS.GATHER_TABLE_STATS(
    ownname => USER,
    tabname => 'SALES_FACT',
    partname => 'P_2024_01',
    cascade => TRUE
);
```

### Enable Incremental Statistics
```sql
EXEC DBMS_STATS.SET_TABLE_PREFS('SALES_FACT', 'INCREMENTAL', 'TRUE');
```

## ILM Management

### View ILM Policies
```sql
SELECT * FROM USER_ILMPOLICIES
WHERE OBJECT_NAME = 'SALES_FACT';
```

### Execute ILM Immediately
```sql
EXEC DBMS_ILM.FLUSH_ALL_SEGMENTS;
```

### Disable ILM Policy
```sql
ALTER TABLE sales_fact ILM DISABLE POLICY policy_name;
```

### Enable ILM Policy
```sql
ALTER TABLE sales_fact ILM ENABLE POLICY policy_name;
```

### Remove ILM Policy
```sql
ALTER TABLE sales_fact ILM DELETE POLICY policy_name;
```

### Check ILM Execution History
```sql
SELECT task_id, object_name, policy_name, state, start_time, completion_time
FROM USER_ILMTASKS
WHERE object_name = 'SALES_FACT'
ORDER BY start_time DESC;
```

## Monitoring Queries

### List All Partitions
```sql
SELECT partition_name, partition_position, high_value,
       num_rows, compression, compress_for, tablespace_name
FROM user_tab_partitions
WHERE table_name = 'SALES_FACT'
ORDER BY partition_position DESC;
```

### Check Partition Sizes
```sql
SELECT tp.partition_name,
       tp.num_rows,
       ROUND(s.bytes/1024/1024, 2) AS size_mb,
       tp.compression,
       tp.compress_for
FROM user_tab_partitions tp
LEFT JOIN user_segments s
    ON s.segment_name = tp.table_name
    AND s.partition_name = tp.partition_name
WHERE tp.table_name = 'SALES_FACT'
ORDER BY tp.partition_position DESC;
```

### Check Heat Map Data
```sql
SELECT object_name, subobject_name AS partition_name,
       segment_write_time, segment_read_time,
       TRUNC(SYSDATE - segment_write_time) AS days_since_write
FROM user_heat_map_segment
WHERE object_name = 'SALES_FACT'
ORDER BY segment_write_time DESC;
```

### Find Unusable Indexes
```sql
SELECT ip.index_name, ip.partition_name, ip.status
FROM user_ind_partitions ip
JOIN user_indexes i ON i.index_name = ip.index_name
WHERE i.table_name = 'SALES_FACT'
AND ip.status != 'USABLE';
```

### Check Statistics Staleness
```sql
SELECT table_name, partition_name, stale_stats, last_analyzed
FROM user_tab_statistics
WHERE object_type = 'PARTITION'
AND table_name = 'SALES_FACT'
AND stale_stats = 'YES';
```

### View Compression Effectiveness
```sql
SELECT
    table_name,
    COUNT(*) AS total_partitions,
    SUM(CASE WHEN compression = 'DISABLED' THEN 1 ELSE 0 END) AS uncompressed,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS total_gb
FROM (
    SELECT tp.table_name, tp.compression, s.bytes
    FROM user_tab_partitions tp
    LEFT JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name = 'SALES_FACT'
)
GROUP BY table_name;
```

## Query Optimization

### Check Partition Pruning
```sql
-- Enable detailed plan
ALTER SESSION SET STATISTICS_LEVEL = ALL;

-- Run query
SELECT * FROM sales_fact WHERE sale_date = DATE '2024-01-15';

-- View plan with partition info
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST PARTITION'));
```

### Direct Partition Access
```sql
-- Query specific partition
SELECT * FROM sales_fact PARTITION (p_2024_01);
```

### Parallel Query
```sql
SELECT /*+ PARALLEL(8) */ *
FROM sales_fact
WHERE sale_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31';
```

## Bulk Loading

### Direct Path Insert
```sql
INSERT /*+ APPEND PARALLEL(8) */ INTO sales_fact
SELECT * FROM staging_table;
COMMIT;
```

### Partition Exchange Load
```sql
-- 1. Load staging table
INSERT INTO sales_staging SELECT * FROM source;

-- 2. Exchange partition
ALTER TABLE sales_fact
    EXCHANGE PARTITION p_2024_01
    WITH TABLE sales_staging
    WITHOUT VALIDATION;

-- 3. Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('SALES_FACT', PARTNAME => 'P_2024_01');
```

## Maintenance Procedures

### Create Future Partitions
```sql
EXEC create_future_partitions('SALES_FACT', 12, 'MONTH');
```

### Compress Old Partitions
```sql
EXEC compress_partitions('SALES_FACT', 'ARCHIVE HIGH', 365);
```

### Move to Cold Storage
```sql
EXEC move_partitions_to_tablespace('SALES_FACT', 'TBS_COLD', 730, 'ARCHIVE HIGH');
```

### Health Check
```sql
EXEC check_partition_health('SALES_FACT');
```

### Size Report
```sql
EXEC partition_size_report('SALES_FACT');
```

## Common Scenarios

### Scenario 1: Quarterly Partition Cleanup
```sql
-- 1. Archive partitions older than 3 years
EXEC move_partitions_to_tablespace('SALES_FACT', 'TBS_ARCHIVE', 1095, 'ARCHIVE HIGH');

-- 2. Drop partitions older than 7 years
BEGIN
    FOR rec IN (
        SELECT partition_name, high_value
        FROM user_tab_partitions
        WHERE table_name = 'SALES_FACT'
    ) LOOP
        DECLARE
            v_date DATE;
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || rec.high_value || ' FROM DUAL' INTO v_date;
            IF v_date < SYSDATE - 2555 THEN
                EXECUTE IMMEDIATE 'ALTER TABLE SALES_FACT DROP PARTITION ' || rec.partition_name;
            END IF;
        END;
    END LOOP;
END;
/

-- 3. Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('SALES_FACT');
```

### Scenario 2: Emergency Space Recovery
```sql
-- 1. Identify large uncompressed partitions
SELECT partition_name, ROUND(bytes/1024/1024/1024, 2) AS size_gb
FROM (
    SELECT tp.partition_name, s.bytes
    FROM user_tab_partitions tp
    JOIN user_segments s
        ON s.segment_name = tp.table_name
        AND s.partition_name = tp.partition_name
    WHERE tp.table_name = 'SALES_FACT'
    AND tp.compression = 'DISABLED'
)
ORDER BY bytes DESC;

-- 2. Compress largest partitions
EXEC compress_partitions('SALES_FACT', 'ARCHIVE HIGH', 30);
```

### Scenario 3: Performance Investigation
```sql
-- 1. Check partition pruning
EXPLAIN PLAN FOR
SELECT * FROM sales_fact WHERE sale_date = DATE '2024-01-15';
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'PARTITION'));

-- 2. Check index health
EXEC check_partition_health('SALES_FACT');

-- 3. Gather fresh statistics
EXEC gather_stale_partition_stats('SALES_FACT', 5, 8);
```

## Troubleshooting

### Problem: ILM Not Working
```sql
-- Check Heat Map enabled
SELECT * FROM V$OPTION WHERE PARAMETER = 'Heat Map';

-- Check policies
SELECT * FROM USER_ILMPOLICIES;

-- Force execution
EXEC DBMS_ILM.FLUSH_ALL_SEGMENTS;
```

### Problem: Queries Not Pruning
```sql
-- Check data types match
DESC sales_fact;

-- Verify WHERE clause uses partition key
-- Good: WHERE sale_date = DATE '2024-01-15'
-- Bad:  WHERE TO_CHAR(sale_date, 'YYYY-MM-DD') = '2024-01-15'
```

### Problem: Load Performance
```sql
-- Use partition exchange instead of INSERT
-- Use APPEND hint for direct path
-- Disable indexes during load, rebuild after
-- Use NOLOGGING for faster loads
```

## Performance Tips

1. **Always use DATE literals** for date partition keys
2. **Enable parallel DML** for large operations
3. **Use local indexes** whenever possible
4. **Gather statistics** after major changes
5. **Monitor partition pruning** in execution plans
6. **Use partition exchange** for bulk loads
7. **Enable incremental statistics** for large tables
8. **Compress old data** to save space and improve I/O
9. **Archive to cheaper storage** based on access patterns
10. **Test ILM policies** on non-production first

## Quick Wins

- Enable Heat Map and wait 30 days
- Compress partitions >90 days old
- Move partitions >1 year to cheaper storage
- Drop partitions beyond retention period
- Enable incremental statistics
- Create future partitions proactively
- Schedule monthly health checks
- Monitor and fix unusable indexes weekly
