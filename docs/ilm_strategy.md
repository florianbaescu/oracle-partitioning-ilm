# Oracle Information Lifecycle Management (ILM) Strategy

## Overview
Information Lifecycle Management (ILM) automates data movement through different storage tiers and compression levels based on data age and access patterns, optimizing cost and performance.

## ILM Lifecycle Stages

### 1. **Active/Hot Data** (0-3 months)
- **Storage:** High-performance SSD/Flash storage
- **Compression:** ROW STORE COMPRESS ADVANCED or none
- **Access Pattern:** Frequent reads/writes
- **Indexing:** Full local and global indexes
- **Statistics:** Real-time or daily updates

### 2. **Warm Data** (3-12 months)
- **Storage:** Standard SAS drives or lower-tier SSD
- **Compression:** COMPRESS FOR QUERY LOW/HIGH
- **Access Pattern:** Moderate reads, minimal writes
- **Indexing:** Local indexes, selective global indexes
- **Statistics:** Weekly updates

### 3. **Cool/Archived Data** (1-3 years)
- **Storage:** SATA drives or object storage
- **Compression:** COMPRESS FOR ARCHIVE HIGH
- **Access Pattern:** Infrequent reads, rare writes
- **Indexing:** Minimal local indexes
- **Statistics:** Monthly or on-demand updates

### 4. **Cold/Compliance Data** (3+ years)
- **Storage:** Tape, cloud archive (S3 Glacier, Azure Archive)
- **Compression:** COMPRESS FOR ARCHIVE HIGH
- **Access Pattern:** Rarely accessed, regulatory retention
- **Indexing:** Partition-level metadata only
- **Statistics:** On-demand only

### 5. **Purged Data** (Beyond retention)
- **Action:** DROP/TRUNCATE partition or export to external archive
- **Compliance:** Ensure regulatory requirements met before purge

## Automatic Data Optimization (ADO)

Oracle ADO automates ILM policies using heat map statistics and user-defined policies.

### Enable Heat Map
```sql
-- Database level
ALTER SYSTEM SET HEAT_MAP = ON SCOPE=BOTH;

-- Verify heat map is enabled
SELECT PARAMETER, VALUE FROM V$OPTION WHERE PARAMETER = 'Heat Map';

-- Check heat map statistics
SELECT * FROM USER_HEAT_MAP_SEGMENT
WHERE SEGMENT_NAME = 'SALES_FACT'
ORDER BY SEGMENT_WRITE_TIME DESC;
```

### Benefits of Heat Map
- Tracks segment and row-level access patterns
- Records read/write timestamps
- Provides data for ILM policy decisions
- Minimal performance overhead (<1%)

## ILM Policy Types

### 1. Row-Level Compression
Automatically compresses inactive rows within active partitions.

### 2. Segment-Level Compression
Compresses entire partitions based on access patterns.

### 3. Storage Tiering
Moves partitions to different tablespaces (storage tiers).

### 4. Partition Archival
Marks partitions as read-only and applies maximum compression.

### 5. Automatic Partition Drop
Removes partitions beyond retention period.

## Policy Implementation Patterns

### Pattern 1: Time-Based Compression Policy
```sql
-- Compress partitions after 90 days of no modification
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tablespace_warm
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 90 DAYS OF NO MODIFICATION;
```

### Pattern 2: Multi-Tier Storage Policy
```sql
-- Move to warm storage after 6 months
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tablespace_warm
    COMPRESS FOR QUERY HIGH
    SEGMENT
    AFTER 6 MONTHS OF NO MODIFICATION;

-- Move to cold storage after 2 years
ALTER TABLE sales_fact ILM ADD POLICY
    TIER TO tablespace_cold
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 24 MONTHS OF NO MODIFICATION;
```

### Pattern 3: Custom Function-Based Policy
```sql
-- Create custom policy function
CREATE OR REPLACE FUNCTION is_eligible_for_archive(
    p_segment_name VARCHAR2,
    p_last_modification_time TIMESTAMP
) RETURN BOOLEAN IS
    v_days_old NUMBER;
BEGIN
    v_days_old := TRUNC(SYSDATE - p_last_modification_time);

    -- Custom logic: Archive if > 365 days old AND size > 10GB
    IF v_days_old > 365 THEN
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
/

-- Apply policy using custom function
ALTER TABLE sales_fact ILM ADD POLICY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    ON function_name(segment_name, last_modification_time);
```

### Pattern 4: Read-Only + Archive
```sql
-- Make partition read-only and archive after 1 year
ALTER TABLE sales_fact ILM ADD POLICY
    READ ONLY
    COMPRESS FOR ARCHIVE HIGH
    SEGMENT
    AFTER 12 MONTHS OF NO MODIFICATION;
```

## Policy Execution and Monitoring

### Manual Policy Execution
```sql
-- Execute ILM policies immediately (instead of waiting for maintenance window)
EXEC DBMS_ILM.EXECUTE_ILM(
    task_id => DBMS_ILM.CREATE_EXECUTION_TASK(
        policy_name => 'SALES_FACT_COMPRESSION_POLICY',
        execution_mode => DBMS_ILM.EXECUTE_OFFLINE
    )
);
```

### Automated Execution
ILM policies execute during maintenance windows defined by:
```sql
-- Set maintenance window
BEGIN
    DBMS_AUTO_TASK_ADMIN.ENABLE(
        client_name => 'auto space advisor',
        operation => NULL,
        window_name => 'MONDAY_WINDOW'
    );
END;
/
```

### Monitor Policy Status
```sql
-- Check ILM policies
SELECT * FROM USER_ILMPOLICIES
WHERE OBJECT_NAME = 'SALES_FACT';

-- Check policy execution history
SELECT * FROM USER_ILMTASKS
WHERE OBJECT_NAME = 'SALES_FACT'
ORDER BY CREATION_TIME DESC;

-- Check policy results
SELECT * FROM USER_ILMRESULTS
WHERE OBJECT_NAME = 'SALES_FACT'
ORDER BY EXECUTION_TIME DESC;

-- View segments eligible for policies
SELECT * FROM USER_ILMEVALUATIONDETAILS
WHERE OBJECT_NAME = 'SALES_FACT';
```

## Compression Strategies by Data Type

### OLTP Compression (Active Data)
- **Type:** ROW STORE COMPRESS ADVANCED
- **Use Case:** Frequently updated data
- **Compression Ratio:** 2-4x
- **DML Impact:** Minimal

### Query Compression (Warm Data)
- **Type:** COMPRESS FOR QUERY LOW/HIGH
- **Use Case:** Read-mostly data
- **Compression Ratio:** 4-10x
- **DML Impact:** Moderate (updates create uncompressed rows)

### Archive Compression (Cold Data)
- **Type:** COMPRESS FOR ARCHIVE LOW/HIGH
- **Use Case:** Rarely accessed data
- **Compression Ratio:** 10-50x
- **DML Impact:** Significant (decompression required)

### Hybrid Columnar Compression (HCC)
- **Type:** Available on Exadata, ZFS, SuperCluster
- **Compression Ratio:** Up to 50x
- **Best For:** Large fact tables, archive data

## Tablespace Strategy for ILM

### Create Tiered Tablespaces
```sql
-- Hot tier: SSD/Flash storage
CREATE TABLESPACE tbs_hot
    DATAFILE '/u01/oradata/hot/hot01.dbf' SIZE 10G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

-- Warm tier: Standard SAS
CREATE TABLESPACE tbs_warm
    DATAFILE '/u02/oradata/warm/warm01.dbf' SIZE 50G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

-- Cold tier: SATA/NAS
CREATE TABLESPACE tbs_cold
    DATAFILE '/u03/oradata/cold/cold01.dbf' SIZE 100G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

-- Archive tier: Low-cost storage
CREATE TABLESPACE tbs_archive
    DATAFILE '/u04/oradata/archive/archive01.dbf' SIZE 500G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;
```

## Data Retention and Compliance

### Retention Policy Matrix

| Data Type | Active | Warm | Cold | Archive | Total Retention |
|-----------|--------|------|------|---------|----------------|
| Transactional | 3 months | 9 months | 2 years | 5 years | 7 years |
| Financial | 3 months | 9 months | 3 years | 7 years | 10 years |
| Web Events | 1 month | 5 months | 1 year | 2 years | 3 years |
| Audit Logs | 6 months | 1.5 years | 3 years | 5 years | 10 years |
| Customer Data | Until end of relationship + 7 years (GDPR/compliance) |

### Automated Partition Purging
```sql
-- Create procedure for automatic partition drop
CREATE OR REPLACE PROCEDURE purge_old_partitions(
    p_table_name VARCHAR2,
    p_retention_days NUMBER
) AS
    v_partition_name VARCHAR2(128);
    v_high_value VARCHAR2(4000);
    v_partition_date DATE;
    v_cutoff_date DATE := SYSDATE - p_retention_days;
BEGIN
    FOR rec IN (
        SELECT partition_name, high_value
        FROM user_tab_partitions
        WHERE table_name = UPPER(p_table_name)
        ORDER BY partition_position
    ) LOOP
        -- Extract date from high_value
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT ' || rec.high_value || ' FROM DUAL'
                INTO v_partition_date;

            IF v_partition_date < v_cutoff_date THEN
                -- Archive partition before dropping (optional)
                DBMS_OUTPUT.PUT_LINE('Dropping partition: ' || rec.partition_name);

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_table_name ||
                    ' DROP PARTITION ' || rec.partition_name;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error processing partition ' || rec.partition_name);
        END;
    END LOOP;
END;
/
```

## Best Practices

### 1. Start with Heat Map
- Enable heat map first to gather access patterns
- Wait 30-90 days before implementing policies
- Analyze heat map data to inform policy decisions

### 2. Test Policies on Non-Production
- Validate compression ratios
- Measure query performance impact
- Test restore/recovery procedures

### 3. Gradual Implementation
- Start with oldest partitions
- Monitor performance and space savings
- Gradually expand to more recent data

### 4. Monitor and Adjust
- Review policy effectiveness quarterly
- Adjust thresholds based on actual usage
- Update policies as business requirements change

### 5. Document Retention Requirements
- Legal and regulatory requirements
- Business operational needs
- Customer contractual obligations
- Industry-specific compliance (HIPAA, SOX, GDPR)

### 6. Plan for Data Recovery
- Test restoration from archived partitions
- Document recovery time objectives (RTO)
- Maintain metadata for archived data
- Consider export to external formats for long-term archive

### 7. Coordinate with Backup Strategy
- Align ILM tiers with backup frequency
- Reduce backup of archived data
- Use incremental backups for active data
- Consider backup elimination for cold data

## Cost Optimization

### Storage Cost Savings
- **Hot → Warm:** 40-60% cost reduction
- **Warm → Cold:** 60-75% cost reduction
- **Cold → Archive:** 80-90% cost reduction

### Performance Considerations
- Query performance on compressed data: 5-15% overhead
- Storage I/O reduction: 50-90% (due to compression)
- Backup time reduction: 50-80%
- Network transfer reduction: 60-90%

## Troubleshooting

### Policy Not Executing
```sql
-- Check if ADO is enabled
SELECT * FROM DBA_AUTO_TASK_CLIENT WHERE CLIENT_NAME = 'auto space advisor';

-- Check maintenance windows
SELECT WINDOW_NAME, ENABLED FROM DBA_SCHEDULER_WINDOWS;

-- Force execution
EXEC DBMS_ILM.FLUSH_ALL_SEGMENTS;
```

### Compression Not Applied
```sql
-- Check segment compression
SELECT segment_name, partition_name, compression, compress_for
FROM user_segments
WHERE segment_name = 'SALES_FACT';

-- Verify policy eligibility
SELECT * FROM USER_ILMEVALUATIONDETAILS
WHERE OBJECT_NAME = 'SALES_FACT';
```

### Performance Degradation
```sql
-- Check for uncompressed rows in compressed partitions
SELECT * FROM TABLE(DBMS_SPACE.OBJECT_SPACE_USAGE_CHAIN('SCHEMA', 'SALES_FACT', 'TABLE'));

-- Rebuild affected segments
ALTER TABLE sales_fact MOVE PARTITION p_2024_01 COMPRESS FOR QUERY HIGH;
```
