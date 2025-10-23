# ILM Policy Design Guide

**Version:** 1.0
**Date:** 2025-10-23
**Target Audience:** DBAs, Data Architects, Data Warehouse Developers

---

## Table of Contents

1. [Introduction](#introduction)
2. [Data Lifecycle Methodology](#data-lifecycle-methodology)
3. [Compression Strategy Selection](#compression-strategy-selection)
4. [Policy Design Process](#policy-design-process)
5. [Testing Procedures](#testing-procedures)
6. [Common Policy Patterns](#common-policy-patterns)
7. [Advanced Techniques](#advanced-techniques)
8. [Performance Considerations](#performance-considerations)
9. [Common Pitfalls](#common-pitfalls)
10. [Policy Templates](#policy-templates)
11. [Monitoring and Tuning](#monitoring-and-tuning)

---

## Introduction

### Purpose

This guide provides a structured methodology for designing effective Information Lifecycle Management (ILM) policies for Oracle data warehouse environments. Well-designed policies balance:

- **Storage costs** - Reducing storage footprint through compression
- **Query performance** - Maintaining acceptable query response times
- **Operational overhead** - Minimizing management complexity
- **Compliance requirements** - Meeting data retention and access requirements

### Key Principles

1. **Data-Driven Design** - Base policies on actual access patterns and data characteristics
2. **Conservative Start** - Begin with less aggressive policies and tune based on results
3. **Test Before Production** - Always test policies on non-production environments first
4. **Monitor and Adjust** - Continuously monitor effectiveness and adjust as needed
5. **Document Everything** - Maintain clear documentation of policy rationale and changes

---

## Data Lifecycle Methodology

### Lifecycle Stages

Data typically progresses through distinct lifecycle stages based on age and access patterns:

| Stage | Age | Access Pattern | Characteristics | Storage Type |
|-------|-----|----------------|-----------------|--------------|
| **HOT** | 0-3 months | Frequent reads/writes | Active transactions, high query volume | Fast SSD, minimal compression |
| **WARM** | 3-12 months | Regular reads, rare writes | Recent history, moderate query volume | Standard SSD/HDD, query compression |
| **COOL** | 1-3 years | Infrequent reads, no writes | Historical analysis, occasional queries | HDD, archive compression |
| **COLD** | 3-7 years | Rare reads, read-only | Compliance, rare ad-hoc queries | Cheap HDD, archive compression, read-only |
| **FROZEN** | 7+ years | Archive/purge candidate | Legal hold, rarely accessed | Archive tier or purge |

### Stage Transitions

Data transitions between stages based on:

1. **Time-Based Aging**
   - Partition age (calculated from partition boundary date)
   - Days/months since creation
   - Most common and predictable approach

2. **Access-Based Aging**
   - Days since last read (requires Heat Map or custom tracking)
   - Read frequency over time windows
   - More dynamic but requires monitoring infrastructure

3. **Business Logic**
   - Order status (open vs closed orders)
   - Customer status (active vs inactive)
   - Product lifecycle (current vs discontinued)
   - Requires custom conditions in policies

### Defining Your Lifecycle

**Step 1: Understand Data Characteristics**

```sql
-- Analyze partition age distribution
SELECT
    table_name,
    COUNT(*) AS partition_count,
    MIN(partition_age_days) AS youngest_partition,
    MAX(partition_age_days) AS oldest_partition,
    ROUND(AVG(partition_age_days), 0) AS avg_age_days,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY partition_age_days) AS median_age,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY partition_age_days) AS p90_age
FROM (
    SELECT
        table_name,
        partition_name,
        TRUNC(SYSDATE - partition_date) AS partition_age_days
    FROM cmr.dwh_ilm_partition_access
)
GROUP BY table_name
ORDER BY table_name;
```

**Step 2: Analyze Query Patterns**

```sql
-- Query historical data access patterns from AWR
-- (Requires Diagnostics Pack license)
SELECT
    object_name AS table_name,
    SUM(CASE WHEN partition_age_days < 90 THEN logical_reads ELSE 0 END) AS reads_0_3m,
    SUM(CASE WHEN partition_age_days BETWEEN 90 AND 365 THEN logical_reads ELSE 0 END) AS reads_3_12m,
    SUM(CASE WHEN partition_age_days BETWEEN 365 AND 1095 THEN logical_reads ELSE 0 END) AS reads_1_3y,
    SUM(CASE WHEN partition_age_days > 1095 THEN logical_reads ELSE 0 END) AS reads_3y_plus
FROM your_custom_tracking_view
GROUP BY object_name;
```

**Step 3: Define Stage Boundaries**

Based on analysis, define specific age thresholds for your environment:

```sql
-- Update lifecycle stage thresholds
UPDATE cmr.dwh_ilm_config
SET config_value = '90'  -- 3 months
WHERE config_key = 'HOT_THRESHOLD_DAYS';

UPDATE cmr.dwh_ilm_config
SET config_value = '365'  -- 12 months
WHERE config_key = 'WARM_THRESHOLD_DAYS';

UPDATE cmr.dwh_ilm_config
SET config_value = '1095'  -- 3 years
WHERE config_key = 'COLD_THRESHOLD_DAYS';

COMMIT;
```

**Example Lifecycle Definitions:**

**Financial Transaction System:**
- HOT: 0-1 month (regulatory requirement for amendments)
- WARM: 1-12 months (frequent reconciliation queries)
- COOL: 1-7 years (audit queries)
- COLD: 7-10 years (compliance archive)
- FROZEN: 10+ years (purge after legal requirements met)

**E-Commerce Order System:**
- HOT: 0-3 months (active orders, returns, customer service)
- WARM: 3-12 months (business analytics, trending)
- COOL: 1-2 years (historical reporting)
- COLD: 2-5 years (customer lifetime value analysis)
- FROZEN: 5+ years (purge with customer consent)

**IoT Sensor Data:**
- HOT: 0-7 days (real-time alerting, dashboards)
- WARM: 7-30 days (recent trend analysis)
- COOL: 1-3 months (pattern detection, ML training)
- COLD: 3-6 months (archive, rarely queried)
- FROZEN: 6+ months (purge, summarized data retained)

---

## Compression Strategy Selection

### Compression Types Overview

Oracle offers multiple compression algorithms, each with different trade-offs:

| Compression Type | Compression Ratio | Query Performance | Write Performance | Use Case |
|-----------------|-------------------|-------------------|-------------------|----------|
| **NONE** | 1x (baseline) | Fast | Fast | HOT data, active transactions |
| **BASIC** | 2-3x | Moderate | Slow | Legacy, not recommended |
| **OLTP** | 2-3x | Fast | Moderate | Active data with updates |
| **QUERY LOW** | 3-5x | Good | Slow | WARM data, moderate query load |
| **QUERY HIGH** | 5-10x | Good | Very Slow | WARM/COOL data, heavy query load |
| **ARCHIVE LOW** | 10-15x | Moderate | Very Slow | COOL data, infrequent queries |
| **ARCHIVE HIGH** | 15-25x | Slower | Very Slow | COLD data, rare queries |

### Compression Selection Matrix

**By Data Age:**

```
0-3 months:     NONE or OLTP (if updates occur)
3-12 months:    QUERY HIGH (optimal balance for DW queries)
1-3 years:      ARCHIVE HIGH (maximize space savings)
3+ years:       ARCHIVE HIGH + read-only (compliance/archive)
```

**By Access Pattern:**

```
Frequent queries (>10/day):    QUERY HIGH (fast decompression)
Occasional queries (1-10/day): ARCHIVE LOW (good balance)
Rare queries (<1/day):         ARCHIVE HIGH (maximize savings)
Archive only (no queries):     ARCHIVE HIGH + read-only
```

**By Table Type:**

```
Fact Tables (large):           Start with QUERY HIGH, move to ARCHIVE HIGH
Dimension Tables (small):      Often no compression needed (small footprint)
SCD2 History:                  QUERY HIGH for recent, ARCHIVE HIGH for old
Event/Log Tables:              Aggressive compression (ARCHIVE HIGH)
Staging Tables:                No compression (short-lived, frequent writes)
```

### Compression Testing

Before committing to a compression strategy, test compression ratios and query performance:

```sql
-- Test compression ratio on a sample partition
ALTER TABLE sales_fact
MODIFY PARTITION p_2023_06
COMPRESS FOR QUERY HIGH;

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS(
    ownname => USER,
    tabname => 'SALES_FACT',
    partname => 'P_2023_06',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    method_opt => 'FOR ALL COLUMNS SIZE AUTO'
);

-- Measure compression ratio
SELECT
    segment_name,
    partition_name,
    ROUND(bytes/1024/1024, 2) AS size_mb,
    compression,
    compress_for
FROM user_segments
WHERE segment_name = 'SALES_FACT'
AND partition_name IN ('P_2023_05', 'P_2023_06')  -- Compare compressed vs uncompressed
ORDER BY partition_name;

-- Test query performance before and after
SET TIMING ON
SET AUTOTRACE TRACEONLY STATISTICS

-- Query uncompressed partition
SELECT COUNT(*), SUM(total_amount)
FROM sales_fact PARTITION (p_2023_05)
WHERE sale_date >= DATE '2023-05-01';

-- Query compressed partition
SELECT COUNT(*), SUM(total_amount)
FROM sales_fact PARTITION (p_2023_06)
WHERE sale_date >= DATE '2023-06-01';

SET AUTOTRACE OFF
SET TIMING OFF
```

**Target Compression Ratios:**

- **Fact Tables:** 5-10x (QUERY HIGH), 10-20x (ARCHIVE HIGH)
- **Dimension Tables:** 2-5x (smaller benefit due to size)
- **SCD2 History:** 5-15x (highly compressible)
- **Event/Log Tables:** 15-30x (repetitive data patterns)

**If compression ratio is lower than expected:**
- Check data distribution (highly random data compresses poorly)
- Consider different compression type
- Verify statistics are current
- Check for VARCHAR2 vs CHAR (CHAR doesn't compress well)

---

## Policy Design Process

### 7-Step Policy Design Methodology

#### Step 1: Define Business Requirements

**Document:**
- Data retention requirements (regulatory, business, operational)
- Access patterns (who queries what, how often)
- Performance SLAs (query response time requirements)
- Storage budget constraints
- Compliance requirements (GDPR, SOX, HIPAA, etc.)

**Example Requirements Document:**

```
Table: SALES_FACT
Purpose: Store daily sales transactions
Retention: 7 years (5 years regulatory + 2 years buffer)
Access Pattern:
  - Current month: 1000+ queries/day (reports, dashboards)
  - Last 12 months: 50-100 queries/day (analytics)
  - 1-3 years: 5-10 queries/day (trend analysis)
  - 3+ years: <1 query/day (audit, ad-hoc)
Performance SLA:
  - Current data: <2 seconds for standard queries
  - Historical data: <30 seconds acceptable
Storage Constraint: Target 60% reduction vs. uncompressed
```

#### Step 2: Analyze Current State

```sql
-- Gather current statistics
SELECT
    t.table_name,
    t.num_rows,
    ROUND(SUM(s.bytes)/1024/1024/1024, 2) AS size_gb,
    COUNT(DISTINCT tp.partition_name) AS partition_count,
    MIN(tp.partition_position) AS oldest_partition_pos,
    MAX(tp.partition_position) AS newest_partition_pos
FROM user_tables t
JOIN user_segments s ON s.segment_name = t.table_name
LEFT JOIN user_tab_partitions tp ON tp.table_name = t.table_name
WHERE t.table_name = 'SALES_FACT'
GROUP BY t.table_name, t.num_rows;

-- Analyze partition age distribution
SELECT
    table_name,
    partition_name,
    num_rows,
    ROUND(bytes/1024/1024, 2) AS size_mb,
    compression,
    last_analyzed,
    TRUNC(SYSDATE - last_analyzed) AS days_since_analyzed
FROM user_tab_partitions tp
JOIN user_segments s
    ON s.segment_name = tp.table_name
    AND s.partition_name = tp.partition_name
WHERE tp.table_name = 'SALES_FACT'
ORDER BY tp.partition_position DESC
FETCH FIRST 20 ROWS ONLY;
```

#### Step 3: Design Policy Set

Based on lifecycle and requirements, design a policy set:

**Example: Financial Transaction Table**

```sql
-- Policy 1: Compress warm data (3-12 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_months, compression_type,
    priority, enabled
) VALUES (
    'FIN_COMPRESS_3M', USER, 'FINANCIAL_TRANSACTIONS',
    'COMPRESSION', 'COMPRESS',
    3, 'QUERY HIGH',
    100, 'Y'
);

-- Policy 2: Move to cold storage and archive compress (12-36 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_months, target_tablespace, compression_type,
    priority, enabled
) VALUES (
    'FIN_TIER_COLD_12M', USER, 'FINANCIAL_TRANSACTIONS',
    'TIERING', 'MOVE',
    12, 'TBS_COLD', 'ARCHIVE HIGH',
    200, 'Y'
);

-- Policy 3: Make read-only (36+ months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'FIN_READONLY_36M', USER, 'FINANCIAL_TRANSACTIONS',
    'ARCHIVAL', 'READ_ONLY',
    36, 300, 'Y'
);

-- Policy 4: Purge (84+ months / 7 years)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'FIN_PURGE_84M', USER, 'FINANCIAL_TRANSACTIONS',
    'PURGE', 'DROP',
    84, 900, 'Y'
);

COMMIT;
```

**Policy Set Design Checklist:**

- [ ] Policies cover all lifecycle stages
- [ ] Age thresholds don't overlap (use distinct boundaries)
- [ ] Priorities are correctly ordered (lower number = higher priority)
- [ ] Compression types match data age and access patterns
- [ ] Tablespace targets exist and have sufficient space
- [ ] Purge policies align with retention requirements
- [ ] Policies are initially disabled for testing (`enabled = 'N'`)

#### Step 4: Validate Policies

Use the built-in validation before enabling:

```sql
-- Validate all policies for the table
SET SERVEROUTPUT ON
EXEC dwh_validate_ilm_policy(1);  -- Policy ID
EXEC dwh_validate_ilm_policy(2);
EXEC dwh_validate_ilm_policy(3);
EXEC dwh_validate_ilm_policy(4);
```

**Validation checks:**
- Table exists and is partitioned
- Tablespace exists and is accessible
- Compression type is valid
- Action type matches policy type
- At least one condition is specified
- Priorities are in valid range (1-999)

#### Step 5: Test in Non-Production

**Create Test Environment:**

```sql
-- Create test table with sample of production data
CREATE TABLE sales_fact_test
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2020_01 VALUES LESS THAN (DATE '2020-02-01')
)
AS
SELECT * FROM sales_fact
WHERE sale_date >= ADD_MONTHS(SYSDATE, -36)  -- Last 3 years
AND ROWNUM <= 1000000;  -- Limit rows for testing

-- Create test policies
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_days, compression_type,
    priority, enabled
) VALUES (
    'TEST_COMPRESS_90D', USER, 'SALES_FACT_TEST',
    'COMPRESSION', 'COMPRESS',
    90, 'QUERY HIGH',
    100, 'Y'
);
COMMIT;
```

**Execute Test Cycle:**

```sql
-- Run policy evaluation
EXEC pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT_TEST');

-- Review what will be executed
SELECT
    q.partition_name,
    p.action_type,
    p.compression_type,
    q.reason,
    a.size_mb,
    a.num_rows
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON q.policy_id = p.policy_id
JOIN cmr.dwh_ilm_partition_access a
    ON q.table_owner = a.table_owner
    AND q.table_name = a.table_name
    AND q.partition_name = a.partition_name
WHERE q.table_name = 'SALES_FACT_TEST'
AND q.eligible = 'Y';

-- Execute policies (limit to 2 partitions for testing)
EXEC pck_dwh_ilm_execution_engine.execute_table(USER, 'SALES_FACT_TEST', p_max_operations => 2);
```

**Measure Results:**

```sql
-- Check compression effectiveness
SELECT
    e.partition_name,
    e.action_type,
    e.status,
    e.size_before_mb,
    e.size_after_mb,
    e.space_saved_mb,
    ROUND(e.compression_ratio, 2) AS compression_ratio,
    ROUND(e.duration_seconds/60, 2) AS duration_minutes
FROM cmr.dwh_ilm_execution_log e
WHERE e.table_name = 'SALES_FACT_TEST'
ORDER BY e.execution_start DESC;

-- Test query performance
SET TIMING ON
-- Test query on compressed partition
SELECT COUNT(*), SUM(total_amount)
FROM sales_fact_test
WHERE sale_date >= DATE '2024-01-01'
AND sale_date < DATE '2024-02-01';

-- Compare to uncompressed partition
SELECT COUNT(*), SUM(total_amount)
FROM sales_fact_test
WHERE sale_date >= DATE '2024-09-01'
AND sale_date < DATE '2024-10-01';
SET TIMING OFF
```

#### Step 6: Deploy to Production

**Pre-Deployment Checklist:**

- [ ] Test results reviewed and approved
- [ ] Compression ratios meet expectations
- [ ] Query performance acceptable
- [ ] No errors in test execution
- [ ] Backup of production table completed
- [ ] Rollback procedure documented
- [ ] Change window scheduled
- [ ] Stakeholders notified

**Phased Deployment:**

```sql
-- Phase 1: Deploy disabled, validate configuration
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_months, compression_type,
    priority, enabled
) VALUES (
    'SALES_COMPRESS_3M', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS',
    3, 'QUERY HIGH',
    100, 'N'  -- Disabled initially
);
COMMIT;

-- Validate
EXEC dwh_validate_ilm_policy((SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'SALES_COMPRESS_3M'));

-- Phase 2: Enable and run on oldest partition only
UPDATE cmr.dwh_ilm_policies
SET enabled = 'Y'
WHERE policy_name = 'SALES_COMPRESS_3M';
COMMIT;

-- Run on single partition
EXEC pck_dwh_ilm_policy_engine.evaluate_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'SALES_COMPRESS_3M')
);

EXEC pck_dwh_ilm_execution_engine.execute_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'SALES_COMPRESS_3M'),
    p_max_operations => 1
);

-- Phase 3: Monitor, then expand to more partitions
-- After 24-48 hours of monitoring:
EXEC pck_dwh_ilm_execution_engine.execute_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'SALES_COMPRESS_3M'),
    p_max_operations => 10
);
```

#### Step 7: Monitor and Tune

**Daily Monitoring (first week):**

```sql
-- Check execution status
SELECT
    execution_id,
    partition_name,
    status,
    execution_start,
    duration_seconds,
    compression_ratio,
    space_saved_mb,
    error_message
FROM cmr.dwh_ilm_execution_log
WHERE table_name = 'SALES_FACT'
AND execution_start > SYSDATE - 1
ORDER BY execution_start DESC;

-- Monitor query performance
-- Compare query times before/after compression
SELECT
    sql_id,
    executions,
    ROUND(elapsed_time/1000000/executions, 2) AS avg_elapsed_sec,
    ROUND(cpu_time/1000000/executions, 2) AS avg_cpu_sec,
    ROUND(buffer_gets/executions, 0) AS avg_buffer_gets
FROM v$sqlarea
WHERE sql_text LIKE '%SALES_FACT%'
AND executions > 10
ORDER BY elapsed_time DESC;
```

**Weekly Review (first month):**

- Review space savings achieved
- Check for any query performance degradation
- Verify no increase in user complaints
- Adjust age thresholds if needed
- Tune compression types if ratios are poor

**Monthly Tuning:**

```sql
-- Analyze policy effectiveness
SELECT
    p.policy_name,
    p.policy_type,
    p.action_type,
    COUNT(e.execution_id) AS executions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    ROUND(SUM(e.space_saved_mb), 2) AS total_space_saved_mb,
    ROUND(AVG(e.compression_ratio), 2) AS avg_compression_ratio,
    ROUND(AVG(e.duration_seconds/60), 2) AS avg_duration_min
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_execution_log e
    ON e.policy_id = p.policy_id
    AND e.execution_start > ADD_MONTHS(SYSDATE, -1)
WHERE p.table_name = 'SALES_FACT'
GROUP BY p.policy_name, p.policy_type, p.action_type
ORDER BY total_space_saved_mb DESC NULLS LAST;
```

---

## Testing Procedures

### Pre-Production Testing Checklist

**1. Syntax Validation**

```sql
-- Test policy syntax
SET SERVEROUTPUT ON
EXEC dwh_validate_ilm_policy(p_policy_id => 1);
```

**2. Dry Run Evaluation**

```sql
-- See what would be executed without actually executing
EXEC pck_dwh_ilm_policy_engine.evaluate_policy(1);

SELECT
    partition_name,
    eligible,
    reason
FROM cmr.dwh_ilm_evaluation_queue
WHERE policy_id = 1
ORDER BY partition_name;
```

**3. Single Partition Test**

```sql
-- Test on oldest, least-accessed partition first
EXEC pck_dwh_ilm_execution_engine.execute_policy(
    p_policy_id => 1,
    p_max_operations => 1  -- Only one partition
);

-- Verify result
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE policy_id = 1
ORDER BY execution_start DESC
FETCH FIRST 1 ROWS ONLY;
```

**4. Query Performance Regression Test**

Create a test suite of representative queries:

```sql
-- Create test query suite
CREATE TABLE ilm_test_queries (
    test_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    query_name VARCHAR2(100),
    query_sql CLOB,
    partition_affected VARCHAR2(128),
    baseline_elapsed_sec NUMBER,
    test_elapsed_sec NUMBER,
    performance_change_pct NUMBER,
    test_date DATE
);

-- Example test query
INSERT INTO ilm_test_queries (query_name, query_sql, partition_affected)
VALUES (
    'Daily Sales Summary',
    'SELECT sale_date, COUNT(*), SUM(total_amount) FROM sales_fact
     WHERE sale_date >= DATE ''2024-01-01'' AND sale_date < DATE ''2024-02-01''
     GROUP BY sale_date',
    'P_2024_01'
);

-- Run baseline (before compression)
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_elapsed NUMBER;
    v_sql CLOB;
    v_dummy NUMBER;
BEGIN
    SELECT query_sql INTO v_sql FROM ilm_test_queries WHERE test_id = 1;

    v_start := SYSTIMESTAMP;
    EXECUTE IMMEDIATE v_sql INTO v_dummy;
    v_end := SYSTIMESTAMP;

    v_elapsed := EXTRACT(SECOND FROM (v_end - v_start));

    UPDATE ilm_test_queries
    SET baseline_elapsed_sec = v_elapsed
    WHERE test_id = 1;
    COMMIT;
END;
/

-- Apply compression...

-- Run test (after compression)
-- ... (similar logic for test_elapsed_sec)

-- Compare results
SELECT
    query_name,
    baseline_elapsed_sec,
    test_elapsed_sec,
    ROUND(((test_elapsed_sec - baseline_elapsed_sec) / baseline_elapsed_sec) * 100, 1) AS pct_change,
    CASE
        WHEN ((test_elapsed_sec - baseline_elapsed_sec) / baseline_elapsed_sec) * 100 < -10 THEN 'FASTER'
        WHEN ((test_elapsed_sec - baseline_elapsed_sec) / baseline_elapsed_sec) * 100 > 10 THEN 'SLOWER'
        ELSE 'NO CHANGE'
    END AS performance_impact
FROM ilm_test_queries;
```

**5. Rollback Test**

Verify you can rollback changes if needed:

```sql
-- Document current state
SELECT segment_name, partition_name, bytes, compression, compress_for
FROM user_segments
WHERE segment_name = 'SALES_FACT_TEST'
AND partition_name = 'P_2024_01';

-- Apply compression
ALTER TABLE sales_fact_test
MODIFY PARTITION p_2024_01
COMPRESS FOR QUERY HIGH;

-- Rollback (decompress)
ALTER TABLE sales_fact_test
MODIFY PARTITION p_2024_01
NOCOMPRESS;

-- Verify rollback
SELECT segment_name, partition_name, bytes, compression, compress_for
FROM user_segments
WHERE segment_name = 'SALES_FACT_TEST'
AND partition_name = 'P_2024_01';
```

**Note:** Decompression requires MOVE operation and will take time proportional to partition size.

---

## Common Policy Patterns

### Pattern 1: Simple Age-Based Compression

**Use Case:** Straightforward lifecycle, compress everything older than X days

```sql
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    priority, enabled
) VALUES (
    'ORDERS_COMPRESS_90D', USER, 'ORDER_FACT',
    'COMPRESSION', 'COMPRESS', 90, 'QUERY HIGH',
    100, 'Y'
);
```

**Pros:** Simple, predictable, easy to understand
**Cons:** Doesn't account for access patterns, may compress frequently-queried data

### Pattern 2: Multi-Stage Progressive Compression

**Use Case:** Balance query performance with storage savings

```sql
-- Stage 1: Light compression for warm data
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    priority, enabled
) VALUES (
    'SALES_COMPRESS_LIGHT_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90, 'QUERY HIGH',
    100, 'Y'
);

-- Stage 2: Heavy compression for cool data
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    priority, enabled
) VALUES (
    'SALES_COMPRESS_HEAVY_365D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 365, 'ARCHIVE HIGH',
    200, 'Y'
);
```

**Pros:** Balances performance and storage at each stage
**Cons:** More complex, policies may conflict if not carefully designed

**Important:** The second policy will re-compress already-compressed partitions. Oracle will decompress and recompress with new type.

### Pattern 3: Temperature-Based (Access Pattern)

**Use Case:** Compress based on actual usage, not just age

```sql
-- Compress cold partitions regardless of age
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, access_pattern, compression_type,
    priority, enabled
) VALUES (
    'ORDERS_COMPRESS_COLD', USER, 'ORDER_FACT',
    'COMPRESSION', 'COMPRESS', 'COLD', 'ARCHIVE HIGH',
    100, 'Y'
);
```

**Pros:** Adapts to actual usage patterns, avoids compressing active data
**Cons:** Requires Heat Map or access tracking, less predictable

### Pattern 4: Business Logic-Based

**Use Case:** Compress based on business status, not age

```sql
-- Compress completed/closed orders
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, custom_condition, compression_type,
    priority, enabled
) VALUES (
    'ORDERS_COMPRESS_CLOSED', USER, 'ORDER_FACT',
    'COMPRESSION', 'COMPRESS', 30, 'order_status = ''CLOSED''', 'QUERY HIGH',
    100, 'Y'
);
```

**Pros:** Aligned with business logic, compress data when no longer updated
**Cons:** More complex, requires column to indicate status

### Pattern 5: Size-Based Selective Compression

**Use Case:** Only compress large partitions (small partitions have minimal benefit)

```sql
-- Only compress partitions > 100 MB
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, size_threshold_mb, compression_type,
    priority, enabled
) VALUES (
    'EVENTS_COMPRESS_LARGE_90D', USER, 'WEB_EVENTS_FACT',
    'COMPRESSION', 'COMPRESS', 90, 100, 'QUERY HIGH',
    100, 'Y'
);
```

**Pros:** Focuses effort on partitions with meaningful space savings
**Cons:** Small partitions never compressed (may not matter)

### Pattern 6: Tiered Storage with Compression

**Use Case:** Move data through storage tiers as it ages

```sql
-- Tier 1: Compress and move to WARM storage
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months,
    target_tablespace, compression_type,
    priority, enabled
) VALUES (
    'SALES_TIER_WARM_6M', USER, 'SALES_FACT',
    'TIERING', 'MOVE', 6,
    'TBS_WARM', 'QUERY HIGH',
    100, 'Y'
);

-- Tier 2: Move to COLD storage with archive compression
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months,
    target_tablespace, compression_type,
    priority, enabled
) VALUES (
    'SALES_TIER_COLD_24M', USER, 'SALES_FACT',
    'TIERING', 'MOVE', 24,
    'TBS_COLD', 'ARCHIVE HIGH',
    200, 'Y'
);
```

**Pros:** Optimizes both performance and storage costs
**Cons:** Requires multiple tablespaces with different storage characteristics

---

## Advanced Techniques

### Technique 1: Partition-Selective Policies

Apply different policies to different partition ranges:

```sql
-- Aggressive compression for very old partitions
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months, compression_type,
    custom_condition,
    priority, enabled
) VALUES (
    'SALES_ARCHIVE_OLD', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 24, 'ARCHIVE HIGH',
    'partition_position < 100',  -- Only first 100 partitions
    100, 'Y'
);
```

### Technique 2: Scheduled Policy Activation

Enable policies only during specific times:

```sql
-- Create procedure to enable/disable policies based on time
CREATE OR REPLACE PROCEDURE toggle_ilm_policies(
    p_policy_pattern VARCHAR2,
    p_enable CHAR
) AS
BEGIN
    UPDATE cmr.dwh_ilm_policies
    SET enabled = p_enable,
        modified_by = USER,
        modified_date = SYSTIMESTAMP
    WHERE policy_name LIKE p_policy_pattern;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' policies');
END;
/

-- Schedule to enable during maintenance window
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'ENABLE_COMPRESSION_POLICIES',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN toggle_ilm_policies(''%_COMPRESS_%'', ''Y''); END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=22; BYMINUTE=0',  -- 10 PM daily
        enabled => TRUE
    );
END;
/
```

### Technique 3: Dynamic Threshold Adjustment

Adjust age thresholds based on table growth rate:

```sql
-- Procedure to adjust thresholds dynamically
CREATE OR REPLACE PROCEDURE adjust_ilm_thresholds AS
    v_growth_rate NUMBER;
    v_partition_count NUMBER;
    v_new_threshold NUMBER;
BEGIN
    -- Calculate monthly partition growth rate
    SELECT COUNT(*) INTO v_partition_count
    FROM user_tab_partitions
    WHERE table_name = 'SALES_FACT';

    -- If growing faster than expected, compress sooner
    IF v_partition_count > 60 THEN  -- More than 5 years of monthly partitions
        v_new_threshold := 60;  -- Compress after 2 months instead of 3
    ELSE
        v_new_threshold := 90;  -- Standard 3 months
    END IF;

    -- Update policy
    UPDATE cmr.dwh_ilm_policies
    SET age_days = v_new_threshold
    WHERE policy_name = 'SALES_COMPRESS_3M';

    COMMIT;
END;
/
```

### Technique 4: Conditional Policy Execution

Execute policies only when certain conditions are met:

```sql
-- Only compress if tablespace is getting full
CREATE OR REPLACE PROCEDURE conditional_compress AS
    v_tablespace_pct_full NUMBER;
BEGIN
    -- Check tablespace usage
    SELECT ROUND((1 - (MAX(bytes)/SUM(bytes))) * 100, 2) INTO v_tablespace_pct_full
    FROM dba_data_files
    WHERE tablespace_name = 'TBS_HOT';

    -- Only enable compression if > 70% full
    IF v_tablespace_pct_full > 70 THEN
        UPDATE cmr.dwh_ilm_policies
        SET enabled = 'Y'
        WHERE policy_name LIKE '%COMPRESS%';
        COMMIT;

        -- Run ILM cycle
        dwh_run_ilm_cycle();
    END IF;
END;
/
```

---

## Performance Considerations

### Compression Performance Impact

**Decompression Overhead:**

Compressed data must be decompressed during queries:

- **QUERY LOW/HIGH:** ~5-10% CPU overhead (optimized for queries)
- **ARCHIVE LOW/HIGH:** ~10-20% CPU overhead (optimized for storage)

**Mitigation Strategies:**

1. **Use Query-Optimized Compression** for frequently accessed data
2. **Compress Only Cool/Cold Data** that's queried infrequently
3. **Test Query Performance** before and after compression
4. **Monitor CPU Usage** and adjust compression types if needed

### Parallel Operations

Compression operations can be parallelized:

```sql
-- Set parallel degree in policy
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    parallel_degree,  -- Enable parallelism
    priority, enabled
) VALUES (
    'SALES_COMPRESS_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90, 'QUERY HIGH',
    8,  -- Use 8 parallel workers
    100, 'Y'
);
```

**Guidelines:**
- Use parallel degree = CPU core count for large partitions (>1 GB)
- Use parallel degree = 1 for small partitions (<100 MB)
- Monitor parallel execution waits in AWR
- Don't over-parallelize during business hours

### Index Impact

Compression affects indexes:

**Local Indexes:**
- Automatically maintained during partition compression
- May become unusable if `rebuild_indexes = 'N'`
- Rebuild overhead adds to execution time

**Global Indexes:**
- Become unusable during partition operations if not maintained
- Must be rebuilt or use `UPDATE INDEXES` clause
- Can significantly increase operation duration

**Recommendation:**
- Use local indexes whenever possible
- Set `rebuild_indexes = 'Y'` in policies (default)
- Schedule index maintenance during low-activity windows
- Monitor index status: `SELECT * FROM user_ind_partitions WHERE status = 'UNUSABLE'`

### Statistics Management

Compression changes statistics significantly:

```sql
-- Ensure statistics are gathered after compression
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    gather_stats,  -- Gather stats after compression
    priority, enabled
) VALUES (
    'SALES_COMPRESS_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90, 'QUERY HIGH',
    'Y',  -- Gather stats
    100, 'Y'
);
```

**Best Practices:**
- Always gather statistics after compression (`gather_stats = 'Y'`)
- Use AUTO_SAMPLE_SIZE for large tables
- Monitor stale statistics: `SELECT * FROM user_tab_statistics WHERE stale_stats = 'YES'`

---

## Common Pitfalls

### Pitfall 1: Overlapping Policy Age Ranges

**Problem:** Multiple policies target the same partitions

```sql
-- BAD: Both policies will try to compress same partitions
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 90, ...);   -- Compress at 90 days
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 120, ...);  -- Compress at 120 days
```

**Solution:** Use distinct age ranges or rely on priority

```sql
-- GOOD: Non-overlapping ranges
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 90, compression_type, 'QUERY HIGH', priority, 100, ...);
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 365, compression_type, 'ARCHIVE HIGH', priority, 200, ...);
```

**Or:** Use priorities to control execution order (lower priority number executes first)

### Pitfall 2: Compressing HOT Data

**Problem:** Compressing actively updated partitions causes performance issues

```sql
-- BAD: Compresses current month data
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 0, ...);  -- Compresses immediately
```

**Solution:** Always leave HOT data uncompressed

```sql
-- GOOD: Only compress data older than 3 months
INSERT INTO cmr.dwh_ilm_policies (..., age_days, 90, ...);
```

### Pitfall 3: Ignoring Query Patterns

**Problem:** Aggressive compression on frequently-queried data

**Solution:** Analyze actual query patterns before designing policies

```sql
-- Check which partitions are actually queried
SELECT
    object_name,
    subobject_name AS partition_name,
    COUNT(*) AS query_count
FROM v$sql_plan p
JOIN v$sql s ON s.sql_id = p.sql_id
WHERE object_name = 'SALES_FACT'
AND timestamp > SYSDATE - 30
GROUP BY object_name, subobject_name
ORDER BY query_count DESC;
```

### Pitfall 4: Not Testing Before Production

**Problem:** Deploying policies directly to production without testing

**Solution:** Always test on non-production copy first

### Pitfall 5: Insufficient Tablespace for Compression Operations

**Problem:** Compression requires temp space and may need space in target tablespace

**Solution:** Ensure adequate free space

```sql
-- Check tablespace free space before compression
SELECT
    tablespace_name,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS free_gb,
    COUNT(*) AS free_extents
FROM dba_free_space
WHERE tablespace_name IN ('TBS_HOT', 'TEMP')
GROUP BY tablespace_name;

-- Ensure at least 2x partition size available
```

### Pitfall 6: Forgetting to Enable Policies

**Problem:** Policies created but left disabled

```sql
-- Check for disabled policies
SELECT policy_name, enabled, created_date
FROM cmr.dwh_ilm_policies
WHERE enabled = 'N'
AND created_date < SYSDATE - 7;  -- Created over a week ago but still disabled
```

**Solution:** Review and enable policies after testing

### Pitfall 7: Not Monitoring Execution

**Problem:** Policies run but failures go unnoticed

**Solution:** Set up email notifications and monitor regularly

```sql
-- Enable email notifications
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';

-- Check for failures daily
SELECT COUNT(*) AS failure_count
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 1;
```

---

## Policy Templates

### Template: Fact Table Lifecycle

```sql
-- Stage 1: Compress warm data (3 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months, compression_type,
    rebuild_indexes, gather_stats, parallel_degree,
    priority, enabled
) VALUES (
    '{TABLE}_COMPRESS_3M', USER, '{TABLE_NAME}',
    'COMPRESSION', 'COMPRESS', 3, 'QUERY HIGH',
    'Y', 'Y', 4,
    100, 'Y'
);

-- Stage 2: Archive compress cool data (12 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months, compression_type,
    rebuild_indexes, gather_stats, parallel_degree,
    priority, enabled
) VALUES (
    '{TABLE}_COMPRESS_12M', USER, '{TABLE_NAME}',
    'COMPRESSION', 'COMPRESS', 12, 'ARCHIVE HIGH',
    'Y', 'Y', 4,
    200, 'Y'
);

-- Stage 3: Read-only (36 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months,
    priority, enabled
) VALUES (
    '{TABLE}_READONLY_36M', USER, '{TABLE_NAME}',
    'ARCHIVAL', 'READ_ONLY', 36,
    300, 'Y'
);

-- Stage 4: Purge (84 months / 7 years)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months,
    priority, enabled
) VALUES (
    '{TABLE}_PURGE_84M', USER, '{TABLE_NAME}',
    'PURGE', 'DROP', 84,
    900, 'N'  -- Disabled by default, enable with caution
);
```

### Template: SCD2 Dimension Table

```sql
-- Compress closed history records (no updates expected)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    custom_condition,
    priority, enabled
) VALUES (
    '{TABLE}_COMPRESS_CLOSED', USER, '{TABLE_NAME}',
    'COMPRESSION', 'COMPRESS', 90, 'QUERY HIGH',
    'valid_to_dttm IS NOT NULL',  -- Closed records
    100, 'Y'
);

-- Archive compress very old history (3 years)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_months, compression_type,
    priority, enabled
) VALUES (
    '{TABLE}_ARCHIVE_3Y', USER, '{TABLE_NAME}',
    'COMPRESSION', 'COMPRESS', 36, 'ARCHIVE HIGH',
    200, 'Y'
);
```

### Template: Event/Log Table (High Volume, Short Retention)

```sql
-- Aggressive compression for events (7 days)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days, compression_type,
    priority, enabled
) VALUES (
    '{TABLE}_COMPRESS_7D', USER, '{TABLE_NAME}',
    'COMPRESSION', 'COMPRESS', 7, 'ARCHIVE HIGH',
    100, 'Y'
);

-- Purge old events (90 days)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    priority, enabled
) VALUES (
    '{TABLE}_PURGE_90D', USER, '{TABLE_NAME}',
    'PURGE', 'DROP', 90,
    200, 'Y'
);
```

---

## Monitoring and Tuning

### Key Metrics to Monitor

**1. Space Savings**

```sql
-- Total space saved by ILM policies
SELECT
    table_name,
    ROUND(SUM(space_saved_mb)/1024, 2) AS space_saved_gb,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio,
    COUNT(*) AS partitions_processed
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > ADD_MONTHS(SYSDATE, -1)
GROUP BY table_name
ORDER BY space_saved_gb DESC;
```

**2. Execution Performance**

```sql
-- Average execution time by policy
SELECT
    policy_name,
    action_type,
    COUNT(*) AS executions,
    ROUND(AVG(duration_seconds)/60, 2) AS avg_duration_min,
    ROUND(MAX(duration_seconds)/60, 2) AS max_duration_min,
    ROUND(AVG(space_saved_mb), 2) AS avg_space_saved_mb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > ADD_MONTHS(SYSDATE, -1)
GROUP BY policy_name, action_type
ORDER BY avg_duration_min DESC;
```

**3. Failure Rate**

```sql
-- Policy failure rate
SELECT
    p.policy_name,
    COUNT(CASE WHEN e.status = 'SUCCESS' THEN 1 END) AS successful,
    COUNT(CASE WHEN e.status = 'FAILED' THEN 1 END) AS failed,
    ROUND(
        COUNT(CASE WHEN e.status = 'FAILED' THEN 1 END) /
        NULLIF(COUNT(*), 0) * 100, 2
    ) AS failure_rate_pct
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_execution_log e ON e.policy_id = p.policy_id
WHERE e.execution_start > ADD_MONTHS(SYSDATE, -1)
GROUP BY p.policy_name
HAVING COUNT(CASE WHEN e.status = 'FAILED' THEN 1 END) > 0
ORDER BY failure_rate_pct DESC;
```

**4. Query Performance Impact**

```sql
-- Compare query performance before/after compression
-- (Requires AWR and careful analysis)
SELECT
    sql_id,
    plan_hash_value,
    MIN(snap_id) AS first_snap,
    MAX(snap_id) AS last_snap,
    ROUND(AVG(elapsed_time_delta)/1000000, 2) AS avg_elapsed_sec,
    ROUND(AVG(cpu_time_delta)/1000000, 2) AS avg_cpu_sec,
    SUM(executions_delta) AS total_executions
FROM dba_hist_sqlstat
WHERE sql_text LIKE '%SALES_FACT%'
AND snap_id BETWEEN (SELECT MIN(snap_id)-100 FROM dba_hist_snapshot)
                AND (SELECT MAX(snap_id) FROM dba_hist_snapshot)
GROUP BY sql_id, plan_hash_value
HAVING SUM(executions_delta) > 10
ORDER BY avg_elapsed_sec DESC;
```

### Tuning Recommendations

**If compression ratio is low (<3x):**
1. Check data distribution - highly random data compresses poorly
2. Try different compression type (ARCHIVE HIGH vs QUERY HIGH)
3. Verify current compression isn't already applied
4. Check VARCHAR2 vs CHAR usage (CHAR doesn't compress well)

**If execution is slow:**
1. Increase parallel degree
2. Schedule during off-peak hours
3. Check temp tablespace size
4. Reduce batch size (max_operations parameter)

**If queries are slower after compression:**
1. Use less aggressive compression (QUERY HIGH instead of ARCHIVE HIGH)
2. Only compress cool/cold data
3. Verify statistics are current
4. Check for plan changes due to compression

**If failures are frequent:**
1. Check tablespace free space
2. Review error messages in execution log
3. Validate policies before enabling
4. Test on single partition first

---

## Summary

### Key Takeaways

1. **Start Conservative** - Begin with less aggressive policies and tune based on results
2. **Test Thoroughly** - Always test policies in non-production before deploying
3. **Monitor Continuously** - Track space savings, performance, and failures
4. **Document Everything** - Maintain clear documentation of rationale and changes
5. **Iterate and Improve** - ILM is not set-and-forget; continuously tune based on results

### Success Criteria Checklist

Before considering your ILM policies production-ready:

- [ ] Business requirements documented
- [ ] Lifecycle stages defined with specific thresholds
- [ ] Policies designed and validated
- [ ] Non-production testing completed successfully
- [ ] Query performance regression testing passed
- [ ] Compression ratios meet expectations (>3x for most tables)
- [ ] Rollback procedures documented and tested
- [ ] Monitoring dashboards configured
- [ ] Email notifications enabled and tested
- [ ] Operations team trained on procedures
- [ ] Phased deployment plan created
- [ ] First week daily monitoring scheduled

### Next Steps

1. **Review this guide** with your team
2. **Identify candidate tables** for ILM policies
3. **Define lifecycle stages** specific to your business
4. **Design policy set** following the 7-step methodology
5. **Test in non-production** environment
6. **Deploy in phases** to production
7. **Monitor and tune** continuously

---

## References

- [Oracle Database VLDB and Partitioning Guide](https://docs.oracle.com/en/database/)
- [Oracle Advanced Compression Documentation](https://docs.oracle.com/en/database/)
- [Custom ILM Framework Guide](custom_ilm_guide.md)
- [Operations Runbook](operations/OPERATIONS_RUNBOOK.md)
- [Table Migration Guide](table_migration_guide.md)

---

**Document Version:** 1.0
**Last Updated:** 2025-10-23
**Authors:** Data Warehouse Team
**Status:** Production Ready
