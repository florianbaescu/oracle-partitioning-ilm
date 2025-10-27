# Quick Schema Profiling for ILM Candidate Identification
## Schema-Level Ranking with Tablespace Analysis (120TB Production Database)

**Document Purpose:** Define a fast, low-impact metadata-only profiling approach to rank schemas for ILM migration, considering tablespace consolidation and space reduction goals.

**Target Audience:** DBA Team, Project Managers
**Document Version:** 2.0
**Created:** 2025-10-27
**Last Updated:** 2025-10-27 - Added schema-level ranking and tablespace analysis

**⚠️ PRODUCTION DATABASE NOTICE:**
This is a **metadata-only** profiling approach - no table data scanning, no sampling, minimal production impact.

**🎯 MIGRATION STRATEGY:**
Migrate entire schemas one at a time to new ILM tablespace sets (HOT/WARM/COLD), reducing overall storage footprint.

---

## Executive Summary

### The Problem with Detailed Profiling
- Full table analysis takes 3-5 hours
- Requires data sampling and analysis
- Higher production impact
- Analyzes tables that may not even be good candidates

### Quick Profiling Solution
- **Duration:** 15-30 minutes total
- **Impact:** VERY LOW - metadata queries only
- **Output:** Ranked list of schemas for ILM migration (with tablespace details)
- **Approach:** Use database dictionary views only (no table data access)

### Schema-First Migration Strategy

**Why Schema-Level Migration:**
1. **Tablespace Consolidation** - Each schema → New ILM tablespace set (HOT/WARM/COLD)
2. **Administrative Simplicity** - Migrate one schema at a time, not cherry-picking tables
3. **Clear Progress Tracking** - "Schema X complete" vs tracking 100s of individual tables
4. **Dependency Management** - All schema objects migrate together
5. **Space Reduction Goal** - Entire schema footprint reduction is measurable

**Two-Phase Strategy:**

**Phase 0: Quick Schema Profiling (THIS DOCUMENT)**
- Fast metadata-only queries (15-30 min)
- Schema-level aggregation and scoring
- Tablespace mapping and analysis
- Identify top 5-10 schemas for ILM migration
- **Then** → Phase 1: Detailed analysis on top schemas only

---

## Table of Contents

1. [Quick Profiling Approach](#1-quick-profiling-approach)
2. [Schema-Level Scoring Model](#2-schema-level-scoring-model)
3. [Metadata Queries](#3-metadata-queries)
4. [Schema Scoring Implementation](#4-schema-scoring-implementation)
5. [Schema Ranking Reports](#5-schema-ranking-reports)
6. [Tablespace Consolidation Strategy](#6-tablespace-consolidation-strategy)
7. [Next Steps](#7-next-steps)

---

## 1. Quick Profiling Approach

### 1.1 What We Can Get from Metadata Only

**From DBA_TABLES:**
- Table size (num_rows, avg_row_len)
- Partitioning status (partitioned Y/N)
- Compression status
- Last analyzed date (statistics freshness)

**From DBA_SEGMENTS:**
- Actual storage size (bytes)
- Tablespace allocation
- Segment type

**From DBA_LOBS:**
- LOB columns present
- **BASICFILE vs SECUREFILE** (critical!)
- LOB sizes
- Compression/deduplication status

**From DBA_TAB_COLUMNS:**
- Presence of DATE/TIMESTAMP columns
- Column count and types

**From DBA_CONSTRAINTS:**
- Foreign key count (complexity indicator)
- Primary key presence

**From DBA_INDEXES:**
- Index count (complexity indicator)

**From DBA_TABLESPACES:**
- Tablespace names per schema
- Tablespace sizes and allocation
- Block size
- Extent management (UNIFORM, AUTOALLOCATE)

**From DBA_DATA_FILES:**
- Datafile sizes per tablespace
- Datafile locations
- Autoextend settings
- Total allocated space vs used space

**From DBA_FREE_SPACE:**
- Free space per tablespace
- Space utilization percentage

### 1.2 What We DON'T Get (Requires Data Scanning)
- ❌ Exact date ranges (min/max dates)
- ❌ % of old data
- ❌ Data distribution patterns
- ❌ NULL percentages
- ❌ Actual row counts for sampling

### 1.3 Why Schema-Level Ranking Is Sufficient

**Good indicators from metadata alone:**

**Schema-Level Metrics:**
1. **Total Schema Size** - Bigger schemas = more storage reduction impact
2. **Tablespace Count** - Fewer tablespaces per schema = easier consolidation
3. **Tablespace Sizes** - Current footprint → Target ILM footprint
4. **Table Count** - Number of objects to migrate

**Table-Level Aggregates:**
5. **BASICFILE LOB Count** - Schemas with many BASICFILE LOBs = high priority
6. **Non-Partitioned Large Tables** - Count per schema
7. **Date Column Availability** - % of tables with partition keys
8. **Average Complexity** - Schema-wide dependency patterns

---

## 2. Schema-Level Scoring Model

### 2.1 Schema-Level Three-Dimensional Scoring

**Dimension 1: Storage Impact (0-100)**
- Total schema size: 60 points (larger = more reduction potential)
- Estimated compression savings: 40 points (based on BASICFILE LOBs, non-partitioned tables)

**Dimension 2: Migration Readiness (0-100)**
- Tablespace simplicity: 30 points (fewer tablespaces = easier consolidation)
- Average table complexity: 30 points (fewer dependencies = faster migration)
- Partitioning readiness: 40 points (% of tables with date columns)

**Dimension 3: Business Value (0-100)**
- Space reduction urgency: 60 points (size / available space ratio)
- BASICFILE migration need: 40 points (count of BASICFILE LOB tables)

**Schema Priority Score:**
```
Schema Priority = (Storage Impact × 0.50) + (Migration Readiness × 0.30) + (Business Value × 0.20)
```

**Range:** 0-100 (higher = better candidate)

### 2.2 Schema Priority Categories

| Score | Category | Action | Timeline |
|-------|----------|--------|----------|
| 80-100 | **QUICK WIN SCHEMA** | Begin ILM migration immediately | Wave 0 (Pilot) |
| 60-79 | **HIGH PRIORITY SCHEMA** | Include in Wave 1 | Quarter 1 |
| 40-59 | **MEDIUM PRIORITY SCHEMA** | Include in Wave 2-3 | Quarter 2-3 |
| 0-39 | **LOW PRIORITY SCHEMA** | Defer to future waves | Quarter 4+ |

### 2.3 Schema Candidate Types (Fast Classification)

| Type | Criteria | Priority | Migration Strategy |
|------|----------|----------|-------------------|
| **Type A: Large Simple Schema** | > 10 TB, few tablespaces (≤3), many non-partitioned tables | HIGH | Full schema to new ILM tablespaces |
| **Type B: BASICFILE Heavy** | > 50% of tables have BASICFILE LOBs | HIGH | LOB migration + partitioning |
| **Type C: Medium Mature Schema** | 1-10 TB, 3-5 tablespaces, good date column coverage | HIGH | Standard ILM migration |
| **Type D: Complex Large Schema** | > 10 TB, many tablespaces (>5), complex dependencies | MEDIUM | Phased migration (table groups) |
| **Type E: Small or Low-Value** | < 1 TB, small savings potential | LOW | Batch migration in later waves |

---

## 3. Metadata Queries

### 3.1 Setup: Tracked Schemas

```sql
-- Create temporary table of tracked schemas
CREATE GLOBAL TEMPORARY TABLE temp_tracked_schemas (
    owner VARCHAR2(128)
) ON COMMIT PRESERVE ROWS;

INSERT INTO temp_tracked_schemas
SELECT DISTINCT owner
FROM LOGS.DWH_PROCESS
WHERE owner IS NOT NULL;

COMMIT;
```

### 3.2 Query 1: Core Table Metadata

```sql
-- Gather all core metadata in one query (FAST - no table data access)
CREATE GLOBAL TEMPORARY TABLE temp_quick_profile (
    owner VARCHAR2(128),
    table_name VARCHAR2(128),
    size_gb NUMBER,
    num_rows NUMBER,
    partitioned VARCHAR2(3),
    compression VARCHAR2(30),
    tablespace_name VARCHAR2(128),
    num_indexes NUMBER,
    num_fk_constraints NUMBER,
    has_pk VARCHAR2(3),
    has_date_columns VARCHAR2(3),
    date_column_count NUMBER,
    has_lobs VARCHAR2(3),
    lob_column_count NUMBER,
    lob_type VARCHAR2(30),  -- NONE, SECUREFILE, BASICFILE, MIXED
    basicfile_lob_count NUMBER,
    lob_total_size_gb NUMBER,
    last_analyzed DATE,
    stats_age_days NUMBER
) ON COMMIT PRESERVE ROWS;

INSERT INTO temp_quick_profile
SELECT
    t.owner,
    t.table_name,
    ROUND(NVL(s.bytes, 0) / 1024 / 1024 / 1024, 2) AS size_gb,
    t.num_rows,
    t.partitioned,
    t.compression,
    t.tablespace_name,
    NVL(idx.index_count, 0) AS num_indexes,
    NVL(fk.fk_count, 0) AS num_fk_constraints,
    CASE WHEN pk.constraint_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS has_pk,
    CASE WHEN dc.date_column_count > 0 THEN 'YES' ELSE 'NO' END AS has_date_columns,
    NVL(dc.date_column_count, 0) AS date_column_count,
    CASE WHEN lob.lob_column_count > 0 THEN 'YES' ELSE 'NO' END AS has_lobs,
    NVL(lob.lob_column_count, 0) AS lob_column_count,
    NVL(lob.lob_type, 'NONE') AS lob_type,
    NVL(lob.basicfile_count, 0) AS basicfile_lob_count,
    ROUND(NVL(lob.lob_total_size, 0) / 1024 / 1024 / 1024, 2) AS lob_total_size_gb,
    t.last_analyzed,
    ROUND(SYSDATE - NVL(t.last_analyzed, SYSDATE - 365)) AS stats_age_days
FROM dba_tables t
JOIN temp_tracked_schemas ts ON t.owner = ts.owner
LEFT JOIN dba_segments s
    ON t.owner = s.owner
    AND t.table_name = s.segment_name
    AND s.segment_type IN ('TABLE', 'TABLE PARTITION')
-- Index count
LEFT JOIN (
    SELECT owner, table_name, COUNT(*) AS index_count
    FROM dba_indexes
    GROUP BY owner, table_name
) idx ON t.owner = idx.owner AND t.table_name = idx.table_name
-- FK constraints
LEFT JOIN (
    SELECT owner, table_name, COUNT(*) AS fk_count
    FROM dba_constraints
    WHERE constraint_type = 'R'
    GROUP BY owner, table_name
) fk ON t.owner = fk.owner AND t.table_name = fk.table_name
-- Primary key
LEFT JOIN (
    SELECT owner, table_name, constraint_name
    FROM dba_constraints
    WHERE constraint_type = 'P'
) pk ON t.owner = pk.owner AND t.table_name = pk.table_name
-- Date columns
LEFT JOIN (
    SELECT owner, table_name, COUNT(*) AS date_column_count
    FROM dba_tab_columns
    WHERE data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)', 'TIMESTAMP(9)')
    GROUP BY owner, table_name
) dc ON t.owner = dc.owner AND t.table_name = dc.table_name
-- LOB analysis
LEFT JOIN (
    SELECT
        l.owner,
        l.table_name,
        COUNT(*) AS lob_column_count,
        SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) AS basicfile_count,
        SUM(CASE WHEN l.securefile = 'YES' THEN 1 ELSE 0 END) AS securefile_count,
        CASE
            WHEN SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) > 0
                 AND SUM(CASE WHEN l.securefile = 'YES' THEN 1 ELSE 0 END) > 0
            THEN 'MIXED'
            WHEN SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) > 0
            THEN 'BASICFILE'
            ELSE 'SECUREFILE'
        END AS lob_type,
        SUM(NVL(s.bytes, 0)) AS lob_total_size
    FROM dba_lobs l
    LEFT JOIN dba_segments s
        ON l.owner = s.owner
        AND l.segment_name = s.segment_name
    GROUP BY l.owner, l.table_name
) lob ON t.owner = lob.owner AND t.table_name = lob.table_name
WHERE t.temporary = 'N'
  AND t.nested = 'NO'
  AND NVL(s.bytes, 0) > 1024 * 1024 * 1024  -- > 1 GB
ORDER BY s.bytes DESC;

COMMIT;
```

**Expected Duration:** 2-5 minutes for 120TB database

### 3.3 Query 2: Tablespace Analysis per Schema

```sql
-- Gather tablespace information for each schema
CREATE GLOBAL TEMPORARY TABLE temp_schema_tablespaces (
    owner VARCHAR2(128),
    tablespace_name VARCHAR2(128),
    tablespace_size_gb NUMBER,
    tablespace_used_gb NUMBER,
    tablespace_free_gb NUMBER,
    pct_used NUMBER,
    block_size NUMBER,
    extent_management VARCHAR2(30),
    datafile_count NUMBER
) ON COMMIT PRESERVE ROWS;

INSERT INTO temp_schema_tablespaces
SELECT
    ts_owner.owner,
    ts.tablespace_name,
    ROUND(SUM(df.bytes) / 1024 / 1024 / 1024, 2) AS tablespace_size_gb,
    ROUND((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / 1024 / 1024 / 1024, 2) AS tablespace_used_gb,
    ROUND(NVL(SUM(fs.bytes), 0) / 1024 / 1024 / 1024, 2) AS tablespace_free_gb,
    ROUND(((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / NULLIF(SUM(df.bytes), 0)) * 100, 1) AS pct_used,
    ts.block_size,
    ts.extent_management,
    COUNT(DISTINCT df.file_id) AS datafile_count
FROM (
    -- Get unique tablespaces per tracked schema
    SELECT DISTINCT t.owner, t.tablespace_name
    FROM dba_tables t
    JOIN temp_tracked_schemas ts ON t.owner = ts.owner
    WHERE t.tablespace_name IS NOT NULL
) ts_owner
JOIN dba_tablespaces ts ON ts_owner.tablespace_name = ts.tablespace_name
LEFT JOIN dba_data_files df ON ts.tablespace_name = df.tablespace_name
LEFT JOIN (
    SELECT tablespace_name, SUM(bytes) AS bytes
    FROM dba_free_space
    GROUP BY tablespace_name
) fs ON ts.tablespace_name = fs.tablespace_name
GROUP BY ts_owner.owner, ts.tablespace_name, ts.block_size, ts.extent_management
ORDER BY ts_owner.owner, tablespace_size_gb DESC;

COMMIT;
```

**Expected Duration:** 30-60 seconds

### 3.4 Query 3: Schema-Level Aggregation

```sql
-- Aggregate table-level metrics to schema level
CREATE GLOBAL TEMPORARY TABLE temp_schema_profile (
    owner VARCHAR2(128),
    -- Size metrics
    total_size_gb NUMBER,
    table_count NUMBER,
    large_table_count NUMBER, -- > 50 GB
    avg_table_size_gb NUMBER,

    -- Tablespace metrics
    tablespace_count NUMBER,
    total_tablespace_size_gb NUMBER,
    avg_tablespace_used_pct NUMBER,

    -- Partitioning metrics
    partitioned_table_count NUMBER,
    non_partitioned_table_count NUMBER,
    pct_partitioned NUMBER,
    tables_with_date_columns NUMBER,
    pct_partition_ready NUMBER,

    -- LOB metrics
    lob_table_count NUMBER,
    basicfile_lob_table_count NUMBER,
    securefile_lob_table_count NUMBER,
    total_basicfile_lob_columns NUMBER,
    pct_tables_with_basicfile NUMBER,

    -- Complexity metrics
    avg_indexes_per_table NUMBER,
    avg_fk_per_table NUMBER,
    tables_with_many_dependencies NUMBER, -- > 5 indexes or > 3 FKs

    -- Estimated savings
    estimated_compression_savings_gb NUMBER,
    estimated_savings_pct NUMBER
) ON COMMIT PRESERVE ROWS;

INSERT INTO temp_schema_profile
SELECT
    p.owner,

    -- Size metrics
    ROUND(SUM(p.size_gb), 2) AS total_size_gb,
    COUNT(*) AS table_count,
    SUM(CASE WHEN p.size_gb > 50 THEN 1 ELSE 0 END) AS large_table_count,
    ROUND(AVG(p.size_gb), 2) AS avg_table_size_gb,

    -- Tablespace metrics
    (SELECT COUNT(DISTINCT tablespace_name)
     FROM temp_schema_tablespaces
     WHERE owner = p.owner) AS tablespace_count,
    (SELECT ROUND(SUM(tablespace_size_gb), 2)
     FROM temp_schema_tablespaces
     WHERE owner = p.owner) AS total_tablespace_size_gb,
    (SELECT ROUND(AVG(pct_used), 1)
     FROM temp_schema_tablespaces
     WHERE owner = p.owner) AS avg_tablespace_used_pct,

    -- Partitioning metrics
    SUM(CASE WHEN p.partitioned = 'YES' THEN 1 ELSE 0 END) AS partitioned_table_count,
    SUM(CASE WHEN p.partitioned = 'NO' THEN 1 ELSE 0 END) AS non_partitioned_table_count,
    ROUND((SUM(CASE WHEN p.partitioned = 'YES' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_partitioned,
    SUM(CASE WHEN p.has_date_columns = 'YES' THEN 1 ELSE 0 END) AS tables_with_date_columns,
    ROUND((SUM(CASE WHEN p.has_date_columns = 'YES' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_partition_ready,

    -- LOB metrics
    SUM(CASE WHEN p.has_lobs = 'YES' THEN 1 ELSE 0 END) AS lob_table_count,
    SUM(CASE WHEN p.lob_type = 'BASICFILE' THEN 1 ELSE 0 END) AS basicfile_lob_table_count,
    SUM(CASE WHEN p.lob_type = 'SECUREFILE' THEN 1 ELSE 0 END) AS securefile_lob_table_count,
    SUM(p.basicfile_lob_count) AS total_basicfile_lob_columns,
    ROUND((SUM(CASE WHEN p.lob_type = 'BASICFILE' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_tables_with_basicfile,

    -- Complexity metrics
    ROUND(AVG(p.num_indexes), 1) AS avg_indexes_per_table,
    ROUND(AVG(p.num_fk_constraints), 1) AS avg_fk_per_table,
    SUM(CASE WHEN p.num_indexes > 5 OR p.num_fk_constraints > 3 THEN 1 ELSE 0 END) AS tables_with_many_dependencies,

    -- Estimated savings (conservative: 40% compression on non-partitioned tables with LOBs)
    ROUND(SUM(
        CASE
            WHEN p.partitioned = 'NO' AND p.has_lobs = 'YES' THEN p.size_gb * 0.40
            WHEN p.partitioned = 'NO' THEN p.size_gb * 0.30
            WHEN p.lob_type = 'BASICFILE' THEN p.lob_total_size_gb * 0.50
            ELSE p.size_gb * 0.20
        END
    ), 2) AS estimated_compression_savings_gb,
    ROUND((SUM(
        CASE
            WHEN p.partitioned = 'NO' AND p.has_lobs = 'YES' THEN p.size_gb * 0.40
            WHEN p.partitioned = 'NO' THEN p.size_gb * 0.30
            WHEN p.lob_type = 'BASICFILE' THEN p.lob_total_size_gb * 0.50
            ELSE p.size_gb * 0.20
        END
    ) / NULLIF(SUM(p.size_gb), 0)) * 100, 1) AS estimated_savings_pct
FROM temp_quick_profile p
GROUP BY p.owner
ORDER BY SUM(p.size_gb) DESC;

COMMIT;
```

**Expected Duration:** 10-30 seconds

---

## 4. Schema Scoring Implementation

### 4.1 Calculate Schema-Level Scores

```sql
-- Add score columns to schema profile table
ALTER TABLE temp_schema_profile ADD storage_impact_score NUMBER;
ALTER TABLE temp_schema_profile ADD migration_readiness_score NUMBER;
ALTER TABLE temp_schema_profile ADD business_value_score NUMBER;
ALTER TABLE temp_schema_profile ADD schema_priority_score NUMBER;
ALTER TABLE temp_schema_profile ADD schema_category VARCHAR2(30);
ALTER TABLE temp_schema_profile ADD schema_type VARCHAR2(30);

-- Calculate Storage Impact Score (0-100)
UPDATE temp_schema_profile SET storage_impact_score = ROUND(
    -- Total size component (60 points)
    (total_size_gb / (SELECT MAX(total_size_gb) FROM temp_schema_profile) * 60) +
    -- Estimated savings component (40 points)
    (estimated_compression_savings_gb / (SELECT MAX(estimated_compression_savings_gb) FROM temp_schema_profile) * 40)
, 1);

-- Calculate Migration Readiness Score (0-100)
UPDATE temp_schema_profile SET migration_readiness_score = ROUND(
    -- Tablespace simplicity (30 points - fewer tablespaces = easier)
    CASE
        WHEN tablespace_count <= 2 THEN 30
        WHEN tablespace_count <= 3 THEN 25
        WHEN tablespace_count <= 5 THEN 15
        ELSE 5
    END +
    -- Average complexity (30 points - fewer dependencies = faster)
    CASE
        WHEN avg_indexes_per_table <= 3 AND avg_fk_per_table <= 1 THEN 30
        WHEN avg_indexes_per_table <= 5 AND avg_fk_per_table <= 2 THEN 20
        ELSE 10
    END +
    -- Partitioning readiness (40 points - % tables with date columns)
    (pct_partition_ready * 0.40)
, 1);

-- Calculate Business Value Score (0-100)
UPDATE temp_schema_profile SET business_value_score = ROUND(
    -- Space reduction urgency (60 points - based on savings potential)
    (estimated_savings_pct * 0.60) +
    -- BASICFILE migration need (40 points)
    CASE
        WHEN pct_tables_with_basicfile >= 50 THEN 40
        WHEN pct_tables_with_basicfile >= 30 THEN 30
        WHEN pct_tables_with_basicfile >= 10 THEN 20
        WHEN total_basicfile_lob_columns > 0 THEN 10
        ELSE 0
    END
, 1);

-- Calculate Schema Priority Score (weighted average)
UPDATE temp_schema_profile SET schema_priority_score = ROUND(
    (storage_impact_score * 0.50) +
    (migration_readiness_score * 0.30) +
    (business_value_score * 0.20)
, 1);

-- Assign schema categories
UPDATE temp_schema_profile SET schema_category = CASE
    WHEN schema_priority_score >= 80 THEN 'QUICK_WIN_SCHEMA'
    WHEN schema_priority_score >= 60 THEN 'HIGH_PRIORITY'
    WHEN schema_priority_score >= 40 THEN 'MEDIUM_PRIORITY'
    ELSE 'LOW_PRIORITY'
END;

-- Assign schema types
UPDATE temp_schema_profile SET schema_type = CASE
    WHEN total_size_gb > 10000 AND tablespace_count <= 3 THEN 'TYPE_A_LARGE_SIMPLE'
    WHEN pct_tables_with_basicfile >= 50 THEN 'TYPE_B_BASICFILE_HEAVY'
    WHEN total_size_gb BETWEEN 1000 AND 10000 AND tablespace_count BETWEEN 3 AND 5 THEN 'TYPE_C_MEDIUM_MATURE'
    WHEN total_size_gb > 10000 AND tablespace_count > 5 THEN 'TYPE_D_COMPLEX_LARGE'
    ELSE 'TYPE_E_SMALL_LOW_VALUE'
END;

COMMIT;
```

### 4.2 Schema Ranking View

```sql
-- Final schema ranking for ILM migration
CREATE OR REPLACE VIEW v_ilm_schema_ranking AS
SELECT
    owner,
    total_size_gb,
    estimated_compression_savings_gb,
    estimated_savings_pct,
    table_count,
    tablespace_count,
    pct_partitioned,
    pct_partition_ready,
    basicfile_lob_table_count,
    total_basicfile_lob_columns,
    storage_impact_score,
    migration_readiness_score,
    business_value_score,
    schema_priority_score,
    schema_category,
    schema_type
FROM temp_schema_profile
ORDER BY schema_priority_score DESC;
```

---

## 5. Sample Output

### 5.1 Executive Summary

```sql
-- Quick summary statistics
SELECT
    COUNT(*) AS total_tables_analyzed,
    ROUND(SUM(size_gb), 1) AS total_size_gb,
    SUM(CASE WHEN priority_score >= 80 THEN 1 ELSE 0 END) AS quick_win_count,
    SUM(CASE WHEN priority_score >= 60 THEN 1 ELSE 0 END) AS high_priority_count,
    SUM(CASE WHEN lob_type = 'BASICFILE' THEN 1 ELSE 0 END) AS basicfile_lob_tables,
    SUM(CASE WHEN partitioned = 'NO' AND size_gb > 50 THEN 1 ELSE 0 END) AS large_nonpart_tables,
    SUM(basicfile_lob_count) AS total_basicfile_lobs
FROM temp_quick_profile;
```

**Sample Output:**
```
TOTAL_TABLES | TOTAL_SIZE_GB | QUICK_WIN | HIGH_PRIORITY | BASICFILE_TABLES | LARGE_NONPART | TOTAL_BASICFILE_LOBS
-------------|---------------|-----------|---------------|------------------|---------------|---------------------
1,247        | 85,234.5      | 12        | 45            | 78               | 34            | 156
```

### 5.2 Top Candidates Report

```sql
-- Top 20 candidates
SELECT
    DENSE_RANK() OVER (ORDER BY priority_score DESC) AS rank,
    owner,
    table_name,
    size_gb,
    partitioned AS part,
    lob_type,
    ROUND(impact_score, 0) AS impact,
    ROUND(ease_score, 0) AS ease,
    ROUND(priority_score, 0) AS priority,
    candidate_type
FROM temp_quick_profile
ORDER BY priority_score DESC
FETCH FIRST 20 ROWS ONLY;
```

**Sample Output:**
```
RANK  OWNER      TABLE_NAME           SIZE_GB  PART  LOB_TYPE    IMPACT  EASE  PRIORITY  TYPE
----  ---------  ------------------   -------  ----  ----------  ------  ----  --------  --------------------
1     DWH_PROD   FACT_SALES_HIST      450.2    NO    BASICFILE   98      85    93        TYPE_B_BASICFILE_LOB
2     DWH_PROD   FACT_ORDERS          380.5    NO    NONE        82      80    81        TYPE_A_LARGE_NONPART
3     APP_SALES  SALES_TRANSACTIONS   280.1    NO    SECUREFILE  76      85    80        TYPE_A_LARGE_NONPART
4     CRM_DATA   CUSTOMER_EVENTS      220.3    NO    MIXED       73      70    72        TYPE_C_LARGE_SIMPLE
```

### 5.3 Candidate Type Distribution

```sql
-- Count by candidate type
SELECT
    candidate_type,
    COUNT(*) AS table_count,
    ROUND(SUM(size_gb), 1) AS total_size_gb,
    ROUND(AVG(priority_score), 1) AS avg_priority
FROM temp_quick_profile
WHERE priority_score >= 40
GROUP BY candidate_type
ORDER BY AVG(priority_score) DESC;
```

---

## 6. Next Steps

### 6.1 Immediate Actions (15-30 minutes)

1. **Execute Quick Profiling:**
   - Run Query 3.1 (setup tracked schemas)
   - Run Query 3.2 (core metadata - 2-5 min)
   - Run Query 4.1 (calculate scores - < 1 min)
   - Run Query 4.2 (generate ranking - < 1 sec)

2. **Review Results:**
   - Export top 50-100 candidates
   - Review with DBA team
   - Identify any obvious exclusions (critical tables, etc.)

3. **Select for Detailed Analysis:**
   - Choose top 50-100 candidates (priority_score >= 40)
   - Create tasks in `cmr.dwh_migration_tasks`
   - Proceed to detailed analysis using `pck_dwh_table_migration_analyzer`

### 6.2 Create Detailed Analysis Tasks

```sql
-- Create migration project for detailed analysis
INSERT INTO cmr.dwh_migration_projects (project_name, description)
VALUES ('ILM_DETAILED_ANALYSIS_2025', 'Detailed analysis of top ILM candidates from quick profiling');

-- Create tasks for top candidates
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    owner,
    table_name,
    priority
)
SELECT
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_DETAILED_ANALYSIS_2025'),
    owner,
    table_name,
    CASE
        WHEN priority_score >= 80 THEN 'CRITICAL'
        WHEN priority_score >= 60 THEN 'HIGH'
        ELSE 'MEDIUM'
    END
FROM temp_quick_profile
WHERE priority_score >= 40  -- Top 50-100 candidates only
ORDER BY priority_score DESC;

COMMIT;

-- Verify task count
SELECT COUNT(*) AS tasks_created
FROM cmr.dwh_migration_tasks
WHERE project_id = (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_DETAILED_ANALYSIS_2025');
```

### 6.3 Proceed to Detailed Analysis

**Only for the top 50-100 candidates:**
- Run `pck_dwh_table_migration_analyzer.analyze_table` on each task
- This will provide:
  - Exact date ranges (min/max dates)
  - Data age distribution estimates
  - Detailed complexity scoring
  - Migration recommendations
  - Storage savings estimates

See `database_profiling_and_candidate_ranking_plan.md` Section 2.2 for detailed analysis procedures.

---

## 7. Advantages of This Approach

### 7.1 Speed
- **15-30 minutes** vs 3-5 hours for full profiling
- Metadata queries only - no table data scanning
- Can run during business hours

### 7.2 Safety
- **VERY LOW** production impact
- No data sampling
- No full table scans
- Quick execution (queries timeout in seconds/minutes)

### 7.3 Efficiency
- Focus detailed analysis on top candidates only
- Avoid wasting time analyzing tables that won't be migrated
- 80/20 rule: Find 80% of value with 20% of effort

### 7.4 Iterative
- Quick feedback loop
- Can rerun easily if criteria change
- Easy to adjust scoring weights

---

## 8. Limitations and Trade-offs

### What You Don't Get (Without Detailed Analysis)

1. **No exact date ranges** - Can't calculate % data over 3 years old
2. **No data distribution** - Can't assess data skew or hot/cold data patterns
3. **No NULL analysis** - Can't assess partition key quality
4. **No sampling-based estimates** - Compression ratios are estimated, not measured
5. **Statistics may be stale** - Relies on last ANALYZE

### When You Need Detailed Analysis

- **Before final migration planning** - Top 50 candidates should get detailed analysis
- **For accurate savings estimates** - Sampling provides better compression ratio estimates
- **For complex tables** - Tables with unusual data patterns need investigation
- **For business-critical tables** - Extra validation before migration

---

## 9. Comparison: Quick vs Full Profiling

| Aspect | Quick Profiling | Full Profiling |
|--------|----------------|----------------|
| **Duration** | 15-30 minutes | 3-5 hours |
| **Production Impact** | VERY LOW | MEDIUM |
| **Tables Analyzed** | All > 1GB | Top 100-200 |
| **Data Access** | Metadata only | Table sampling |
| **Accuracy** | Directional | Precise |
| **Output** | Top 50-100 candidates | Detailed migration plans |
| **Use Case** | Initial screening | Final candidate selection |
| **Can Run During** | Business hours | Off-peak/weekend |

---

## Document Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-27 | Claude/DBA Team | Initial quick profiling plan created as lightweight alternative to full profiling |

---

**Document Approval:**

| Role | Name | Signature | Date |
|------|------|-----------|------|
| DBA Lead | __________ | __________ | _____ |
| Project Manager | __________ | __________ | _____ |

---

**References:**
- Full Profiling Plan: `database_profiling_and_candidate_ranking_plan.md`
- Oracle Database Reference: DBA Views Documentation

---

**Recommended Workflow:**

```
Step 1: Quick Profiling (THIS DOCUMENT)
   ↓ 15-30 minutes
   ↓ Identify top 50-100 candidates
   ↓
Step 2: Review & Validate
   ↓ 1-2 hours
   ↓ DBA team review, exclude critical tables
   ↓
Step 3: Detailed Analysis (Full Profiling Plan)
   ↓ 2-4 hours (top candidates only)
   ↓ Run analyze_table on selected candidates
   ↓
Step 4: Final Ranking & Wave Planning
   ↓ 1-2 days
   ↓ Create migration roadmap
```
