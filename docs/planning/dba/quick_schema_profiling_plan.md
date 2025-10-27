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
CREATE TABLE cmr.dwh_tables_quick_profile (
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
    stats_age_days NUMBER,
    profile_date DATE DEFAULT SYSDATE,
    CONSTRAINT dwh_tables_quick_profile_pk PRIMARY KEY (owner, table_name)
);

INSERT INTO cmr.dwh_tables_quick_profile
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
CREATE TABLE cmr.dwh_schema_tablespaces (
    owner VARCHAR2(128),
    tablespace_name VARCHAR2(128),
    tablespace_size_gb NUMBER,
    tablespace_used_gb NUMBER,
    tablespace_free_gb NUMBER,
    pct_used NUMBER,
    block_size NUMBER,
    extent_management VARCHAR2(30),
    datafile_count NUMBER,
    profile_date DATE DEFAULT SYSDATE,
    CONSTRAINT dwh_schema_tablespaces_pk PRIMARY KEY (owner, tablespace_name)
);

INSERT INTO cmr.dwh_schema_tablespaces
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
CREATE TABLE cmr.dwh_schema_profile (
    owner VARCHAR2(128) PRIMARY KEY,
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
    estimated_savings_pct NUMBER,

    -- Scoring columns (added by scoring procedure)
    storage_impact_score NUMBER,
    migration_readiness_score NUMBER,
    business_value_score NUMBER,
    schema_priority_score NUMBER,
    schema_category VARCHAR2(30),
    schema_type VARCHAR2(30),

    -- Metadata
    profile_date DATE DEFAULT SYSDATE
);

INSERT INTO cmr.dwh_schema_profile
SELECT
    p.owner,

    -- Size metrics
    ROUND(SUM(p.size_gb), 2) AS total_size_gb,
    COUNT(*) AS table_count,
    SUM(CASE WHEN p.size_gb > 50 THEN 1 ELSE 0 END) AS large_table_count,
    ROUND(AVG(p.size_gb), 2) AS avg_table_size_gb,

    -- Tablespace metrics
    (SELECT COUNT(DISTINCT tablespace_name)
     FROM cmr.dwh_schema_tablespaces
     WHERE owner = p.owner) AS tablespace_count,
    (SELECT ROUND(SUM(tablespace_size_gb), 2)
     FROM cmr.dwh_schema_tablespaces
     WHERE owner = p.owner) AS total_tablespace_size_gb,
    (SELECT ROUND(AVG(pct_used), 1)
     FROM cmr.dwh_schema_tablespaces
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
    ) / NULLIF(SUM(p.size_gb), 0)) * 100, 1) AS estimated_savings_pct,

    -- Scoring columns (NULL initially, populated by scoring procedure)
    NULL, NULL, NULL, NULL, NULL, NULL,

    -- Profile date
    SYSDATE
FROM cmr.dwh_tables_quick_profile p
GROUP BY p.owner
ORDER BY SUM(p.size_gb) DESC;

COMMIT;
```

**Expected Duration:** 10-30 seconds

---

## 4. Schema Scoring Implementation

### 4.1 Calculate Schema-Level Scores

```sql
-- Calculate Storage Impact Score (0-100)
UPDATE cmr.dwh_schema_profile SET storage_impact_score = ROUND(
    -- Total size component (60 points)
    (total_size_gb / (SELECT MAX(total_size_gb) FROM cmr.dwh_schema_profile) * 60) +
    -- Estimated savings component (40 points)
    (estimated_compression_savings_gb / (SELECT MAX(estimated_compression_savings_gb) FROM cmr.dwh_schema_profile) * 40)
, 1);

-- Calculate Migration Readiness Score (0-100)
UPDATE cmr.dwh_schema_profile SET migration_readiness_score = ROUND(
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
UPDATE cmr.dwh_schema_profile SET business_value_score = ROUND(
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
UPDATE cmr.dwh_schema_profile SET schema_priority_score = ROUND(
    (storage_impact_score * 0.50) +
    (migration_readiness_score * 0.30) +
    (business_value_score * 0.20)
, 1);

-- Assign schema categories
UPDATE cmr.dwh_schema_profile SET schema_category = CASE
    WHEN schema_priority_score >= 80 THEN 'QUICK_WIN_SCHEMA'
    WHEN schema_priority_score >= 60 THEN 'HIGH_PRIORITY'
    WHEN schema_priority_score >= 40 THEN 'MEDIUM_PRIORITY'
    ELSE 'LOW_PRIORITY'
END;

-- Assign schema types
UPDATE cmr.dwh_schema_profile SET schema_type = CASE
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
CREATE OR REPLACE VIEW cmr.v_dwh_ilm_schema_ranking AS
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
    schema_type,
    profile_date
FROM cmr.dwh_schema_profile
ORDER BY schema_priority_score DESC;
```

---

## 5. Schema Ranking Reports

### 5.1 Executive Summary - Database-Wide

```sql
-- Overall profiling summary
SELECT
    COUNT(DISTINCT owner) AS total_schemas_analyzed,
    ROUND(SUM(total_size_gb), 1) AS total_database_size_gb,
    ROUND(SUM(estimated_compression_savings_gb), 1) AS total_potential_savings_gb,
    ROUND(AVG(estimated_savings_pct), 1) AS avg_savings_pct,
    SUM(CASE WHEN schema_category = 'QUICK_WIN_SCHEMA' THEN 1 ELSE 0 END) AS quick_win_schemas,
    SUM(CASE WHEN schema_category = 'HIGH_PRIORITY' THEN 1 ELSE 0 END) AS high_priority_schemas,
    SUM(table_count) AS total_tables,
    SUM(tablespace_count) AS total_tablespaces,
    SUM(basicfile_lob_table_count) AS total_basicfile_lob_tables
FROM cmr.dwh_schema_profile;
```

**Sample Output:**
```
TOTAL_SCHEMAS | DB_SIZE_GB | POTENTIAL_SAVINGS_GB | AVG_SAVINGS_PCT | QUICK_WIN | HIGH_PRIORITY | TOTAL_TABLES | TOTAL_TABLESPACES | BASICFILE_LOB_TABLES
--------------|------------|---------------------|-----------------|-----------|---------------|--------------|-------------------|---------------------
45            | 118,456.3  | 47,382.5            | 40.0            | 3         | 8             | 12,847       | 156               | 234
```

### 5.2 Top Schema Candidates Report

```sql
-- Top 10 schemas ranked by priority
SELECT
    DENSE_RANK() OVER (ORDER BY schema_priority_score DESC) AS rank,
    owner,
    ROUND(total_size_gb, 1) AS size_gb,
    tablespace_count AS ts_count,
    table_count AS tables,
    ROUND(estimated_compression_savings_gb, 1) AS savings_gb,
    ROUND(estimated_savings_pct, 1) AS savings_pct,
    basicfile_lob_table_count AS basicfile_tables,
    ROUND(pct_partition_ready, 0) AS partition_ready_pct,
    ROUND(schema_priority_score, 0) AS priority,
    schema_category AS category,
    schema_type AS type
FROM cmr.dwh_schema_profile
ORDER BY schema_priority_score DESC
FETCH FIRST 10 ROWS ONLY;
```

**Sample Output:**
```
RANK  OWNER        SIZE_GB   TS_CNT  TABLES  SAVINGS_GB  SAVINGS%  BASICFILE  READY%  PRIORITY  CATEGORY           TYPE
----  -----------  --------  ------  ------  ----------  --------  ---------  ------  --------  -----------------  ----------------------
1     DWH_PROD     28,456.2  3       2,847   11,382.5    40        78         85      92        QUICK_WIN_SCHEMA   TYPE_A_LARGE_SIMPLE
2     APP_SALES    15,234.8  2       1,456   6,093.9     40        45         78      88        QUICK_WIN_SCHEMA   TYPE_C_MEDIUM_MATURE
3     CRM_DATA     12,678.5  4       1,023   5,071.4     40        34         72      85        QUICK_WIN_SCHEMA   TYPE_C_MEDIUM_MATURE
4     ETL_STAGING  9,456.3   3       856     3,782.5     40        67         90      78        HIGH_PRIORITY      TYPE_B_BASICFILE_HEAVY
5     ANALYTICS    8,234.1   5       645     3,293.6     40        12         65      72        HIGH_PRIORITY      TYPE_C_MEDIUM_MATURE
```

### 5.3 Tablespace Breakdown per Schema

```sql
-- Detailed tablespace information for top schemas
SELECT
    st.owner,
    st.tablespace_name,
    ROUND(st.tablespace_size_gb, 1) AS size_gb,
    ROUND(st.tablespace_used_gb, 1) AS used_gb,
    ROUND(st.pct_used, 1) AS pct_used,
    st.extent_management AS extent_mgmt,
    st.datafile_count AS files,
    sp.schema_category
FROM cmr.dwh_schema_tablespaces st
JOIN cmr.dwh_schema_profile sp ON st.owner = sp.owner
WHERE sp.schema_category IN ('QUICK_WIN_SCHEMA', 'HIGH_PRIORITY')
ORDER BY sp.schema_priority_score DESC, st.tablespace_size_gb DESC;
```

**Sample Output:**
```
OWNER        TABLESPACE_NAME      SIZE_GB  USED_GB  PCT_USED  EXTENT_MGMT    FILES  CATEGORY
-----------  -------------------  -------  -------  --------  -------------  -----  ----------------
DWH_PROD     DWH_PROD_DATA        15,234   14,890   97.7      AUTOALLOCATE   8      QUICK_WIN_SCHEMA
DWH_PROD     DWH_PROD_INDEXES     8,456    8,012    94.7      AUTOALLOCATE   5      QUICK_WIN_SCHEMA
DWH_PROD     DWH_PROD_LOBS        4,766    4,480    94.0      AUTOALLOCATE   3      QUICK_WIN_SCHEMA
APP_SALES    APP_SALES_DATA       12,234   11,890   97.2      AUTOALLOCATE   6      QUICK_WIN_SCHEMA
APP_SALES    APP_SALES_INDEXES    3,001    2,870    95.6      AUTOALLOCATE   2      QUICK_WIN_SCHEMA
```

### 5.4 Schema Type Distribution

```sql
-- Distribution of schemas by type
SELECT
    schema_type,
    COUNT(*) AS schema_count,
    ROUND(SUM(total_size_gb), 1) AS total_size_gb,
    ROUND(SUM(estimated_compression_savings_gb), 1) AS total_savings_gb,
    ROUND(AVG(schema_priority_score), 1) AS avg_priority
FROM cmr.dwh_schema_profile
GROUP BY schema_type
ORDER BY AVG(schema_priority_score) DESC;
```

**Sample Output:**
```
SCHEMA_TYPE              SCHEMA_COUNT  TOTAL_SIZE_GB  TOTAL_SAVINGS_GB  AVG_PRIORITY
-----------------------  ------------  -------------  ----------------  ------------
TYPE_A_LARGE_SIMPLE      2             42,690.5       17,076.2          90.0
TYPE_B_BASICFILE_HEAVY   3             18,456.3       7,382.5           82.5
TYPE_C_MEDIUM_MATURE     12            56,234.8       22,493.9          75.2
TYPE_D_COMPLEX_LARGE     8             82,345.6       32,938.2          58.3
TYPE_E_SMALL_LOW_VALUE   20            15,234.2       4,570.3           35.8
```

---

## 6. Tablespace Consolidation Strategy

### 6.1 Current State (Pre-ILM)

**Typical Schema Tablespace Pattern:**
```
DWH_PROD Schema:
├── DWH_PROD_DATA (15 TB)
├── DWH_PROD_INDEXES (8 TB)
└── DWH_PROD_LOBS (5 TB)
Total: 28 TB across 3 tablespaces
```

### 6.2 Target State (Post-ILM)

**ILM Tablespace Architecture:**
```
DWH_PROD Schema:
├── DWH_PROD_HOT (Recent data, compressed)
│   ├── DATA (current year)
│   ├── INDEXES
│   └── LOBS (SECUREFILE, compressed)
│
├── DWH_PROD_WARM (1-3 years, higher compression)
│   ├── DATA (partitioned by month/quarter)
│   ├── INDEXES (local partitioned)
│   └── LOBS (SECUREFILE, compressed + deduplicated)
│
└── DWH_PROD_COLD (>3 years, highest compression)
    ├── DATA (partitioned by year, HCC if available)
    └── LOBS (SECUREFILE, compressed + deduplicated)

Estimated Post-ILM: 11.5 TB (59% reduction)
```

### 6.3 Migration Approach per Schema

**Phase 1: Create ILM Tablespace Set**
```sql
-- Example for DWH_PROD schema
CREATE TABLESPACE DWH_PROD_HOT
    DATAFILE SIZE 1G AUTOEXTEND ON NEXT 1G MAXSIZE 10G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

CREATE TABLESPACE DWH_PROD_WARM
    DATAFILE SIZE 1G AUTOEXTEND ON NEXT 1G MAXSIZE 20G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;

CREATE TABLESPACE DWH_PROD_COLD
    DATAFILE SIZE 1G AUTOEXTEND ON NEXT 1G MAXSIZE 50G
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE
    SEGMENT SPACE MANAGEMENT AUTO;
```

**Phase 2: Migrate Schema Tables**
- Partition tables by date column
- Move hot data → DWH_PROD_HOT
- Move warm data → DWH_PROD_WARM
- Move cold data → DWH_PROD_COLD
- Convert BASICFILE → SECUREFILE LOBs
- Apply appropriate compression

**Phase 3: Decommission Old Tablespaces**
- Verify all data migrated
- Drop old tablespaces (DWH_PROD_DATA, DWH_PROD_INDEXES, DWH_PROD_LOBS)
- Reclaim storage

### 6.4 Space Reduction Example

| Schema | Current Size | Current TS Count | Post-ILM Size | Post-ILM TS Count | Savings | Savings % |
|--------|-------------|------------------|---------------|-------------------|---------|-----------|
| DWH_PROD | 28.5 TB | 3 | 11.4 TB | 3 (HOT/WARM/COLD) | 17.1 TB | 60% |
| APP_SALES | 15.2 TB | 2 | 6.1 TB | 3 (HOT/WARM/COLD) | 9.1 TB | 60% |
| CRM_DATA | 12.7 TB | 4 | 5.1 TB | 3 (HOT/WARM/COLD) | 7.6 TB | 60% |
| **Total Top 3** | **56.4 TB** | **9** | **22.6 TB** | **9** | **33.8 TB** | **60%** |

---

## 7. Next Steps

### 7.1 Immediate Actions (15-30 minutes)

1. **Execute Quick Profiling:**
   - Run Query 3.1 (setup tracked schemas)
   - Run Query 3.2 (table metadata - 2-5 min)
   - Run Query 3.3 (tablespace analysis - 30-60 sec)
   - Run Query 3.4 (schema aggregation - 10-30 sec)
   - Run Section 4.1 (calculate scores - < 1 min)
   - Run Section 4.2 (generate ranking - < 1 sec)

2. **Review Results:**
   - Export top 5-10 schemas
   - Review tablespace breakdown per schema
   - Review with DBA team
   - Identify any business-critical schemas to exclude

3. **Select Pilot Schema:**
   - Choose top 1-2 schemas (Quick Win category)
   - Verify tablespace consolidation is feasible
   - Proceed to detailed analysis for selected schemas

### 7.2 Create Detailed Analysis Tasks for Top Schemas

```sql
-- Create project for top schema detailed analysis
INSERT INTO cmr.dwh_migration_projects (project_name, description)
VALUES ('ILM_SCHEMA_PILOT_2025', 'Detailed analysis of top schemas from quick profiling');

-- Create tasks for all tables in top 3-5 schemas
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    owner,
    table_name,
    priority
)
SELECT
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_SCHEMA_PILOT_2025'),
    p.owner,
    p.table_name,
    CASE
        WHEN s.schema_category = 'QUICK_WIN_SCHEMA' THEN 'CRITICAL'
        WHEN s.schema_category = 'HIGH_PRIORITY' THEN 'HIGH'
        ELSE 'MEDIUM'
    END
FROM cmr.dwh_tables_quick_profile p
JOIN cmr.dwh_schema_profile s ON p.owner = s.owner
WHERE s.schema_category IN ('QUICK_WIN_SCHEMA', 'HIGH_PRIORITY')  -- Top 5-10 schemas
ORDER BY s.schema_priority_score DESC, p.size_gb DESC;

COMMIT;

-- Verify task count by schema
SELECT
    t.owner,
    COUNT(*) AS table_count,
    s.schema_category
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_schema_profile s ON t.owner = s.owner
WHERE t.project_id = (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_SCHEMA_PILOT_2025')
GROUP BY t.owner, s.schema_category
ORDER BY s.schema_priority_score DESC;
```

### 7.3 Proceed to Detailed Analysis (Per Schema)

**For each selected schema:**
1. Run `pck_dwh_table_migration_analyzer.analyze_table` on all tables in schema
2. This provides per-table:
   - Exact date ranges (min/max dates)
   - Data age distribution estimates
   - Detailed complexity scoring
   - Migration recommendations
   - Storage savings estimates

3. Aggregate results to validate schema-level estimates
4. Design ILM tablespace set (HOT/WARM/COLD)
5. Create schema migration plan

See `database_profiling_and_candidate_ranking_plan.md` for detailed analysis procedures.

---

## 8. Advantages of Schema-Level Approach

### 8.1 Speed
- **15-30 minutes** vs 3-5 hours for full profiling
- Metadata queries only - no table data scanning
- Can run during business hours

### 8.2 Safety
- **VERY LOW** production impact
- No data sampling
- No full table scans
- Quick execution (queries timeout in seconds/minutes)

### 8.3 Administrative Simplicity
- Migrate entire schemas (not cherry-picking individual tables)
- Clear progress tracking: "Schema X complete"
- All dependencies migrate together
- Tablespace consolidation per schema

### 8.4 Measurable Results
- Schema-level space reduction is clearly measurable
- Easy to demonstrate ROI per schema
- Business understands "Schema X saved 15 TB"
- Clear before/after metrics

### 8.5 Iterative
- Quick feedback loop
- Can rerun easily if criteria change
- Easy to adjust scoring weights
- Learn from each schema migration

---

## 9. Limitations and Trade-offs

### What You Don't Get (Without Detailed Analysis)

1. **No exact date ranges** - Can't calculate % data over 3 years old precisely
2. **No data distribution** - Can't assess data skew or hot/cold data patterns
3. **No NULL analysis** - Can't assess partition key quality
4. **No sampling-based estimates** - Compression ratios are estimated (conservative), not measured
5. **Statistics may be stale** - Relies on last ANALYZE

### When You Need Detailed Analysis

- **Before schema migration** - Top 3-5 schemas need detailed table-level analysis
- **For accurate savings estimates** - Sampling provides better compression ratio estimates
- **For complex schemas** - Schemas with many dependencies need thorough investigation
- **For business-critical schemas** - Extra validation before migration

---

## 10. Comparison: Schema-Level vs Table-Level Profiling

| Aspect | Schema-Level (Quick) | Table-Level (Full) |
|--------|---------------------|---------------------|
| **Duration** | 15-30 minutes | 3-5 hours |
| **Production Impact** | VERY LOW | MEDIUM |
| **Focus** | Top 5-10 schemas | Top 100-200 tables |
| **Granularity** | Schema aggregates | Individual tables |
| **Migration Unit** | Entire schema | Cherry-picked tables |
| **Tablespace Strategy** | Consolidation (old → ILM sets) | Table-by-table moves |
| **Progress Tracking** | "Schema X complete" | Track 100s of tables |
| **Tables Analyzed** | All > 1GB | Top 100-200 |
| **Data Access** | Metadata only | Table sampling |
| **Accuracy** | Directional | Precise |
| **Output** | Top 5-10 schemas ranked | Top 100-200 tables analyzed |
| **Use Case** | Schema selection | Table-level planning |
| **Can Run During** | Business hours | Off-peak/weekend |
| **ROI** | Quick identification of big wins | Detailed execution plans |

---

## Document Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-27 | Claude/DBA Team | Initial quick profiling plan created as lightweight alternative to full profiling |
| 2.0 | 2025-10-27 | Claude/DBA Team | **SCHEMA-LEVEL FOCUS**: Changed from table-level to schema-level ranking. Added tablespace analysis (Query 2) and schema aggregation (Query 3). Implemented 3-dimensional schema scoring. Added tablespace consolidation strategy. Updated reports to show schema candidates with tablespace breakdown. |

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
Step 1: Quick Schema Profiling (THIS DOCUMENT)
   ↓ 15-30 minutes
   ↓ Rank all schemas with tablespace analysis
   ↓ Identify top 5-10 schemas for ILM migration
   ↓
Step 2: Review & Schema Selection
   ↓ 1-2 hours
   ↓ DBA team review schema rankings
   ↓ Review tablespace consolidation opportunities
   ↓ Select top 1-2 schemas for pilot (Quick Win category)
   ↓
Step 3: Detailed Schema Analysis
   ↓ 2-4 hours per schema
   ↓ Run analyze_table on all tables in selected schemas
   ↓ Design ILM tablespace set (HOT/WARM/COLD)
   ↓ Create schema migration plan
   ↓
Step 4: Schema Migration Execution
   ↓ Per schema timeline
   ↓ Create new ILM tablespaces
   ↓ Migrate tables to HOT/WARM/COLD tiers
   ↓ Decommission old tablespaces
   ↓ Measure space savings
   ↓
Step 5: Iterate & Scale
   ↓ Apply lessons learned
   ↓ Move to next priority schema
   ↓ Repeat until all target schemas migrated
```

**Key Decision Point:**
After Step 1, you'll know which schema to start with for maximum impact and easiest implementation.
