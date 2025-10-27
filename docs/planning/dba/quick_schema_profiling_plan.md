# Quick Schema Profiling for ILM Candidate Identification
## Lightweight Metadata-Only Assessment (120TB Production Database)

**Document Purpose:** Define a fast, low-impact metadata-only profiling approach to quickly identify top ILM candidates without table data scanning or detailed analysis.

**Target Audience:** DBA Team, Project Managers
**Document Version:** 1.0
**Created:** 2025-10-27

**⚠️ PRODUCTION DATABASE NOTICE:**
This is a **metadata-only** profiling approach - no table data scanning, no sampling, minimal production impact.

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
- **Output:** Ranked list of top 50-100 candidates for detailed analysis
- **Approach:** Use database dictionary views only (no table data access)

### Two-Phase Strategy

**Phase 0: Quick Profiling (THIS DOCUMENT)**
- Fast metadata-only queries (15-30 min)
- Simple scoring algorithm
- Identify top 50-100 candidates
- **Then** → Phase 1: Detailed analysis on top candidates only

---

## Table of Contents

1. [Quick Profiling Approach](#1-quick-profiling-approach)
2. [Lightweight Scoring Model](#2-lightweight-scoring-model)
3. [Metadata Queries](#3-metadata-queries)
4. [Quick Scoring Implementation](#4-quick-scoring-implementation)
5. [Sample Output](#5-sample-output)
6. [Next Steps](#6-next-steps)

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

### 1.2 What We DON'T Get (Requires Data Scanning)
- ❌ Exact date ranges (min/max dates)
- ❌ % of old data
- ❌ Data distribution patterns
- ❌ NULL percentages
- ❌ Actual row counts for sampling

### 1.3 Why This Is Sufficient for Initial Ranking

**Good indicators from metadata alone:**
1. **Size** - Bigger tables = more impact
2. **BASICFILE LOBs** - Immediate migration need
3. **Non-partitioned large tables** - Clear partition candidates
4. **Has date columns** - Partition potential
5. **Low complexity** - Fewer dependencies = easier migration

---

## 2. Lightweight Scoring Model

### 2.1 Simplified Two-Dimensional Scoring

**Dimension 1: Impact Potential (0-100)**
- Table size: 70 points (bigger = better)
- LOB migration opportunity: 30 points (BASICFILE = high score)

**Dimension 2: Migration Ease (0-100)**
- NOT partitioned: +40 points (easier than repartitioning)
- Few indexes: +20 points (< 5 indexes)
- Few constraints: +20 points (< 3 FKs)
- Has date columns: +20 points (partition key available)

**Quick Priority Score:**
```
Priority = (Impact × 0.60) + (Ease × 0.40)
```

### 2.2 Priority Categories

| Score | Category | Action |
|-------|----------|--------|
| 80-100 | **QUICK WIN** | Detailed analysis immediately |
| 60-79 | **HIGH PRIORITY** | Include in detailed analysis |
| 40-59 | **MEDIUM** | Consider for detailed analysis |
| 0-39 | **DEFER** | Skip detailed analysis for now |

### 2.3 Candidate Types (Fast Classification)

| Type | Criteria | Priority |
|------|----------|----------|
| **Type A: Large Non-Partitioned** | > 50 GB, not partitioned, has date column | HIGH |
| **Type B: BASICFILE LOBs** | Any size, has BASICFILE LOBs | HIGH |
| **Type C: Large Simple Tables** | > 100 GB, few dependencies | HIGH |
| **Type D: Medium Tables** | 20-50 GB, not partitioned | MEDIUM |
| **Type E: Small or Complex** | < 20 GB or many dependencies | LOW |

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

---

## 4. Quick Scoring Implementation

### 4.1 Calculate Scores

```sql
-- Add score columns to temp table
ALTER TABLE temp_quick_profile ADD impact_score NUMBER;
ALTER TABLE temp_quick_profile ADD ease_score NUMBER;
ALTER TABLE temp_quick_profile ADD priority_score NUMBER;
ALTER TABLE temp_quick_profile ADD candidate_type VARCHAR2(30);

-- Calculate Impact Score (0-100)
UPDATE temp_quick_profile SET impact_score = ROUND(
    -- Size component (70 points)
    (size_gb / (SELECT MAX(size_gb) FROM temp_quick_profile) * 70) +
    -- LOB migration opportunity (30 points)
    CASE
        WHEN lob_type = 'BASICFILE' THEN 30
        WHEN lob_type = 'MIXED' THEN 20
        ELSE 0
    END
, 1);

-- Calculate Ease Score (0-100)
UPDATE temp_quick_profile SET ease_score = ROUND(
    -- Not partitioned (40 points - easier to partition from scratch)
    CASE WHEN partitioned = 'NO' THEN 40 ELSE 0 END +
    -- Few indexes (20 points)
    CASE
        WHEN num_indexes = 0 THEN 20
        WHEN num_indexes <= 3 THEN 15
        WHEN num_indexes <= 5 THEN 10
        ELSE 0
    END +
    -- Few FK constraints (20 points)
    CASE
        WHEN num_fk_constraints = 0 THEN 20
        WHEN num_fk_constraints <= 2 THEN 15
        ELSE 5
    END +
    -- Has date columns (20 points - partition key available)
    CASE WHEN has_date_columns = 'YES' THEN 20 ELSE 0 END
, 1);

-- Calculate Priority Score
UPDATE temp_quick_profile SET priority_score = ROUND(
    (impact_score * 0.60) + (ease_score * 0.40)
, 1);

-- Assign candidate types
UPDATE temp_quick_profile SET candidate_type = CASE
    WHEN size_gb > 50 AND partitioned = 'NO' AND has_date_columns = 'YES' THEN 'TYPE_A_LARGE_NONPART'
    WHEN lob_type = 'BASICFILE' THEN 'TYPE_B_BASICFILE_LOB'
    WHEN size_gb > 100 AND num_indexes <= 5 AND num_fk_constraints <= 2 THEN 'TYPE_C_LARGE_SIMPLE'
    WHEN size_gb BETWEEN 20 AND 50 AND partitioned = 'NO' THEN 'TYPE_D_MEDIUM'
    ELSE 'TYPE_E_LOW_PRIORITY'
END;

COMMIT;
```

### 4.2 Final Ranking View

```sql
-- Top candidates for detailed analysis
SELECT
    owner,
    table_name,
    size_gb,
    partitioned,
    lob_type,
    basicfile_lob_count,
    has_date_columns,
    date_column_count,
    num_indexes,
    num_fk_constraints,
    impact_score,
    ease_score,
    priority_score,
    candidate_type,
    CASE
        WHEN priority_score >= 80 THEN 'QUICK_WIN'
        WHEN priority_score >= 60 THEN 'HIGH'
        WHEN priority_score >= 40 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS priority_category
FROM temp_quick_profile
WHERE priority_score >= 40  -- Focus on MEDIUM and above
ORDER BY priority_score DESC
FETCH FIRST 100 ROWS ONLY;
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
