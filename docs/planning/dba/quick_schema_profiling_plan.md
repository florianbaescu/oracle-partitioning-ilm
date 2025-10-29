# Quick Schema Profiling for ILM Candidate Identification
## Schema-Level Ranking with Tablespace Analysis (120TB Production Database)

**Document Purpose:** Define a fast, low-impact metadata-only profiling approach to rank schemas for ILM migration, considering tablespace consolidation and space reduction goals.

**Target Audience:** DBA Team, Project Managers
**Document Version:** 3.0
**Created:** 2025-10-27
**Last Updated:** 2025-10-28 - Converted to process description (implementation in schema_profiling_setup.sql)

**⚠️ PRODUCTION DATABASE NOTICE:**
This is a **metadata-only** profiling approach - no table data scanning, no sampling, minimal production impact.

**🎯 MIGRATION STRATEGY:**
Migrate entire schemas one at a time to new ILM tablespace sets (HOT/WARM/COLD), reducing overall storage footprint.

**📄 IMPLEMENTATION:**
See `scripts/schema_profiling_setup.sql` for complete implementation including:
- Table definitions (cmr.dwh_tables_quick_profile, cmr.dwh_schema_tablespaces, cmr.dwh_schema_profile)
- Package: cmr.pck_dwh_schema_profiler
- View: cmr.v_dwh_ilm_schema_ranking

---

## Executive Summary

### The Problem with Detailed Profiling
- Full table analysis takes 3-5 hours
- Requires data sampling and analysis
- Higher production impact
- Analyzes tables that may not even be good candidates

### Quick Profiling Solution
- **Duration:** 5-10 minutes total (with performance optimizations)
- **Impact:** VERY LOW - metadata queries only
- **Output:** Ranked list of schemas for ILM migration (with tablespace details)
- **Approach:** Use database dictionary views only (no table data access)
- **Execution:** Single procedure call: `pck_dwh_schema_profiler.run_profiling()`

### Schema-First Migration Strategy

**Why Schema-Level Migration:**
1. **Tablespace Consolidation** - Each schema → New ILM tablespace set (HOT/WARM/COLD)
2. **Administrative Simplicity** - Migrate one schema at a time, not cherry-picking tables
3. **Clear Progress Tracking** - "Schema X complete" vs tracking 100s of individual tables
4. **Dependency Management** - All schema objects migrate together
5. **Space Reduction Goal** - Entire schema footprint reduction is measurable

**Two-Phase Strategy:**

**Phase 0: Quick Schema Profiling (THIS DOCUMENT)**
- Fast metadata-only queries (5-10 min with parallel execution)
- Schema-level aggregation and scoring
- Tablespace mapping and analysis
- Identify top 5-10 schemas for ILM migration
- **Then** → Phase 1: Detailed analysis on top schemas only

---

## Table of Contents

1. [Quick Profiling Approach](#1-quick-profiling-approach)
2. [Schema-Level Scoring Model](#2-schema-level-scoring-model)
3. [Profiling Workflow](#3-profiling-workflow)
4. [Tablespace Consolidation Strategy](#4-tablespace-consolidation-strategy)
5. [Implementation Details](#5-implementation-details)
6. [Usage Examples](#6-usage-examples)
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
- Actual storage size (bytes) - aggregated for partitioned tables
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
- Datafile sizes per tablespace (aggregated)
- Datafile count
- Total allocated space vs used space

**From DBA_FREE_SPACE:**
- Free space per tablespace (aggregated)
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

### 2.1 Three-Dimensional Scoring

**Dimension 1: Storage Impact (0-100 points)**
- Total schema size: **60 points** (larger = more reduction potential)
- Estimated compression savings: **40 points** (based on BASICFILE LOBs, non-partitioned tables)

**Dimension 2: Migration Readiness (0-100 points)**
- Tablespace simplicity: **30 points** (fewer tablespaces = easier consolidation)
  - ≤2 tablespaces: 30 points
  - 3 tablespaces: 25 points
  - 4-5 tablespaces: 15 points
  - >5 tablespaces: 5 points
- Average table complexity: **30 points** (fewer dependencies = faster migration)
  - ≤3 indexes AND ≤1 FK: 30 points
  - ≤5 indexes AND ≤2 FKs: 20 points
  - More complex: 10 points
- Partitioning readiness: **40 points** (% of tables with date columns)

**Dimension 3: Business Value (0-100 points)**
- Space reduction urgency: **60 points** (size / available space ratio)
- BASICFILE migration need: **40 points** (count of BASICFILE LOB tables)
  - ≥50% tables with BASICFILE: 40 points
  - ≥30%: 30 points
  - ≥10%: 20 points
  - >0: 10 points

**Schema Priority Score Formula:**
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

## 3. Profiling Workflow

### 3.1 High-Level Process

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Profile Tables (profile_tables)                    │
│ - Query dba_tables for all tracked schemas                 │
│ - Aggregate dba_segments (handles partitioned tables)       │
│ - Join dba_indexes, dba_constraints, dba_tab_columns       │
│ - Aggregate dba_lobs (BASICFILE vs SECUREFILE)             │
│ - Store in: cmr.dwh_tables_quick_profile                   │
│ Duration: 2-4 minutes (with PARALLEL 4)                    │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Profile Tablespaces (profile_tablespaces)          │
│ - Query unique tablespaces per schema                      │
│ - Aggregate dba_data_files per tablespace                  │
│ - Aggregate dba_free_space per tablespace                  │
│ - Calculate usage percentages                              │
│ - Store in: cmr.dwh_schema_tablespaces                     │
│ Duration: 20-30 seconds (with PARALLEL 2)                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Aggregate to Schema Level (aggregate_to_schema_level)│
│ - GROUP BY owner to aggregate table metrics                │
│ - Calculate size totals, averages, percentages             │
│ - Join tablespace metrics                                  │
│ - Estimate compression savings                             │
│ - Store in: cmr.dwh_schema_profile                         │
│ Duration: 10-15 seconds (with PARALLEL 2)                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Calculate Scores (calculate_scores)                │
│ - Calculate Storage Impact Score (0-100)                   │
│ - Calculate Migration Readiness Score (0-100)              │
│ - Calculate Business Value Score (0-100)                   │
│ - Calculate weighted Schema Priority Score                 │
│ - Assign schema categories (QUICK_WIN, HIGH, MEDIUM, LOW)  │
│ - Assign schema types (TYPE_A through TYPE_E)              │
│ Duration: 10-15 seconds (with PARALLEL 2)                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Output: View Results (v_dwh_ilm_schema_ranking)            │
│ - Schemas ranked by priority score                         │
│ - All metrics visible                                      │
│ - Ready for decision making                                │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Performance Optimizations

**Parallel Execution:**
- DBA_TABLES/DBA_SEGMENTS: PARALLEL(4) - largest scans
- Other DBA views: PARALLEL(2) - balanced parallelism
- All DML operations: ENABLE_PARALLEL_DML hint

**Join Strategy:**
- USE_HASH hints for aggregated subqueries
- NO_MERGE hints to control execution order
- Pre-aggregated subqueries to avoid cartesian products

**Direct-Path Loading:**
- APPEND hint on all INSERT statements
- Bypasses buffer cache for faster loading

**Key Aggregations:**
1. **dba_segments**: Aggregated by owner/segment_name to handle partitioned tables
2. **dba_data_files**: Aggregated by tablespace_name to avoid multiplying free space
3. **dba_free_space**: Aggregated by tablespace_name

### 3.3 Schema Selection Options

**Automatic Selection:**
- Schemas from `LOGS.DWH_PROCESS` table (tracked schemas)

**Manual Selection:**
- Use `p_additional_schemas` parameter
- Comma-separated list: `'HR,SALES,FINANCE'`
- Combined with automatic selection via UNION

---

## 4. Tablespace Consolidation Strategy

### 4.1 Current State (Pre-ILM)

**Typical Schema Tablespace Pattern:**
```
DWH_PROD Schema:
├── DWH_PROD_DATA (15 TB)
├── DWH_PROD_INDEXES (8 TB)
└── DWH_PROD_LOBS (5 TB)
Total: 28 TB across 3 tablespaces
```

### 4.2 Target State (Post-ILM)

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

### 4.3 Migration Approach per Schema

**Phase 1: Create ILM Tablespace Set**
- Create 3 tablespaces per schema (HOT/WARM/COLD)
- Configure appropriate compression levels
- Set extent management and space policies

**Phase 2: Migrate Schema Tables**
- Partition tables by date column
- Move hot data → SCHEMA_HOT tablespace
- Move warm data → SCHEMA_WARM tablespace
- Move cold data → SCHEMA_COLD tablespace
- Convert BASICFILE → SECUREFILE LOBs
- Apply appropriate compression levels

**Phase 3: Decommission Old Tablespaces**
- Verify all data migrated
- Drop old tablespaces (SCHEMA_DATA, SCHEMA_INDEXES, SCHEMA_LOBS)
- Reclaim storage

### 4.4 Space Reduction Example

| Schema | Current Size | Current TS Count | Post-ILM Size | Post-ILM TS Count | Savings | Savings % |
|--------|-------------|------------------|---------------|-------------------|---------|-----------|
| DWH_PROD | 28.5 TB | 3 | 11.4 TB | 3 (HOT/WARM/COLD) | 17.1 TB | 60% |
| APP_SALES | 15.2 TB | 2 | 6.1 TB | 3 (HOT/WARM/COLD) | 9.1 TB | 60% |
| CRM_DATA | 12.7 TB | 4 | 5.1 TB | 3 (HOT/WARM/COLD) | 7.6 TB | 60% |
| **Total Top 3** | **56.4 TB** | **9** | **22.6 TB** | **9** | **33.8 TB** | **60%** |

---

## 5. Implementation Details

### 5.1 Database Objects Created

**Tables:**
1. **cmr.dwh_tables_quick_profile** - Table-level metrics
   - Primary Key: (owner, table_name)
   - Stores: size, partitioning status, LOB info, complexity metrics

2. **cmr.dwh_schema_tablespaces** - Tablespace info per schema
   - Primary Key: (owner, tablespace_name)
   - Stores: size, usage, free space, datafile count

3. **cmr.dwh_schema_profile** - Schema-level aggregates and scores
   - Primary Key: (owner)
   - Stores: aggregated metrics, scores, categories, types

**View:**
- **cmr.v_dwh_ilm_schema_ranking** - Ranked schema list ordered by priority score

**Package:**
- **cmr.pck_dwh_schema_profiler** - All profiling procedures

### 5.2 Package Procedures

| Procedure/Function | Purpose | Parameters |
|-------------------|---------|------------|
| `run_profiling` | Complete profiling workflow | p_min_table_size_gb, p_calculate_scores, p_truncate_before, p_additional_schemas |
| `profile_tables` | Table-level profiling | p_min_table_size_gb, p_additional_schemas |
| `profile_tablespaces` | Tablespace profiling | None |
| `aggregate_to_schema_level` | Schema aggregation | None |
| `calculate_scores` | Scoring and ranking | None |
| `truncate_profiling_tables` | Clear all data | None |
| `generate_migration_tasks` | Generate migration tasks for schema | p_owner, p_project_name, p_min_table_size_gb, p_max_tables, p_use_compression, p_compression_type, p_apply_ilm_policies, p_auto_analyze |

### 5.3 Expected Duration (120TB Database)

| Step | Before Optimization | After Optimization | Improvement |
|------|-------------------|-------------------|-------------|
| Table Profiling | 5 min | 2-4 min | 40-60% faster |
| Tablespace Profiling | 60 sec | 20-30 sec | 50-66% faster |
| Schema Aggregation | 30 sec | 10-15 sec | 50-66% faster |
| Score Calculation | 30 sec | 10-15 sec | 50-66% faster |
| **Total Workflow** | **15-30 min** | **5-10 min** | **66-80% faster** |

---

## 6. Usage Examples

### 6.1 Basic Usage (All Tracked Schemas)

```sql
-- Execute profiling for all schemas in LOGS.DWH_PROCESS
BEGIN
    cmr.pck_dwh_schema_profiler.run_profiling(
        p_min_table_size_gb => 1,
        p_calculate_scores => TRUE
    );
END;
/

-- View ranked results
SELECT * FROM cmr.v_dwh_ilm_schema_ranking;
```

### 6.2 With Additional Schemas

```sql
-- Profile tracked schemas + manually specified schemas
BEGIN
    cmr.pck_dwh_schema_profiler.run_profiling(
        p_min_table_size_gb => 1,
        p_calculate_scores => TRUE,
        p_truncate_before => TRUE,
        p_additional_schemas => 'HR,SALES,FINANCE'
    );
END;
/
```

### 6.3 Step-by-Step Execution

```sql
-- If you need to run steps individually:

-- Step 0: Clear existing data (optional)
BEGIN
    cmr.pck_dwh_schema_profiler.truncate_profiling_tables;
END;
/

-- Step 1: Profile tables
BEGIN
    cmr.pck_dwh_schema_profiler.profile_tables(
        p_min_table_size_gb => 1,
        p_additional_schemas => 'CUSTOM_SCHEMA'
    );
END;
/

-- Step 2: Profile tablespaces
BEGIN
    cmr.pck_dwh_schema_profiler.profile_tablespaces;
END;
/

-- Step 3: Aggregate to schema level
BEGIN
    cmr.pck_dwh_schema_profiler.aggregate_to_schema_level;
END;
/

-- Step 4: Calculate scores
BEGIN
    cmr.pck_dwh_schema_profiler.calculate_scores;
END;
/

-- View results
SELECT * FROM cmr.v_dwh_ilm_schema_ranking;
```

### 6.4 Sample Queries

**Top 10 Schema Candidates:**
```sql
SELECT
    DENSE_RANK() OVER (ORDER BY schema_priority_score DESC) AS rank,
    owner,
    ROUND(total_size_gb, 1) AS size_gb,
    tablespace_count AS ts_count,
    table_count,
    ROUND(estimated_compression_savings_gb, 1) AS savings_gb,
    ROUND(estimated_savings_pct, 1) AS savings_pct,
    ROUND(schema_priority_score, 0) AS priority,
    schema_category,
    schema_type
FROM cmr.dwh_schema_profile
ORDER BY schema_priority_score DESC
FETCH FIRST 10 ROWS ONLY;
```

**Tablespace Breakdown for Top Schemas:**
```sql
SELECT
    st.owner,
    st.tablespace_name,
    ROUND(st.tablespace_size_gb, 1) AS size_gb,
    ROUND(st.pct_used, 1) AS pct_used,
    st.datafile_count AS files,
    sp.schema_category
FROM cmr.dwh_schema_tablespaces st
JOIN cmr.dwh_schema_profile sp ON st.owner = sp.owner
WHERE sp.schema_category IN ('QUICK_WIN_SCHEMA', 'HIGH_PRIORITY')
ORDER BY sp.schema_priority_score DESC, st.tablespace_size_gb DESC;
```

**Schema Type Distribution:**
```sql
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

---

## 7. Next Steps

### 7.1 Immediate Actions (5-10 minutes)

1. **Setup (One-Time):**
   ```sql
   @scripts/schema_profiling_setup.sql
   ```

2. **Execute Profiling:**
   ```sql
   BEGIN
       cmr.pck_dwh_schema_profiler.run_profiling();
   END;
   /
   ```

3. **Review Results:**
   - Export top 5-10 schemas from `v_dwh_ilm_schema_ranking`
   - Review tablespace breakdown
   - Review with DBA team
   - Identify any business-critical schemas to exclude

4. **Select Pilot Schema:**
   - Choose top 1-2 schemas (Quick Win category)
   - Verify tablespace consolidation is feasible
   - Proceed to detailed analysis for selected schemas

### 7.2 Generate Migration Tasks for Top Schemas

Once top schemas are identified, use the automated task generation function:

**Function:** `pck_dwh_schema_profiler.generate_migration_tasks()`

**Purpose:** Automatically creates a migration project and tasks for all tables in a schema from profiling results.

**Parameters:**
- `p_owner` (required): Schema/owner name
- `p_project_name`: Custom project name (default: 'ILM Migration - {OWNER}')
- `p_min_table_size_gb`: Minimum table size in GB (default: 1)
- `p_max_tables`: Maximum number of tables (default: NULL = all tables)
- `p_use_compression`: Enable compression (default: Y)
- `p_compression_type`: Compression type (default: OLTP)
- `p_apply_ilm_policies`: Apply ILM policies (default: Y)
- `p_auto_analyze`: Run analysis after creation (default: TRUE)

**Returns:** project_id

**Example 1: Generate tasks for all tables in top-ranked schema**
```sql
DECLARE
    v_project_id NUMBER;
BEGIN
    v_project_id := cmr.pck_dwh_schema_profiler.generate_migration_tasks(
        p_owner => 'DWH_PROD',
        p_min_table_size_gb => 1,
        p_auto_analyze => TRUE
    );

    DBMS_OUTPUT.PUT_LINE('Project ID: ' || v_project_id);
END;
/
```

**Example 2: Pilot migration - top 50 largest tables only**
```sql
DECLARE
    v_project_id NUMBER;
BEGIN
    v_project_id := cmr.pck_dwh_schema_profiler.generate_migration_tasks(
        p_owner => 'DWH_PROD',
        p_project_name => 'ILM Pilot - Top 50 Tables',
        p_min_table_size_gb => 5,
        p_max_tables => 50,
        p_compression_type => 'QUERY HIGH',
        p_auto_analyze => TRUE
    );
END;
/
```

**Example 3: Manual analysis (review tasks before analyzing)**
```sql
DECLARE
    v_project_id NUMBER;
BEGIN
    -- Generate tasks without auto-analysis
    v_project_id := cmr.pck_dwh_schema_profiler.generate_migration_tasks(
        p_owner => 'SALES_DWH',
        p_max_tables => 20,
        p_auto_analyze => FALSE
    );

    -- Review generated tasks
    SELECT task_name, source_table
    FROM cmr.dwh_migration_tasks
    WHERE project_id = v_project_id;

    -- Run analysis manually after review
    cmr.pck_dwh_table_migration_analyzer.analyze_all_pending_tasks(v_project_id);
END;
/
```

**What This Function Does:**
1. Validates schema exists in profiling results
2. Creates migration project in `cmr.dwh_migration_projects`
3. Creates tasks for all (or top X) tables from `cmr.dwh_tables_quick_profile`
4. Orders tasks by size (largest tables first)
5. Sets defaults: CTAS method, compression, ILM policies
6. Optionally runs detailed analysis on all tasks
7. Returns project_id for tracking

**Next Steps After Task Generation:**
1. Review analysis results:
   ```sql
   SELECT t.task_name, t.validation_status,
          a.recommended_partition_type, a.recommended_partition_key
   FROM cmr.dwh_migration_tasks t
   JOIN cmr.dwh_migration_analysis a ON t.task_id = a.task_id
   WHERE t.project_id = {project_id};
   ```
2. Design ILM tablespace set (HOT/WARM/COLD) per schema
3. Create schema migration execution plan
4. Begin migrations using `pck_dwh_table_migration_executor`

### 7.3 Proceed to Detailed Analysis (Per Schema)

**For each selected schema:**
1. Run detailed analysis on all tables
2. Get exact date ranges (min/max dates per table)
3. Calculate data age distribution
4. Detailed complexity scoring
5. Migration recommendations per table
6. Accurate storage savings estimates

See `database_profiling_and_candidate_ranking_plan.md` for detailed analysis procedures.

---

## 8. Advantages of Schema-Level Approach

### 8.1 Speed
- **5-10 minutes** vs 3-5 hours for full profiling
- Metadata queries only - no table data scanning
- Can run during business hours
- Parallel execution for optimal performance

### 8.2 Safety
- **VERY LOW** production impact
- No data sampling
- No full table scans
- Quick execution (timeouts in seconds/minutes)

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
| **Duration** | 5-10 minutes | 3-5 hours |
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

**References:**
- Implementation: `scripts/schema_profiling_setup.sql`
- Full Profiling Plan: `database_profiling_and_candidate_ranking_plan.md`
- Oracle Database Reference: DBA Views Documentation

---

**Recommended Workflow:**

```
Step 1: Quick Schema Profiling (THIS DOCUMENT)
   ↓ 5-10 minutes
   ↓ @scripts/schema_profiling_setup.sql (one-time setup)
   ↓ pck_dwh_schema_profiler.run_profiling() (execution)
   ↓ Rank all schemas with tablespace analysis
   ↓ Identify top 5-10 schemas for ILM migration
   ↓
Step 2: Review & Schema Selection
   ↓ 1-2 hours
   ↓ DBA team review schema rankings
   ↓ Review tablespace consolidation opportunities
   ↓ Select top 1-2 schemas for pilot (Quick Win category)
   ↓
Step 3: Generate Migration Tasks & Analyze
   ↓ 10-30 minutes per schema
   ↓ Use pck_dwh_schema_profiler.generate_migration_tasks(p_owner => 'SCHEMA_NAME')
   ↓ Function creates project and tasks for all tables in schema
   ↓ Auto-analyzes all tasks (or manual if p_auto_analyze => FALSE)
   ↓ Review recommendations (partition types, keys, compression)
   ↓
Step 4: Design ILM Architecture
   ↓ 2-4 hours per schema
   ↓ Design ILM tablespace set (HOT/WARM/COLD)
   ↓ Create schema migration execution plan
   ↓ Review analysis results and adjust strategies
   ↓
Step 5: Schema Migration Execution
   ↓ Per schema timeline
   ↓ Create new ILM tablespaces
   ↓ Migrate tables to HOT/WARM/COLD tiers
   ↓ Decommission old tablespaces
   ↓ Measure space savings
   ↓
Step 6: Iterate & Scale
   ↓ Apply lessons learned
   ↓ Move to next priority schema
   ↓ Repeat until all target schemas migrated
```

**Key Decision Point:**
After Step 1, you'll know which schema to start with for maximum impact and easiest implementation.
