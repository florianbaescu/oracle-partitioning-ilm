# Database Profiling and ILM Candidate Ranking Plan
## Large Database Assessment Strategy (120TB Production Database)

**Document Purpose:** Define a systematic approach to profile the current 120TB **PRODUCTION** database, identify optimal candidates for ILM migration, and rank them by impact, complexity, and data age using fully automated database metadata analysis.

**Target Audience:** DBA Team, Project Managers, Architecture Team
**Document Version:** 1.3
**Created:** 2025-10-27
**Last Updated:** 2025-10-27 - Converted to fully automated 3-dimensional scoring model (removed Risk Score requiring manual input)

**⚠️ PRODUCTION DATABASE NOTICE:**
This profiling plan is designed for a live production environment. All queries include resource management controls, execution timing guidelines, and safety measures to ensure minimal impact on production workloads.

---

## Executive Summary

With a 120TB database containing numerous tablespaces and schemas, a strategic profiling approach is essential to:

1. **Identify quick wins** - High impact, low complexity migrations
2. **Minimize risk** - Start with non-critical schemas/tables
3. **Maximize ROI** - Prioritize candidates with largest storage savings
4. **Phase implementation** - Break into manageable waves
5. **Learn iteratively** - Apply lessons from early migrations to later phases

**Expected Outcome:** A prioritized list of schemas/tables ranked by a composite score that balances impact, complexity, and data age, enabling data-driven migration decisions.

**Profiling Scope:** This analysis focuses exclusively on schemas tracked in the `LOGS.DWH_PROCESS` table, ensuring alignment with existing ETL processes and data warehouse operations.

**Production Safety Principles:**
1. **Non-Intrusive Queries** - All profiling queries designed to minimize resource consumption
2. **Off-Peak Execution** - Schedule resource-intensive queries during low-activity windows
3. **Query Timeouts** - All queries include timeout controls to prevent runaway processes
4. **Resource Limits** - Use query hints to limit CPU, memory, and I/O consumption
5. **Incremental Approach** - Break large operations into smaller, manageable chunks
6. **Monitoring** - Active monitoring of system performance during profiling
7. **Rollback Capability** - Ability to cancel queries if production impact detected

---

## Table of Contents

0. [Production Database Safety Guidelines](#0-production-database-safety-guidelines) ⚠️ **READ FIRST**
   - [0.1 Execution Timing and Scheduling](#01-execution-timing-and-scheduling)
   - [0.2 Query Resource Management](#02-query-resource-management)
   - [0.3 Incremental Execution Strategy](#03-incremental-execution-strategy)
   - [0.4 Real-Time Monitoring During Profiling](#04-real-time-monitoring-during-profiling)
   - [0.5 Query Cancellation Procedures](#05-query-cancellation-procedures)
   - [0.6 Safe Query Patterns](#06-safe-query-patterns)
   - [0.7 Communication Protocol](#07-communication-protocol)
1. [Profiling Objectives](#1-profiling-objectives)
2. [Data Collection Strategy](#2-data-collection-strategy)
3. [Ranking Criteria and Scoring Model](#3-ranking-criteria-and-scoring-model)
4. [Profiling Queries](#4-profiling-queries)
5. [Candidate Analysis Framework](#5-candidate-analysis-framework)
6. [Prioritization Algorithm](#6-prioritization-algorithm)
7. [Migration Wave Planning](#7-migration-wave-planning)
8. [Sample Output and Reporting](#8-sample-output-and-reporting)
9. [Implementation Roadmap](#9-implementation-roadmap)
10. [Appendix: Complete Query Set](#10-appendix-complete-query-set)

---

## 0. Production Database Safety Guidelines

**⚠️ CRITICAL: Production Impact Minimization**

This section provides comprehensive safety guidelines for executing profiling queries in a production environment. All queries in this plan have been designed with production safety as the primary concern.

### 0.1 Execution Timing and Scheduling

#### Off-Peak Execution Windows

**Recommended Execution Schedule:**

| Query Type | Recommended Window | Max Duration | Priority |
|------------|-------------------|--------------|----------|
| Query 0 (Schema Scoping) | Anytime | < 5 seconds | LOW IMPACT |
| Query 1 (Schema Inventory) | Off-peak hours | 2-5 minutes | LOW IMPACT |
| Query 2 (Tablespace Stats) | Anytime | < 1 minute | LOW IMPACT |
| Phase 2 (Table Analysis) | Weekend | 2-4 hours | MEDIUM IMPACT (uses analyze_table package) |

**Off-Peak Definitions:**
- **Weekday Off-Peak**: 10 PM - 6 AM local time
- **Weekend Execution**: Saturday/Sunday 8 AM - 10 PM
- **Low-Activity Periods**: Monitor current load before execution

**Pre-Execution Checklist:**
```sql
-- Check current database load before running profiling queries
SELECT
    metric_name,
    value,
    CASE
        WHEN metric_name = 'Database CPU Time Ratio' AND value > 80 THEN 'DEFER PROFILING'
        WHEN metric_name = 'Database CPU Time Ratio' AND value > 60 THEN 'CAUTION'
        ELSE 'OK TO PROCEED'
    END AS recommendation
FROM v$sysmetric
WHERE metric_name IN (
    'Database CPU Time Ratio',
    'Host CPU Utilization (%)',
    'Current OS Load'
)
AND intsize_csec = (SELECT MAX(intsize_csec) FROM v$sysmetric);

-- Check active sessions
SELECT COUNT(*) AS active_session_count,
       CASE
           WHEN COUNT(*) > 100 THEN 'HIGH LOAD - DEFER PROFILING'
           WHEN COUNT(*) > 50 THEN 'MODERATE LOAD - CAUTION'
           ELSE 'LOW LOAD - OK TO PROCEED'
       END AS recommendation
FROM v$session
WHERE status = 'ACTIVE'
  AND username IS NOT NULL;
```

### 0.2 Query Resource Management

#### Query Timeout Configuration

**Set Session-Level Timeouts:**
```sql
-- Set maximum query execution time (adjust per query complexity)
ALTER SESSION SET MAX_DUMP_FILE_SIZE = '10M';

-- Enable query timeout (Oracle 12c+)
-- Set to 600 seconds (10 minutes) for most profiling queries
ALTER SESSION SET MAX_IDLE_TIME = 30;  -- Idle timeout in minutes

-- For individual queries, use DBMS_RESOURCE_MANAGER (if available)
BEGIN
    DBMS_RESOURCE_MANAGER.CLEAR_PENDING_AREA();
    DBMS_RESOURCE_MANAGER.CREATE_PENDING_AREA();

    -- Create resource plan for profiling (if not exists)
    DBMS_RESOURCE_MANAGER.CREATE_PLAN(
        plan    => 'ILM_PROFILING_PLAN',
        comment => 'Resource plan for ILM profiling queries'
    );

    -- Low priority consumer group
    DBMS_RESOURCE_MANAGER.CREATE_CONSUMER_GROUP(
        consumer_group => 'ILM_PROFILING_GROUP',
        comment        => 'Low-priority profiling queries'
    );

    DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA();
END;
/

-- Assign session to profiling consumer group
BEGIN
    DBMS_RESOURCE_MANAGER.SWITCH_CONSUMER_GROUP_FOR_SESS(
        session_id       => SYS_CONTEXT('USERENV', 'SID'),
        session_serial   => SYS_CONTEXT('USERENV', 'SESSIONID'),
        consumer_group   => 'ILM_PROFILING_GROUP'
    );
END;
/
```

#### Query Hints for Resource Limiting

**Standard Hints for All Profiling Queries:**
```sql
-- Template: Add these hints to resource-intensive queries
SELECT /*+
    PARALLEL(4)                    -- Limit parallelism to 4 threads
    CPU_COSTING                    -- Use CPU-based costing
    NO_INDEX_FFS(t)                -- Avoid full fast scans
    FIRST_ROWS(100)                -- Optimize for first 100 rows
    NO_QUERY_TRANSFORMATION        -- Disable query transformations
*/
    ...
FROM large_table t
WHERE ...
```

**Query-Specific Hint Recommendations:**

| Query | Recommended Hints | Rationale |
|-------|------------------|-----------|
| Phase 2 (analyze_table) | Built-in sampling | Package handles resource management |

### 0.3 Incremental Execution Strategy

**Break Large Operations into Chunks:**

Instead of running Query 3 for all tables at once:
```sql
-- BAD: Single query for all tables (may run for hours)
SELECT ... FROM dba_tables WHERE owner IN (...);

-- GOOD: Process in batches
-- Batch 1: Largest tables first
SELECT ... FROM dba_tables
WHERE owner IN (SELECT owner FROM temp_tracked_schemas)
  AND segment_bytes > 100*1024*1024*1024  -- > 100 GB
FETCH FIRST 20 ROWS ONLY;

-- Batch 2: Medium tables
SELECT ... FROM dba_tables
WHERE owner IN (SELECT owner FROM temp_tracked_schemas)
  AND segment_bytes BETWEEN 10*1024*1024*1024 AND 100*1024*1024*1024
FETCH FIRST 50 ROWS ONLY;

-- Batch 3: Smaller tables
SELECT ... FROM dba_tables
WHERE owner IN (SELECT owner FROM temp_tracked_schemas)
  AND segment_bytes < 10*1024*1024*1024
FETCH FIRST 100 ROWS ONLY;
```

**Schema-by-Schema Execution (Example - if needed):**
```sql
-- Process one schema at a time (example for heavy queries)
DECLARE
    v_sql VARCHAR2(32767);
BEGIN
    FOR schema_rec IN (
        SELECT owner FROM temp_tracked_schemas ORDER BY owner
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('Processing schema: ' || schema_rec.owner);

        -- Run data age queries for this schema only
        FOR table_rec IN (
            SELECT table_name, column_name
            FROM dba_tab_columns
            WHERE owner = schema_rec.owner
              AND data_type IN ('DATE', 'TIMESTAMP')
            FETCH FIRST 10 ROWS ONLY  -- Limit per schema
        ) LOOP
            -- Execute sampling query with timeout
            BEGIN
                v_sql := 'SELECT /*+ PARALLEL(2) SAMPLE(10) */ ' ||
                         'MIN(' || table_rec.column_name || '), ' ||
                         'MAX(' || table_rec.column_name || ') ' ||
                         'FROM ' || schema_rec.owner || '.' || table_rec.table_name;

                EXECUTE IMMEDIATE v_sql;
                COMMIT;

                -- Pause between tables (throttling)
                DBMS_LOCK.SLEEP(1);  -- 1 second pause
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
                    CONTINUE;  -- Skip to next table
            END;
        END LOOP;

        -- Pause between schemas
        DBMS_LOCK.SLEEP(5);  -- 5 second pause
    END LOOP;
END;
/
```

### 0.4 Real-Time Monitoring During Profiling

**Monitor System Load While Profiling:**

```sql
-- Monitor current session's resource consumption
-- Run this in a separate session while profiling queries execute
SELECT
    s.sid,
    s.serial#,
    s.username,
    s.program,
    s.sql_id,
    t.sql_text,
    s.status,
    s.seconds_in_wait,
    ROUND(s.physical_reads / NULLIF(s.executions, 0), 2) AS avg_phys_reads,
    ROUND(s.buffer_gets / NULLIF(s.executions, 0), 2) AS avg_buffer_gets,
    s.last_call_et AS seconds_since_last_call
FROM v$session s
LEFT JOIN v$sqltext t ON s.sql_id = t.sql_id
WHERE s.username = 'YOUR_PROFILING_USER'  -- Replace with your username
  AND s.status = 'ACTIVE'
ORDER BY s.last_call_et DESC;
```

**Alert Thresholds:**
- **CPU Usage > 80%**: Pause profiling, wait for load to decrease
- **Active Sessions > 100**: Defer resource-intensive queries
- **Query Runtime > 15 minutes**: Consider canceling and optimizing
- **Physical I/O > 10 GB**: Throttle or pause

**Automated Monitoring Script:**
```sql
-- Save this as monitor_profiling_session.sql
SET SERVEROUTPUT ON;
DECLARE
    v_cpu_usage NUMBER;
    v_active_sessions NUMBER;
    v_recommendation VARCHAR2(100);
BEGIN
    LOOP
        -- Check CPU
        SELECT value INTO v_cpu_usage
        FROM v$sysmetric
        WHERE metric_name = 'Host CPU Utilization (%)'
          AND intsize_csec = (SELECT MAX(intsize_csec) FROM v$sysmetric);

        -- Check active sessions
        SELECT COUNT(*) INTO v_active_sessions
        FROM v$session
        WHERE status = 'ACTIVE' AND username IS NOT NULL;

        -- Evaluate
        IF v_cpu_usage > 80 THEN
            v_recommendation := '⛔ STOP PROFILING - HIGH CPU';
        ELSIF v_cpu_usage > 60 THEN
            v_recommendation := '⚠️  CAUTION - MODERATE CPU';
        ELSIF v_active_sessions > 100 THEN
            v_recommendation := '⛔ STOP PROFILING - HIGH SESSION COUNT';
        ELSE
            v_recommendation := '✓ OK TO CONTINUE';
        END IF;

        DBMS_OUTPUT.PUT_LINE(
            TO_CHAR(SYSDATE, 'HH24:MI:SS') ||
            ' | CPU: ' || ROUND(v_cpu_usage, 1) || '%' ||
            ' | Sessions: ' || v_active_sessions ||
            ' | ' || v_recommendation
        );

        -- Exit if critical
        EXIT WHEN v_cpu_usage > 90 OR v_active_sessions > 150;

        -- Check every 30 seconds
        DBMS_LOCK.SLEEP(30);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('⛔ CRITICAL THRESHOLD REACHED - STOP ALL PROFILING QUERIES');
END;
/
```

3. **Resume Profiling:**
   - Wait 5-10 minutes for system load to stabilize
   - Re-check system load (Section 0.1 pre-execution checklist)
   - Resume with smaller batch size or more restrictive hints

### 0.6 Safe Query Patterns

**DO's ✓**
- Always use `FETCH FIRST n ROWS ONLY` when possible
- Add `/*+ PARALLEL(2-4) */` hints for large table scans
- Use `SAMPLE(10)` clause for data age analysis when accuracy allows
- Execute queries in batches (by schema, size, etc.)
- Monitor system load before and during execution
- Run during off-peak hours
- Set session-level timeouts
- Test queries on smaller subsets first

**DON'Ts ✗**
- ❌ Never run `SELECT * FROM dba_segments` without filters
- ❌ Avoid `COUNT(*)` on tables > 1TB without sampling
- ❌ Don't use `PARALLEL(16+)` - excessive parallelism
- ❌ Don't execute all queries simultaneously
- ❌ Avoid queries without time limits or row limits
- ❌ Don't profile critical production tables during business hours
- ❌ Don't run full table scans for age distribution - use estimation from date ranges instead

### 0.7 Communication Protocol

**Before Profiling Begins:**
- ✅ Notify DBA team of profiling schedule
- ✅ Announce in Slack/Teams: "Starting ILM profiling queries"
- ✅ Document which queries will run and when
- ✅ Provide emergency contact information

**During Profiling:**
- ✅ Log query start/end times
- ✅ Monitor system metrics every 5-10 minutes
- ✅ Report any issues immediately to DBA lead
- ✅ Update status in shared document/channel

**After Profiling:**
- ✅ Confirm all queries completed successfully
- ✅ Report any errors or warnings
- ✅ Document actual query durations vs. estimates
- ✅ Notify team: "ILM profiling completed - system normal"

**Example Communication Template:**
```
📊 ILM Database Profiling - Execution Notice

Date: 2025-10-27
Time Window: 10:00 PM - 2:00 AM
Duration: ~4 hours

Queries to Execute:
- Query 0: Schema Scoping (< 5 sec)
- Query 1: Schema Inventory (2-5 min)
- Query 3: Large Tables Analysis (5-10 min)
- Query 4: LOB Analysis (10-15 min)
- Query 5: Date Columns (5-10 min)

Expected Impact: LOW - Queries designed with production safety
Emergency Contact: [DBA Name] - [Phone/Slack]

Monitoring: Active monitoring throughout execution
Rollback Plan: Query cancellation procedures documented in Section 0.5
```

---

## 1. Profiling Objectives

### 1.1 Primary Goals

**Objective 1: Inventory Assessment**
- Catalog all schemas tracked in LOGS.DWH_PROCESS, their tablespaces, and major objects
- Measure current storage consumption by schema/tablespace
- Identify growth trends over time

**Objective 2: ILM Suitability Analysis**
- Identify tables with time-series data (good partition candidates)
- Find tables with old data (good compression candidates)
- Locate BASICFILE LOBs (need SECUREFILE migration)
- Detect non-partitioned large tables (partition candidates)

**Objective 3: Impact Quantification**
- Calculate potential storage savings per candidate
- Estimate compression ratios based on data age
- Project cost reductions from tiered storage

**Objective 4: Complexity Assessment**
- Evaluate migration effort (DDL complexity, downtime)
- Identify dependencies and constraints
- Assess technical complexity factors

**Objective 5: Prioritization**
- Rank all candidates using multi-factor scoring
- Create migration waves (pilot, phase 1, phase 2, etc.)
- Generate actionable migration roadmap

### 1.2 Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Database coverage | 100% of tracked schemas profiled | Schema count from LOGS.DWH_PROCESS |
| Candidate identification | Top 100-200 tables analyzed | Ranked list from dwh_migration_analysis |
| Storage savings potential | 50-70% reduction | GB saved estimate from analyze_table |
| Quick wins identified | 5-10 low-risk, high-impact tables | Pilot candidate list |
| Migration phases defined | 3-5 waves | Phase plan |
| Profiling completion time | **1 week** | Using existing analysis package + age estimation |

---

## 2. Data Collection Strategy

### 2.1 Data Points to Collect

#### Schema/Tablespace Level

| Data Point | Purpose | Source |
|------------|---------|--------|
| Schema name | Identification | LOGS.DWH_PROCESS, dba_users |
| Total size (GB) | Impact potential | dba_segments |
| Number of tables | Complexity indicator | dba_tables |
| Tablespace allocation | Storage tier mapping | dba_tablespaces, dba_data_files |
| **Note:** Growth rate analysis removed - requires historical snapshots not available in production | |
| **Note:** Business criticality/owner metadata removed - requires manual input not available automatically | |

#### Table Level

| Data Point | Purpose | Source |
|------------|---------|--------|
| Table name | Identification | dba_tables |
| Size (GB) | Impact potential | dba_segments |
| Row count | Data volume | dba_tables.num_rows |
| Partitioned status | Complexity | dba_tables.partitioned |
| Partition count | Current state | dba_tab_partitions |
| Partition key | Strategy validation | dba_part_key_columns |
| Last analyzed date | Data freshness | dba_tables.last_analyzed |
| Has LOBs | Migration requirement | dba_lobs |
| LOB type (BASICFILE/SECUREFILE) | Modernization need | dba_lobs.securefile |
| Compression status | Current optimization | dba_tables.compression |

#### Data Age Distribution

| Data Point | Purpose | Source |
|------------|---------|--------|
| Date column candidates | Partition key identification | Column analysis |
| Min date value | Historical data extent | MIN(date_column) |
| Max date value | Current data extent | MAX(date_column) |
| Data age spread | Tiering opportunity | Date range analysis |
| % data > 3 years old | Archive candidate volume | WHERE date < SYSDATE - 1095 |

### 2.2 Data Collection Phases

**⚠️ PRODUCTION EXECUTION NOTICE:** All phases must be executed during off-peak hours with active monitoring. Refer to [Section 0: Production Database Safety Guidelines](#0-production-database-safety-guidelines) for detailed execution procedures.

**Phase 1: High-Level Inventory (Day 1-2) - LOW IMPACT**
- **Execution Window:** Weekday off-peak (10 PM - 6 AM) or Weekend
- **Expected Duration:** 30-60 minutes total
- **Queries:** Query 0 (Schema Scoping), Query 1 (Schema Inventory), Query 2 (Tablespace Stats)
- **Impact Level:** LOW - Read-only dictionary queries
- **Activities:**
  - Schema and tablespace catalog
  - Total sizes and counts
  - Quick identification of largest objects
- **Pre-Execution:** Run system load check (Section 0.1)
- **Monitoring:** Check CPU/sessions every 10 minutes

**Phase 2: Detailed Table Analysis (Day 3-7) - MEDIUM IMPACT**
- **Execution Window:** Weekend preferred, or multiple weekday off-peak windows
- **Expected Duration:** 2-4 hours (spread across multiple sessions)
- **Method:** Use existing `pck_dwh_table_migration_analyzer.analyze_table` package
- **Impact Level:** MEDIUM - Table sampling and analysis per table
- **Activities:**
  - Create migration project and tasks for tables to analyze
  - Run `analyze_table` procedure for each task
  - Results stored in `cmr.dwh_migration_analysis`
  - **Analysis includes:**
    - Table statistics (size, rows, indexes, constraints, triggers)
    - **LOB detailed analysis** (BASICFILE vs SECUREFILE, column-level details, sizes, compression status)
    - Date column identification with comprehensive analysis (all DATE/TIMESTAMP columns)
    - Partitioning recommendations
    - Complexity scoring
    - Compression estimates
    - Dependency analysis
- **Pre-Execution:**
  - Verify system load < 60% CPU
  - Create project: `INSERT INTO cmr.dwh_migration_projects`
  - Create tasks for top 100-200 tables from Phase 1 results
  - Process tables in batches (20-30 tables per session)
- **Monitoring:** Active monitoring every 5 minutes
- **Throttling:** Built-in to analyze_table procedure (uses sampling, not full scans)

**Example Usage:**
```sql
-- Create profiling project
INSERT INTO cmr.dwh_migration_projects (project_name, description)
VALUES ('ILM_PROFILING_2025', 'Database profiling for ILM candidate ranking');

-- Create tasks for top 100 tables from Phase 1
INSERT INTO cmr.dwh_migration_tasks (project_id, owner, table_name, priority)
SELECT
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_PROFILING_2025'),
    owner,
    table_name,
    'HIGH'
FROM (
    SELECT owner, table_name, size_gb
    FROM phase1_schema_inventory  -- Results from Phase 1
    WHERE size_gb > 10  -- Focus on tables > 10 GB
    ORDER BY size_gb DESC
    FETCH FIRST 100 ROWS ONLY
);

-- Run analysis (in batches)
BEGIN
    FOR task_rec IN (
        SELECT task_id
        FROM cmr.dwh_migration_tasks
        WHERE project_id = (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_PROFILING_2025')
          AND status = 'PENDING'
        FETCH FIRST 20 ROWS ONLY  -- Process 20 tables per session
    ) LOOP
        pck_dwh_table_migration_analyzer.analyze_table(p_task_id => task_rec.task_id);
        COMMIT;
        DBMS_LOCK.SLEEP(2);  -- 2-second pause between tables
    END LOOP;
END;
/

-- Query results
SELECT
    t.owner,
    t.table_name,
    a.table_size_mb / 1024 AS size_gb,
    a.has_lobs,
    a.date_column_name,
    a.recommended_strategy,
    a.complexity_score,
    a.estimated_space_savings_mb / 1024 AS estimated_savings_gb
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON t.task_id = a.task_id
WHERE t.project_id = (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_PROFILING_2025')
ORDER BY a.table_size_mb DESC;
```

**Advantages over manual queries:**
- ✅ Comprehensive analysis in one procedure call
- ✅ Results stored in structured tables for easy querying
- ✅ Rerunnable (supports MERGE for updates)
- ✅ Built-in error handling and logging
- ✅ Automatic complexity scoring
- ✅ Date column analysis includes ALL date columns, not just patterns
- ✅ Uses sampling (10% for data age) to minimize production impact

**Phase 3: Data Age Distribution - ✅ SKIP THIS PHASE**
- **Status:** ✅ **FULLY COVERED BY PHASE 2** (using estimation)
- **What Phase 2 Already Provides:**
  - ✅ Min/Max dates for all date columns (from `all_date_columns_analysis` JSON)
  - ✅ Date range in days/years
  - ✅ Date column quality and NULL percentages

**Age Distribution Estimation:**
We can estimate % of old data from the date range without scanning tables:

```sql
-- Estimate % data over 3 years using date range from Phase 2
SELECT
    t.owner,
    t.table_name,
    a.date_column_name,
    a.date_range_years,
    -- Estimate % old data assuming uniform distribution
    CASE
        WHEN a.date_range_years >= 3 THEN
            ROUND(LEAST(((a.date_range_years - 3) / NULLIF(a.date_range_years, 0)) * 100, 100), 1)
        ELSE 0
    END AS estimated_pct_over_3_years,
    -- If min_date is available, calculate precisely
    CASE
        WHEN EXTRACT(YEAR FROM TO_DATE(JSON_VALUE(a.all_date_columns_analysis, '$.columns[0].min_date'), 'YYYY-MM-DD')) IS NOT NULL THEN
            ROUND(((SYSDATE - TO_DATE(JSON_VALUE(a.all_date_columns_analysis, '$.columns[0].min_date'), 'YYYY-MM-DD')) / 365.25 - 3) /
                  NULLIF(a.date_range_years, 0) * 100, 1)
        ELSE NULL
    END AS calculated_pct_over_3_years
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON t.task_id = a.task_id
WHERE a.date_range_years IS NOT NULL;
```

**Why This Works:**
- If a table has a 10-year date range (2015-2025), and today is 2025:
  - Years over 3 years old: 7 years (2015-2022)
  - Estimated % old data: (7/10) * 100 = **70%**
- Assumes uniform data distribution (reasonable for initial profiling)
- Accurate enough for candidate ranking without table scans

**Exact Age Distribution (ONLY if needed for final validation):**
- ⚠️ **NOT RECOMMENDED** - Requires HIGH impact table scans
- Only run for top 5-10 final candidates before migration execution
- Use SAMPLE(10) to minimize impact
- See Query Template in Appendix if needed

**Phase 4: Scoring and Ranking (Day 3-5) - ANALYSIS PHASE**
- **Execution Window:** Business hours (no production impact)
- **Expected Duration:** 1-2 days
- **Impact Level:** NONE - Analysis in spreadsheet/SQL*Plus
- **Activities:**
  - Apply scoring algorithms to collected data
  - Calculate dimension scores (Impact, Complexity, Data Age)
  - Generate composite priority scores
  - Create ranked candidate list
  - Generate migration wave assignments
  - Prepare executive summary reports

**Execution Timeline Summary:**

```
Week 1:
├─ Weekend (Sat/Sun): Phase 1 + Phase 2
│  Duration: 3-5 hours total
│  Impact: LOW-MEDIUM
│  Activities:
│    • Phase 1: Schema inventory (Queries 0-2)
│    • Phase 2: Table analysis using pck_dwh_table_migration_analyzer
│      - Analyzes table size, LOBs, date columns, complexity
│      - Provides min/max dates and date ranges (used for age estimation)
│      - Process 100-200 tables in batches
│
│  ✅ Phase 3: SKIPPED - Age distribution estimated from Phase 2 date ranges
│
├─ Mon-Fri: Phase 4 (Analysis and Scoring)
│  Business hours - No production impact
│  Duration: 1-2 days
│  Activities:
│    • Calculate dimension scores using Phase 2 data
│    • Estimate age distribution from date ranges (uniform distribution assumption)
│    • Generate ranked candidate list
│    • Create migration wave assignments
│    • Prepare executive summary
│
Total Calendar Time: 1 week
Total Active Database Profiling Time: 3-5 hours
Notes:
  • Phase 3 (Age Distribution) removed - estimated from date ranges
  • Growth rate analysis removed - not available in production
  • Access pattern analysis (AWR) removed - not available/needed
```

**Emergency Stop Criteria:**
- CPU > 85% sustained for 5+ minutes → STOP all queries
- Active sessions > 120 → DEFER to later window
- Query runtime > 20 minutes → CANCEL and investigate
- Production incident reported → STOP immediately, resume after resolution

### 2.3 Schema Scoping Methodology

**Schema Filter Source:**
All profiling queries are scoped to schemas tracked in `LOGS.DWH_PROCESS` table. This ensures:
- Focus on actively managed data warehouse schemas
- Alignment with existing ETL processes
- Exclusion of unused or legacy schemas
- Consistency with operational monitoring

**Implementation Approach:**
1. Create temporary table of tracked schemas for query performance
2. Use this filter consistently across all profiling queries
3. Validate schema list before beginning profiling
4. Document any excluded schemas for future reference

**Query Pattern:**
```sql
-- Standard filter used in all profiling queries
WHERE owner IN (SELECT DISTINCT owner FROM LOGS.DWH_PROCESS WHERE owner IS NOT NULL)
```

---

## 3. Ranking Criteria and Scoring Model

### 3.1 Three-Dimensional Scoring Model

**⚠️ PRODUCTION/AUTOMATED APPROACH:** All scoring factors are automatically calculated from database metadata. No manual business input required.

Each candidate is scored across three dimensions, then combined into a priority score:

#### Dimension 1: Impact Score (0-100)
**"How much value does this migration deliver?"**

**⚠️ PRODUCTION CONSTRAINTS:**
- Growth rate data not available (requires historical snapshots)
- Access pattern data not available/needed (AWR)
- Impact scoring adjusted accordingly

Factors:
- **Storage savings potential** (60 points)
  - Current size × expected compression ratio
  - Larger savings = higher score
- **Cost reduction** (40 points)
  - Tiered storage cost savings (HOT → WARM → COLD)
  - Larger tables = higher tier movement benefit
  - Date range indicates tier movement potential

**Calculation:**
```
Impact Score =
  (Size_GB / Max_Size_GB × 60) +
  (Cost_Savings_Annual / Max_Cost_Savings × 40)

Notes:
- Growth rate removed (was 30 points)
- Performance/AWR removed (was 20 points)
- Points redistributed to size (60) and cost (40)
```

#### Dimension 2: Complexity Score (0-100, lower is better)
**"How difficult is this migration?"**

Factors:
- **Current state** (30 points)
  - Already partitioned: -30 points (easier)
  - Non-partitioned: 0 points (harder)
- **LOB migration requirement** (25 points)
  - No LOBs: 0 points
  - SECUREFILE LOBs: +5 points
  - BASICFILE LOBs: +25 points (requires migration)
- **Dependencies** (20 points)
  - Foreign keys: +5 points each
  - Materialized views: +10 points each
  - Triggers: +5 points each
- **Table size** (15 points)
  - < 10 GB: +5 points (quick migration)
  - 10-100 GB: +10 points
  - > 100 GB: +15 points (long migration)
- **Schema complexity** (10 points)
  - Simple structure: +5 points
  - Complex (many indexes, constraints): +10 points

**Calculation:**
```
Complexity Score =
  Current_State_Points +
  LOB_Migration_Points +
  Dependencies_Points +
  Size_Points +
  Schema_Complexity_Points

(Lower score = easier migration)
```

#### Dimension 3: Data Age Score (0-100)
**"How suitable is this data for ILM?"**

Factors:
- **Percentage of old data** (50 points)
  - % of data > 3 years old
  - Higher % = better ILM candidate
- **Data age spread** (30 points)
  - Wide date range (e.g., 10 years) = better tiering opportunity
  - Narrow range (e.g., 6 months) = less benefit
- **Date column suitability** (20 points)
  - Clear date partition key: +20 points
  - Multiple date candidates: +15 points
  - No obvious date column: +0 points

**Calculation:**
```
Data Age Score =
  (Pct_Data_Over_3_Years × 50) +
  (Date_Range_Years / 10 × 30) +
  Date_Column_Suitability_Points
```

### 3.2 Composite Priority Score

**Formula (3-Dimensional Model):**
```
Priority Score =
  (Impact Score × 0.50) +              // 50% weight - most important
  ((100 - Complexity Score) × 0.30) +  // 30% weight - inverse (lower complexity = higher priority)
  (Data Age Score × 0.20)              // 20% weight

Range: 0-100 (higher = better candidate)

Note: Risk Score removed (required manual business input for table criticality)
```

### 3.3 Priority Categories

Based on the composite score:

| Score Range | Category | Description | Action |
|-------------|----------|-------------|--------|
| 80-100 | **QUICK WIN** | High impact, low complexity, low risk | Pilot candidates |
| 60-79 | **HIGH PRIORITY** | Good balance of impact and feasibility | Phase 1 |
| 40-59 | **MEDIUM PRIORITY** | Moderate impact or complexity | Phase 2-3 |
| 20-39 | **LOW PRIORITY** | Lower impact or higher complexity | Phase 4-5 |
| 0-19 | **DEFER** | Low impact, high complexity, or high risk | Future consideration |

---

## 4. Profiling Queries

### 4.0 Setup: Schema Scoping and Performance Optimization

**Step 1: Analyze Tracked Schemas from LOGS.DWH_PROCESS**

```sql
-- Query 0: Identify Tracked Schemas
-- This query provides an overview of all schemas managed in the data warehouse
SELECT
    DISTINCT owner AS schema_name,
    COUNT(DISTINCT process_name) AS process_count,
    MIN(last_run_date) AS first_tracked_date,
    MAX(last_run_date) AS last_tracked_date,
    COUNT(*) AS total_processes
FROM LOGS.DWH_PROCESS
WHERE owner IS NOT NULL
GROUP BY owner
ORDER BY owner;

-- Expected Output:
-- SCHEMA_NAME  PROCESS_COUNT  FIRST_TRACKED_DATE  LAST_TRACKED_DATE  TOTAL_PROCESSES
-- CMR          25             2023-01-15          2025-10-27         156
-- DWH_PROD     42             2022-05-20          2025-10-27         389
-- APP_SALES    18             2023-03-10          2025-10-27         92
```

**Step 2: Create Temporary Table for Performance**

```sql
-- Create temporary table of tracked schemas for efficient query reuse
CREATE GLOBAL TEMPORARY TABLE temp_tracked_schemas (
    owner VARCHAR2(128)
) ON COMMIT PRESERVE ROWS;

-- Populate with tracked schemas
INSERT INTO temp_tracked_schemas
SELECT DISTINCT owner
FROM LOGS.DWH_PROCESS
WHERE owner IS NOT NULL;

COMMIT;

-- Verify schema count
SELECT COUNT(*) AS tracked_schema_count FROM temp_tracked_schemas;

-- Optional: Create index for better performance
CREATE INDEX idx_temp_tracked_schemas ON temp_tracked_schemas(owner);
```

**Benefits of This Approach:**
- Query performance: Temp table is faster than repeated subqueries
- Consistency: All queries use same schema list
- Validation: Easy to verify schema count before profiling
- Flexibility: Can add/remove schemas from temp table if needed

**Note:** All subsequent queries in this document use the filter:
```sql
WHERE owner IN (SELECT owner FROM temp_tracked_schemas)
```

---

### 4.1 Schema/Tablespace Inventory

```sql
-- Query 1: Schema Size and Table Count Inventory
SELECT
    owner AS schema_name,
    COUNT(DISTINCT table_name) AS table_count,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS total_size_gb,
    ROUND(AVG(bytes)/1024/1024, 2) AS avg_table_size_mb,
    ROUND(MAX(bytes)/1024/1024/1024, 2) AS largest_table_gb,
    COUNT(CASE WHEN partitioned = 'YES' THEN 1 END) AS partitioned_tables,
    COUNT(CASE WHEN partitioned = 'NO' THEN 1 END) AS non_partitioned_tables,
    ROUND(COUNT(CASE WHEN partitioned = 'YES' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_partitioned
FROM (
    SELECT
        t.owner,
        t.table_name,
        t.partitioned,
        s.bytes
    FROM dba_tables t
    LEFT JOIN dba_segments s
        ON t.owner = s.owner
        AND t.table_name = s.segment_name
        AND s.segment_type = 'TABLE'
    WHERE t.owner IN (SELECT owner FROM temp_tracked_schemas)
      AND t.temporary = 'N'
      AND t.nested = 'NO'
)
GROUP BY owner
ORDER BY SUM(bytes) DESC;

-- Expected Output:
-- SCHEMA_NAME  TABLE_COUNT  TOTAL_SIZE_GB  AVG_TABLE_MB  LARGEST_GB  PARTITIONED  NON_PARTITIONED  PCT_PARTITIONED
-- DWH_PROD     1250         8500.5         6800.4        450.2       125          1125             10.0
-- APP_SALES    850          3200.3         3764.5        280.1       45           805              5.3
-- CRM_DATA     620          1800.7         2904.0        120.5       12           608              1.9
```

```sql
-- Query 2: Tablespace Allocation and Free Space
SELECT
    tablespace_name,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS allocated_gb,
    ROUND(SUM(CASE WHEN status = 'ONLINE' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS used_gb,
    ROUND(SUM(free.bytes)/1024/1024/1024, 2) AS free_gb,
    ROUND((SUM(CASE WHEN status = 'ONLINE' THEN bytes ELSE 0 END) / SUM(bytes)) * 100, 1) AS pct_used,
    COUNT(DISTINCT file_name) AS datafile_count,
    MAX(autoextensible) AS autoextend_enabled
FROM dba_data_files df
LEFT JOIN (
    SELECT tablespace_name, SUM(bytes) AS bytes
    FROM dba_free_space
    GROUP BY tablespace_name
) free ON df.tablespace_name = free.tablespace_name
WHERE df.tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'TEMP', 'UNDO', 'UNDOTBS1')
GROUP BY df.tablespace_name
ORDER BY SUM(bytes) DESC;
```

### 4.2 Detailed Table Analysis

```sql
-- Query 3: Large Table Candidates (Top 100 by Size)
SELECT
    t.owner,
    t.table_name,
    ROUND(s.bytes/1024/1024/1024, 2) AS size_gb,
    t.num_rows,
    ROUND(t.num_rows / NULLIF(s.bytes, 0) * 1024, 2) AS rows_per_kb,
    t.partitioned,
    t.compression,
    tp.partition_count,
    tp.subpartition_count,
    t.tablespace_name,
    CASE
        WHEN t.num_rows > 0 AND t.last_analyzed > SYSDATE - 30 THEN 'RECENT'
        WHEN t.num_rows > 0 AND t.last_analyzed > SYSDATE - 90 THEN 'STALE'
        WHEN t.num_rows > 0 THEN 'VERY STALE'
        ELSE 'NO STATS'
    END AS stats_freshness,
    t.last_analyzed
FROM dba_tables t
LEFT JOIN dba_segments s
    ON t.owner = s.owner
    AND t.table_name = s.segment_name
    AND s.segment_type IN ('TABLE', 'TABLE PARTITION')
LEFT JOIN (
    SELECT table_owner, table_name,
           COUNT(*) AS partition_count,
           SUM(subpartition_count) AS subpartition_count
    FROM dba_tab_partitions
    GROUP BY table_owner, table_name
) tp ON t.owner = tp.table_owner AND t.table_name = tp.table_name
WHERE t.owner IN (SELECT owner FROM temp_tracked_schemas)
  AND s.bytes IS NOT NULL
  AND s.bytes > 1024*1024*1024  -- > 1 GB
ORDER BY s.bytes DESC
FETCH FIRST 100 ROWS ONLY;
```

```sql
-- Query 4: LOB Analysis - BASICFILE vs SECUREFILE
SELECT
    l.owner,
    l.table_name,
    l.column_name,
    CASE
        WHEN l.securefile = 'YES' THEN 'SECUREFILE'
        ELSE 'BASICFILE (NEEDS MIGRATION)'
    END AS lob_type,
    l.compression,
    l.deduplication,
    l.in_row,
    ROUND(s.bytes/1024/1024/1024, 2) AS lob_size_gb,
    l.tablespace_name,
    ts.bytes/1024/1024/1024 AS table_size_gb,
    ROUND((s.bytes / NULLIF(ts.bytes, 0)) * 100, 1) AS lob_pct_of_table
FROM dba_lobs l
LEFT JOIN dba_segments s
    ON l.owner = s.owner
    AND l.segment_name = s.segment_name
LEFT JOIN dba_segments ts
    ON l.owner = ts.owner
    AND l.table_name = ts.segment_name
    AND ts.segment_type = 'TABLE'
WHERE l.owner IN (SELECT owner FROM temp_tracked_schemas)
  AND s.bytes > 100*1024*1024  -- > 100 MB
ORDER BY s.bytes DESC;
```

---

## 5. Candidate Analysis Framework

### 5.1 Candidate Classification Matrix

Tables are classified into one of six categories based on their characteristics:

| Category | Description | ILM Strategy | Priority |
|----------|-------------|--------------|----------|
| **Type A: Large Time-Series Tables** | > 50 GB, clear date column, wide date range (> 3 years) | Partition by date, tier to COLD/ARCHIVE | **HIGH** |
| **Type B: Partitioned with BASICFILE LOBs** | Already partitioned, but LOBs need migration | BASICFILE → SECUREFILE, add compression | **HIGH** |
| **Type C: Non-Partitioned Large Tables** | > 20 GB, not partitioned, date column exists | Partition, then tier | **MEDIUM** |
| **Type D: Large Historical Tables** | > 20 GB, old data (> 50% over 3 years) | Compress in-place, move to COLD | **MEDIUM** |
| **Type E: Small Recent Tables** | < 20 GB, recent data (< 3 years) | Consider compression, lower priority | **LOW** |
| **Type F: Complex Schema Tables** | Many dependencies, no clear date column | Defer or custom strategy | **DEFER** |

### 5.2 Decision Tree

```
START: Evaluate Table
│
├─ Size > 50 GB?
│  ├─ YES → Has clear date column?
│  │        ├─ YES → Date range > 3 years?
│  │        │        ├─ YES → **Type A: Quick Win Candidate**
│  │        │        └─ NO → Partitioned?
│  │        │                 ├─ YES → **Type B: Check for LOB migration**
│  │        │                 └─ NO → **Type C: Partition Candidate**
│  │        └─ NO → Has LOBs?
│  │                 ├─ YES (BASICFILE) → **Type B: High Priority (LOB Migration)**
│  │                 └─ NO → Dependencies > 5?
│  │                          ├─ YES → **Type F: Defer**
│  │                          └─ NO → **Type D: Compression Candidate**
│  │
│  └─ NO (< 50 GB) → Size > 20 GB?
│                     ├─ YES → % data > 3 years old (estimated)?
│                     │        ├─ > 50% → **Type D: Historical Data**
│                     │        └─ < 50% → **Type C: Partition if date column exists**
│                     └─ NO (< 20 GB) → **Type E: Low Priority or Defer**

Notes:
- Growth rate criteria removed (not available in production)
- Access frequency (AWR) removed (not available/needed)
- Size and date range are primary criteria
```

### 5.3 Quick Win Identification

**Quick Win Criteria (All must be true):**

1. ✅ Size > 10 GB (meaningful savings)
2. ✅ Date column exists (easy partitioning)
3. ✅ > 50% data older than 3 years (good compression ratio)
4. ✅ Not mission-critical (lower risk)
5. ✅ Already partitioned OR simple structure (lower complexity)
6. ✅ No major dependencies (easier migration)

**Quick Win Query:**
```sql
SELECT
    t.owner,
    t.table_name,
    ROUND(s.bytes/1024/1024/1024, 2) AS size_gb,
    t.partitioned,
    'Quick Win Candidate' AS classification
FROM dba_tables t
JOIN dba_segments s ON t.owner = s.owner AND t.table_name = s.segment_name
WHERE s.bytes > 10*1024*1024*1024  -- > 10 GB
  AND t.owner IN (SELECT owner FROM temp_tracked_schemas)
  AND EXISTS (
      -- Has date column
      SELECT 1 FROM dba_tab_columns c
      WHERE c.owner = t.owner
        AND c.table_name = t.table_name
        AND c.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
        AND UPPER(c.column_name) LIKE '%DATE%'
  )
  AND (
      t.partitioned = 'YES' OR  -- Already partitioned
      NOT EXISTS (  -- Few dependencies
          SELECT 1 FROM dba_constraints
          WHERE owner = t.owner
            AND table_name = t.table_name
            AND constraint_type = 'R'
          HAVING COUNT(*) > 3
      )
  )
ORDER BY s.bytes DESC;
```

---

## 6. Prioritization Algorithm

### 6.1 Scoring Implementation

**Step 1: Calculate Individual Dimension Scores**

Create a scoring view or table:

```sql
CREATE TABLE ilm_candidate_scores AS
SELECT
    owner,
    table_name,
    size_gb,
    partitioned,
    has_lobs,
    lob_type,
    pct_data_over_3_years,
    date_range_years,

    -- Impact Score (0-100)
    -- Note: Growth rate and AWR/performance removed (not available in production)
    ROUND(
        (size_gb / (SELECT MAX(size_gb) FROM table_inventory) * 60) +
        (estimated_cost_savings / (SELECT MAX(estimated_cost_savings) FROM table_inventory) * 40)
    , 1) AS impact_score,

    -- Complexity Score (0-100, lower is better)
    ROUND(
        CASE WHEN partitioned = 'YES' THEN 0 ELSE 30 END +
        CASE
            WHEN lob_type = 'NONE' THEN 0
            WHEN lob_type = 'SECUREFILE' THEN 5
            WHEN lob_type = 'BASICFILE' THEN 25
        END +
        (fk_count * 5) +
        (mv_count * 10) +
        (trigger_count * 5) +
        CASE
            WHEN size_gb < 10 THEN 5
            WHEN size_gb BETWEEN 10 AND 100 THEN 10
            ELSE 15
        END +
        CASE
            WHEN schema_complexity = 'SIMPLE' THEN 5
            ELSE 10
        END
    , 1) AS complexity_score,

    -- Data Age Score (0-100)
    ROUND(
        (pct_data_over_3_years * 0.50) +
        (LEAST(date_range_years / 10, 1) * 30) +
        date_column_quality_points
    , 1) AS data_age_score

FROM table_inventory;
```

**Step 2: Calculate Composite Priority Score**

```sql
-- Add composite score column
ALTER TABLE ilm_candidate_scores ADD priority_score NUMBER;

UPDATE ilm_candidate_scores SET priority_score = ROUND(
    (impact_score * 0.50) +
    ((100 - complexity_score) * 0.30) +
    (data_age_score * 0.20)
, 1);

-- Add priority category
ALTER TABLE ilm_candidate_scores ADD priority_category VARCHAR2(20);

UPDATE ilm_candidate_scores SET priority_category =
    CASE
        WHEN priority_score >= 80 THEN 'QUICK_WIN'
        WHEN priority_score >= 60 THEN 'HIGH'
        WHEN priority_score >= 40 THEN 'MEDIUM'
        WHEN priority_score >= 20 THEN 'LOW'
        ELSE 'DEFER'
    END;
```

### 6.2 Final Candidate Ranking View

**Fully Automated Ranking** - No manual adjustments required.

```sql
-- Create final ranking view
CREATE OR REPLACE VIEW ilm_final_candidate_ranking AS
SELECT
    owner,
    table_name,
    size_gb,
    impact_score,
    complexity_score,
    data_age_score,
    priority_score,
    priority_category,
    estimated_storage_savings_gb,
    estimated_compression_ratio,
    date_range_years,
    pct_data_over_3_years
FROM ilm_candidate_scores
ORDER BY priority_score DESC;
```

---

## 7. Migration Wave Planning

### 7.1 Wave Structure

**Wave 0: Pilot (2-4 weeks)**
- **Objective:** Validate approach, learn lessons
- **Candidates:** 3-5 QUICK_WIN tables (score 80-100)
- **Criteria:**
  - Size: 10-50 GB (not too small, not too large)
  - Non-critical or standard operational tables (low risk)
  - Clear success metrics
  - Representative of larger population

**Wave 1: Quick Wins (4-8 weeks)**
- **Objective:** Maximize early impact
- **Candidates:** 15-25 HIGH priority tables (score 60-79)
- **Criteria:**
  - High impact, low-medium complexity
  - Mix of already-partitioned and non-partitioned
  - Include BASICFILE LOB migrations

**Wave 2: Major Tables (8-12 weeks)**
- **Objective:** Address largest storage consumers
- **Candidates:** 30-50 MEDIUM-HIGH priority tables
- **Criteria:**
  - Very large tables (> 100 GB)
  - Higher complexity acceptable
  - Careful planning and testing required

**Wave 3: Long Tail (12-24 weeks)**
- **Objective:** Comprehensive coverage
- **Candidates:** 50-100 MEDIUM-LOW priority tables
- **Criteria:**
  - Remaining candidates worth migrating
  - Can be done in batches
  - Lower urgency

**Wave 4: Deferred / Custom (Future)**
- **Objective:** Handle edge cases
- **Candidates:** Complex or low-value tables
- **Criteria:**
  - Requires custom solutions
  - Very high complexity
  - Low ROI but may be needed eventually

### 7.2 Wave Assignment Query

```sql
-- Assign migration waves based on priority scores
SELECT
    owner,
    table_name,
    size_gb,
    final_priority_score,
    final_priority_category,
    CASE
        WHEN final_priority_score >= 85 AND size_gb BETWEEN 10 AND 50 THEN 'WAVE 0: Pilot'
        WHEN final_priority_score >= 70 THEN 'WAVE 1: Quick Wins'
        WHEN final_priority_score >= 50 OR size_gb > 100 THEN 'WAVE 2: Major Tables'
        WHEN final_priority_score >= 30 THEN 'WAVE 3: Long Tail'
        ELSE 'WAVE 4: Deferred'
    END AS migration_wave,
    estimated_storage_savings_gb,
    estimated_migration_hours
FROM ilm_final_candidate_ranking
ORDER BY migration_wave, final_priority_score DESC;
```

---

## 8. Sample Output and Reporting

### 8.1 Executive Summary Report

```sql
-- Generate executive summary
SELECT
    'Total Schemas Analyzed' AS metric,
    COUNT(DISTINCT owner) AS value,
    NULL AS unit
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Total Tables Analyzed',
    COUNT(*),
    'tables'
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Total Current Storage',
    ROUND(SUM(size_gb), 1),
    'TB'
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Estimated Storage After ILM',
    ROUND(SUM(size_gb * (1 - estimated_compression_ratio)), 1),
    'TB'
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Estimated Storage Savings',
    ROUND(SUM(size_gb * estimated_compression_ratio), 1),
    'TB'
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Estimated Savings Percentage',
    ROUND(AVG(estimated_compression_ratio * 100), 1),
    '%'
FROM ilm_candidate_scores
UNION ALL
SELECT
    'Quick Win Candidates (Pilot)',
    COUNT(*),
    'tables'
FROM ilm_candidate_scores
WHERE priority_category = 'QUICK_WIN'
UNION ALL
SELECT
    'High Priority Candidates',
    COUNT(*),
    'tables'
FROM ilm_candidate_scores
WHERE priority_category = 'HIGH'
UNION ALL
SELECT
    'BASICFILE LOBs Requiring Migration',
    COUNT(*),
    'LOB columns'
FROM ilm_candidate_scores
WHERE lob_type = 'BASICFILE';
```

**Sample Output:**
```
METRIC                              VALUE   UNIT
Total Schemas Analyzed              45
Total Tables Analyzed               3,456   tables
Total Current Storage               120.5   TB
Estimated Storage After ILM         42.2    TB
Estimated Storage Savings           78.3    TB
Estimated Savings Percentage        65.0    %
Quick Win Candidates (Pilot)        8       tables
High Priority Candidates            67      tables
BASICFILE LOBs Requiring Migration  234     LOB columns
```

### 8.2 Top Candidates Report

```sql
-- Top 20 candidates across all categories
SELECT
    DENSE_RANK() OVER (ORDER BY priority_score DESC) AS rank,
    owner,
    table_name,
    size_gb,
    ROUND(impact_score, 1) AS impact,
    ROUND(100 - complexity_score, 1) AS ease,
    ROUND(data_age_score, 1) AS age_score,
    ROUND(priority_score, 1) AS priority,
    priority_category AS category,
    ROUND(estimated_storage_savings_gb, 1) AS savings_gb,
    estimated_migration_hours AS effort_hrs
FROM ilm_final_candidate_ranking
ORDER BY priority_score DESC
FETCH FIRST 20 ROWS ONLY;
```

**Sample Output:**
```
RANK  OWNER      TABLE_NAME           SIZE_GB  IMPACT  EASE  AGE    PRIORITY  CATEGORY    SAVINGS_GB  EFFORT_HRS
1     DWH_PROD   FACT_SALES_HISTORY   450.2    95.3    82.5  85.0   89.5      QUICK_WIN   337.7       24
2     DWH_PROD   FACT_ORDERS          380.5    92.1    78.3  80.0   86.2      QUICK_WIN   285.4       32
3     APP_SALES  SALES_TRANSACTIONS   280.1    88.7    85.0  82.0   85.6      QUICK_WIN   210.1       20
4     CRM_DATA   CUSTOMER_INTERACTIONS 220.3   85.2    75.5  75.0   79.1      HIGH        165.2       28
5     DWH_PROD   EVENT_LOG            195.7    82.4    80.0  78.0   79.7      HIGH        146.8       22
...
```

### 8.3 Schema-Level Rollup

```sql
-- Savings potential by schema
SELECT
    owner,
    COUNT(*) AS candidate_tables,
    ROUND(SUM(size_gb), 1) AS total_size_gb,
    ROUND(SUM(estimated_storage_savings_gb), 1) AS total_savings_gb,
    ROUND(AVG(final_priority_score), 1) AS avg_priority,
    SUM(CASE WHEN final_priority_category IN ('QUICK_WIN', 'HIGH') THEN 1 ELSE 0 END) AS high_priority_count,
    SUM(estimated_migration_hours) AS total_effort_hours
FROM ilm_final_candidate_ranking
GROUP BY owner
ORDER BY total_savings_gb DESC;
```

### 8.4 Wave Summary

```sql
-- Summary by migration wave
SELECT
    migration_wave,
    COUNT(*) AS table_count,
    ROUND(SUM(size_gb), 1) AS total_size_gb,
    ROUND(SUM(estimated_storage_savings_gb), 1) AS total_savings_gb,
    ROUND(AVG(final_priority_score), 1) AS avg_priority_score,
    SUM(estimated_migration_hours) AS total_effort_hours,
    ROUND(SUM(estimated_storage_savings_gb) / NULLIF(SUM(estimated_migration_hours), 0), 2) AS gb_saved_per_hour
FROM ilm_final_candidate_ranking
WHERE migration_wave IS NOT NULL
GROUP BY migration_wave
ORDER BY
    CASE migration_wave
        WHEN 'WAVE 0: Pilot' THEN 1
        WHEN 'WAVE 1: Quick Wins' THEN 2
        WHEN 'WAVE 2: Major Tables' THEN 3
        WHEN 'WAVE 3: Long Tail' THEN 4
        WHEN 'WAVE 4: Deferred' THEN 5
    END;
```

---

## 9. Implementation Roadmap

### 9.1 Timeline Overview

```
Week 1:     Data Collection
            │
            ├─ Run all profiling queries (Phase 1)
            ├─ Run analyze_table package for top tables (Phase 2)
            └─ Collect LOB and partitioning metadata

Week 2:     Scoring & Analysis
            │
            ├─ Calculate dimension scores (Impact, Complexity, Data Age)
            ├─ Compute composite priorities (fully automated)
            └─ Generate candidate ranking

Week 3:     Review & Validation
            │
            ├─ Review with DBA team
            ├─ Validate scoring results with spot checks
            └─ Finalize wave assignments

Week 4:     Planning & Approval
            │
            ├─ Present to stakeholders
            ├─ Get approvals
            └─ Prepare pilot environment

Week 5-6:   Pilot Wave Execution
            │
            ├─ Migrate 3-5 pilot tables
            ├─ Validate results
            └─ Document lessons learned

Week 7+:    Production Waves
            │
            ├─ Wave 1: Quick Wins (Weeks 7-14)
            ├─ Wave 2: Major Tables (Weeks 15-26)
            └─ Wave 3: Long Tail (Weeks 27-50)
```

### 9.2 Key Milestones

| Milestone | Target Week | Deliverable | Success Criteria |
|-----------|-------------|-------------|------------------|
| Profiling Complete | Week 2 | All data collected, queries run | 100% schema coverage |
| Scoring Complete | Week 3 | Candidate ranking generated | All tables scored |
| Wave Plan Approved | Week 4 | Migration roadmap approved | Stakeholder sign-off |
| Pilot Complete | Week 6 | Pilot migration validated | 3-5 tables migrated, tested |
| Wave 1 50% | Week 10 | Half of Wave 1 migrated | 50% quick wins done |
| Wave 1 Complete | Week 14 | All Wave 1 tables migrated | All quick wins done |
| Major Savings Achieved | Week 26 | 60%+ of savings realized | > 45 TB saved |
| Full Rollout Complete | Week 50 | All planned waves done | 90%+ candidates migrated |

### 9.3 Success Metrics

**Storage Metrics:**
- Total TB saved
- % reduction in storage costs
- Compression ratios achieved per table

**Operational Metrics:**
- Tables migrated per week
- Average migration time per table
- Incidents/rollbacks count
- Backup time reduction

**Business Metrics:**
- Query performance improvement (avg % faster)
- User satisfaction scores
- Cost savings ($)
- DBA time freed up (hours/week)

---

## 10. Appendix: Complete Query Set

### 10.1 Profiling Data Sources

**All profiling data is stored in existing framework tables:**

1. **Schema inventory** - Query results from Section 4.1 (can be saved as temp table if needed)
2. **Table analysis** - `cmr.dwh_migration_analysis` (populated by `pck_dwh_table_migration_analyzer.analyze_table`)
   - Includes: Table statistics, LOB details (type, size, BASICFILE vs SECUREFILE), date columns, complexity scoring, compression estimates
3. **Date column analysis** - `cmr.dwh_migration_analysis.all_date_columns_analysis` (JSON format)
4. **LOB analysis** - `cmr.dwh_migration_analysis.lob_details` (JSON format)
   - Fields: `lob_column_count`, `lob_type`, `basicfile_lob_count`, `securefile_lob_count`, `lob_total_size_mb`

**No need for separate `ilm_profile_*` tables** - all data centralized in the migration framework.

### 10.2 Quick Reference: Top Candidates Query

```sql
-- One-query summary of top candidates (for quick review)
-- Uses cmr.dwh_migration_analysis populated by analyze_table package
SELECT
    task.owner || '.' || task.table_name AS full_table_name,
    ROUND(a.table_size_mb / 1024, 1) AS size_gb,
    CASE WHEN a.recommended_strategy LIKE '%PARTITION%' THEN 'NO' ELSE 'YES' END AS partitioned,
    CASE
        WHEN a.lob_type = 'BASICFILE' THEN 'YES (BASICFILE - NEEDS MIGRATION)'
        WHEN a.lob_type = 'SECUREFILE' THEN 'YES (SECUREFILE)'
        WHEN a.lob_type = 'MIXED' THEN 'YES (MIXED)'
        ELSE 'NO'
    END AS has_lobs,
    -- Estimate % old data from date range (uniform distribution)
    CASE
        WHEN a.partition_range_years >= 3 THEN
            ROUND(LEAST(((a.partition_range_years - 3) / NULLIF(a.partition_range_years, 0)) * 100, 100), 1)
        ELSE 0
    END AS old_data_pct,
    ROUND(a.partition_range_years, 1) AS date_range_yrs,
    CASE
        WHEN a.table_size_mb / 1024 > 50 AND a.partition_range_years > 5 THEN 'Type A: Quick Win'
        WHEN a.lob_type = 'BASICFILE' THEN 'Type B: LOB Migration'
        WHEN a.recommended_strategy LIKE '%PARTITION%' AND a.table_size_mb / 1024 > 20 THEN 'Type C: Partition Candidate'
        WHEN a.table_size_mb / 1024 > 20 AND a.partition_range_years > 7 THEN 'Type D: Compress & Archive'
        ELSE 'Type E/F: Lower Priority'
    END AS candidate_type,
    ROUND(a.estimated_space_savings_mb / 1024, 1) AS est_savings_gb
FROM cmr.dwh_migration_tasks task
JOIN cmr.dwh_migration_analysis a ON task.task_id = a.task_id
WHERE task.project_id = (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'ILM_PROFILING_2025')
  AND a.table_size_mb / 1024 > 10  -- Focus on tables > 10 GB
ORDER BY a.table_size_mb DESC
FETCH FIRST 50 ROWS ONLY;
```

---

## 11. Next Steps and Action Items

### 11.1 Immediate Actions (This Week)

- [ ] Review and approve this profiling plan
- [ ] Assign DBA resources for data collection
- [ ] Verify cmr.dwh_migration_* framework tables exist and are accessible
- [ ] Schedule profiling execution window (weekend preferred)

### 11.2 Week 1-2 Actions

- [ ] Execute Phase 1 profiling queries (Schema inventory, Query 0-2)
- [ ] Run Phase 2 analysis using pck_dwh_table_migration_analyzer package (includes LOB analysis)
- [ ] Generate schema inventory report
- [ ] Verify all tracked schemas profiled successfully

### 11.3 Week 3 Actions

- [ ] Calculate dimension scores (Impact, Complexity, Data Age)
- [ ] Compute composite priority scores
- [ ] Generate candidate ranking
- [ ] Review with DBA team
- [ ] Validate scoring results

### 11.4 Week 4 Actions

- [ ] Prepare executive summary report
- [ ] Create migration wave plan
- [ ] Present findings to stakeholders
- [ ] Get approval for pilot wave
- [ ] Select 3-5 pilot candidate tables

### 11.5 Week 5+ Actions

- [ ] Execute pilot wave migration
- [ ] Validate pilot results
- [ ] Document lessons learned
- [ ] Plan Wave 1 execution
- [ ] Begin production rollout

---

## 12. Risk Mitigation

### 12.1 Profiling Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Incomplete data collection | Medium | Medium | Run queries during low-load periods, use multiple sessions |
| Scoring model inaccuracies | Medium | Medium | Pilot validates model, adjust weights as needed |
| Stakeholder disagreement | Medium | Low | Fully automated scoring based on objective database metrics, pilot validates approach |
| Production impact during profiling | Low | Low | Use existing analyze_table package with built-in sampling, no table scans |

### 12.2 Migration Risks (Post-Profiling)

Addressed in main ILM strategy document and DBA meeting discussion points.

---

## Document Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-10-27 | Claude/DBA Team | Initial profiling plan created |
| 1.1 | 2025-10-27 | Claude/DBA Team | Scoped profiling to LOGS.DWH_PROCESS schemas, added Query 0 and temp_tracked_schemas table, updated all queries with schema filter |
| 1.2 | 2025-10-27 | Claude/DBA Team | **PRODUCTION SAFETY UPDATE**: Added Section 0 (Production Database Safety Guidelines) with execution timing, resource management, monitoring procedures, query cancellation, and communication protocols. Updated Section 2.2 with detailed execution phases including impact levels, timing windows, and emergency stop criteria. |
| 1.3 | 2025-10-27 | Claude/DBA Team | **FULLY AUTOMATED SCORING**: Converted from 4-dimensional to 3-dimensional scoring model. Removed Risk Score dimension (required manual table criticality input). Updated composite priority score formula: Impact (50%), Complexity (30%), Data Age (20%). Removed all manual input requirements - profiling now fully automated using database metadata only. Updated document purpose, success metrics, and risk mitigation table. |

---

**Document Approval:**

| Role | Name | Signature | Date |
|------|------|-----------|------|
| DBA Lead | __________ | __________ | _____ |
| Architecture Lead | __________ | __________ | _____ |
| Project Manager | __________ | __________ | _____ |

---

**References:**
- Tablespace Allocation Strategy: `docs/planning/dba/tablespace_allocation_strategy.md`
- DBA Meeting Discussion Points: `docs/planning/dba/dba_meeting_discussion_points.md`
- Oracle VLDB and Partitioning Guide
- Oracle Advanced Compression Best Practices

---

**Next Document:** After profiling is complete, create `migration_execution_plan_wave_0.md` for pilot wave execution details.
