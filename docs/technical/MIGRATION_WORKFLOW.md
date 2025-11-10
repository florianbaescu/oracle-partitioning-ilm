# Table Migration Workflow Documentation

## Overview

This document explains the **complete end-to-end workflow** for migrating non-partitioned tables to partitioned tables using the Oracle Data Warehouse Partitioning and ILM Framework.

## The Critical Missing Link

**IMPORTANT**: The original framework had a design gap where analysis results were stored in `dwh_migration_analysis` but execution read from `dwh_migration_tasks`. The new `apply_recommendations()` procedure bridges this gap.

---

## Complete Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 1: Create Task                                              │
│ INSERT INTO dwh_migration_tasks (minimal config)                 │
│ - DO NOT specify partition_type, partition_key, interval_clause  │
│ - Status: PENDING                                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: Analyze Table                                            │
│ pck_dwh_table_migration_analyzer.analyze_table(task_id)          │
│ - Discovers all date columns                                     │
│ - Recommends partition strategy                                  │
│ - Stores recommendations in dwh_migration_analysis               │
│ - Status: ANALYZING → ANALYZED                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 3: Review Recommendations (OPTIONAL)                        │
│ SELECT * FROM v_dwh_migration_task_status                        │
│ - Review recommended_strategy                                    │
│ - Check complexity_score                                         │
│ - Verify no blocking issues                                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ *** STEP 4: Apply Recommendations (KEY STEP!) ***                │
│ pck_dwh_table_migration_executor.apply_recommendations(task_id)  │
│ - Copies recommended_strategy → partition_type                   │
│ - Copies date_column_name → partition_key                        │
│ - Parses and sets interval_clause                                │
│ - Sets migration_method                                          │
│ - Status: ANALYZED → READY                                       │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 5: Simulate Migration (OPTIONAL)                            │
│ execute_migration(task_id, p_simulate => TRUE)                   │
│ - Preview DDL without executing                                  │
│ - Validate configuration                                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 6: Execute Migration                                        │
│ pck_dwh_table_migration_executor.execute_migration(task_id)      │
│ - Creates partitioned table                                      │
│ - Copies data (with optional date conversion)                    │
│ - Recreates indexes and constraints                              │
│ - Status: READY → RUNNING → COMPLETED                            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 7: Validate & Apply ILM                                     │
│ - Row count validation (automatic)                               │
│ - ILM policies applied (if enabled)                              │
│ - Space savings calculated                                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Status Flow

```
Task Status:
  PENDING → ANALYZING → ANALYZED → READY → RUNNING → COMPLETED
                           ↓                            ↓
                        BLOCKED                      FAILED

Validation Status:
  NULL → READY (if no blocking issues)
      → BLOCKED (if issues found)
```

---

## Quick Start Example

### Automated Workflow (Recommended)

```sql
-- 1. Create minimal task
INSERT INTO cmr.dwh_migration_tasks (
    task_name, source_table, status
) VALUES (
    'Migrate SALES_FACT', 'SALES_FACT', 'PENDING'
) RETURNING task_id INTO v_task_id;

-- 2. Analyze
EXEC pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

-- 3. Apply recommendations (KEY STEP!)
EXEC pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

-- 4. Execute
EXEC pck_dwh_table_migration_executor.execute_migration(v_task_id);
```

### Manual Workflow (Advanced)

If you want to specify the partition strategy yourself:

```sql
-- 1. Create task with explicit strategy
INSERT INTO cmr.dwh_migration_tasks (
    task_name, source_table,
    partition_type,          -- Specify explicitly
    partition_key,           -- Specify explicitly
    interval_clause,         -- Specify explicitly
    status
) VALUES (
    'Migrate SALES_FACT', 'SALES_FACT',
    'RANGE(sale_date)',
    'sale_date',
    'NUMTOYMINTERVAL(1,''MONTH'')',
    'PENDING'
);

-- 2. Analyze (validates your strategy)
EXEC pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

-- 3. Skip apply_recommendations() - already configured

-- 4. Execute
EXEC pck_dwh_table_migration_executor.execute_migration(v_task_id);
```

---

## What Gets Stored Where?

### `dwh_migration_tasks` Table
**What execution reads:**
- `partition_type` - e.g., "RANGE(sale_date)"
- `partition_key` - e.g., "sale_date"
- `interval_clause` - e.g., "NUMTOYMINTERVAL(1,'MONTH')"
- `migration_method` - e.g., "CTAS", "ONLINE", "EXCHANGE"
- `status` - Current execution status
- `validation_status` - Ready/blocked status

### `dwh_migration_analysis` Table
**What analysis produces:**
- `recommended_strategy` - Full strategy string
- `recommendation_reason` - Why this strategy
- `date_column_name` - Primary date column selected
- `date_column_type` - Original data type
- `date_format_detected` - If NUMBER/VARCHAR conversion needed
- `date_conversion_expr` - Conversion expression
- `requires_conversion` - Y/N flag
- `all_date_columns_analysis` - JSON with ALL date columns
- `complexity_score` - 1-10 difficulty rating
- `estimated_downtime_minutes` - Time estimate
- `recommended_method` - CTAS/ONLINE/EXCHANGE
- `blocking_issues` - Things that must be fixed
- `warnings` - Non-blocking concerns

---

## The apply_recommendations() Procedure

### What It Does

1. **Reads** analysis results from `dwh_migration_analysis`
2. **Parses** `recommended_strategy` to extract:
   - Partition type (e.g., "RANGE(sale_date)")
   - Interval clause keywords (MONTHLY → NUMTOYMINTERVAL(1,'MONTH'))
   - Subpartition clause (if present)
3. **Copies** `date_column_name` → `partition_key`
4. **Handles** date conversion (appends "_CONVERTED" if needed)
5. **Determines** best migration method
6. **Updates** `dwh_migration_tasks` with all values
7. **Sets** status to 'READY'

### When to Use It

✅ **USE apply_recommendations() when:**
- You created a minimal task (no partition strategy specified)
- You want the analyzer to recommend the strategy
- You trust the analyzer's intelligence

❌ **SKIP apply_recommendations() when:**
- You specified partition_type/partition_key when creating task
- You want manual control over the strategy
- Analysis is just validation

### Error Handling

The procedure will error if:
- Task not analyzed yet (`status != 'ANALYZED'`)
- No analysis results found
- Blocking issues exist
- No strategy recommended

---

## Migration Methods

### CTAS (Create Table As Select)
- **When:** Most common, standard migrations
- **Downtime:** Required (seconds to minutes)
- **Speed:** Fast
- **Supports:** Date conversion ✓
- **Requirements:** 2x disk space temporarily

### ONLINE (DBMS_REDEFINITION)
- **When:** Large tables, zero downtime needed
- **Downtime:** Near-zero (seconds for final swap)
- **Speed:** Slower (incremental sync)
- **Supports:** Date conversion ✗
- **Requirements:** Enterprise Edition, 2x disk space

### EXCHANGE (Partition Exchange)
- **When:** Staging-to-production ETL loads
- **Downtime:** Seconds
- **Speed:** Instant (metadata-only)
- **Supports:** Date conversion ✗
- **Requirements:** Data fits single partition, exact structure match

---

## Advanced Features

### Date Column Analysis

The analyzer examines **ALL** date/timestamp columns and stores comprehensive analysis:

```sql
-- View all date columns analyzed
SELECT * FROM cmr.v_dwh_date_column_analysis
WHERE task_id = 123;

-- Parse JSON to see details
SELECT jt.*
FROM cmr.dwh_migration_analysis a,
JSON_TABLE(
    a.all_date_columns_analysis, '$[*]'
    COLUMNS (
        column_name VARCHAR2(128) PATH '$.column_name',
        min_date VARCHAR2(20) PATH '$.min_date',
        max_date VARCHAR2(20) PATH '$.max_date',
        range_days NUMBER PATH '$.range_days',
        is_primary VARCHAR2(5) PATH '$.is_primary'
    )
) jt
WHERE a.task_id = 123;
```

### Non-Standard Date Formats

Automatically detects and converts:
- **NUMBER columns:** YYYYMMDD, YYYYMM, UNIX_TIMESTAMP
- **VARCHAR columns:** ISO8601 strings
- Generates conversion expressions
- Handles in CTAS migration method

---

## Monitoring

### Dashboard View
```sql
SELECT * FROM cmr.v_dwh_migration_dashboard;
```

### Task Status
```sql
SELECT * FROM cmr.v_dwh_migration_task_status
WHERE project_name = 'YOUR_PROJECT';
```

### Execution Log
```sql
SELECT step_number, step_name, status, duration_seconds
FROM cmr.dwh_migration_execution_log
WHERE task_id = 123
ORDER BY step_number;
```

### Running Migrations
```sql
SELECT task_id, task_name, status, execution_start,
       ROUND((SYSTIMESTAMP - execution_start) * 24 * 60, 1) AS running_minutes
FROM cmr.dwh_migration_tasks
WHERE status = 'RUNNING';
```

---

## Rollback

```sql
-- Check if rollback possible
SELECT task_id, backup_table_name, can_rollback
FROM cmr.dwh_migration_tasks
WHERE can_rollback = 'Y';

-- Rollback migration
EXEC pck_dwh_table_migration_executor.rollback_migration(task_id);
```

---

## Configuration

Control behavior via `cmr.dwh_ilm_config`:

```sql
-- View current config
SELECT config_key, config_value, description
FROM cmr.dwh_ilm_config
WHERE config_key LIKE 'MIGRATION%';

-- Update config
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'MIGRATION_BACKUP_ENABLED';
```

Key settings:
- `MIGRATION_BACKUP_ENABLED` - Create backup tables (default: Y)
- `MIGRATION_VALIDATE_ENABLED` - Validate row counts (default: Y)
- `MIGRATION_AUTO_ILM_ENABLED` - Auto-create ILM policies (default: Y)
- `MIGRATION_PARALLEL_DEGREE` - Parallelism (default: 4)

---

## Troubleshooting

### "Task not ready for migration"
→ Did you call `apply_recommendations()`?

### "No analysis results found"
→ Did you call `analyze_table()` first?

### "Cannot apply recommendations: blocking issues exist"
→ Check `dwh_migration_analysis.blocking_issues` and resolve

### Partition type is NULL in execution
→ You forgot `apply_recommendations()`!

### ORA-00904: invalid identifier
→ Check date conversion settings in analysis results

---

## File Reference

- **Setup:** `scripts/table_migration_setup.sql`
- **Analysis:** `scripts/table_migration_analysis.sql`
- **Execution:** `scripts/table_migration_execution.sql`
- **Examples:** `examples/table_migration_examples.sql`
- **Complete Workflow:** `examples/complete_migration_workflow.sql`

---

## Summary: The Key Takeaway

**The migration workflow has THREE phases, not TWO:**

1. ✅ **Analyze** - Discover and recommend (`analyze_table`)
2. ✅ **Apply** - Copy recommendations to task (`apply_recommendations`) ← **NEW!**
3. ✅ **Execute** - Perform migration (`execute_migration`)

Without step 2, the recommendations sit unused in `dwh_migration_analysis` and execution has no partition strategy to work with!
