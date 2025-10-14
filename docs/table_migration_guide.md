---
# Table Migration Framework Guide

A comprehensive framework for migrating non-partitioned Oracle tables to partitioned structures with integrated ILM policy application.

## Overview

This framework provides end-to-end support for converting non-partitioned tables to partitioned tables, including:

- **Automated Analysis**: Analyzes table structure and recommends optimal partitioning strategy
- **Multiple Migration Methods**: CTAS, Online Redefinition, Exchange Partition
- **ILM Integration**: Automatically applies ILM policies to newly partitioned tables
- **Project Management**: Track multiple migrations as organized projects
- **Validation & Rollback**: Verify migration success and rollback if needed
- **Complete Audit Trail**: Detailed logging of all migration steps

## Architecture

```
┌──────────────────────────────────────────────┐
│       Migration Framework Components          │
├──────────────────────────────────────────────┤
│                                               │
│  1. Candidate Identification                  │
│     └─> View non-partitioned tables          │
│                                               │
│  2. Project & Task Creation                   │
│     └─> Organize migrations                  │
│                                               │
│  3. Analysis Engine                           │
│     ├─> Analyze table structure              │
│     ├─> Recommend partition strategy         │
│     ├─> Estimate complexity & downtime       │
│     └─> Identify blocking issues             │
│                                               │
│  4. Apply Recommendations                     │
│     ├─> Copy recommended strategy to task    │
│     ├─> Set partition_type & partition_key   │
│     ├─> Set interval_clause & method         │
│     └─> Update task status to READY          │
│                                               │
│  5. Execution Engine                          │
│     ├─> Create backup                        │
│     ├─> Build partitioned table              │
│     ├─> Copy data                            │
│     ├─> Recreate indexes & constraints       │
│     └─> Swap tables                          │
│                                               │
│  6. ILM Integration                           │
│     └─> Apply policy templates               │
│                                               │
│  7. Validation & Rollback                     │
│     ├─> Verify row counts                    │
│     └─> Rollback if needed                   │
│                                               │
└──────────────────────────────────────────────┘
```

## Installation

### Prerequisites

- Oracle Database 12c or higher
- DBA or schema owner privileges
- Custom ILM framework installed (optional but recommended)

### Installation Steps

```sql
-- Install in order:
@scripts/table_migration_setup.sql
@scripts/table_migration_analysis.sql
@scripts/table_migration_execution.sql
```

### Verify Installation

```sql
-- Check metadata tables
SELECT table_name FROM user_tables WHERE table_name LIKE 'MIGRATION_%';

-- Check packages
SELECT object_name, status FROM user_objects
WHERE object_name LIKE 'TABLE_MIGRATION_%'
AND object_type = 'PACKAGE';
```

## Quick Start

### 1. Identify Candidates

```sql
-- View tables suitable for partitioning
SELECT * FROM cmr.dwh_v_migration_candidates
WHERE migration_priority IN ('HIGH PRIORITY', 'MEDIUM PRIORITY')
ORDER BY size_mb DESC;
```

### 2. Create Project

```sql
INSERT INTO cmr.dwh_migration_projects (project_name, description)
VALUES ('Q1_MIGRATION', 'Partition fact tables for Q1');
COMMIT;
```

### 3. Create Migration Task

```sql
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    migration_method,
    use_compression,
    apply_ilm_policies,
    ilm_policy_template,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_MIGRATION'),
    'Migrate SALES_FACT',
    USER,
    'SALES_FACT',
    'CTAS',
    'Y',
    'Y',
    'FACT_TABLE_STANDARD',
    'PENDING'
);
COMMIT;
```

### 4. Analyze Table

```sql
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
END;
/
```

### 5. Review Analysis

```sql
SELECT
    t.task_name,
    a.recommended_strategy,
    a.complexity_score,
    a.estimated_partitions,
    a.estimated_downtime_minutes
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
WHERE t.task_name = 'Migrate SALES_FACT';
```

### 6. Apply Recommendations

```sql
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    -- Copy recommended strategy from analysis to task
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Task is now READY for execution
END;
/
```

### 7. Execute Migration

```sql
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate SALES_FACT';

    pck_dwh_table_migration_executor.execute_migration(v_task_id);
END;
/
```

## Detailed Usage

### Migration Projects

Projects help organize multiple related migrations:

```sql
-- Create project
INSERT INTO migration_projects (
    project_name,
    description,
    status
) VALUES (
    'DWH_OPTIMIZATION_2024',
    'Partition all fact tables for improved performance',
    'PLANNING'
);

-- Update project status
UPDATE migration_projects
SET status = 'IN_PROGRESS', started_date = SYSTIMESTAMP
WHERE project_name = 'DWH_OPTIMIZATION_2024';

-- View project dashboard
SELECT * FROM v_migration_dashboard;
```

### Migration Tasks

Tasks represent individual table migrations:

#### Automatic Strategy (Recommended)

```sql
-- Framework will analyze and recommend strategy
INSERT INTO migration_tasks (
    project_id,
    task_name,
    source_table,
    migration_method,
    use_compression,
    compression_type
) VALUES (
    1,
    'Migrate ORDERS',
    'ORDERS',
    'CTAS',
    'Y',
    'QUERY HIGH'
);
```

#### Explicit Strategy

```sql
-- Specify exact partitioning strategy
INSERT INTO migration_tasks (
    project_id,
    task_name,
    source_table,
    partition_type,
    partition_key,
    interval_clause,
    migration_method
) VALUES (
    1,
    'Migrate SALES',
    'SALES',
    'RANGE(sale_date)',
    'sale_date',
    'NUMTOYMINTERVAL(1,''MONTH'')',
    'CTAS'
);
```

#### Composite Partitioning

```sql
-- Range-Hash composite partitioning
INSERT INTO migration_tasks (
    project_id,
    task_name,
    source_table,
    partition_type,
    partition_key,
    subpartition_type,
    subpartition_key,
    interval_clause
) VALUES (
    1,
    'Migrate WEB_EVENTS',
    'WEB_EVENTS',
    'RANGE(event_date)',
    'event_date',
    'HASH(session_id) SUBPARTITIONS 8',
    'session_id',
    'NUMTODSINTERVAL(1,''DAY'')'
);
```

#### Row Movement Configuration

The `enable_row_movement` option controls whether Oracle can physically move rows between partitions when partition key values change.

**What is Row Movement?**

Row movement allows Oracle to:
- Move rows from one partition to another when partition key values are updated
- Automatically redistribute data for optimal partition placement
- Execute ILM policies that move data between partitions
- Handle interval partition expansion efficiently

**Why Enable Row Movement?**

| Scenario | Without Row Movement | With Row Movement |
|----------|---------------------|-------------------|
| Update partition key | ❌ ORA-14402 error | ✅ Row moves to correct partition |
| ILM data movement | ❌ Policies fail | ✅ Data moves automatically |
| Interval partitioning | ⚠️ Limited functionality | ✅ Full functionality |
| Partition maintenance | ⚠️ Manual splits needed | ✅ Automatic handling |

**Configuration:**

```sql
-- Default (enabled - recommended):
INSERT INTO cmr.dwh_migration_tasks (
    source_table,
    migration_method,
    use_compression
) VALUES (
    'SALES_FACT',
    'CTAS',
    'Y'
);  -- enable_row_movement defaults to 'Y'

-- Explicitly enable:
INSERT INTO cmr.dwh_migration_tasks (
    source_table,
    migration_method,
    enable_row_movement
) VALUES (
    'SALES_FACT',
    'CTAS',
    'Y'
);

-- Disable (not recommended):
INSERT INTO cmr.dwh_migration_tasks (
    source_table,
    migration_method,
    enable_row_movement
) VALUES (
    'REFERENCE_DATA',
    'CTAS',
    'N'  -- Only if partition keys never change
);
```

**When to Disable:**

Consider disabling row movement only when:
- Partition key columns are **never** updated
- Performance is critical and you can guarantee no partition key changes
- Legacy applications with strict partition placement requirements
- Reference/lookup tables with immutable partition keys

**Common Errors Without Row Movement:**

```sql
-- Example: Attempting to update partition key without row movement
UPDATE sales_fact
SET sale_date = sale_date + 365  -- Move to different partition
WHERE sale_id = 12345;

-- Error without row movement:
-- ORA-14402: updating partition key column would cause a partition change
```

**Best Practice:**

✅ **Always enable row movement** unless you have a specific reason not to. It's required for:
- Interval partitioned tables
- ILM policies (compression, tiering, archival)
- Applications that may update partition key values
- Future-proofing against schema changes

**Applied Across All Methods:**

Row movement is automatically configured for all migration methods:
- ✅ CTAS (Create Table As Select)
- ✅ ONLINE (DBMS_REDEFINITION)
- ✅ EXCHANGE (Partition Exchange)

### Analysis

The analysis engine examines tables and recommends strategies:

#### Single Table Analysis

```sql
EXEC table_migration_analyzer.analyze_table(p_task_id => 1);
```

#### Batch Analysis

```sql
-- Analyze all pending tasks in a project
EXEC table_migration_analyzer.analyze_all_pending_tasks(p_project_id => 1);
```

#### Analysis Results

```sql
SELECT
    t.task_name,
    a.table_rows,
    ROUND(a.table_size_mb, 2) AS size_mb,
    a.recommended_strategy,
    a.recommendation_reason,
    a.estimated_partitions,
    ROUND(a.avg_partition_size_mb, 2) AS avg_part_size_mb,
    a.complexity_score,
    a.estimated_downtime_minutes,
    t.validation_status
FROM migration_tasks t
JOIN migration_analysis a ON a.task_id = t.task_id
WHERE t.project_id = 1;
```

#### Blocking Issues

```sql
-- Check for issues that prevent migration
SELECT
    t.task_name,
    a.blocking_issues,
    a.complexity_factors
FROM migration_tasks t
JOIN migration_analysis a ON a.task_id = t.task_id
WHERE t.validation_status = 'BLOCKED';
```

### Migration Execution

#### Execution Methods

| Method | Description | Use Case | Downtime |
|--------|-------------|----------|----------|
| **CTAS** | Create Table As Select | Most tables, standard migration | Seconds to minutes |
| **ONLINE** | DBMS_REDEFINITION | Large tables, minimal downtime | Near-zero (EE only) |
| **EXCHANGE** | Exchange partition | Staging tables, ETL loads | Seconds (instant) |

#### Execute Single Migration

```sql
EXEC table_migration_executor.execute_migration(p_task_id => 1);
```

#### Execute Multiple Migrations

```sql
-- Execute up to 3 ready tasks
EXEC table_migration_executor.execute_all_ready_tasks(
    p_project_id => 1,
    p_max_tasks => 3
);
```

#### Migration Process

The executor performs these steps:

1. **Validate** - Check task status and prerequisites
2. **Backup** - Create backup of original table (if enabled)
3. **Create** - Build new partitioned table structure
4. **Copy** - Transfer data to partitioned table
5. **Indexes** - Recreate local indexes
6. **Constraints** - Recreate constraints
7. **Statistics** - Gather fresh statistics
8. **Swap** - Rename tables (old→backup, new→original)
9. **ILM** - Apply ILM policies (if enabled)
10. **Validate** - Verify row counts match

#### Constraint Recreation Behavior

The framework recreates all constraints from the source table in a specific order to ensure dependencies are met:

##### Order of Recreation

1. **PRIMARY KEY Constraints** (Highest Priority)
   - Created first
   - **CRITICAL**: Migration fails if PK creation fails
   - Uses `DBMS_METADATA.GET_DDL` for accurate extraction
   - Preserves all PK features: deferrable, enable/disable, etc.

2. **CHECK & UNIQUE Constraints**
   - Created after PRIMARY KEY
   - **CRITICAL**: Migration fails if creation fails
   - Skips NOT NULL constraints (already in column definitions)
   - Includes user-created CHECK constraints

3. **FOREIGN KEY Constraints** (Lowest Priority)
   - Created last (depends on PKs existing)
   - **NON-CRITICAL**: Migration continues with WARNING if FK creation fails
   - Uses `DBMS_METADATA.GET_DDL('REF_CONSTRAINT', ...)`

##### Foreign Key Special Handling

FOREIGN KEY constraints receive special treatment because they reference other tables:

**Behavior:**
- FK creation failures produce **WARNING** (not ERROR)
- Migration continues even if FK creation fails
- Failure logged to `dwh_migration_execution_log`

**Why FKs May Fail:**
- Referenced table hasn't been migrated yet
- Referenced table is in a different schema
- Referenced table is being migrated in parallel

**Best Practice for Batch Migrations:**

When migrating multiple related tables:

```sql
-- Step 1: Migrate all tables (FKs will fail with warnings)
BEGIN
    FOR task IN (
        SELECT task_id FROM cmr.dwh_migration_tasks
        WHERE project_id = 123
        AND status = 'READY'
    ) LOOP
        pck_dwh_table_migration_executor.execute_migration(task.task_id);
    END LOOP;
END;
/

-- Step 2: Check which FK constraints failed
SELECT
    t.task_name,
    t.source_table,
    l.step_name,
    l.error_message
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE l.step_name LIKE 'Recreate FK constraint:%'
AND l.status = 'FAILED'
ORDER BY t.task_id, l.step_number;

-- Step 3: Manually recreate failed FK constraints
-- After all referenced tables are migrated, use DBMS_METADATA to get FK DDL
-- from source table and execute on migrated table
```

**Example: Manual FK Recreation**

```sql
-- Get FK constraint DDL from source table
SELECT DBMS_METADATA.GET_DDL('REF_CONSTRAINT', 'FK_ORDER_CUSTOMER', 'SCHEMA_NAME')
FROM DUAL;

-- Replace source table name with migrated table name
-- Execute the modified DDL on the migrated table
```

##### Error Handling Summary

| Constraint Type | Error Behavior | Migration Continues? |
|----------------|----------------|---------------------|
| PRIMARY KEY | FAIL with `-20503` | ❌ No - Critical |
| CHECK | FAIL with `-20501` | ❌ No - Critical |
| UNIQUE | FAIL with `-20501` | ❌ No - Critical |
| FOREIGN KEY | WARN (logged) | ✅ Yes - Manual fix |

### Monitoring

#### Project Dashboard

```sql
SELECT * FROM v_migration_dashboard
ORDER BY created_date DESC;
```

#### Task Status

```sql
SELECT * FROM v_migration_task_status
WHERE project_name = 'Q1_MIGRATION'
ORDER BY task_id;
```

#### Execution Log

```sql
SELECT
    step_number,
    step_name,
    step_type,
    TO_CHAR(start_time, 'HH24:MI:SS') AS start_time,
    duration_seconds,
    status,
    error_message
FROM migration_execution_log
WHERE task_id = 1
ORDER BY step_number;
```

#### Running Migrations

```sql
-- Monitor currently running migrations
SELECT
    t.task_name,
    t.source_table,
    TO_CHAR(t.execution_start, 'YYYY-MM-DD HH24:MI:SS') AS started,
    ROUND((SYSTIMESTAMP - t.execution_start) * 24 * 60, 1) AS minutes_running,
    l.step_name AS current_step
FROM migration_tasks t
JOIN (
    SELECT task_id, step_name
    FROM (
        SELECT task_id, step_name,
               ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY step_number DESC) AS rn
        FROM migration_execution_log
    )
    WHERE rn = 1
) l ON l.task_id = t.task_id
WHERE t.status = 'RUNNING';
```

### ILM Integration

#### Policy Templates

```sql
-- View available templates
SELECT * FROM migration_ilm_templates;

-- Create custom template
INSERT INTO migration_ilm_templates (
    template_name,
    description,
    table_type,
    policies_json
) VALUES (
    'CUSTOM_FACT',
    'Custom fact table policies',
    'FACT',
    '[{"policy_name": "{TABLE}_COMPRESS", "age_days": 60, "action": "COMPRESS"}]'
);
```

#### Apply Templates

Templates are applied automatically if `apply_ilm_policies = 'Y'` in the migration task.

### Validation & Rollback

#### Validate Migration

```sql
EXEC table_migration_executor.validate_migration(p_task_id => 1);
```

#### Check Rollback Capability

```sql
SELECT
    task_id,
    task_name,
    backup_table_name,
    can_rollback
FROM migration_tasks
WHERE can_rollback = 'Y';
```

#### Rollback Migration

```sql
-- WARNING: This reverts to the old non-partitioned table
EXEC table_migration_executor.rollback_migration(p_task_id => 1);
```

#### Clean Up Backups

```sql
-- After confirming migration success, drop backup tables
DECLARE
    v_sql VARCHAR2(1000);
BEGIN
    FOR rec IN (
        SELECT source_owner, backup_table_name
        FROM migration_tasks
        WHERE backup_table_name IS NOT NULL
        AND status = 'COMPLETED'
        AND execution_end < SYSDATE - 30  -- Older than 30 days
    ) LOOP
        v_sql := 'DROP TABLE ' || rec.source_owner || '.' ||
                rec.backup_table_name || ' PURGE';
        EXECUTE IMMEDIATE v_sql;

        UPDATE migration_tasks
        SET can_rollback = 'N', backup_table_name = NULL
        WHERE backup_table_name = rec.backup_table_name;
    END LOOP;
    COMMIT;
END;
/
```

## Configuration

```sql
-- View configuration
SELECT * FROM ilm_config WHERE config_key LIKE 'MIGRATION%';

-- Disable automatic backups
UPDATE ilm_config SET config_value = 'N'
WHERE config_key = 'MIGRATION_BACKUP_ENABLED';

-- Adjust parallel degree
UPDATE ilm_config SET config_value = '8'
WHERE config_key = 'MIGRATION_PARALLEL_DEGREE';

COMMIT;
```

## Best Practices

### 1. Start Small

- Begin with one non-critical table
- Test the entire workflow
- Verify before scaling up

### 2. Analyze First

- Always run analysis before execution
- Review complexity scores
- Check for blocking issues

### 3. Test in Non-Production

- Practice on dev/test environments
- Understand timing and resource requirements
- Develop rollback procedures

### 4. Plan Downtime Windows

- Even "online" migrations have brief locks
- Plan migrations during low-activity periods
- Communicate to stakeholders

### 5. Monitor Resources

- Watch temp space usage during data copy
- Monitor redo log generation
- Check for table/index locks

### 6. Validate Thoroughly

- Compare row counts
- Verify constraints
- Test application queries
- Check performance

### 7. Gradual Rollout

```sql
-- Migrate tables by priority
-- 1. Staging tables (short retention, easy to recreate)
-- 2. Small fact tables
-- 3. Large fact tables
-- 4. Large dimensions (if needed)
```

### 8. Keep Backups Initially

- Wait 30+ days before dropping backup tables
- Verify application compatibility
- Confirm performance improvements

### 9. Enable Row Movement

- **Always enable row movement** for partitioned tables (default: 'Y')
- Required for ILM policies to work correctly
- Prevents ORA-14402 errors when updating partition keys
- Essential for interval partitioning
- Only disable if partition key columns are guaranteed immutable

```sql
-- Verify row movement is enabled after migration
SELECT table_name, row_movement
FROM dba_tables
WHERE table_name = 'SALES_FACT';

-- Enable manually if needed
ALTER TABLE sales_fact ENABLE ROW MOVEMENT;
```

## Non-Standard Date Column Handling

The migration framework automatically detects and converts date columns stored as NUMBER or VARCHAR/CHAR types during migration.

### Supported Formats

#### NUMBER-Based Dates

| Format | Example | Range | Description |
|--------|---------|-------|-------------|
| **YYYYMMDD** | 20241002 | 19000101-21001231 | 8-digit numeric date |
| **YYMMDD** | 241002 | 000101-991231 | 6-digit numeric date |
| **UNIX_TIMESTAMP** | 1727894400 | 946684800-2147483647 | Seconds since 1970-01-01 |

#### VARCHAR/CHAR-Based Dates

| Format | Example | Description |
|--------|---------|-------------|
| **YYYY-MM-DD** | '2024-10-02' | ISO date format |
| **DD/MM/YYYY** | '02/10/2024' | European format |
| **MM/DD/YYYY** | '10/02/2024' | US format |
| **YYYYMMDD** | '20241002' | Compact string format |
| **YYYY-MM-DD HH24:MI:SS** | '2024-10-02 14:30:00' | ISO datetime |

### How It Works

1. **Automatic Detection**: Framework scans for date-like column names (containing DATE, TIME, DTTM, etc.)
2. **Format Identification**: Samples data to determine the format
3. **Conversion During Migration**: Creates new DATE column with `_CONVERTED` suffix
4. **Partitioning**: Uses converted column for partitioning

### Example: NUMBER Date Column

```sql
-- Source table with NUMBER date column
CREATE TABLE legacy_sales (
    sale_id NUMBER(18),
    sale_date NUMBER(8),  -- YYYYMMDD format: 20241002
    amount NUMBER(12,2)
);

-- Create migration task
INSERT INTO migration_tasks (
    task_name, source_table, migration_method
) VALUES (
    'Migrate LEGACY_SALES', 'LEGACY_SALES', 'CTAS'
);

-- Analyze table (detects NUMBER date format)
EXEC table_migration_analyzer.analyze_table(1);

-- Check analysis results
SELECT
    date_column_name,
    date_column_type,
    date_format_detected,
    date_conversion_expr,
    recommended_strategy
FROM migration_analysis
WHERE task_id = 1;

-- Results:
-- date_column_name: SALE_DATE
-- date_column_type: NUMBER
-- date_format_detected: YYYYMMDD
-- date_conversion_expr: TO_DATE(TO_CHAR(sale_date), 'YYYYMMDD')
-- recommended_strategy: RANGE(sale_date_CONVERTED) INTERVAL MONTHLY

-- Execute migration
EXEC table_migration_executor.execute_migration(1);

-- Resulting partitioned table structure:
-- LEGACY_SALES (
--     sale_id NUMBER(18),
--     sale_date NUMBER(8),
--     sale_date_CONVERTED DATE NOT NULL,  -- Converted column
--     amount NUMBER(12,2)
-- ) PARTITION BY RANGE (sale_date_CONVERTED) ...
```

### Example: VARCHAR Date Column

```sql
-- Source table with VARCHAR date column
CREATE TABLE event_log (
    event_id NUMBER(18),
    event_date VARCHAR2(10),  -- Format: 'YYYY-MM-DD'
    event_type VARCHAR2(50)
);

-- Migration automatically detects and converts
-- Resulting table has event_date_CONVERTED as DATE type
```

### Example: Unix Timestamp

```sql
-- Source table with Unix timestamp
CREATE TABLE sensor_data (
    sensor_id NUMBER(12),
    capture_time NUMBER(10),  -- Unix timestamp
    temperature NUMBER(5,2)
);

-- Conversion: TO_DATE('1970-01-01', 'YYYY-MM-DD') + (capture_time / 86400)
-- Result: capture_time_CONVERTED DATE
```

### Manual Override

If automatic detection fails or you want to specify conversion manually:

```sql
-- Update migration task with explicit conversion
UPDATE migration_tasks
SET partition_type = 'RANGE(sale_date)',
    partition_key = 'TO_DATE(TO_CHAR(sale_date), ''YYYYMMDD'')'
WHERE task_id = 1;
```

### Column Naming

- **Original Column**: Retained as-is for backward compatibility
- **Converted Column**: Original name + `_CONVERTED` suffix
- **Partition Key**: Uses converted column

### Analysis View

Query to see all tables with non-standard dates:

```sql
SELECT
    t.task_name,
    t.source_table,
    a.date_column_name,
    a.date_column_type,
    a.date_format_detected,
    a.requires_conversion
FROM migration_tasks t
JOIN migration_analysis a ON a.task_id = t.task_id
WHERE a.requires_conversion = 'Y'
ORDER BY t.task_id;
```

### Limitations

- Only one date column per table can be auto-converted
- Column must have date-related naming (DATE, TIME, DTTM, etc.)
- Conversion happens during CTAS; not supported for online redefinition yet
- Original column retained for compatibility (may increase storage)

### Best Practices

1. **Verify Detection**: Always check analysis results before migration
2. **Test Conversion**: Validate conversion on sample data first
3. **Update Applications**: Modify queries to use `_CONVERTED` column for partitioning pruning
4. **Consider Cleanup**: Drop original column after migration if not needed (requires app changes)

## Troubleshooting

### Migration Fails

```sql
-- Find failed migrations
SELECT
    task_id,
    task_name,
    error_message,
    (SELECT step_name FROM migration_execution_log
     WHERE task_id = t.task_id AND status = 'FAILED'
     ORDER BY step_number DESC FETCH FIRST 1 ROW ONLY) AS failed_step
FROM migration_tasks t
WHERE status = 'FAILED';

-- View detailed error
SELECT * FROM migration_execution_log
WHERE task_id = 1 AND status = 'FAILED'
ORDER BY step_number;
```

### Insufficient Space

- Ensure adequate temp space
- Check target tablespace has room
- Consider smaller batch sizes

### Lock Timeout

- Ensure no active sessions on table
- Check for long-running queries
- Retry during quieter period

### Retry Failed Migration

```sql
-- Reset task status
UPDATE migration_tasks
SET status = 'READY',
    error_message = NULL,
    execution_start = NULL
WHERE task_id = 1;
COMMIT;

-- Fix underlying issue, then retry
EXEC table_migration_executor.execute_migration(1);
```

## Performance Tuning

### Parallel Degree

```sql
-- Increase parallelism for large tables
UPDATE migration_tasks
SET parallel_degree = 16
WHERE source_table = 'LARGE_FACT_TABLE';
```

### Compression During Migration

```sql
-- Apply compression during copy
UPDATE migration_tasks
SET use_compression = 'Y',
    compression_type = 'QUERY HIGH'
WHERE task_id = 1;
```

### Nologging (Use with Caution)

```sql
-- For faster loads, but data not recoverable until backup
-- ALTER TABLE ... NOLOGGING
-- Implemented in migration if needed
```

## Partitioning Strategy Recommendations

### SCD2 Tables (Slowly Changing Dimension Type 2)

#### Pattern 1: effective_date + current_flag

```sql
-- Detected by: effective_date/expiry_date columns with current_flag
partition_type: 'RANGE(effective_date)'
interval_clause: 'NUMTOYMINTERVAL(1,''YEAR'')'
recommended_ilm: 'SCD2_EFFECTIVE_DATE'

-- Example:
CREATE TABLE customer_dim_scd2 (
    customer_sk NUMBER(18),
    effective_date DATE NOT NULL,
    expiry_date DATE DEFAULT DATE '9999-12-31',
    current_flag CHAR(1) DEFAULT 'Y',
    ...
) PARTITION BY RANGE (effective_date)
  INTERVAL (NUMTOYMINTERVAL(1,'YEAR'));
```

#### Pattern 2: valid_from_dttm/valid_to_dttm + is_current

```sql
-- Detected by: valid_from_dttm/valid_to_dttm columns with is_current
partition_type: 'RANGE(valid_from_dttm)'
interval_clause: 'NUMTOYMINTERVAL(1,''YEAR'')'
recommended_ilm: 'SCD2_VALID_FROM_TO'

-- Example:
CREATE TABLE employee_dim_scd2 (
    employee_sk NUMBER(18),
    valid_from_dttm TIMESTAMP NOT NULL,
    valid_to_dttm TIMESTAMP DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current CHAR(1) DEFAULT 'Y',
    ...
) PARTITION BY RANGE (valid_from_dttm)
  INTERVAL (NUMTOYMINTERVAL(1,'YEAR'));
```

**Migration Notes for SCD2:**
- Historical versions remain in older partitions
- Current records in latest partitions
- Yearly intervals prevent partition explosion
- ILM policies preserve compliance history
- Compress old versions after 1-2 years

### Events Tables (High-Volume Time-Series)

#### Clickstream/IoT Events (Daily Partitioning)

```sql
-- Detected by: EVENT_DTTM column or event-related table names
partition_type: 'RANGE(event_dttm) SUBPARTITION BY LIST(event_type)'
interval_clause: 'NUMTODSINTERVAL(1,''DAY'')'
recommended_ilm: 'EVENTS_SHORT_RETENTION'

-- Example:
CREATE TABLE app_events (
    event_id NUMBER(18),
    event_dttm TIMESTAMP NOT NULL,
    event_type VARCHAR2(50),
    ...
) PARTITION BY RANGE (event_dttm)
  SUBPARTITION BY LIST (event_type)
  INTERVAL (NUMTODSINTERVAL(1,'DAY'));
```

#### Audit/Compliance Events (Monthly Partitioning)

```sql
-- Detected by: AUDIT_DTTM or table name containing AUDIT
partition_type: 'RANGE(audit_dttm)'
interval_clause: 'NUMTOYMINTERVAL(1,''MONTH'')'
recommended_ilm: 'EVENTS_COMPLIANCE'

-- Longer retention (7 years) with archive compression
```

**Migration Notes for Events:**
- Daily partitions for clickstream/IoT (high volume)
- Monthly partitions for audit/compliance
- Aggressive compression after 30-90 days
- Automated purging based on retention policy
- Consider partition-wise parallel loads

### Staging Tables (ETL/CDC)

#### Transactional Staging (7-day retention)

```sql
-- Detected by: STG_ prefix or LOAD_DTTM column
partition_type: 'RANGE(load_dttm)'
interval_clause: 'NUMTODSINTERVAL(1,''DAY'')'
recommended_ilm: 'STAGING_7DAY'

-- Example:
CREATE TABLE stg_sales_transactions (
    staging_id NUMBER(18),
    load_dttm TIMESTAMP DEFAULT SYSTIMESTAMP,
    processing_status VARCHAR2(20),
    ...
) PARTITION BY RANGE (load_dttm)
  INTERVAL (NUMTODSINTERVAL(1,'DAY'))
  NOLOGGING;
```

#### CDC Staging (30-day retention)

```sql
partition_type: 'RANGE(cdc_timestamp)'
interval_clause: 'NUMTODSINTERVAL(1,''DAY'')'
recommended_ilm: 'STAGING_CDC'
```

#### Error Quarantine (1-year retention)

```sql
partition_type: 'RANGE(error_dttm)'
interval_clause: 'NUMTODSINTERVAL(1,''DAY'')'
recommended_ilm: 'STAGING_ERROR_QUARANTINE'
```

**Migration Notes for Staging:**
- NOLOGGING for faster loads
- Daily partitions with short retention
- Automatic purging after processing
- No compression (temporary data)
- Parallel DML enabled

### Date-Based Data (Recommended: RANGE)

```sql
-- Monthly interval partitioning
partition_type: 'RANGE(transaction_date)'
interval_clause: 'NUMTOYMINTERVAL(1,''MONTH'')'
```

### High-Volume Uniform Data (Recommended: HASH)

```sql
-- Even distribution
partition_type: 'HASH(customer_id) PARTITIONS 16'
```

### Categorical Data (Recommended: LIST)

```sql
-- By region/category
partition_type: 'LIST(region)'
```

### Complex Requirements (Recommended: COMPOSITE)

```sql
-- Range-Hash for time-series with even distribution
partition_type: 'RANGE(order_date)'
subpartition_type: 'HASH(order_id) SUBPARTITIONS 8'
interval_clause: 'NUMTOYMINTERVAL(1,''MONTH'')'
```

## Security

### Grants

```sql
-- Grant migration privileges
GRANT SELECT, INSERT, UPDATE ON migration_projects TO migration_admin;
GRANT SELECT, INSERT, UPDATE ON migration_tasks TO migration_admin;
GRANT EXECUTE ON table_migration_analyzer TO migration_admin;
GRANT EXECUTE ON table_migration_executor TO migration_admin;
```

### Audit

All actions are logged:

```sql
-- View audit trail
SELECT
    task_name,
    created_by,
    created_date,
    execution_start,
    execution_end,
    status
FROM migration_tasks
ORDER BY created_date DESC;
```

## Integration with Custom ILM

The migration framework integrates seamlessly with the custom ILM framework:

1. **Partitioned tables created** during migration
2. **ILM policies applied** automatically from templates
3. **Lifecycle management** begins immediately
4. **No manual intervention** required

## Limitations

- **Online redefinition**: Not fully implemented (uses CTAS fallback)
- **Exchange partition**: Not fully implemented (uses CTAS fallback)
- **IOT/Materialized Views**: Not supported
- **Cross-schema migration**: Requires additional grants

## Future Enhancements

- Full online redefinition support
- Parallel table migrations
- Pre-migration impact analysis
- Post-migration performance comparison
- Automated rollback on validation failure

## Summary

The Table Migration Framework provides enterprise-grade capability for converting non-partitioned tables to partitioned structures with:

- ✅ Automated analysis and recommendations
- ✅ Multiple migration strategies
- ✅ Integrated ILM policy application
- ✅ Complete audit trail
- ✅ Validation and rollback
- ✅ Project-based organization
- ✅ Production-ready reliability

Combined with the Custom ILM Framework, you have a complete solution for managing table partitioning and lifecycle management in Oracle data warehouses.
