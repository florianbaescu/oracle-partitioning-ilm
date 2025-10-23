---
# Custom PL/SQL ILM Framework Guide

A complete, metadata-driven Information Lifecycle Management framework implemented in PL/SQL without Oracle ADO dependencies.

## Overview

This custom ILM framework provides full control over partition lifecycle management through:
- **Policy-driven**: Define rules in metadata tables
- **Automated**: Scheduler jobs for evaluation and execution
- **Auditable**: Complete execution history and logging
- **Flexible**: Custom conditions and actions
- **Independent**: No ADO or Heat Map required (though can leverage them)

## Architecture

### Components

```
┌─────────────────────────────────────────────┐
│         ILM Framework Components            │
├─────────────────────────────────────────────┤
│                                             │
│  ┌──────────────┐      ┌──────────────┐   │
│  │   Metadata   │      │  Scheduler   │   │
│  │    Tables    │──────│     Jobs     │   │
│  └──────────────┘      └──────────────┘   │
│         │                      │           │
│         ▼                      ▼           │
│  ┌──────────────┐      ┌──────────────┐   │
│  │    Policy    │      │  Execution   │   │
│  │    Engine    │◄─────│    Engine    │   │
│  └──────────────┘      └──────────────┘   │
│         │                      │           │
│         ▼                      ▼           │
│  ┌──────────────┐      ┌──────────────┐   │
│  │  Evaluation  │      │  Execution   │   │
│  │    Queue     │──────│     Log      │   │
│  └──────────────┘      └──────────────┘   │
│                                             │
└─────────────────────────────────────────────┘
```

### Data Flow

1. **Policy Definition**: Define ILM policies in `cmr.dwh_ilm_policies` table
2. **Access Tracking**: Track partition access in `ilm_partition_access`
3. **Evaluation**: Policy engine identifies eligible partitions
4. **Queuing**: Eligible partitions added to `ilm_evaluation_queue`
5. **Execution**: Execution engine performs actions
6. **Logging**: Results recorded in `cmr.dwh_ilm_execution_log`

## Installation

### Step 1: Create Framework

```sql
-- Run setup scripts in order
@scripts/custom_ilm_setup.sql
@scripts/custom_ilm_policy_engine.sql
@scripts/custom_ilm_execution_engine.sql
@scripts/custom_ilm_scheduler.sql
```

### Step 2: Verify Installation

```sql
-- Check metadata tables
SELECT table_name FROM user_tables WHERE table_name LIKE 'ILM_%';

-- Check packages
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name LIKE 'ILM_%';

-- Check scheduler jobs
SELECT * FROM v_ilm_scheduler_status;
```

### Step 3: Configure

```sql
-- Set execution window (optional)
UPDATE cmr.dwh_ilm_config SET config_value = '22:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config SET config_value = '06:00'
WHERE config_key = 'EXECUTION_WINDOW_END';

COMMIT;
```

## Usage

### Define Policies

#### Basic Compression Policy

```sql
INSERT INTO cmr.dwh_ilm_policies (
    policy_name,
    table_owner,
    table_name,
    policy_type,
    action_type,
    age_days,
    compression_type,
    priority,
    enabled
) VALUES (
    'COMPRESS_SALES_90D',
    USER,
    'SALES_FACT',
    'COMPRESSION',
    'COMPRESS',
    90,                    -- Age threshold
    'QUERY HIGH',
    100,                   -- Priority (lower = earlier)
    'Y'
);
COMMIT;
```

#### Multi-Tier Storage Policy

```sql
-- Warm tier (3 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, target_tablespace, compression_type, priority, enabled
) VALUES (
    'TIER_WARM_3M', USER, 'SALES_FACT', 'TIERING', 'MOVE',
    3, 'TBS_WARM', 'QUERY HIGH', 100, 'Y'
);

-- Cold tier (12 months)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, target_tablespace, compression_type, priority, enabled
) VALUES (
    'TIER_COLD_12M', USER, 'SALES_FACT', 'TIERING', 'MOVE',
    12, 'TBS_COLD', 'ARCHIVE HIGH', 200, 'Y'
);

-- Archive tier (36 months) + Read-only
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, target_tablespace, compression_type, priority, enabled
) VALUES (
    'TIER_ARCHIVE_36M', USER, 'SALES_FACT', 'TIERING', 'MOVE',
    36, 'TBS_ARCHIVE', 'ARCHIVE HIGH', 300, 'Y'
);

INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'READONLY_36M', USER, 'SALES_FACT', 'ARCHIVAL', 'READ_ONLY',
    36, 301, 'Y'
);

COMMIT;
```

#### Purge Policy

```sql
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_months, priority, enabled
) VALUES (
    'PURGE_SALES_84M', USER, 'SALES_FACT', 'PURGE', 'DROP',
    84,  -- 7 years
    900,
    'Y'
);
COMMIT;
```

#### Advanced: Custom Conditions

```sql
-- Compress only partitions matching specific criteria
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, custom_condition, compression_type, priority, enabled
) VALUES (
    'COMPRESS_COMPLETED_ORDERS', USER, 'ORDER_FACT', 'COMPRESSION', 'COMPRESS',
    60, 'order_status = ''COMPLETED''', 'QUERY HIGH', 100, 'Y'
);
COMMIT;
```

#### Advanced: Size-Based Policy

```sql
-- Compress only large partitions
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, size_threshold_mb, compression_type, priority, enabled
) VALUES (
    'COMPRESS_LARGE_PARTITIONS', USER, 'SALES_FACT', 'COMPRESSION', 'COMPRESS',
    30, 1024,  -- Only if > 1 GB
    'ARCHIVE HIGH', 100, 'Y'
);
COMMIT;
```

#### Policy Templates by Table Type

The framework includes predefined policy templates for common table types used with the Table Migration Framework:

**SCD2 Tables - effective_date Pattern**
```sql
-- Template: SCD2_EFFECTIVE_DATE
-- Preserves historical versions with gradual compression

-- Compress versions older than 1 year
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'CUSTOMER_COMPRESS_365D', USER, 'CUSTOMER_DIM_SCD2', 'COMPRESSION', 'COMPRESS',
    365, 'QUERY HIGH', 100, 'Y'
);

-- Archive compress versions older than 3 years
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'CUSTOMER_ARCHIVE_1095D', USER, 'CUSTOMER_DIM_SCD2', 'COMPRESSION', 'COMPRESS',
    1095, 'ARCHIVE HIGH', 200, 'Y'
);

-- Read-only after 5 years (compliance)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'CUSTOMER_READONLY_1825D', USER, 'CUSTOMER_DIM_SCD2', 'ARCHIVAL', 'READ_ONLY',
    1825, 300, 'Y'
);
COMMIT;
```

**SCD2 Tables - valid_from_dttm Pattern**
```sql
-- Template: SCD2_VALID_FROM_TO
-- Same lifecycle as effective_date pattern

-- Compress historical records older than 1 year
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'EMPLOYEE_COMPRESS_365D', USER, 'EMPLOYEE_DIM_SCD2', 'COMPRESSION', 'COMPRESS',
    365, 'QUERY HIGH', 100, 'Y'
);
COMMIT;
```

**Events Tables - Short Retention (90 days)**
```sql
-- Template: EVENTS_SHORT_RETENTION
-- For clickstream, IoT, and high-volume events

-- Compress after 7 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'EVENTS_COMPRESS_7D', USER, 'APP_EVENTS', 'COMPRESSION', 'COMPRESS',
    7, 'QUERY HIGH', 100, 'Y'
);

-- Archive compress after 30 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'EVENTS_ARCHIVE_30D', USER, 'APP_EVENTS', 'COMPRESSION', 'COMPRESS',
    30, 'ARCHIVE HIGH', 200, 'Y'
);

-- Purge after 90 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'EVENTS_PURGE_90D', USER, 'APP_EVENTS', 'PURGE', 'DROP',
    90, 900, 'Y'
);
COMMIT;
```

**Events Tables - Compliance (7 years)**
```sql
-- Template: EVENTS_COMPLIANCE
-- For audit logs and regulatory compliance

-- Compress after 90 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'AUDIT_COMPRESS_90D', USER, 'AUDIT_EVENTS', 'COMPRESSION', 'COMPRESS',
    90, 'QUERY HIGH', 100, 'Y'
);

-- Archive compress after 1 year
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'AUDIT_ARCHIVE_365D', USER, 'AUDIT_EVENTS', 'COMPRESSION', 'COMPRESS',
    365, 'ARCHIVE HIGH', 200, 'Y'
);

-- Read-only after 2 years
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'AUDIT_READONLY_730D', USER, 'AUDIT_EVENTS', 'ARCHIVAL', 'READ_ONLY',
    730, 300, 'Y'
);

-- Purge after 7 years (regulatory requirement)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'AUDIT_PURGE_2555D', USER, 'AUDIT_EVENTS', 'PURGE', 'DROP',
    2555, 900, 'Y'
);
COMMIT;
```

**Staging Tables - 7 Day Retention**
```sql
-- Template: STAGING_7DAY
-- For transactional staging tables

-- Purge after 7 days (no compression needed)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'STG_PURGE_7D', USER, 'STG_SALES_TRANSACTIONS', 'PURGE', 'DROP',
    7, 100, 'Y'
);
COMMIT;
```

**Staging Tables - CDC (30 days)**
```sql
-- Template: STAGING_CDC
-- For change data capture staging

-- Purge after 30 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'CDC_PURGE_30D', USER, 'STG_CDC_CHANGES', 'PURGE', 'DROP',
    30, 100, 'Y'
);
COMMIT;
```

**Staging Tables - Error Quarantine (1 year)**
```sql
-- Template: STAGING_ERROR_QUARANTINE
-- For error/exception records requiring investigation

-- Compress after 30 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, compression_type, priority, enabled
) VALUES (
    'ERROR_COMPRESS_30D', USER, 'STG_ERROR_QUARANTINE', 'COMPRESSION', 'COMPRESS',
    30, 'QUERY HIGH', 100, 'Y'
);

-- Purge after 1 year
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, priority, enabled
) VALUES (
    'ERROR_PURGE_365D', USER, 'STG_ERROR_QUARANTINE', 'PURGE', 'DROP',
    365, 200, 'Y'
);
COMMIT;
```

> **Note**: These templates are automatically applied by the Table Migration Framework when `apply_ilm_policies = 'Y'` is set in the migration task. See [Table Migration Guide](table_migration_guide.md) for details.

### Run ILM Cycle

#### Automatic Execution

```sql
-- Enable automatic execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Jobs will run automatically per schedule
-- Default: Evaluate daily at 2 AM, Execute every 2 hours
```

#### Manual Execution

```sql
-- Complete ILM cycle
EXEC dwh_run_ilm_cycle();

-- Or run individual steps:
EXEC dwh_refresh_partition_access_tracking();
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions();
```

#### Execute Specific Policy

```sql
-- Evaluate and execute specific policy
EXEC ilm_policy_engine.evaluate_policy(1);  -- policy_id
EXEC ilm_execution_engine.execute_policy(1);
```

#### Direct Execution (Bypass Queue)

```sql
-- Compress partition directly
EXEC ilm_execution_engine.compress_partition(
    p_table_owner => USER,
    p_table_name => 'SALES_FACT',
    p_partition_name => 'P_2023_01',
    p_compression_type => 'ARCHIVE HIGH',
    p_rebuild_indexes => TRUE,
    p_gather_stats => TRUE
);

-- Move partition
EXEC ilm_execution_engine.move_partition(
    p_table_owner => USER,
    p_table_name => 'SALES_FACT',
    p_partition_name => 'P_2023_01',
    p_target_tablespace => 'TBS_COLD',
    p_compression_type => 'ARCHIVE HIGH'
);
```

## Monitoring

### Active Policies

```sql
SELECT * FROM v_ilm_active_policies
ORDER BY priority;
```

### Execution Statistics

```sql
SELECT * FROM v_ilm_execution_stats
ORDER BY total_space_saved_mb DESC;
```

### Partition Temperature

```sql
SELECT * FROM v_ilm_partition_temperature
WHERE table_name = 'SALES_FACT'
ORDER BY days_since_write DESC;
```

### Recent Executions

```sql
SELECT
    execution_id,
    policy_name,
    partition_name,
    action_type,
    TO_CHAR(execution_start, 'YYYY-MM-DD HH24:MI') AS exec_time,
    duration_seconds,
    status,
    space_saved_mb
FROM cmr.dwh_ilm_execution_log
ORDER BY execution_start DESC
FETCH FIRST 20 ROWS ONLY;
```

### Space Savings Report

```sql
SELECT
    table_name,
    COUNT(*) AS partitions_processed,
    ROUND(SUM(space_saved_mb), 2) AS total_saved_mb,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
GROUP BY table_name
ORDER BY total_saved_mb DESC;
```

### Scheduler Status

```sql
-- Job status
SELECT * FROM v_ilm_scheduler_status;

-- Job history
SELECT * FROM v_ilm_job_history
WHERE log_date > SYSDATE - 7
ORDER BY log_date DESC;
```

## Policy Configuration Reference

### Policy Types

| Type | Description | Actions |
|------|-------------|---------|
| COMPRESSION | Apply compression | COMPRESS |
| TIERING | Move to different storage tier | MOVE |
| ARCHIVAL | Archive old data | MOVE, READ_ONLY |
| PURGE | Remove old data | DROP, TRUNCATE |
| CUSTOM | Custom action | CUSTOM |

### Action Types

| Action | Description | Parameters |
|--------|-------------|------------|
| COMPRESS | Compress partition | compression_type |
| MOVE | Move to tablespace | target_tablespace, compression_type |
| READ_ONLY | Make read-only | None |
| DROP | Drop partition | None |
| TRUNCATE | Truncate partition | None |
| CUSTOM | Custom PL/SQL | custom_action |

### Compression Types

- `QUERY LOW` - Basic query compression (4-6x ratio)
- `QUERY HIGH` - Advanced query compression (6-10x ratio)
- `ARCHIVE LOW` - Basic archive compression (10-20x ratio)
- `ARCHIVE HIGH` - Advanced archive compression (15-50x ratio)

### Condition Criteria

| Criterion | Description |
|-----------|-------------|
| age_days | Partition age in days |
| age_months | Partition age in months |
| access_pattern | HOT, WARM, or COLD |
| size_threshold_mb | Minimum partition size |
| custom_condition | Custom SQL WHERE clause |

### Priority

- Lower number = Higher priority (executes first)
- Use gaps (100, 200, 300) for easier insertion
- Related policies should have sequential priorities

## Configuration Parameters

```sql
SELECT * FROM cmr.dwh_ilm_config ORDER BY config_key;
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| ENABLE_AUTO_EXECUTION | Y | Enable automatic execution |
| EXECUTION_WINDOW_START | 22:00 | Start time for ILM operations |
| EXECUTION_WINDOW_END | 06:00 | End time for ILM operations |
| MAX_CONCURRENT_OPERATIONS | 4 | Max concurrent partition ops |
| ACCESS_TRACKING_ENABLED | Y | Enable access tracking |
| HOT_THRESHOLD_DAYS | 90 | HOT temperature threshold |
| WARM_THRESHOLD_DAYS | 365 | WARM temperature threshold |
| COLD_THRESHOLD_DAYS | 1095 | COLD temperature threshold |
| LOG_RETENTION_DAYS | 365 | Log retention period |

## Best Practices

### 1. Start Conservative

```sql
-- Begin with one policy and test
INSERT INTO cmr.dwh_ilm_policies (...) VALUES (...);

-- Evaluate without executing
EXEC ilm_policy_engine.evaluate_policy(1);

-- Review what would be executed
SELECT * FROM ilm_evaluation_queue WHERE eligible = 'Y';

-- Execute limited number
EXEC ilm_execution_engine.execute_pending_actions(p_max_operations => 5);
```

### 2. Use Priority Wisely

```sql
-- Logical priority sequence:
-- 100-199: Compression policies
-- 200-299: Warm tier moves
-- 300-399: Cold tier moves
-- 400-499: Archive/Read-only
-- 900-999: Purge operations
```

### 3. Test on Non-Production

- Test all policies on dev/test first
- Verify compression ratios meet expectations
- Confirm query performance acceptable
- Test restore procedures

### 4. Monitor Executions

```sql
-- Check for failures regularly
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 7
ORDER BY execution_start DESC;

-- Monitor space savings
SELECT * FROM v_ilm_execution_stats;
```

### 5. Staged Rollout

```sql
-- Start with oldest partitions
UPDATE cmr.dwh_ilm_policies
SET custom_condition = 'partition_date < DATE ''2020-01-01'''
WHERE policy_name = 'COMPRESS_SALES_90D';

-- Gradually remove restriction
UPDATE cmr.dwh_ilm_policies
SET custom_condition = NULL
WHERE policy_name = 'COMPRESS_SALES_90D';
```

## Troubleshooting

### Partition Not Eligible

```sql
-- Check why partition doesn't qualify
DECLARE
    v_eligible BOOLEAN;
    v_reason VARCHAR2(500);
BEGIN
    v_eligible := ilm_policy_engine.is_partition_eligible(
        p_policy_id => 1,
        p_table_owner => USER,
        p_table_name => 'SALES_FACT',
        p_partition_name => 'P_2024_01',
        p_reason => v_reason
    );

    DBMS_OUTPUT.PUT_LINE('Eligible: ' || CASE WHEN v_eligible THEN 'YES' ELSE 'NO' END);
    DBMS_OUTPUT.PUT_LINE('Reason: ' || v_reason);
END;
/
```

### Failed Executions

```sql
-- Find recent failures
SELECT
    execution_id,
    policy_name,
    partition_name,
    error_code,
    error_message
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
ORDER BY execution_start DESC
FETCH FIRST 10 ROWS ONLY;
```

### Scheduler Issues

```sql
-- Check if jobs are running
SELECT * FROM v_ilm_scheduler_status;

-- Check job failures
SELECT * FROM v_ilm_job_history
WHERE status = 'FAILED'
ORDER BY log_date DESC;

-- Force job to run now
EXEC run_ilm_job_now('ILM_JOB_EVALUATE');
```

### Clear Stuck Queue

```sql
-- Clear evaluation queue
EXEC ilm_policy_engine.clear_queue();

-- Re-evaluate
EXEC ilm_policy_engine.evaluate_all_policies();
```

## Maintenance

### Disable Policy Temporarily

```sql
UPDATE cmr.dwh_ilm_policies
SET enabled = 'N'
WHERE policy_name = 'COMPRESS_SALES_90D';
COMMIT;
```

### Update Policy Parameters

```sql
UPDATE cmr.dwh_ilm_policies
SET age_days = 120,
    compression_type = 'ARCHIVE HIGH',
    modified_date = SYSTIMESTAMP,
    modified_by = USER
WHERE policy_name = 'COMPRESS_SALES_90D';
COMMIT;
```

### Pause All ILM Operations

```sql
-- Stop scheduler jobs
EXEC dwh_stop_ilm_jobs();

-- Or disable auto execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;
```

### Resume Operations

```sql
-- Start scheduler jobs
EXEC dwh_start_ilm_jobs();

-- Or enable auto execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;
```

### Cleanup Old Logs

```sql
-- Manual cleanup
EXEC cleanup_execution_logs();

-- Or adjust retention
UPDATE cmr.dwh_ilm_config
SET config_value = '180'  -- 6 months
WHERE config_key = 'LOG_RETENTION_DAYS';
COMMIT;
```

## Advanced Features

### Custom Actions

```sql
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name, policy_type, action_type,
    age_days, custom_action, priority, enabled
) VALUES (
    'CUSTOM_EXPORT_OLD_DATA', USER, 'SALES_FACT', 'CUSTOM', 'CUSTOM',
    1095,
    'BEGIN
        export_partition_to_external(
            p_table_name => ''SALES_FACT'',
            p_partition_name => :partition_name
        );
     END;',
    800,
    'Y'
);
COMMIT;
```

### Access Pattern Tracking

```sql
-- Custom implementation to track actual access
-- Hook into application logging or use triggers

CREATE OR REPLACE TRIGGER trg_track_partition_write
AFTER INSERT OR UPDATE OR DELETE ON sales_fact
FOR EACH ROW
DECLARE
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_partition_name VARCHAR2(128);
BEGIN
    -- Get partition name from ROWID
    SELECT subobject_name INTO v_partition_name
    FROM user_objects
    WHERE object_name = 'SALES_FACT'
    AND data_object_id = DBMS_ROWID.ROWID_OBJECT(:NEW.ROWID);

    -- Update access tracking
    UPDATE ilm_partition_access
    SET last_write_time = SYSTIMESTAMP,
        write_count = write_count + 1
    WHERE table_name = 'SALES_FACT'
    AND partition_name = v_partition_name;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        NULL;  -- Ignore errors in access tracking
END;
/
```

## Performance Tuning

### Parallel Execution

```sql
-- Adjust parallel degree in policies
UPDATE cmr.dwh_ilm_policies
SET parallel_degree = 8
WHERE table_name = 'SALES_FACT';
COMMIT;
```

### Batch Size

```sql
-- Limit executions per run to avoid long-running operations
EXEC ilm_execution_engine.execute_pending_actions(p_max_operations => 10);
```

### Index Rebuild

```sql
-- Skip index rebuild for faster execution (rebuild separately)
UPDATE cmr.dwh_ilm_policies
SET rebuild_indexes = 'N'
WHERE policy_name = 'COMPRESS_SALES_90D';
COMMIT;
```

## Integration with Oracle Features

### Use Oracle Heat Map (Optional)

```sql
-- Enable Heat Map
ALTER SYSTEM SET HEAT_MAP = ON;

-- Integrate with custom tracking
CREATE OR REPLACE PROCEDURE refresh_partition_access_tracking AS
BEGIN
    MERGE INTO ilm_partition_access a
    USING (
        SELECT
            h.object_owner,
            h.object_name,
            h.subobject_name,
            h.segment_write_time,
            h.segment_read_time
        FROM dba_heat_map_segment h
        WHERE h.object_type = 'TABLE PARTITION'
    ) src
    ON (a.table_owner = src.object_owner
        AND a.table_name = src.object_name
        AND a.partition_name = src.subobject_name)
    WHEN MATCHED THEN UPDATE SET
        a.last_write_time = src.segment_write_time,
        a.last_read_time = src.segment_read_time,
        a.last_updated = SYSTIMESTAMP;

    COMMIT;
END;
/
```

## Security

### Grant Access

```sql
-- Grant policy management to specific user
GRANT SELECT, INSERT, UPDATE ON cmr.dwh_ilm_policies TO ilm_admin;
GRANT SELECT ON v_ilm_active_policies TO ilm_admin;

-- Grant execution to specific user
GRANT EXECUTE ON ilm_execution_engine TO ilm_admin;
GRANT EXECUTE ON ilm_policy_engine TO ilm_admin;
```

### Audit Trail

```sql
-- All changes are tracked via audit fields:
SELECT
    policy_name,
    created_by,
    created_date,
    modified_by,
    modified_date
FROM cmr.dwh_ilm_policies
ORDER BY modified_date DESC;
```

## Comparison: Custom ILM vs Oracle ADO

| Feature | Custom ILM | Oracle ADO |
|---------|-----------|------------|
| **License Required** | No | Yes (EE + ADO option) |
| **Control** | Full control | Limited |
| **Customization** | Highly customizable | Pre-defined actions |
| **Execution Window** | Custom windows | Maintenance windows |
| **Audit Trail** | Complete logging | DBA views |
| **Learning Curve** | Moderate | Low |
| **Complexity** | Moderate | Low |
| **Flexibility** | High | Medium |
| **Cost** | Free | Licensed feature |

## Summary

The custom PL/SQL ILM framework provides enterprise-grade partition lifecycle management without ADO dependencies. It offers complete control, customization, and auditability while being free of additional licensing costs.

Use this framework when you need:
- Full control over ILM policies
- Custom business logic
- Detailed audit trails
- No ADO license
- Integration with existing systems
