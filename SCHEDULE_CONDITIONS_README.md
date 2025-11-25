# Schedule Conditions Framework - Implementation Guide

## Overview

The **Schedule Conditions Framework** is a generalized execution scheduling system that allows custom business logic to gate job execution. It extends the ILM scheduler to support flexible, reusable scheduling for any job type (ILM, BACKUP, ETL, etc.).

### Key Features

- **Custom Conditions**: SQL queries, PL/SQL functions, or anonymous blocks
- **Logical Operators**: Combine multiple conditions with AND/OR logic
- **Evaluation Tracking**: Full history of condition evaluations with success/failure metrics
- **Fail-Safe Design**: Configurable error handling (fail-safe vs continue-on-error)
- **Generalized Framework**: Reusable across different job types
- **Complete Observability**: Dedicated monitoring views for conditions and schedule readiness

---

## Installation

### Prerequisites

1. Core ILM framework must be installed:
   - `custom_ilm_setup.sql`
   - `custom_ilm_policy_engine.sql`
   - `custom_ilm_validation.sql`

2. Scheduler enhancement must be installed:
   - `scheduler_enhancement_setup.sql`

### Installation Steps

Run the following scripts in order:

```sql
-- Step 1: Migrate to new generalized schema
@scripts/schedule_conditions_migration.sql

-- Step 2: Update execution engine with condition evaluation
@scripts/scheduler_enhancement_engine.sql

-- Step 3: Create/update monitoring views
@scripts/schedule_conditions_monitoring.sql

-- Step 4: (Optional) Review examples
@scripts/scheduler_conditions_examples.sql
```

### Verification

After installation, verify everything is working:

```sql
-- Check tables
SELECT COUNT(*) FROM cmr.dwh_execution_schedules;
SELECT COUNT(*) FROM cmr.dwh_schedule_conditions;
SELECT COUNT(*) FROM cmr.dwh_execution_state;

-- Check package compiles
SHOW ERRORS PACKAGE pck_dwh_ilm_execution_engine;

-- Check views
SELECT view_name FROM all_views
WHERE owner = 'CMR'
AND (view_name LIKE '%SCHEDULE%' OR view_name LIKE '%CONDITION%')
ORDER BY view_name;

-- Check readiness
SELECT * FROM cmr.v_dwh_schedule_readiness;
```

---

## Usage

### Creating Conditions

Conditions are stored in the `dwh_schedule_conditions` table and must be linked to a schedule.

#### Example 1: CPU Threshold (SQL Type)

Only run if CPU usage is below 70%:

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'CPU_THRESHOLD_70PCT',
    'SQL',
    'SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
     FROM v$sysmetric
     WHERE metric_name = ''CPU Usage Per Sec''
     AND group_id = 2',
    1,
    'AND',
    'Y',
    'Y',
    'Only run if average CPU usage < 70% over last minute'
);
COMMIT;
```

#### Example 2: Backup Coordination (SQL Type)

Wait for RMAN backup completion:

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'NO_BACKUP_RUNNING',
    'SQL',
    'SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
     FROM v$rman_backup_job_details
     WHERE start_time >= TRUNC(SYSDATE)
     AND status = ''RUNNING''',
    2,
    'AND',
    'Y',
    'Y',
    'Only run if no RMAN backup is currently running'
);
COMMIT;
```

#### Example 3: Custom Function (FUNCTION Type)

Use a custom PL/SQL function for complex logic:

```sql
-- First, create the function
CREATE OR REPLACE FUNCTION check_system_ready_for_ilm
RETURN BOOLEAN
AS
    v_cpu NUMBER;
    v_memory NUMBER;
BEGIN
    SELECT AVG(value) INTO v_cpu
    FROM v$sysmetric
    WHERE metric_name = 'CPU Usage Per Sec' AND group_id = 2;

    SELECT AVG(value) INTO v_memory
    FROM v$sysmetric
    WHERE metric_name = 'Memory Usage' AND group_id = 2;

    RETURN (v_cpu < 70 AND v_memory < 80);
END;
/

-- Then create the condition
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'SYSTEM_READY_CHECK',
    'FUNCTION',
    'RETURN check_system_ready_for_ilm()',
    3,
    'AND',
    'Y',
    'Y',
    'Check multiple system metrics'
);
COMMIT;
```

#### Example 4: PL/SQL Block (PLSQL Type)

Complex date logic in anonymous block:

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    logical_operator,
    enabled,
    fail_on_error,
    description
) VALUES (
    (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE'),
    'FIRST_MONDAY_OF_MONTH',
    'PLSQL',
    'DECLARE
        v_day_of_week VARCHAR2(10);
        v_day_of_month NUMBER;
    BEGIN
        v_day_of_week := TO_CHAR(SYSDATE, ''DY'');
        v_day_of_month := TO_NUMBER(TO_CHAR(SYSDATE, ''DD''));
        v_result := (v_day_of_week = ''MON'' AND v_day_of_month BETWEEN 1 AND 7);
        RETURN v_result;
    END;',
    4,
    'OR',
    'Y',
    'N',
    'Special handling on first Monday of month'
);
COMMIT;
```

---

## Condition Types

### SQL Type

- **Purpose**: Simple SELECT query returning 1 (TRUE) or 0 (FALSE)
- **Format**: Single SELECT statement
- **Returns**: Single row, single column with value 1 or 0
- **Use Case**: System metrics, table queries, simple checks

### FUNCTION Type

- **Purpose**: Call existing PL/SQL function
- **Format**: `RETURN function_name(params)`
- **Returns**: Function must return BOOLEAN
- **Use Case**: Reusable business logic, complex calculations

### PLSQL Type

- **Purpose**: Full PL/SQL anonymous block
- **Format**: `BEGIN ... RETURN <boolean>; END;`
- **Returns**: Must set v_result and RETURN
- **Use Case**: Complex multi-step logic, multiple queries

---

## Logical Operators

Conditions are evaluated in `evaluation_order` and combined using `logical_operator`:

### AND Logic

All conditions must pass:

```sql
Condition 1 (AND) → Condition 2 (AND) → Condition 3
Result: TRUE only if ALL pass
```

### OR Logic

At least one condition must pass:

```sql
Condition 1 (OR) → Condition 2 (OR) → Condition 3
Result: TRUE if ANY pass
```

### Mixed Logic

Combine AND/OR for complex rules:

```sql
Condition 1 (AND) → Condition 2 (OR) → Condition 3
Result: (Condition1 AND Condition2) OR Condition3
```

### Short-Circuit Optimization

- **AND chains**: Stop on first failure (performance optimization)
- **OR chains**: Continue until success found

---

## Error Handling

The `fail_on_error` flag controls error behavior:

| fail_on_error | Behavior | Use Case |
|---------------|----------|----------|
| **Y** (default) | Error = FALSE (fail-safe, don't run) | Critical checks (CPU, memory, backup) |
| **N** | Error = TRUE (continue despite error) | Non-critical checks (logging, monitoring) |

---

## Monitoring

### View: v_dwh_schedule_conditions

Shows all conditions with evaluation metrics:

```sql
SELECT
    schedule_name,
    condition_name,
    condition_type,
    enabled,
    last_result_text,
    success_rate_pct,
    evaluation_count,
    minutes_since_last_eval
FROM cmr.v_dwh_schedule_conditions
ORDER BY schedule_name, evaluation_order;
```

### View: v_dwh_condition_failures

Shows currently failing conditions:

```sql
SELECT
    schedule_name,
    condition_name,
    last_evaluation_error,
    failure_rate_pct,
    minutes_since_failure
FROM cmr.v_dwh_condition_failures
ORDER BY last_evaluation_time DESC;
```

### View: v_dwh_schedule_readiness

Overall schedule readiness check:

```sql
SELECT
    schedule_name,
    schedule_type,
    readiness_status,
    running_batches,
    pending_work,
    total_conditions,
    failing_conditions
FROM cmr.v_dwh_schedule_readiness
ORDER BY schedule_name;
```

**Readiness Status Values**:
- `READY` - All checks passed, can execute
- `NOT_READY:SCHEDULE_DISABLED` - Schedule not enabled
- `NOT_READY:ALREADY_RUNNING` - Batch already running
- `NOT_READY:NO_PENDING_WORK` - Queue is empty
- `NOT_READY:NO_WINDOW_TODAY` - Not scheduled for today
- `NOT_READY:CONDITIONS_FAILING` - Custom conditions not met

---

## Testing Conditions

### Test Individual Condition

```sql
-- Get condition code
SELECT condition_code
FROM cmr.dwh_schedule_conditions
WHERE condition_name = 'CPU_THRESHOLD_70PCT';

-- Execute manually (for SQL type)
SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
FROM v$sysmetric
WHERE metric_name = 'CPU Usage Per Sec' AND group_id = 2;
```

### Test All Conditions for Schedule

```sql
DECLARE
    v_result BOOLEAN;
    v_schedule_id NUMBER;
BEGIN
    SELECT schedule_id INTO v_schedule_id
    FROM cmr.dwh_execution_schedules
    WHERE schedule_name = 'DEFAULT_SCHEDULE';

    v_result := pck_dwh_ilm_execution_engine.evaluate_schedule_conditions(v_schedule_id);

    DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result THEN 'TRUE (can execute)' ELSE 'FALSE (blocked)' END);
END;
/
```

### Check Evaluation History

```sql
SELECT
    condition_name,
    last_evaluation_time,
    last_evaluation_result,
    last_evaluation_error,
    evaluation_count,
    success_count,
    failure_count
FROM cmr.dwh_schedule_conditions
WHERE schedule_id = (SELECT schedule_id FROM cmr.dwh_execution_schedules WHERE schedule_name = 'DEFAULT_SCHEDULE')
ORDER BY evaluation_order;
```

---

## Troubleshooting

### Condition Always Failing

1. **Check evaluation history**:
   ```sql
   SELECT * FROM cmr.v_dwh_condition_failures;
   ```

2. **Review error message**:
   ```sql
   SELECT condition_name, last_evaluation_error
   FROM cmr.dwh_schedule_conditions
   WHERE last_evaluation_result = 'N';
   ```

3. **Test condition manually** (see Testing section above)

### Schedule Not Executing

1. **Check overall readiness**:
   ```sql
   SELECT * FROM cmr.v_dwh_schedule_readiness;
   ```

2. **Identify blocker**:
   - `SCHEDULE_DISABLED`: Enable schedule
   - `ALREADY_RUNNING`: Wait for current batch to complete
   - `NO_PENDING_WORK`: Add work to queue
   - `NO_WINDOW_TODAY`: Check day-of-week configuration
   - `CONDITIONS_FAILING`: Fix failing conditions

### Condition Evaluation Errors

1. **Check permissions**:
   ```sql
   -- Ensure user has access to v$sysmetric, v$session, etc.
   GRANT SELECT ON v$sysmetric TO cmr;
   ```

2. **Validate SQL syntax**:
   - Execute condition_code manually
   - Check for missing table/view

3. **Review fail_on_error setting**:
   - Set to 'N' for non-critical checks
   - Set to 'Y' for critical checks

---

## Best Practices

### Condition Design

1. **Keep conditions simple**: One check per condition
2. **Use evaluation_order**: Order from fastest to slowest checks
3. **Set appropriate fail_on_error**: Critical checks = 'Y', optional checks = 'N'
4. **Add descriptive names**: Use clear, meaningful condition names

### Performance

1. **Leverage short-circuit**: Put most restrictive AND conditions first
2. **Avoid heavy queries**: Use v$sysmetric (last 60 seconds) not full table scans
3. **Cache function results**: For FUNCTION type, cache values if possible
4. **Monitor evaluation time**: Check `minutes_since_last_eval` for slow conditions

### Maintenance

1. **Review condition history regularly**:
   ```sql
   SELECT * FROM cmr.v_dwh_schedule_conditions;
   ```

2. **Disable unused conditions**: Set `enabled='N'` instead of deleting

3. **Archive old conditions**: Keep for reference/rollback

4. **Document business logic**: Use `description` column thoroughly

---

## Migration from Old System

If upgrading from the old `dwh_ilm_execution_schedules` system:

1. **Backup existing configuration**:
   ```sql
   CREATE TABLE dwh_ilm_config_backup AS SELECT * FROM cmr.dwh_ilm_config;
   ```

2. **Run migration script**:
   ```sql
   @scripts/schedule_conditions_migration.sql
   ```

3. **Verify data migrated**:
   ```sql
   SELECT COUNT(*) FROM cmr.dwh_execution_schedules;
   SELECT COUNT(*) FROM cmr.dwh_execution_state;
   ```

4. **Update packages and views**:
   ```sql
   @scripts/scheduler_enhancement_engine.sql
   @scripts/schedule_conditions_monitoring.sql
   ```

5. **Test with existing schedule**:
   ```sql
   SELECT * FROM cmr.v_dwh_schedule_readiness;
   ```

6. **After verification (e.g., 1 week), drop old tables**:
   ```sql
   DROP TABLE cmr.dwh_ilm_execution_schedules_old PURGE;
   DROP TABLE cmr.dwh_ilm_execution_state_old PURGE;
   ```

---

## Examples

See `scripts/scheduler_conditions_examples.sql` for 10 working examples covering:

1. CPU Threshold
2. Memory Threshold
3. Backup Coordination
4. Business Calendar
5. Queue Size Check
6. Tablespace Free Space
7. Active Session Count
8. Custom Function
9. First Monday of Month
10. Multi-Condition with OR Logic

---

## Support

For questions or issues:

1. Check design document: `SCHEDULE_CONDITIONS_DESIGN.md`
2. Review examples: `scripts/scheduler_conditions_examples.sql`
3. Check monitoring views: `scripts/schedule_conditions_monitoring.sql`

---

**Version**: 1.0
**Date**: 2025-11-24
**Status**: Production Ready
