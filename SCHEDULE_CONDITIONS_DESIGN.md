# Schedule Conditions Framework - Design Document

## Executive Summary

This document describes the design of a **generalized schedule conditions framework** that allows custom business logic to gate the execution of scheduled jobs. The framework is designed to be reusable across different types of scheduled operations, not just ILM.

**Key Changes**:
- Rename `dwh_ilm_execution_schedules` → `dwh_execution_schedules` (generalized)
- Rename `dwh_ilm_execution_state` → `dwh_execution_state` (generalized)
- Create new `dwh_schedule_conditions` table (condition gates)
- Support multiple conditions per schedule with logical operators
- Track condition evaluation history for observability
- **No backward compatibility** - direct migration to new structure

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Schema Design](#schema-design)
3. [Condition Evaluation Logic](#condition-evaluation-logic)
4. [Integration Points](#integration-points)
5. [Use Cases](#use-cases)
6. [Migration Plan](#migration-plan)
7. [Implementation Phases](#implementation-phases)
8. [Examples](#examples)

---

## Architecture Overview

### Current State

```
dwh_ilm_execution_schedules (ILM-specific)
    ├─ schedule_id
    ├─ schedule_name
    ├─ enabled (Y/N)
    ├─ monday_hours ... sunday_hours
    └─ batch_cooldown_minutes

dwh_ilm_execution_state (ILM-specific)
    ├─ execution_batch_id
    ├─ schedule_id (FK)
    ├─ status
    └─ execution metrics

Evaluation Logic:
1. Check concurrent execution
2. Check pending work
3. Check time window (day + hours)
4. Run job
```

### Future State (New Schema)

```
dwh_execution_schedules (Generalized)
    ├─ schedule_id
    ├─ schedule_name
    ├─ schedule_type (ILM, BACKUP, ETL, etc.)  ← NEW
    ├─ enabled (Y/N)
    ├─ monday_hours ... sunday_hours
    └─ batch_cooldown_minutes

dwh_execution_state (Generalized)
    ├─ execution_batch_id
    ├─ schedule_id (FK)
    ├─ status
    └─ execution metrics

dwh_schedule_conditions (NEW)
    ├─ condition_id
    ├─ schedule_id (FK)
    ├─ condition_name
    ├─ condition_type (SQL, FUNCTION, PLSQL)
    ├─ condition_code (CLOB)
    ├─ evaluation_order
    ├─ logical_operator (AND, OR)
    ├─ enabled (Y/N)
    └─ last_evaluation_*

Enhanced Evaluation Logic:
1. Check concurrent execution
2. Check pending work
3. Check time window (day + hours)
4. ⭐ Check custom conditions (NEW)
5. Run job
```

---

## Schema Design

### Table 1: `dwh_execution_schedules` (Renamed from `dwh_ilm_execution_schedules`)

**Purpose**: Define when and how scheduled jobs should run

**Changes from Original**:
- Rename table: `dwh_ilm_execution_schedules` → `dwh_execution_schedules`
- Add `schedule_type` column to support multiple job types
- Add `schedule_description` for documentation
- Add audit columns (created_by, modified_by)

```sql
CREATE TABLE cmr.dwh_execution_schedules (
    schedule_id             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_name           VARCHAR2(100) NOT NULL UNIQUE,
    schedule_type           VARCHAR2(50) DEFAULT 'ILM' NOT NULL,
    schedule_description    VARCHAR2(500),
    enabled                 CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),

    -- Execution Windows: One column per day (NULL = don't run that day)
    -- Format: 'HH24:MI-HH24:MI' (e.g., '22:00-06:00' for 10 PM to 6 AM)
    monday_hours            VARCHAR2(11),
    tuesday_hours           VARCHAR2(11),
    wednesday_hours         VARCHAR2(11),
    thursday_hours          VARCHAR2(11),
    friday_hours            VARCHAR2(11),
    saturday_hours          VARCHAR2(11),
    sunday_hours            VARCHAR2(11),

    -- Execution Control
    batch_cooldown_minutes  NUMBER DEFAULT 5 CHECK (batch_cooldown_minutes >= 0),

    -- Resumability
    enable_checkpointing    CHAR(1) DEFAULT 'Y' CHECK (enable_checkpointing IN ('Y','N')),
    checkpoint_frequency    NUMBER DEFAULT 5 CHECK (checkpoint_frequency > 0),

    -- Audit
    created_by              VARCHAR2(50) DEFAULT USER,
    created_date            DATE DEFAULT SYSDATE,
    modified_by             VARCHAR2(50) DEFAULT USER,
    modified_date           DATE DEFAULT SYSDATE,

    -- Validation: Ensure correct time format HH24:MI-HH24:MI
    CONSTRAINT chk_monday_hours CHECK (monday_hours IS NULL OR REGEXP_LIKE(monday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_tuesday_hours CHECK (tuesday_hours IS NULL OR REGEXP_LIKE(tuesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_wednesday_hours CHECK (wednesday_hours IS NULL OR REGEXP_LIKE(wednesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_thursday_hours CHECK (thursday_hours IS NULL OR REGEXP_LIKE(thursday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_friday_hours CHECK (friday_hours IS NULL OR REGEXP_LIKE(friday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_saturday_hours CHECK (saturday_hours IS NULL OR REGEXP_LIKE(saturday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_sunday_hours CHECK (sunday_hours IS NULL OR REGEXP_LIKE(sunday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$'))
);

CREATE INDEX idx_exec_sched_type ON cmr.dwh_execution_schedules(schedule_type, enabled);

COMMENT ON TABLE cmr.dwh_execution_schedules IS
'Centralized execution schedules with per-day time windows.
Supports multiple schedule types (ILM, BACKUP, ETL, etc.).
Each day column contains time range in HH24:MI-HH24:MI format.
NULL value = no execution on that day.';

COMMENT ON COLUMN cmr.dwh_execution_schedules.schedule_type IS
'Type of scheduled operation: ILM, BACKUP, ETL, MAINTENANCE, etc.
Allows same scheduling framework to be used for different job types.';

COMMENT ON COLUMN cmr.dwh_execution_schedules.batch_cooldown_minutes IS
'Pause between batches in minutes.
0 = Continuous execution (run next batch immediately)
>0 = Pause N minutes between batches (e.g., 5 = 5-minute cooldown)';
```

**New Column Details**:

| Column | Type | Purpose |
|--------|------|---------|
| `schedule_type` | VARCHAR2(50) | Job category (ILM, BACKUP, ETL, etc.) |
| `schedule_description` | VARCHAR2(500) | Human-readable description |
| `created_by` | VARCHAR2(50) | Audit: who created |
| `modified_by` | VARCHAR2(50) | Audit: who last modified |

---

### Table 2: `dwh_schedule_conditions` (NEW)

**Purpose**: Define custom conditions that must evaluate to TRUE before schedule executes

```sql
CREATE TABLE cmr.dwh_schedule_conditions (
    condition_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_id             NUMBER NOT NULL,
    condition_name          VARCHAR2(100) NOT NULL,
    condition_type          VARCHAR2(20) NOT NULL CHECK (condition_type IN ('SQL', 'FUNCTION', 'PLSQL')),
    condition_code          CLOB NOT NULL,
    evaluation_order        NUMBER DEFAULT 1 NOT NULL CHECK (evaluation_order > 0),
    logical_operator        VARCHAR2(10) DEFAULT 'AND' CHECK (logical_operator IN ('AND', 'OR')),
    enabled                 CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),
    fail_on_error           CHAR(1) DEFAULT 'Y' CHECK (fail_on_error IN ('Y','N')),
    description             VARCHAR2(500),

    -- Evaluation History
    last_evaluation_time    TIMESTAMP,
    last_evaluation_result  CHAR(1) CHECK (last_evaluation_result IN ('Y','N')),
    last_evaluation_error   VARCHAR2(4000),
    evaluation_count        NUMBER DEFAULT 0,
    success_count           NUMBER DEFAULT 0,
    failure_count           NUMBER DEFAULT 0,

    -- Audit
    created_by              VARCHAR2(50) DEFAULT USER,
    created_date            DATE DEFAULT SYSDATE,
    modified_by             VARCHAR2(50) DEFAULT USER,
    modified_date           DATE DEFAULT SYSDATE,

    CONSTRAINT fk_sched_cond_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_execution_schedules(schedule_id)
        ON DELETE CASCADE,

    CONSTRAINT uk_sched_cond_name
        UNIQUE (schedule_id, condition_name)
);

CREATE INDEX idx_sched_cond_schedule ON cmr.dwh_schedule_conditions(schedule_id, evaluation_order);
CREATE INDEX idx_sched_cond_enabled ON cmr.dwh_schedule_conditions(schedule_id, enabled);

COMMENT ON TABLE cmr.dwh_schedule_conditions IS
'Custom conditions that gate schedule execution.
Each condition must evaluate to TRUE (or be skipped if disabled).
Multiple conditions per schedule are evaluated in order with logical operators.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.condition_type IS
'Type of condition code:
- SQL: SELECT statement returning 1 (TRUE) or 0 (FALSE)
- FUNCTION: PL/SQL function call returning BOOLEAN
- PLSQL: PL/SQL anonymous block with RETURN statement';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.condition_code IS
'Executable code for condition evaluation:
- SQL: SELECT 1 FROM DUAL WHERE <condition>
- FUNCTION: RETURN my_check_function(param1, param2)
- PLSQL: BEGIN ... RETURN <boolean_expression>; END;';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.evaluation_order IS
'Order in which conditions are evaluated (1, 2, 3...).
Lower numbers evaluated first.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.logical_operator IS
'How to combine with NEXT condition:
- AND: Both this and next must be TRUE
- OR: Either this or next must be TRUE
Last condition''s operator is ignored.';

COMMENT ON COLUMN cmr.dwh_schedule_conditions.fail_on_error IS
'How to handle evaluation errors:
- Y: Treat evaluation error as FALSE (fail-safe, prevent execution)
- N: Ignore errors and treat as TRUE (continue execution)';
```

**Column Details**:

| Column | Type | Purpose |
|--------|------|---------|
| `condition_id` | NUMBER | Primary key (auto-generated) |
| `schedule_id` | NUMBER | FK to dwh_execution_schedules |
| `condition_name` | VARCHAR2(100) | Unique name within schedule |
| `condition_type` | VARCHAR2(20) | SQL, FUNCTION, or PLSQL |
| `condition_code` | CLOB | Executable condition logic |
| `evaluation_order` | NUMBER | Execution sequence (1, 2, 3...) |
| `logical_operator` | VARCHAR2(10) | AND/OR with next condition |
| `enabled` | CHAR(1) | Y=active, N=skip |
| `fail_on_error` | CHAR(1) | Y=error→FALSE, N=error→TRUE |
| `description` | VARCHAR2(500) | Human-readable explanation |
| `last_evaluation_time` | TIMESTAMP | When last evaluated |
| `last_evaluation_result` | CHAR(1) | Y=passed, N=failed |
| `last_evaluation_error` | VARCHAR2(4000) | Last error message |
| `evaluation_count` | NUMBER | Total evaluations |
| `success_count` | NUMBER | Successful evaluations |
| `failure_count` | NUMBER | Failed evaluations |

---

### Table 3: `dwh_execution_state` (Renamed from `dwh_ilm_execution_state`)

**Purpose**: Track batch execution state for checkpointing

**Changes**:
- Rename: `dwh_ilm_execution_state` → `dwh_execution_state`
- Update FK reference to new schedule table

```sql
CREATE TABLE cmr.dwh_execution_state (
    execution_batch_id      VARCHAR2(50) PRIMARY KEY,
    schedule_id             NUMBER NOT NULL,
    status                  VARCHAR2(20) NOT NULL CHECK (status IN ('RUNNING','COMPLETED','INTERRUPTED','FAILED')),
    start_time              TIMESTAMP NOT NULL,
    end_time                TIMESTAMP,
    last_checkpoint         TIMESTAMP,
    operations_completed    NUMBER DEFAULT 0,
    operations_total        NUMBER,
    operations_remaining    NUMBER,
    last_queue_id           NUMBER,
    elapsed_seconds         NUMBER,

    CONSTRAINT fk_exec_state_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_execution_schedules(schedule_id)
        ON DELETE CASCADE
);

CREATE INDEX idx_exec_state_schedule ON cmr.dwh_execution_state(schedule_id, status);
CREATE INDEX idx_exec_state_status ON cmr.dwh_execution_state(status, start_time);

COMMENT ON TABLE cmr.dwh_execution_state IS
'Tracks batch execution state for checkpointing and resumability.
One row per batch execution. Supports interruption and resume.';
```

---

## Condition Evaluation Logic

### Condition Types

#### 1. SQL Type
**Purpose**: Simple SELECT query returning 1 (TRUE) or 0 (FALSE)

**Format**:
```sql
SELECT <expression> FROM <tables> WHERE <conditions>
-- Must return single row with single column containing 1 or 0
```

**Examples**:
```sql
-- CPU threshold check
SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
FROM v$sysmetric
WHERE metric_name = 'CPU Usage Per Sec'

-- Pending queue size check
SELECT CASE WHEN COUNT(*) > 100 THEN 1 ELSE 0 END
FROM dwh_ilm_evaluation_queue
WHERE execution_status = 'PENDING'

-- Tablespace free space check
SELECT CASE WHEN MIN(pct_free) > 20 THEN 1 ELSE 0 END
FROM (
    SELECT tablespace_name,
           (1 - SUM(bytes)/SUM(maxbytes)) * 100 AS pct_free
    FROM dba_data_files
    GROUP BY tablespace_name
)
```

**Evaluation**:
```sql
EXECUTE IMMEDIATE condition_code INTO v_result;
v_condition_passed := (v_result = 1);
```

---

#### 2. FUNCTION Type
**Purpose**: Call existing PL/SQL function that returns BOOLEAN

**Format**:
```sql
RETURN function_name(param1, param2, ...)
-- Function must return BOOLEAN
```

**Examples**:
```sql
-- Call custom function
RETURN check_backup_completed()

-- Call with parameters
RETURN is_business_day(SYSDATE)

-- Call with complex parameters
RETURN check_system_resources(
    p_cpu_threshold => 70,
    p_memory_threshold => 80,
    p_io_threshold => 60
)
```

**Evaluation**:
```sql
v_block := 'DECLARE v_result BOOLEAN; BEGIN v_result := ' || condition_code ||
           '; :1 := CASE WHEN v_result THEN 1 ELSE 0 END; END;';
EXECUTE IMMEDIATE v_block USING OUT v_result;
v_condition_passed := (v_result = 1);
```

---

#### 3. PLSQL Type
**Purpose**: Full PL/SQL block for complex logic

**Format**:
```sql
BEGIN
    -- Variable declarations
    -- Logic
    RETURN <boolean_expression>;
END;
```

**Examples**:
```sql
-- Complex multi-check
BEGIN
    DECLARE
        v_cpu NUMBER;
        v_sessions NUMBER;
        v_backup_running NUMBER;
    BEGIN
        SELECT AVG(value) INTO v_cpu
        FROM v$sysmetric
        WHERE metric_name = 'CPU Usage Per Sec';

        SELECT COUNT(*) INTO v_sessions
        FROM v$session
        WHERE status = 'ACTIVE';

        SELECT COUNT(*) INTO v_backup_running
        FROM v$rman_backup_job_details
        WHERE status = 'RUNNING';

        RETURN (v_cpu < 70 AND v_sessions < 100 AND v_backup_running = 0);
    END;
END;

-- First Monday of month check
BEGIN
    DECLARE
        v_day_of_week VARCHAR2(10);
        v_day_of_month NUMBER;
    BEGIN
        v_day_of_week := TO_CHAR(SYSDATE, 'DY');
        v_day_of_month := TO_NUMBER(TO_CHAR(SYSDATE, 'DD'));

        RETURN (v_day_of_week = 'MON' AND v_day_of_month BETWEEN 1 AND 7);
    END;
END;
```

**Evaluation**:
```sql
v_block := 'DECLARE v_result BOOLEAN; BEGIN ' || condition_code ||
           ' :1 := CASE WHEN v_result THEN 1 ELSE 0 END; END;';
EXECUTE IMMEDIATE v_block USING OUT v_result;
v_condition_passed := (v_result = 1);
```

---

### Logical Operators

Conditions are combined using `logical_operator` column:

```
Condition 1 (AND) → Condition 2 (AND) → Condition 3 (OR) → Condition 4
= ((Condition1 AND Condition2 AND Condition3) OR Condition4)
```

**Evaluation Algorithm**:

```sql
v_result := TRUE;  -- Start with TRUE

FOR cond IN (
    SELECT * FROM dwh_schedule_conditions
    WHERE schedule_id = p_schedule_id
    AND enabled = 'Y'
    ORDER BY evaluation_order
) LOOP
    v_cond_result := evaluate_single_condition(cond);

    IF cond.evaluation_order = 1 THEN
        -- First condition
        v_result := v_cond_result;
    ELSE
        -- Apply previous condition's operator
        IF v_prev_operator = 'AND' THEN
            v_result := v_result AND v_cond_result;
        ELSIF v_prev_operator = 'OR' THEN
            v_result := v_result OR v_cond_result;
        END IF;
    END IF;

    v_prev_operator := cond.logical_operator;

    -- Short-circuit optimization
    IF NOT v_result AND cond.logical_operator = 'AND' THEN
        EXIT;  -- AND chain failed, no need to continue
    END IF;
END LOOP;

RETURN v_result;
```

---

### Error Handling

The `fail_on_error` column controls error behavior:

| `fail_on_error` | Behavior |
|----------------|----------|
| **Y** (Default) | Evaluation error → Return FALSE (fail-safe, don't run) |
| **N** | Evaluation error → Return TRUE (continue despite error) |

**Rationale**:
- **fail_on_error='Y'**: Safe default - if condition can't be evaluated, don't run (prevent issues)
- **fail_on_error='N'**: Use for non-critical checks (e.g., logging, monitoring)

---

## Integration Points

### Integration Point 1: `should_execute_now()` Function

**Location**: `scheduler_enhancement_engine.sql`

**Updated Logic**:
```sql
FUNCTION should_execute_now(p_schedule_name VARCHAR2) RETURN BOOLEAN AS
    v_schedule schedule_rec;
    v_running_count NUMBER;
    v_pending_work NUMBER;
BEGIN
    v_schedule := get_schedule_config(p_schedule_name);

    -- Priority 1: Check if schedule enabled
    IF v_schedule.enabled = 'N' THEN
        RETURN FALSE;
    END IF;

    -- Priority 2: Prevent concurrent execution
    SELECT COUNT(*) INTO v_running_count
    FROM cmr.dwh_execution_state
    WHERE schedule_id = v_schedule.schedule_id
    AND status = 'RUNNING';

    IF v_running_count > 0 THEN
        RETURN FALSE;
    END IF;

    -- Priority 3: Check if work exists
    SELECT COUNT(*) INTO v_pending_work
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING'
    AND eligible = 'Y';

    IF v_pending_work = 0 THEN
        RETURN FALSE;
    END IF;

    -- Priority 4: Check execution window (day + time)
    IF NOT is_in_execution_window(v_schedule) THEN
        RETURN FALSE;
    END IF;

    -- Priority 5: ⭐ NEW - Check custom conditions
    IF NOT evaluate_schedule_conditions(v_schedule.schedule_id) THEN
        log_info('Custom schedule conditions not met for schedule: ' || p_schedule_name);
        RETURN FALSE;
    END IF;

    -- All checks passed
    RETURN TRUE;
END should_execute_now;
```

---

### Integration Point 2: Update Package and Views

**Objects to Update**:

1. **Package Type**: `schedule_rec` in `pck_dwh_ilm_execution_engine`
   - Add `schedule_type` field

2. **Package Functions**:
   - Update `get_schedule_config()` to query new table name
   - All other functions remain the same

3. **Monitoring Views** (rename, update table references):
   - `v_dwh_ilm_active_batches` → `v_dwh_active_batches`
   - `v_dwh_ilm_schedule_stats` → `v_dwh_schedule_stats`
   - `v_dwh_ilm_batch_progress` → `v_dwh_batch_progress`
   - `v_dwh_ilm_current_window` → `v_dwh_current_window`
   - `v_dwh_ilm_recent_batches` → `v_dwh_recent_batches`

4. **New Monitoring Views** (for conditions):
   - `v_dwh_schedule_conditions` - Current condition status
   - `v_dwh_condition_evaluation_history` - Evaluation history

---

## Use Cases

### Use Case 1: CPU Threshold Gating

**Scenario**: Only run ILM if database CPU < 70%

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id, condition_name, condition_type, condition_code,
    evaluation_order, logical_operator, description
) VALUES (
    1,
    'CPU_THRESHOLD_70PCT',
    'SQL',
    'SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
     FROM v$sysmetric
     WHERE metric_name = ''CPU Usage Per Sec''
     AND group_id = 2',  -- Last 60 seconds
    1,
    'AND',
    'Only run if average CPU usage < 70% over last minute'
);
```

---

### Use Case 2: Backup Coordination

**Scenario**: Only run ILM after nightly backup completes

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id, condition_name, condition_type, condition_code,
    evaluation_order, logical_operator, description
) VALUES (
    1,
    'BACKUP_COMPLETED_CHECK',
    'SQL',
    'SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
     FROM v$rman_backup_job_details
     WHERE start_time >= TRUNC(SYSDATE)
     AND (status = ''RUNNING'' OR end_time IS NULL)',
    2,
    'AND',
    'Only run if no backup is currently running today'
);
```

---

### Use Case 3: Business Calendar

**Scenario**: Only run on business days (weekdays, not holidays)

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id, condition_name, condition_type, condition_code,
    evaluation_order, logical_operator, description
) VALUES (
    1,
    'BUSINESS_DAY_CHECK',
    'SQL',
    'SELECT CASE
        WHEN TO_CHAR(SYSDATE, ''DY'') NOT IN (''SAT'', ''SUN'')
        AND NOT EXISTS (
            SELECT 1 FROM company_holidays
            WHERE holiday_date = TRUNC(SYSDATE)
        )
        THEN 1 ELSE 0 END
     FROM DUAL',
    3,
    'AND',
    'Only run on business days (weekdays excluding company holidays)'
);
```

---

## Migration Plan

### Step 1: Rename Existing Tables

```sql
-- Rename old tables (keep as backup)
ALTER TABLE cmr.dwh_ilm_execution_schedules RENAME TO dwh_ilm_execution_schedules_old;
ALTER TABLE cmr.dwh_ilm_execution_state RENAME TO dwh_ilm_execution_state_old;
```

---

### Step 2: Create New Tables

```sql
-- Create new generalized tables
CREATE TABLE cmr.dwh_execution_schedules (...);
CREATE TABLE cmr.dwh_execution_state (...);
CREATE TABLE cmr.dwh_schedule_conditions (...);
```

---

### Step 3: Migrate Data

```sql
-- Migrate schedules
INSERT INTO cmr.dwh_execution_schedules (
    schedule_name, schedule_type, enabled,
    monday_hours, tuesday_hours, wednesday_hours,
    thursday_hours, friday_hours, saturday_hours, sunday_hours,
    batch_cooldown_minutes, enable_checkpointing, checkpoint_frequency,
    created_date
)
SELECT
    schedule_name, 'ILM', enabled,
    monday_hours, tuesday_hours, wednesday_hours,
    thursday_hours, friday_hours, saturday_hours, sunday_hours,
    batch_cooldown_minutes, enable_checkpointing, checkpoint_frequency,
    created_date
FROM cmr.dwh_ilm_execution_schedules_old;

-- Migrate execution state
INSERT INTO cmr.dwh_execution_state
SELECT * FROM cmr.dwh_ilm_execution_state_old;

COMMIT;
```

---

### Step 4: Update Package and Views

```sql
-- Update package to use new table names
-- Update all monitoring views to use new table names
-- Add new condition-related views
```

---

### Step 5: Test and Verify

```sql
-- Verify data migrated correctly
SELECT COUNT(*) FROM cmr.dwh_execution_schedules;
SELECT COUNT(*) FROM cmr.dwh_execution_state;

-- Test scheduler still works
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions();
```

---

### Step 6: Drop Old Tables (After Verification)

```sql
-- After successful verification (e.g., 1 week)
DROP TABLE cmr.dwh_ilm_execution_schedules_old PURGE;
DROP TABLE cmr.dwh_ilm_execution_state_old PURGE;
```

---

## Implementation Phases

### Phase 1: Schema Migration (2-3 hours)

**Deliverables**:
- ✅ Rename old tables (_old suffix)
- ✅ Create `dwh_execution_schedules` table
- ✅ Create `dwh_execution_state` table
- ✅ Create `dwh_schedule_conditions` table
- ✅ Migrate existing data
- ✅ Create indexes

**Testing**:
- Verify data migration
- Check FK constraints
- Verify indexes created

---

### Phase 2: Core Evaluation (2-3 hours)

**Deliverables**:
- ✅ Add `evaluate_schedule_conditions()` function
- ✅ Update `should_execute_now()` integration
- ✅ Add condition evaluation logging
- ✅ Error handling for condition failures

**Testing**:
- Test with SQL condition
- Test with FUNCTION condition
- Test with PLSQL condition
- Test error handling
- Test AND/OR logic
- Test fail_on_error behavior

---

### Phase 3: Package & View Updates (2-3 hours)

**Deliverables**:
- ✅ Update package to use new table names
- ✅ Update `schedule_rec` type
- ✅ Update all monitoring views
- ✅ Create new condition monitoring views

**Testing**:
- Package compiles
- Views return correct data
- Old queries still work

---

### Phase 4: Monitoring & Helpers (2-3 hours)

**Deliverables**:
- ✅ Create `v_dwh_schedule_conditions` view
- ✅ Create condition helper procedures
- ✅ Document condition library examples
- ✅ Update operational runbooks

**Testing**:
- View condition evaluation results
- Test helper procedures
- Validate examples

---

## Examples

### Example 1: Simple CPU Check

```sql
INSERT INTO cmr.dwh_schedule_conditions (
    schedule_id,
    condition_name,
    condition_type,
    condition_code,
    evaluation_order,
    description
) VALUES (
    1,
    'CPU_THRESHOLD',
    'SQL',
    'SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END
     FROM v$sysmetric
     WHERE metric_name = ''CPU Usage Per Sec'' AND group_id = 2',
    1,
    'Only run if CPU < 70%'
);
```

---

### Example 2: Multiple Conditions with AND Logic

```sql
-- Condition 1: CPU < 70%
INSERT INTO cmr.dwh_schedule_conditions VALUES (
    NULL, 1, 'CPU_CHECK', 'SQL',
    'SELECT CASE WHEN AVG(value) < 70 THEN 1 ELSE 0 END FROM v$sysmetric WHERE metric_name = ''CPU Usage Per Sec''',
    1, 'AND', 'Y', 'Y', 'CPU below 70%', NULL, NULL, NULL, 0, 0, 0, USER, SYSDATE, USER, SYSDATE
);

-- Condition 2: No backup running
INSERT INTO cmr.dwh_schedule_conditions VALUES (
    NULL, 1, 'NO_BACKUP', 'SQL',
    'SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM v$rman_backup_job_details WHERE status = ''RUNNING''',
    2, 'AND', 'Y', 'Y', 'No backup running', NULL, NULL, NULL, 0, 0, 0, USER, SYSDATE, USER, SYSDATE
);

-- Logic: CPU_CHECK AND NO_BACKUP
```

---

## Summary

### Benefits

1. **Generalized Framework**: Not just for ILM, supports any scheduled job type
2. **Flexible Conditions**: SQL, FUNCTION, or PLSQL conditions
3. **Complex Logic**: Multiple conditions with AND/OR operators
4. **Observability**: Track condition evaluation history
5. **Fail-Safe**: Configurable error handling
6. **Simple Migration**: Direct rename and data migration (no backward compatibility needed)

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Rename tables (remove ILM) | Make framework reusable for other job types |
| CLOB for condition_code | Support complex PL/SQL blocks |
| evaluation_order + logical_operator | Flexible AND/OR combinations |
| fail_on_error flag | Allow both fail-safe and continue-on-error behaviors |
| Track evaluation history | Enable troubleshooting and monitoring |
| No backward compatibility | Clean break, simpler migration |

### Next Steps

1. **Review and approve this design**
2. **Begin Phase 1 implementation** (schema migration)
3. **Iterative testing** after each phase
4. **Update all dependent code** to use new table names

---

## Implementation Summary

### ✅ Implementation Complete (2025-11-24)

All phases of the schedule conditions framework have been successfully implemented:

**Phase 1: Schema Migration** ✅
- Created `schedule_conditions_migration.sql`
- Renames old tables with `_old` suffix
- Creates new generalized tables (dwh_execution_schedules, dwh_execution_state, dwh_schedule_conditions)
- Migrates existing data with schedule_type='ILM'

**Phase 2: Core Evaluation** ✅
- Updated `scheduler_enhancement_engine.sql`
- Added `evaluate_schedule_conditions()` function with full implementation
- Supports SQL, FUNCTION, and PLSQL condition types
- Implements AND/OR logical operators with short-circuit optimization
- Tracks evaluation history (success/failure counts, last result, last error)

**Phase 3: Integration** ✅
- Updated `should_execute_now()` to check conditions (Priority 4)
- Updated `schedule_rec` type to include `schedule_type` field
- Updated `get_schedule_config()` to query new table
- Updated all references to dwh_execution_state table

**Phase 4: Monitoring** ✅
- Created `schedule_conditions_monitoring.sql`
- Updated existing views to use new table names
- Created 3 new condition-specific views:
  - `v_dwh_schedule_conditions` - All conditions with metrics
  - `v_dwh_condition_failures` - Currently failing conditions
  - `v_dwh_schedule_readiness` - Overall readiness check

**Phase 5: Examples** ✅
- Created `scheduler_conditions_examples.sql`
- 10 working examples covering all condition types
- Includes testing instructions and cleanup procedures

### Files Created/Modified

| File | Type | Description |
|------|------|-------------|
| `scripts/scheduler_enhancement_setup.sql` | MODIFIED | Consolidated setup with migration + monitoring |
| `scripts/scheduler_enhancement_engine.sql` | MODIFIED | Added condition evaluation |
| `scripts/scheduler_conditions_examples.sql` | NEW | Usage examples |
| `SCHEDULE_CONDITIONS_DESIGN.md` | NEW | This document |

### Installation Order

```sql
-- 1. Run consolidated setup (migration + schema + monitoring)
@scripts/scheduler_enhancement_setup.sql

-- 2. Update execution engine (adds condition evaluation)
@scripts/scheduler_enhancement_engine.sql

-- 3. Create scheduler jobs
@scripts/scheduler_enhancement_scheduler.sql

-- 4. (Optional) Review examples
@scripts/scheduler_conditions_examples.sql
```

### Testing

After installation, verify with:

```sql
-- 1. Check tables were created
SELECT COUNT(*) FROM cmr.dwh_execution_schedules;
SELECT COUNT(*) FROM cmr.dwh_schedule_conditions;
SELECT COUNT(*) FROM cmr.dwh_execution_state;

-- 2. Check package compiles
SHOW ERRORS PACKAGE pck_dwh_ilm_execution_engine;

-- 3. Check views exist
SELECT view_name FROM all_views
WHERE owner = 'CMR'
AND view_name LIKE '%SCHEDULE%'
OR view_name LIKE '%CONDITION%'
ORDER BY view_name;

-- 4. Test condition evaluation
SELECT * FROM cmr.v_dwh_schedule_readiness;
```

---

**Document Version**: 2.0
**Created**: 2025-11-24
**Updated**: 2025-11-24 (Implementation complete)
**Status**: ✅ IMPLEMENTED
