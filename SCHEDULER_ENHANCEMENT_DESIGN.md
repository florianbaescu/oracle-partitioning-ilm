# ILM Scheduler Enhancement Design

## 🎯 Enhancement Goals

1. **Day-of-Week Scheduling** - Run on specific days (Mon-Fri, weekends, custom)
2. **Continuous Execution** - Run continuously during execution window until work complete
3. **Configurable Cooldown** - Optional pause between batches (0 = continuous, >0 = pause N minutes)
4. **Resumable Execution** - Save state, resume after interruption/failure
5. **Production Hardening** - Concurrent execution prevention, backlog management, observability
6. **Simplified Configuration** - Batch size controlled globally, window defines natural duration limit

## 📚 Reference Documentation

- **[ILM_ARCHITECTURE_ANALYSIS.md](./ILM_ARCHITECTURE_ANALYSIS.md)** - Complete architecture analysis, orchestration flow, best practices, and edge cases
- **[PARTITION_UTILITIES_REFACTORING.md](./PARTITION_UTILITIES_REFACTORING.md)** - Partition utilities refactoring status

## 📊 Current Architecture Analysis

### Current Behavior
```
┌─────────────────────────────────────────────────────────────┐
│ Job: DWH_ILM_JOB_EXECUTE                                    │
│ Schedule: FREQ=HOURLY; INTERVAL=2 (every 2 hours, always)  │
│ Executes: execute_pending_actions(p_max_operations => 10)  │
│ Issues:                                                     │
│  - Runs 24/7 regardless of day                             │
│  - Fixed periodic interval (every 2 hours)                  │
│  - Doesn't run continuously when work exists               │
│  - No resumption capability                                 │
└─────────────────────────────────────────────────────────────┘
```

### Current Flow
```
execute_pending_actions(p_max_operations => 10)
  ├─ Checks execution window (time-based only)
  ├─ Executes one batch (up to 10 operations)
  ├─ Exits (waits 2 hours for next scheduler run)
  ├─ No checkpointing
  └─ If interrupted, starts over from beginning

Problem: If queue has 1000 operations, processes only 10 every 2 hours
         = 200 hours (8+ days) to clear queue
```

## 📋 Current vs Enhanced Configuration

### Current Configuration (in `dwh_ilm_config`) - TO BE REMOVED

The system currently has basic time-window configuration:

```sql
-- Current configs in dwh_ilm_config table (WILL BE DELETED)
EXECUTION_WINDOW_START = '22:00'  -- Start time
EXECUTION_WINDOW_END   = '06:00'  -- End time

-- Used by function (WILL BE DELETED):
is_execution_window_open() → Boolean
```

**Limitations**:
- ❌ No day-of-week control
- ❌ Same window every day
- ❌ No continuous execution (stops after one batch)
- ❌ No resumability
- ❌ Inefficient for large backlogs

### Enhanced Configuration (NEW)

The new design **replaces** the basic time-window config with rich scheduling capabilities.

**Migration Strategy - CLEAN BREAK**:
1. ❌ **DELETE** `EXECUTION_WINDOW_START/END` from `dwh_ilm_config`
2. ❌ **DROP** `is_execution_window_open()` function
3. ✅ **CREATE** new schedule-based system with clean naming (no _v2 suffixes)

## 🏗️ Enhanced Architecture

### New Configuration Table (ULTRA-SIMPLIFIED)

```sql
CREATE TABLE cmr.dwh_ilm_execution_schedules (
    schedule_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    schedule_name       VARCHAR2(100) NOT NULL UNIQUE,
    enabled             CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),

    -- Execution Windows: One column per day (NULL = don't run that day)
    -- Format: 'HH24:MI-HH24:MI' (e.g., '22:00-06:00' for 10 PM to 6 AM)
    monday_hours        VARCHAR2(11),   -- e.g., '22:00-06:00' or NULL (no run)
    tuesday_hours       VARCHAR2(11),
    wednesday_hours     VARCHAR2(11),
    thursday_hours      VARCHAR2(11),
    friday_hours        VARCHAR2(11),
    saturday_hours      VARCHAR2(11),
    sunday_hours        VARCHAR2(11),

    -- Execution Control
    batch_cooldown_minutes  NUMBER DEFAULT 5 CHECK (batch_cooldown_minutes >= 0),
    -- 0 = Continuous execution (no pause between batches)
    -- >0 = Pause N minutes between batches

    -- Resumability
    enable_checkpointing    CHAR(1) DEFAULT 'Y' CHECK (enable_checkpointing IN ('Y','N')),
    checkpoint_frequency    NUMBER DEFAULT 5 CHECK (checkpoint_frequency > 0),

    created_date        DATE DEFAULT SYSDATE,
    modified_date       DATE DEFAULT SYSDATE,

    CONSTRAINT chk_monday_hours CHECK (monday_hours IS NULL OR REGEXP_LIKE(monday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_tuesday_hours CHECK (tuesday_hours IS NULL OR REGEXP_LIKE(tuesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_wednesday_hours CHECK (wednesday_hours IS NULL OR REGEXP_LIKE(wednesday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_thursday_hours CHECK (thursday_hours IS NULL OR REGEXP_LIKE(thursday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_friday_hours CHECK (friday_hours IS NULL OR REGEXP_LIKE(friday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_saturday_hours CHECK (saturday_hours IS NULL OR REGEXP_LIKE(saturday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$')),
    CONSTRAINT chk_sunday_hours CHECK (sunday_hours IS NULL OR REGEXP_LIKE(sunday_hours, '^\d{2}:\d{2}-\d{2}:\d{2}$'))
);

COMMENT ON TABLE cmr.dwh_ilm_execution_schedules IS
'ILM execution schedules with per-day time windows.
Each day column contains time range in HH24:MI-HH24:MI format (e.g., ''22:00-06:00'').
NULL value = no execution on that day.';

COMMENT ON COLUMN dwh_ilm_execution_schedules.monday_hours IS
'Execution window for Monday in format HH24:MI-HH24:MI (e.g., ''22:00-06:00'').
NULL = no execution on Monday.
Window can span midnight (e.g., ''22:00-06:00'' runs from 10 PM Monday to 6 AM Tuesday).';

COMMENT ON COLUMN dwh_ilm_execution_schedules.batch_cooldown_minutes IS
'Pause between batches in minutes.
0 = Continuous execution (run next batch immediately)
>0 = Pause N minutes between batches (e.g., 5 = 5-minute cooldown)
Batch size controlled globally by MAX_CONCURRENT_OPERATIONS in dwh_ilm_config.
Window close time provides natural duration limit.';

-- Default schedule: Mon-Fri 22:00-06:00, no weekends
INSERT INTO cmr.dwh_ilm_execution_schedules (
    schedule_name,
    monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours,
    saturday_hours, sunday_hours,
    batch_cooldown_minutes,
    enable_checkpointing, checkpoint_frequency
) VALUES (
    'DEFAULT_SCHEDULE',
    '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00', '22:00-06:00',  -- Mon-Fri
    NULL, NULL,                                                                   -- No weekends
    5,                                                                            -- 5-minute cooldown
    'Y', 5                                                                        -- Checkpoint every 5 ops
);
```

**Key Benefits**:
- ✅ **7 columns instead of 11** (7 day flags + 4 hour columns eliminated)
- ✅ **Different hours per day** (Monday 22:00-06:00, Saturday 20:00-08:00, etc.)
- ✅ **Clear intent**: NULL = don't run, otherwise run during specified hours
- ✅ **No weekday/weekend grouping** - each day is independent
- ✅ **Human-readable format** ('22:00-06:00' instead of separate start/end integers)
- ✅ **Regex validation** ensures correct HH24:MI-HH24:MI format

### New Execution State Table (for Resumability)

```sql
CREATE TABLE cmr.dwh_ilm_execution_state (
    state_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_batch_id  VARCHAR2(50) NOT NULL UNIQUE,
    schedule_id         NUMBER NOT NULL,

    -- Execution Progress
    start_time          TIMESTAMP DEFAULT SYSTIMESTAMP,
    last_checkpoint     TIMESTAMP,
    end_time            TIMESTAMP,
    status              VARCHAR2(20) DEFAULT 'RUNNING',  -- RUNNING, COMPLETED, INTERRUPTED, FAILED

    -- Work Tracking
    last_queue_id       NUMBER,       -- Last queue_id processed (for resumption)
    operations_completed NUMBER DEFAULT 0,
    operations_total    NUMBER,       -- Total pending when batch started
    operations_remaining NUMBER GENERATED ALWAYS AS (operations_total - operations_completed) VIRTUAL,

    -- Timing
    elapsed_seconds     NUMBER,

    CONSTRAINT fk_exec_state_schedule
        FOREIGN KEY (schedule_id)
        REFERENCES cmr.dwh_ilm_execution_schedules(schedule_id)
);

-- Index for quick lookups
CREATE INDEX idx_exec_state_status ON cmr.dwh_ilm_execution_state(status, start_time);
CREATE INDEX idx_exec_state_batch ON cmr.dwh_ilm_execution_state(execution_batch_id);
CREATE INDEX idx_exec_state_schedule ON cmr.dwh_ilm_execution_state(schedule_id, status);
```

### Enhanced Queue Table (Add Resume Support)

```sql
-- Add to existing dwh_ilm_evaluation_queue table
ALTER TABLE cmr.dwh_ilm_evaluation_queue ADD (
    execution_batch_id  VARCHAR2(50),       -- Link to execution batch
    execution_order     NUMBER,             -- Order within batch
    checkpoint_id       NUMBER              -- Checkpoint marker
);

CREATE INDEX idx_eval_queue_batch ON cmr.dwh_ilm_evaluation_queue(execution_batch_id, execution_order);
CREATE INDEX idx_eval_queue_checkpoint ON cmr.dwh_ilm_evaluation_queue(checkpoint_id);
```

## 🔄 Enhanced Execution Flow

### New Main Procedure Signature

```sql
PROCEDURE execute_pending_actions(
    p_schedule_name VARCHAR2 DEFAULT 'DEFAULT_SCHEDULE',
    p_resume_batch_id VARCHAR2 DEFAULT NULL,  -- Resume specific batch
    p_force_run BOOLEAN DEFAULT FALSE         -- Bypass day/time checks
);
```

**Note**: This REPLACES the old `execute_pending_actions(p_max_operations)` procedure.
**Key Change**: Runs **continuously** during window (LOOP) instead of one batch and exit.

### Continuous Execution Logic

```sql
PROCEDURE execute_pending_actions(
    p_schedule_name VARCHAR2 DEFAULT 'DEFAULT_SCHEDULE',
    p_resume_batch_id VARCHAR2 DEFAULT NULL,
    p_force_run BOOLEAN DEFAULT FALSE
) AS
    v_schedule SCHEDULE_REC;
    v_batch_id VARCHAR2(50);
    v_window_end_time TIMESTAMP;
    v_pending_count NUMBER;
    v_batch_count NUMBER := 0;
    v_max_operations NUMBER;
BEGIN
    v_schedule := get_schedule_config(p_schedule_name);

    -- Get batch size from global config
    SELECT TO_NUMBER(config_value) INTO v_max_operations
    FROM cmr.dwh_ilm_config
    WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';

    -- Check if should run today
    IF NOT p_force_run AND NOT should_run_today(v_schedule) THEN
        log_info('Not scheduled to run today');
        RETURN;
    END IF;

    -- Check if in execution window
    IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
        log_info('Outside execution window');
        RETURN;
    END IF;

    -- Calculate window end time
    v_window_end_time := calculate_window_end_time(v_schedule);

    log_info('Starting continuous execution (cooldown=' || v_schedule.batch_cooldown_minutes ||
             ' min, batch_size=' || v_max_operations || ')');

    -- ================================================================
    -- ⭐ CONTINUOUS EXECUTION LOOP (runs until queue empty or window closes)
    -- ================================================================
    LOOP
        -- Check if still in window
        IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
            log_info('Window closed - ending execution');
            EXIT;
        END IF;

        -- Check if work exists
        SELECT COUNT(*) INTO v_pending_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE execution_status = 'PENDING' AND eligible = 'Y';

        IF v_pending_count = 0 THEN
            log_info('No more work in queue - ending execution');
            EXIT;
        END IF;

        -- Generate new batch ID
        v_batch_count := v_batch_count + 1;
        v_batch_id := 'BATCH_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDD_HH24MISS') ||
                      '_' || LPAD(v_batch_count, 3, '0');

        log_info('Batch ' || v_batch_count || ': ' || v_pending_count || ' operations pending');

        -- Create execution state record
        INSERT INTO cmr.dwh_ilm_execution_state (
            execution_batch_id, schedule_id, start_time, status, operations_total
        ) VALUES (
            v_batch_id, v_schedule.schedule_id, SYSTIMESTAMP, 'RUNNING',
            LEAST(v_pending_count, v_max_operations)
        );
        COMMIT;

        -- Execute one batch
        BEGIN
            execute_single_batch(
                p_batch_id => v_batch_id,
                p_schedule => v_schedule,
                p_max_operations => v_max_operations
            );

            UPDATE cmr.dwh_ilm_execution_state
            SET status = 'COMPLETED', end_time = SYSTIMESTAMP
            WHERE execution_batch_id = v_batch_id;
            COMMIT;

        EXCEPTION
            WHEN OTHERS THEN
                UPDATE cmr.dwh_ilm_execution_state
                SET status = 'FAILED', end_time = SYSTIMESTAMP
                WHERE execution_batch_id = v_batch_id;
                COMMIT;
                log_error('Batch failed: ' || SQLERRM);
        END;

        -- ⭐ Cooldown between batches (if configured)
        IF v_schedule.batch_cooldown_minutes > 0 THEN
            log_info('Cooldown: ' || v_schedule.batch_cooldown_minutes || ' minutes');
            DBMS_LOCK.SLEEP(v_schedule.batch_cooldown_minutes * 60);

            -- Check if window closed during cooldown
            IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
                log_info('Window closed during cooldown - ending execution');
                EXIT;
            END IF;
        END IF;

    END LOOP;
    -- ================================================================

    log_info('Continuous execution ended - Total batches: ' || v_batch_count);

END execute_pending_actions;
```

**Key Differences from Old Approach**:
- ✅ **LOOP** instead of single batch execution
- ✅ Runs until **queue empty** OR **window closes**
- ✅ Configurable **cooldown between batches** (0-N minutes)
- ✅ Batch size from **global config** (not per-schedule)
- ✅ Multiple batches tracked separately (BATCH_001, BATCH_002, etc.)

### Single Batch Execution (with Checkpointing)

```sql
PROCEDURE execute_single_batch(
    p_batch_id VARCHAR2,
    p_schedule SCHEDULE_REC,
    p_max_operations NUMBER
) AS
    v_ops_count NUMBER := 0;
    v_checkpoint_counter NUMBER := 0;
BEGIN
    -- Get queue items ordered by policy priority
    FOR queue_item IN (
        SELECT q.queue_id, q.policy_id, p.priority
        FROM cmr.dwh_ilm_evaluation_queue q
        JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
        WHERE q.execution_status = 'PENDING'
        AND q.eligible = 'Y'
        ORDER BY p.priority, q.evaluation_date, q.queue_id
        FETCH FIRST p_max_operations ROWS ONLY  -- ⭐ Limit by global config
    ) LOOP

        -- Execute action
        BEGIN
            execute_single_action(queue_item.queue_id);
            v_ops_count := v_ops_count + 1;
            v_checkpoint_counter := v_checkpoint_counter + 1;

            -- Periodic checkpoint
            IF v_checkpoint_counter >= p_schedule.checkpoint_frequency THEN
                checkpoint(p_batch_id, queue_item.queue_id, v_ops_count, 'RUNNING');
                v_checkpoint_counter := 0;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                log_error('Operation failed for queue_id=' || queue_item.queue_id || ': ' || SQLERRM);
                -- Continue with next item (don't stop batch on single failure)
        END;

    END LOOP;

    -- Final update
    UPDATE cmr.dwh_ilm_execution_state
    SET operations_completed = v_ops_count
    WHERE execution_batch_id = p_batch_id;

END execute_single_batch;
```

**Key Points**:
- ✅ Processes up to `MAX_CONCURRENT_OPERATIONS` from global config
- ✅ Ordered by policy priority (high priority first)
- ✅ Checkpoints every N operations (configurable)
- ✅ Single operation failure doesn't stop batch

### Checkpoint Procedure

```sql
PROCEDURE checkpoint(
    p_batch_id VARCHAR2,
    p_last_queue_id NUMBER,
    p_operations_completed NUMBER,
    p_status VARCHAR2
) AS
BEGIN
    UPDATE cmr.dwh_ilm_execution_state
    SET last_checkpoint = SYSTIMESTAMP,
        last_queue_id = p_last_queue_id,
        operations_completed = p_operations_completed,
        status = p_status,
        end_time = CASE WHEN p_status IN ('COMPLETED', 'FAILED')
                        THEN SYSTIMESTAMP
                        ELSE NULL END,
        elapsed_seconds = EXTRACT(SECOND FROM (SYSTIMESTAMP - start_time)) +
                         EXTRACT(MINUTE FROM (SYSTIMESTAMP - start_time)) * 60 +
                         EXTRACT(HOUR FROM (SYSTIMESTAMP - start_time)) * 3600
    WHERE execution_batch_id = p_batch_id;

    COMMIT;  -- Commit checkpoint
END;
```

## 📅 Enhanced Scheduler Jobs

### New Job Configuration

Instead of single fixed schedule, create **dynamic scheduler** that checks configuration:

```sql
-- Main scheduler job (runs frequently, checks config)
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'DWH_ILM_JOB_EXECUTE',
        program_name => 'DWH_ILM_EXECUTE_ACTIONS',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=1',  -- Check every hour
        enabled => TRUE,
        comments => 'Smart scheduler - checks config and executes if criteria met'
    );
END;
```

**Note**: This REPLACES the old `DWH_ILM_JOB_EXECUTE` job (which had fixed 2-hour interval).

### Execution Program

```sql
BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'DWH_ILM_EXECUTE_ACTIONS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            DECLARE
                v_should_run BOOLEAN;
            BEGIN
                -- Check if should run based on schedule config
                v_should_run := pck_dwh_ilm_execution_engine.should_execute_now(
                    p_schedule_name => ''DEFAULT_SCHEDULE''
                );

                IF v_should_run THEN
                    pck_dwh_ilm_execution_engine.execute_pending_actions(
                        p_schedule_name => ''DEFAULT_SCHEDULE''
                    );
                ELSE
                    DBMS_OUTPUT.PUT_LINE(''Not scheduled to run at this time'');
                END IF;
            END;',
        enabled => TRUE,
        comments => 'Checks schedule config and executes if appropriate'
    );
END;
```

**Note**: This REPLACES the old `DWH_ILM_EXECUTE_ACTIONS` program.

## 🎯 New Helper Functions

### Should Run Today?

```sql
FUNCTION get_today_hours(
    p_schedule SCHEDULE_REC
) RETURN VARCHAR2
AS
    v_day_of_week VARCHAR2(10);
BEGIN
    v_day_of_week := TRIM(TO_CHAR(SYSDATE, 'DAY'));

    RETURN CASE v_day_of_week
        WHEN 'MONDAY'    THEN p_schedule.monday_hours
        WHEN 'TUESDAY'   THEN p_schedule.tuesday_hours
        WHEN 'WEDNESDAY' THEN p_schedule.wednesday_hours
        WHEN 'THURSDAY'  THEN p_schedule.thursday_hours
        WHEN 'FRIDAY'    THEN p_schedule.friday_hours
        WHEN 'SATURDAY'  THEN p_schedule.saturday_hours
        WHEN 'SUNDAY'    THEN p_schedule.sunday_hours
        ELSE NULL
    END;
END;
```

### Is In Execution Window?

```sql
FUNCTION is_in_execution_window(
    p_schedule SCHEDULE_REC
) RETURN BOOLEAN
AS
    v_today_hours VARCHAR2(11);
    v_start_time VARCHAR2(5);
    v_end_time VARCHAR2(5);
    v_start_hour NUMBER;
    v_start_min NUMBER;
    v_end_hour NUMBER;
    v_end_min NUMBER;
    v_current_time NUMBER;  -- Minutes since midnight
    v_start_minutes NUMBER; -- Window start in minutes since midnight
    v_end_minutes NUMBER;   -- Window end in minutes since midnight
BEGIN
    -- Get today's hours (e.g., '22:00-06:00' or NULL)
    v_today_hours := get_today_hours(p_schedule);

    -- If NULL, no execution today
    IF v_today_hours IS NULL THEN
        RETURN FALSE;
    END IF;

    -- Parse 'HH24:MI-HH24:MI' format
    v_start_time := SUBSTR(v_today_hours, 1, 5);   -- '22:00'
    v_end_time := SUBSTR(v_today_hours, 7, 5);     -- '06:00'

    v_start_hour := TO_NUMBER(SUBSTR(v_start_time, 1, 2));
    v_start_min := TO_NUMBER(SUBSTR(v_start_time, 4, 2));
    v_end_hour := TO_NUMBER(SUBSTR(v_end_time, 1, 2));
    v_end_min := TO_NUMBER(SUBSTR(v_end_time, 4, 2));

    -- Convert to minutes since midnight
    v_current_time := TO_NUMBER(TO_CHAR(SYSDATE, 'HH24')) * 60 + TO_NUMBER(TO_CHAR(SYSDATE, 'MI'));
    v_start_minutes := v_start_hour * 60 + v_start_min;
    v_end_minutes := v_end_hour * 60 + v_end_min;

    -- Handle windows that cross midnight (e.g., '22:00-06:00')
    IF v_start_minutes > v_end_minutes THEN
        RETURN v_current_time >= v_start_minutes OR v_current_time < v_end_minutes;
    ELSE
        RETURN v_current_time >= v_start_minutes AND v_current_time < v_end_minutes;
    END IF;
END;
```

### Should Execute Now? (Work-Based Check - SIMPLIFIED)

```sql
FUNCTION should_execute_now(
    p_schedule_name VARCHAR2
) RETURN BOOLEAN
AS
    v_schedule SCHEDULE_REC;
    v_running_count NUMBER;
    v_pending_work NUMBER;
BEGIN
    v_schedule := get_schedule_config(p_schedule_name);

    -- Priority 1: Prevent concurrent execution
    SELECT COUNT(*) INTO v_running_count
    FROM cmr.dwh_ilm_execution_state
    WHERE schedule_id = v_schedule.schedule_id
    AND status = 'RUNNING';

    IF v_running_count > 0 THEN
        RETURN FALSE;  -- Already running
    END IF;

    -- Priority 2: Check if work exists
    SELECT COUNT(*) INTO v_pending_work
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING'
    AND eligible = 'Y';

    IF v_pending_work = 0 THEN
        RETURN FALSE;  -- No work to do
    END IF;

    -- Priority 3: Check execution window (also checks if should run today)
    IF NOT is_in_execution_window(v_schedule) THEN
        RETURN FALSE;  -- Either not scheduled today (NULL) or outside time window
    END IF;

    -- All checks passed
    RETURN TRUE;
END;
```

**Key Changes from Old Design**:
- ❌ Removed interval check (continuous execution loop handles spacing via cooldown)
- ✅ Added work-based check (skip if queue empty - saves resources)
- ✅ Added concurrency check (prevent multiple scheduler jobs running)
- ✅ Checks prioritized for efficiency (fail fast)

## 📊 Monitoring Views

### Active Batch Status

```sql
CREATE OR REPLACE VIEW v_dwh_ilm_active_batches AS
SELECT
    es.execution_batch_id,
    es.status,
    es.start_time,
    es.last_checkpoint,
    ROUND((EXTRACT(SECOND FROM (SYSTIMESTAMP - es.start_time)) +
           EXTRACT(MINUTE FROM (SYSTIMESTAMP - es.start_time)) * 60 +
           EXTRACT(HOUR FROM (SYSTIMESTAMP - es.start_time)) * 3600) / 60, 2) AS elapsed_minutes,
    es.operations_completed,
    es.operations_total,
    ROUND(es.operations_completed * 100.0 / NULLIF(es.operations_total, 0), 1) AS pct_complete,
    sch.schedule_name,
    sch.batch_cooldown_minutes,
    cfg.config_value AS max_operations_per_batch
FROM cmr.dwh_ilm_execution_state es
JOIN cmr.dwh_ilm_execution_schedules sch ON sch.schedule_id = es.schedule_id
CROSS JOIN (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'MAX_CONCURRENT_OPERATIONS') cfg
WHERE es.status IN ('RUNNING', 'INTERRUPTED')
ORDER BY es.start_time DESC;
```

### Schedule Effectiveness

```sql
CREATE OR REPLACE VIEW v_dwh_ilm_schedule_stats AS
SELECT
    sch.schedule_name,
    sch.enabled,
    COUNT(DISTINCT es.execution_batch_id) AS total_executions,
    SUM(CASE WHEN es.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN es.status = 'INTERRUPTED' THEN 1 ELSE 0 END) AS interrupted,
    SUM(CASE WHEN es.status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(es.operations_completed), 1) AS avg_ops_per_run,
    ROUND(AVG(es.elapsed_seconds / 60), 1) AS avg_duration_minutes,
    MAX(es.start_time) AS last_execution
FROM cmr.dwh_ilm_execution_schedules sch
LEFT JOIN cmr.dwh_ilm_execution_state es ON es.schedule_id = sch.schedule_id
GROUP BY sch.schedule_name, sch.enabled
ORDER BY last_execution DESC NULLS LAST;
```

## 🚀 Usage Examples

### Example 1: Weekdays Only, Continuous Execution

```sql
-- Configure for weekdays only, continuous execution during window
UPDATE cmr.dwh_ilm_execution_schedules
SET monday_hours = '20:00-08:00',
    tuesday_hours = '20:00-08:00',
    wednesday_hours = '20:00-08:00',
    thursday_hours = '20:00-08:00',
    friday_hours = '20:00-08:00',
    saturday_hours = NULL,  -- No execution on Saturday
    sunday_hours = NULL,    -- No execution on Sunday
    batch_cooldown_minutes = 0  -- Continuous (no pause between batches)
WHERE schedule_name = 'DEFAULT_SCHEDULE';

-- Batch size controlled globally
UPDATE cmr.dwh_ilm_config
SET config_value = '20'
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';
```

### Example 2: Weekend Only, Extended Hours with Cooldown

```sql
-- Create weekend-only schedule with longer windows and cooldown
INSERT INTO cmr.dwh_ilm_execution_schedules (
    schedule_name,
    monday_hours, tuesday_hours, wednesday_hours, thursday_hours, friday_hours,
    saturday_hours, sunday_hours,
    batch_cooldown_minutes
) VALUES (
    'WEEKEND_BATCH',
    NULL, NULL, NULL, NULL, NULL,  -- No weekdays
    '00:00-23:59',                  -- Saturday: all day
    '00:00-23:59',                  -- Sunday: all day
    10                              -- 10-minute pause between batches (breathing room)
);

-- Configure larger batch size for weekend processing
UPDATE cmr.dwh_ilm_config
SET config_value = '100'
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';
```

### Example 3: Different Hours Per Day

```sql
-- Different execution windows for each day
UPDATE cmr.dwh_ilm_execution_schedules
SET monday_hours = '22:00-06:00',    -- Mon night to Tue morning
    tuesday_hours = '22:00-06:00',   -- Tue night to Wed morning
    wednesday_hours = '22:00-06:00', -- Wed night to Thu morning
    thursday_hours = '22:00-06:00',  -- Thu night to Fri morning
    friday_hours = '20:00-10:00',    -- Fri night to Sat morning (longer window)
    saturday_hours = '18:00-12:00',  -- Sat afternoon to Sun noon (extended weekend)
    sunday_hours = NULL,             -- No execution on Sunday
    batch_cooldown_minutes = 5
WHERE schedule_name = 'VARIABLE_SCHEDULE';
```

### Example 4: Resume Interrupted Batch

```sql
-- Find interrupted batch
SELECT execution_batch_id
FROM v_dwh_ilm_active_batches
WHERE status = 'INTERRUPTED';

-- Resume manually
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(
    p_resume_batch_id => 'BATCH_20250121_143052'
);
```

### Example 5: Force Run Outside Schedule

```sql
-- Run immediately regardless of day/time
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(
    p_schedule_name => 'DEFAULT_SCHEDULE',
    p_force_run => TRUE
);
```

## 🚨 Critical Edge Cases & Solutions

### Edge Case 1: Interrupted Batch Resumption

**Problem**: If scheduler job fails mid-execution (DB bounce, job kill, session timeout), all progress is lost.

**Solution**: Automatic checkpoint detection and resume

```sql
PROCEDURE execute_pending_actions(...) AS
    v_interrupted_batch VARCHAR2(50);
BEGIN
    -- Check for interrupted batch from previous run
    SELECT execution_batch_id INTO v_interrupted_batch
    FROM cmr.dwh_ilm_execution_state
    WHERE status = 'INTERRUPTED'
    AND schedule_id = v_schedule.schedule_id
    ORDER BY start_time DESC
    FETCH FIRST 1 ROW ONLY;

    IF v_interrupted_batch IS NOT NULL THEN
        log_info('Resuming interrupted batch: ' || v_interrupted_batch);
        execute_batch(
            p_batch_id => v_interrupted_batch,
            p_schedule => v_schedule,
            p_resuming => TRUE
        );
        RETURN;
    END IF;

    -- No interrupted batch - start new
    v_new_batch_id := generate_batch_id();
    execute_batch(p_batch_id => v_new_batch_id, ...);
END;
```

**Test Scenarios**:
- Database bounce during execution
- Scheduler job killed by DBA
- Session timeout (60+ minute operations)

---

### Edge Case 2: Concurrent Execution Prevention

**Problem**: If batch runs longer than interval (3-hour job on 2-hour interval), multiple jobs execute concurrently.

**Solution**: Lock-based concurrency check

```sql
PROCEDURE execute_pending_actions(...) AS
    v_running_count NUMBER;
    v_lock_acquired BOOLEAN := FALSE;
BEGIN
    -- Check for already running batches
    SELECT COUNT(*) INTO v_running_count
    FROM cmr.dwh_ilm_execution_state
    WHERE status = 'RUNNING'
    AND schedule_id = v_schedule.schedule_id;

    IF v_running_count > 0 THEN
        log_info('Batch already running - skipping this execution');
        RETURN;
    END IF;

    -- Try to acquire application lock
    v_lock_result := DBMS_LOCK.REQUEST(
        id => DBMS_UTILITY.GET_HASH_VALUE('DWH_ILM_EXEC_LOCK', 0, 1073741824),
        lockmode => DBMS_LOCK.X_MODE,
        timeout => 0,  -- No wait
        release_on_commit => FALSE
    );

    IF v_lock_result != 0 THEN
        log_info('Could not acquire execution lock - another job running');
        RETURN;
    END IF;

    -- Proceed with execution
    BEGIN
        execute_batch(...);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_LOCK.RELEASE(v_lock_id);
            RAISE;
    END;

    DBMS_LOCK.RELEASE(v_lock_id);
END;
```

**Test Scenarios**:
- Manually run job while scheduler job running
- Long COMPRESS operation (3+ hours)
- Multiple scheduler jobs triggered simultaneously

---

### Edge Case 3: Window Close Time Enforcement

**Problem**: Execution continues past window close time.

**Solution**: Time-aware execution with graceful shutdown

```sql
PROCEDURE execute_batch(...) AS
    v_start_time TIMESTAMP := SYSTIMESTAMP;
    v_window_end_time TIMESTAMP;
BEGIN
    -- Calculate execution window end time
    v_window_end_time := calculate_window_end_time(p_schedule);

    FOR queue_item IN (...) LOOP
        -- Check if time to stop BEFORE starting next operation
        IF SYSTIMESTAMP >= v_window_end_time THEN
            log_info('Window closing - stopping gracefully');
            checkpoint(p_batch_id, queue_item.queue_id, v_ops_count, 'INTERRUPTED');
            EXIT;
        END IF;

        -- Estimate if next operation will complete in time
        v_avg_duration := get_average_operation_duration(queue_item.action_type);
        IF SYSTIMESTAMP + v_avg_duration > v_window_end_time THEN
            log_info('Insufficient time for next operation - stopping');
            checkpoint(p_batch_id, queue_item.queue_id, v_ops_count, 'INTERRUPTED');
            EXIT;
        END IF;

        execute_single_action(queue_item.queue_id);
    END LOOP;
END;
```

**Test Scenarios**:
- Execution started at 05:30, window closes at 06:00
- Last operation takes 45 minutes (started at 05:50, would finish at 06:35)
- Cooldown period overlaps with window close time

---

### Edge Case 4: Queue Backlog Management

**Problem**: Queue grows faster than execution capacity → infinite backlog.

**Solution**: Multi-tier alerting and emergency capacity

```sql
-- Monitor backlog growth rate
CREATE OR REPLACE VIEW v_dwh_ilm_queue_backlog AS
SELECT
    p.priority,
    p.policy_name,
    COUNT(*) AS pending_count,
    MIN(q.evaluation_date) AS oldest_pending,
    TRUNC(SYSDATE - MIN(q.evaluation_date)) AS days_waiting
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
WHERE q.execution_status = 'PENDING'
AND q.eligible = 'Y'
GROUP BY p.priority, p.policy_name
ORDER BY p.priority;

-- Alert thresholds
PROCEDURE check_queue_backlog AS
    v_total_backlog NUMBER;
    v_max_wait_days NUMBER;
BEGIN
    SELECT COUNT(*), MAX(TRUNC(SYSDATE - evaluation_date))
    INTO v_total_backlog, v_max_wait_days
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING';

    -- CRITICAL: Backlog > 1000 or waiting > 7 days
    IF v_total_backlog > 1000 OR v_max_wait_days > 7 THEN
        send_alert('CRITICAL',
            'ILM queue backlog: ' || v_total_backlog || ' items, ' ||
            'max wait: ' || v_max_wait_days || ' days');
    -- WARNING: Backlog > 500 or waiting > 3 days
    ELSIF v_total_backlog > 500 OR v_max_wait_days > 3 THEN
        send_alert('WARNING',
            'ILM queue backlog growing: ' || v_total_backlog || ' items');
    END IF;
END;

-- Emergency execution mode
PROCEDURE emergency_execute(
    p_batch_size NUMBER DEFAULT 100
) AS
BEGIN
    execute_pending_actions(
        p_schedule_name => 'DEFAULT_SCHEDULE',
        p_force_run => TRUE,  -- Bypass day/time checks
        p_override_batch_size => p_batch_size  -- Larger batch
    );
END;
```

**Metrics to Track**:
- Queue size trend (daily)
- Evaluation rate vs execution rate
- Oldest pending item age

**Alert Strategy**:
- **WARNING**: Backlog > 500 OR age > 3 days
- **CRITICAL**: Backlog > 1000 OR age > 7 days

---

### Edge Case 5: Policy Priority Starvation

**Problem**: Low-priority policies never execute if high-priority queue never empties.

**Solution**: Age-based priority boost or guaranteed minimum allocations

**Option A: Age-Based Priority Boost**
```sql
-- Add virtual priority column
ALTER TABLE cmr.dwh_ilm_evaluation_queue ADD (
    effective_priority NUMBER GENERATED ALWAYS AS (
        CASE
            WHEN TRUNC(SYSDATE - evaluation_date) > 7
            THEN policy_priority - 50  -- Boost priority after 7 days
            WHEN TRUNC(SYSDATE - evaluation_date) > 14
            THEN policy_priority - 100  -- Further boost after 14 days
            ELSE policy_priority
        END
    ) VIRTUAL
);

-- Use effective priority in execution order
ORDER BY effective_priority, policy_id, evaluation_date
```

**Option B: Guaranteed Minimum Allocation**
```sql
-- Execute at least 1 operation per priority tier
PROCEDURE execute_batch(...) AS
    v_ops_by_priority PRIORITY_OPS_MAP;
BEGIN
    -- Ensure each priority gets at least min_ops
    FOR priority_tier IN (SELECT DISTINCT priority FROM policies ORDER BY priority) LOOP
        v_min_ops := CASE
            WHEN priority_tier <= 200 THEN 5  -- High priority: 5 ops minimum
            WHEN priority_tier <= 500 THEN 3  -- Med priority: 3 ops minimum
            ELSE 1  -- Low priority: 1 op minimum
        END;

        -- Execute minimum operations for this priority
        execute_policy_tier(priority_tier, v_min_ops);
    END LOOP;
END;
```

**Recommended**: Use **Option A** (age-based boost) for fairness without complexity.

---

### Edge Case 6: Partial Batch Completion Status

**Problem**: Batch status unclear when stopped early (5/10 operations completed).

**Solution**: Distinguish COMPLETED vs INTERRUPTED

```sql
-- Status definitions
COMPLETED    - All pending operations executed successfully
INTERRUPTED  - Stopped due to time/duration limits, more work pending
FAILED       - Critical error prevented execution

-- Set status logic
PROCEDURE checkpoint(..., p_status VARCHAR2) AS
    v_final_status VARCHAR2(20);
    v_remaining_count NUMBER;
BEGIN
    -- Check if work remaining
    SELECT COUNT(*) INTO v_remaining_count
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_batch_id = p_batch_id
    AND execution_status = 'PENDING';

    -- Determine final status
    IF p_status = 'RUNNING' AND v_remaining_count = 0 THEN
        v_final_status := 'COMPLETED';
    ELSIF p_status = 'RUNNING' AND v_remaining_count > 0 THEN
        v_final_status := 'INTERRUPTED';  -- More work pending
    ELSE
        v_final_status := p_status;  -- FAILED, etc.
    END IF;

    UPDATE cmr.dwh_ilm_execution_state
    SET status = v_final_status,
        ...
    WHERE execution_batch_id = p_batch_id;
END;

-- Auto-resume interrupted batches
SELECT execution_batch_id
FROM cmr.dwh_ilm_execution_state
WHERE status = 'INTERRUPTED'
AND schedule_id = ...
ORDER BY start_time DESC
FETCH FIRST 1 ROW ONLY;
```

---

### Edge Case 7: Timezone & Daylight Saving Time

**Problem**: Execution window defined in server timezone, DST transitions cause issues.

**Solution**: Document timezone behavior, handle DST transitions

```sql
-- Document timezone assumptions
COMMENT ON COLUMN dwh_ilm_execution_schedules.weekday_start_hour IS
'Execution window start hour (0-23) in DATABASE SERVER TIMEZONE.
DST transitions handled automatically by database.
Example: 22 = 10 PM server time';

-- DST-safe time comparisons
FUNCTION is_in_execution_window(p_schedule SCHEDULE_REC) RETURN BOOLEAN AS
    v_current_hour NUMBER := TO_NUMBER(TO_CHAR(SYSTIMESTAMP, 'HH24'));
    v_start_hour NUMBER;
    v_end_hour NUMBER;
BEGIN
    -- Use SYSTIMESTAMP (timezone-aware) not SYSDATE
    -- Database handles DST automatically

    v_is_weekend := TO_CHAR(SYSTIMESTAMP, 'DY') IN ('SAT', 'SUN');

    IF v_is_weekend THEN
        v_start_hour := p_schedule.weekend_start_hour;
        v_end_hour := p_schedule.weekend_end_hour;
    ELSE
        v_start_hour := p_schedule.weekday_start_hour;
        v_end_hour := p_schedule.weekday_end_hour;
    END IF;

    -- Handle windows crossing midnight (e.g., 22:00-06:00)
    IF v_start_hour > v_end_hour THEN
        RETURN v_current_hour >= v_start_hour OR v_current_hour < v_end_hour;
    ELSE
        RETURN v_current_hour >= v_start_hour AND v_current_hour < v_end_hour;
    END IF;
END;
```

**DST Test Scenarios**:
- Spring forward: 2 AM → 3 AM (lose 1 hour)
- Fall back: 2 AM → 1 AM (gain 1 hour)
- Window 22:00-06:00 on DST transition night

**Recommendation**: Oracle database handles DST automatically when using SYSTIMESTAMP.

---

### Edge Case 8: Long-Running Single Operation

**Problem**: Single COMPRESS operation takes 4 hours, blocks all other operations.

**Solution**: Operation-level timeout and parallel execution design

```sql
-- Add operation timeout to config
ALTER TABLE cmr.dwh_ilm_execution_schedules ADD (
    max_operation_duration_minutes NUMBER DEFAULT 60
);

-- Timeout wrapper for single operation
PROCEDURE execute_single_action_with_timeout(
    p_queue_id NUMBER,
    p_timeout_minutes NUMBER
) AS
    v_start_time TIMESTAMP := SYSTIMESTAMP;
    v_job_name VARCHAR2(100);
    v_job_status VARCHAR2(30);
BEGIN
    -- Create one-time job for this operation
    v_job_name := 'ILM_OP_' || p_queue_id;

    DBMS_SCHEDULER.CREATE_JOB(
        job_name => v_job_name,
        job_type => 'PLSQL_BLOCK',
        job_action => '
            BEGIN
                execute_single_action_impl(' || p_queue_id || ');
            END;',
        enabled => TRUE,
        auto_drop => TRUE
    );

    -- Wait for completion or timeout
    LOOP
        DBMS_LOCK.SLEEP(10);  -- Check every 10 seconds

        SELECT state INTO v_job_status
        FROM dba_scheduler_jobs
        WHERE job_name = v_job_name;

        EXIT WHEN v_job_status IN ('SUCCEEDED', 'FAILED');

        -- Check timeout
        IF SYSTIMESTAMP - v_start_time > INTERVAL '1' MINUTE * p_timeout_minutes THEN
            -- Stop job
            DBMS_SCHEDULER.STOP_JOB(v_job_name, force => TRUE);

            -- Log timeout
            log_error(p_queue_id, 'Operation timeout after ' || p_timeout_minutes || ' minutes');

            EXIT;
        END IF;
    END LOOP;
END;
```

**Alternative: Parallel Execution**
```sql
-- Allow multiple operations to run in parallel (different partitions)
max_concurrent_operations = 3  -- Run 3 partitions simultaneously

-- Use DBMS_SCHEDULER job queue
-- Each operation runs as separate scheduler job
-- Framework manages concurrency limit
```

**Recommendation**: Implement operation timeout (60 min default) to prevent indefinite blocking.

---

## 🏭 Production Requirements

### 1. Monitoring & Observability

**Required Metrics**:

```sql
-- Real-time execution dashboard
CREATE OR REPLACE VIEW v_dwh_ilm_execution_dashboard AS
SELECT
    'Active Batches' AS metric,
    COUNT(*) AS value
FROM cmr.dwh_ilm_execution_state
WHERE status = 'RUNNING'
UNION ALL
SELECT
    'Queue Backlog',
    COUNT(*)
FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'PENDING'
UNION ALL
SELECT
    'Today Success Rate',
    ROUND(
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    )
FROM cmr.dwh_ilm_execution_log
WHERE execution_start >= TRUNC(SYSDATE)
UNION ALL
SELECT
    'Today Space Saved (GB)',
    ROUND(SUM(space_saved_mb) / 1024, 2)
FROM cmr.dwh_ilm_execution_log
WHERE execution_start >= TRUNC(SYSDATE)
AND status = 'SUCCESS';
```

**Performance Trending**:
```sql
CREATE OR REPLACE VIEW v_dwh_ilm_performance_trend AS
SELECT
    TRUNC(execution_start) AS execution_date,
    COUNT(*) AS total_operations,
    ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
    ROUND(STDDEV(duration_seconds), 1) AS stddev_duration_sec,
    MAX(duration_seconds) AS max_duration_sec,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures
FROM cmr.dwh_ilm_execution_log
WHERE execution_start >= SYSDATE - 30
GROUP BY TRUNC(execution_start)
ORDER BY execution_date DESC;
```

---

### 2. Alerting Strategy

**Critical Alerts** (Immediate Response):

```sql
PROCEDURE check_critical_alerts AS
    v_failure_rate NUMBER;
    v_queue_backlog NUMBER;
    v_running_batches NUMBER;
    v_stalled_duration NUMBER;
BEGIN
    -- Alert 1: High failure rate
    SELECT
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)
    INTO v_failure_rate
    FROM cmr.dwh_ilm_execution_log
    WHERE execution_start >= SYSTIMESTAMP - INTERVAL '4' HOUR;

    IF v_failure_rate > 20 THEN
        send_alert('CRITICAL', 'ILM failure rate: ' || v_failure_rate || '%');
    END IF;

    -- Alert 2: Queue backlog excessive
    SELECT COUNT(*) INTO v_queue_backlog
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING';

    IF v_queue_backlog > 1000 THEN
        send_alert('CRITICAL', 'ILM queue backlog: ' || v_queue_backlog || ' items');
    END IF;

    -- Alert 3: Batch stalled (running > 6 hours)
    SELECT COUNT(*) INTO v_running_batches
    FROM cmr.dwh_ilm_execution_state
    WHERE status = 'RUNNING'
    AND start_time < SYSTIMESTAMP - INTERVAL '6' HOUR;

    IF v_running_batches > 0 THEN
        send_alert('CRITICAL', 'ILM batch stalled > 6 hours');
    END IF;

    -- Alert 4: No executions in 24 hours (scheduler disabled?)
    SELECT COUNT(*) INTO v_running_batches
    FROM cmr.dwh_ilm_execution_state
    WHERE start_time >= SYSTIMESTAMP - INTERVAL '24' HOUR;

    IF v_running_batches = 0 THEN
        send_alert('CRITICAL', 'No ILM executions in 24 hours - check scheduler');
    END IF;
END;
```

**Warning Alerts** (Next Business Day):

```sql
-- Execution duration trending upward
SELECT
    ROUND(AVG(duration_seconds) OVER (ORDER BY execution_date ROWS 7 PRECEDING), 1) AS avg_7d,
    ROUND(AVG(duration_seconds) OVER (ORDER BY execution_date ROWS 30 PRECEDING), 1) AS avg_30d
FROM (
    SELECT TRUNC(execution_start) AS execution_date, AVG(duration_seconds) AS duration_seconds
    FROM cmr.dwh_ilm_execution_log
    WHERE execution_start >= SYSDATE - 30
    GROUP BY TRUNC(execution_start)
)
WHERE avg_7d > avg_30d * 1.1;  -- 10% increase
```

---

### 3. Capacity Planning

**Storage Trend Analysis**:
```sql
CREATE OR REPLACE VIEW v_dwh_ilm_capacity_trend AS
SELECT
    TO_CHAR(execution_start, 'YYYY-MM') AS month,
    SUM(space_saved_mb) / 1024 AS space_saved_gb,
    COUNT(*) AS operations,
    ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
GROUP BY TO_CHAR(execution_start, 'YYYY-MM')
ORDER BY month DESC;

-- Projection: 3-month moving average
WITH monthly_savings AS (
    SELECT
        TO_CHAR(execution_start, 'YYYY-MM') AS month,
        SUM(space_saved_mb) / 1024 AS space_saved_gb
    FROM cmr.dwh_ilm_execution_log
    WHERE status = 'SUCCESS'
    GROUP BY TO_CHAR(execution_start, 'YYYY-MM')
)
SELECT
    month,
    space_saved_gb,
    ROUND(AVG(space_saved_gb) OVER (ORDER BY month ROWS 2 PRECEDING), 2) AS moving_avg_3m
FROM monthly_savings
ORDER BY month DESC;
```

---

### 4. Health Checks

**Automated Health Check Procedure**:
```sql
CREATE OR REPLACE PROCEDURE check_ilm_health AS
    v_health_status VARCHAR2(20) := 'HEALTHY';
    v_issues VARCHAR2(4000);
BEGIN
    -- Check 1: Scheduler jobs enabled
    FOR job IN (
        SELECT job_name, enabled
        FROM dba_scheduler_jobs
        WHERE job_name LIKE 'DWH_ILM%'
        AND enabled = 'FALSE'
    ) LOOP
        v_health_status := 'WARNING';
        v_issues := v_issues || 'Job disabled: ' || job.job_name || '; ';
    END LOOP;

    -- Check 2: Recent job failures
    FOR job IN (
        SELECT job_name, error#, additional_info
        FROM dba_scheduler_job_run_details
        WHERE job_name LIKE 'DWH_ILM%'
        AND log_date >= SYSDATE - 1
        AND status = 'FAILED'
    ) LOOP
        v_health_status := 'WARNING';
        v_issues := v_issues || 'Job failed: ' || job.job_name || '; ';
    END LOOP;

    -- Check 3: Stale execution state
    FOR batch IN (
        SELECT execution_batch_id, start_time
        FROM cmr.dwh_ilm_execution_state
        WHERE status = 'RUNNING'
        AND start_time < SYSDATE - 1
    ) LOOP
        v_health_status := 'CRITICAL';
        v_issues := v_issues || 'Stalled batch: ' || batch.execution_batch_id || '; ';
    END LOOP;

    -- Check 4: Queue backlog excessive
    DECLARE
        v_backlog NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_backlog
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE execution_status = 'PENDING';

        IF v_backlog > 1000 THEN
            v_health_status := 'WARNING';
            v_issues := v_issues || 'Queue backlog: ' || v_backlog || '; ';
        END IF;
    END;

    -- Log health check result
    INSERT INTO cmr.dwh_ilm_health_log (
        check_time,
        health_status,
        issues
    ) VALUES (
        SYSTIMESTAMP,
        v_health_status,
        v_issues
    );

    COMMIT;

    -- Alert if unhealthy
    IF v_health_status != 'HEALTHY' THEN
        send_alert(v_health_status, 'ILM Health Check: ' || v_issues);
    END IF;
END;
```

**Schedule health check**: Run every 4 hours

---

### 5. Disaster Recovery

**Backup Requirements**:
- ✅ **Critical tables**: `dwh_ilm_policies`, `dwh_ilm_execution_schedules`, `dwh_ilm_config`
- ✅ **Audit trail**: `dwh_ilm_execution_log`, `dwh_ilm_partition_merges`
- ⚠️ **Rebuildable tables**: `dwh_ilm_evaluation_queue` (can be regenerated via `evaluate_all_policies()`)
- ⚠️ **Transient state**: `dwh_ilm_execution_state` (interrupted batches can be marked FAILED and rerun)

**Recovery Procedures**:

```sql
-- Scenario 1: Clear stuck executions after DB bounce
UPDATE cmr.dwh_ilm_execution_state
SET status = 'FAILED',
    end_time = SYSTIMESTAMP
WHERE status = 'RUNNING'
AND start_time < SYSTIMESTAMP - INTERVAL '6' HOUR;

COMMIT;

-- Scenario 2: Rebuild evaluation queue
TRUNCATE TABLE cmr.dwh_ilm_evaluation_queue;
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();

-- Scenario 3: Restart scheduler jobs
EXEC dwh_start_ilm_jobs();
```

---

### 6. Performance Tuning

**Batch Size Tuning**:
```
Too Small (< 5 operations):
  - Excessive scheduler overhead
  - Checkpoint commit overhead
  - Slow queue processing

Too Large (> 50 operations):
  - Long-running batches
  - Reduced resumability granularity
  - Risk of timeout

RECOMMENDED: 10-20 operations per batch
```

**Checkpoint Frequency Tuning**:
```
Too Frequent (every 1-2 operations):
  - Commit overhead
  - Increased I/O

Too Infrequent (every 50+ operations):
  - Risk losing significant progress
  - Long recovery time

RECOMMENDED: Every 5-10 operations
```

**Parallel Execution**:
```sql
-- Current: PARALLEL 4 for DDL operations
-- Tune based on system capacity

-- Check current parallel settings
SELECT name, value
FROM v$parameter
WHERE name LIKE 'parallel%';

-- Adjust per-session
ALTER SESSION SET PARALLEL_DEGREE_POLICY = AUTO;
ALTER SESSION SET PARALLEL_MAX_SERVERS = 8;
```

---

## 🔄 Removal of Old Configuration System

### Objects to DELETE

```sql
-- 1. Drop function
DROP FUNCTION is_execution_window_open;

-- 2. Delete config entries
DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START';
DELETE FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END';
COMMIT;
```

### Replaced By

```sql
-- Old: is_execution_window_open() → Boolean
-- New: pck_dwh_ilm_execution_engine.should_execute_now('schedule_name') → Boolean

-- Old configs:
-- EXECUTION_WINDOW_START = '22:00'
-- EXECUTION_WINDOW_END   = '06:00'

-- New schedule table columns:
weekday_start_hour = 22
weekday_end_hour = 6
weekend_start_hour = 20
weekend_end_hour = 8
```

**Decision**: Clean break - delete old system entirely, no backward compatibility needed.

## ✅ Benefits

1. **Flexible Scheduling** - Run on specific days with different intervals
2. **Batch Control** - Limit operations per run to control resource usage
3. **Resumability** - Never lose progress, resume from checkpoint
4. **Day-Aware** - Different behavior weekdays vs weekends
5. **Self-Regulating** - Smart scheduler checks config dynamically
6. **Auditable** - Complete execution history with checkpoints
7. **Tunable** - Adjust batch sizes, intervals, windows without code changes
8. **Clean Design** - No legacy compatibility overhead, fresh start

## 🔧 Configuration Consolidation

### Configs to KEEP in `dwh_ilm_config`

These are **global settings** not specific to scheduling:

```sql
ENABLE_AUTO_EXECUTION       = 'Y'    -- Master on/off switch
MAX_CONCURRENT_OPERATIONS   = '5'    -- Global limit
ENABLE_EMAIL_NOTIFICATIONS  = 'Y'    -- Alerting
ALERT_EMAIL_RECIPIENTS      = 'dba@example.com'
ALERT_FAILURE_THRESHOLD     = '3'
AUTO_MERGE_PARTITIONS       = 'Y'    -- Auto-merge feature
MERGE_LOCK_TIMEOUT          = '30'   -- DDL timeout
```

### Configs to DELETE from `dwh_ilm_config`

These are **removed** and replaced by schedule table:

```sql
-- DELETE THESE:
EXECUTION_WINDOW_START  = '22:00'  → replaced by weekday_start_hour, weekend_start_hour
EXECUTION_WINDOW_END    = '06:00'  → replaced by weekday_end_hour, weekend_end_hour
```

### New Configs in Schedule Table

Per-schedule configurations (in `dwh_ilm_execution_schedules`):
- Day-of-week flags (run_on_monday, run_on_tuesday, etc.)
- Time windows (weekday/weekend specific)
- Intervals (weekday_interval_hours, weekend_interval_hours)
- Batch sizing (max_operations_per_run, max_duration_minutes)
- Checkpointing (enable_checkpointing, checkpoint_frequency)

## 📋 Implementation Checklist

### Phase 0: Cleanup Old System
- [ ] Drop `is_execution_window_open()` function
- [ ] Delete `EXECUTION_WINDOW_START` from `dwh_ilm_config`
- [ ] Delete `EXECUTION_WINDOW_END` from `dwh_ilm_config`
- [ ] Update `custom_ilm_execution_engine.sql` to remove references

### Phase 1: Schema Changes
- [ ] Create `dwh_ilm_execution_schedules` table (simplified with only 8 config fields)
- [ ] Create `dwh_ilm_execution_state` table
- [ ] Alter `dwh_ilm_evaluation_queue` (add batch columns if needed)
- [ ] Create indexes for performance
- [ ] Insert default schedule (batch_cooldown_minutes=5)

### Phase 2: Package Updates (pck_dwh_ilm_execution_engine)
- [ ] Add helper functions (should_run_today, is_in_window, should_execute_now with work-based check)
- [ ] Add batch creation/initialization logic
- [ ] Add checkpointing procedures
- [ ] Add resume logic
- [ ] Replace `execute_pending_actions` with continuous execution LOOP version
- [ ] Implement cooldown logic (DBMS_LOCK.SLEEP when batch_cooldown_minutes > 0)
- [ ] Read batch size from global MAX_CONCURRENT_OPERATIONS config
- [ ] Keep other procedures (execute_policy, execute_single_action, compress_partition, etc.)

### Phase 3: Scheduler Updates
- [ ] Drop old `DWH_ILM_EXECUTE_ACTIONS` program
- [ ] Drop old `DWH_ILM_JOB_EXECUTE` job
- [ ] Create new `DWH_ILM_EXECUTE_ACTIONS` program (enhanced)
- [ ] Create new `DWH_ILM_JOB_EXECUTE` job (hourly check)

### Phase 4: Monitoring
- [ ] Create `v_dwh_ilm_active_batches` view
- [ ] Create `v_dwh_ilm_schedule_stats` view
- [ ] Create `v_dwh_ilm_batch_progress` view
- [ ] Update existing monitoring views if needed

### Phase 5: Testing & Validation
- [ ] **Unit Tests**: Individual functions (should_run_today, is_in_window, checkpoint)
- [ ] **Integration Tests**: Complete batch execution cycle
- [ ] **Edge Case Tests**: All 8 critical edge cases documented above
- [ ] **Performance Tests**: Batch sizing, checkpoint frequency, concurrency
- [ ] **Disaster Recovery Tests**: DB bounce, job kill, session timeout
- [ ] **Documentation**: User guide and operational runbook

### Phase 6: Integrate Refactored Utilities
- [ ] Update execution engine to call partition utilities with OUT parameters
- [ ] Capture `p_sql_executed`, `p_status`, `p_error_message` from utilities
- [ ] Log results to `dwh_ilm_execution_log`

## 📊 Summary: What Changes?

### New Tables (2)
1. **`dwh_ilm_execution_schedules`** - Schedule configurations (ultra-simplified: 7 day columns instead of 11)
2. **`dwh_ilm_execution_state`** - Batch execution state tracking

### Modified Tables (1)
1. **`dwh_ilm_evaluation_queue`** - Add batch tracking columns (if needed)

### Modified Package (1)
1. **`pck_dwh_ilm_execution_engine`** - Enhanced with continuous execution, checkpointing, work-based scheduling

### Modified Scheduler Objects (2)
1. **`DWH_ILM_EXECUTE_ACTIONS`** program - Enhanced with smart scheduling logic
2. **`DWH_ILM_JOB_EXECUTE`** job - Changed to hourly check (from 2-hour fixed)

### Deleted Objects (3)
1. **`EXECUTION_WINDOW_START`** config - Replaced by schedule table
2. **`EXECUTION_WINDOW_END`** config - Replaced by schedule table
3. **`is_execution_window_open()`** function - Replaced by `should_execute_now()`

### Key Features Enabled
✅ Per-day scheduling with custom hours (different windows for each day)
✅ Continuous execution during window (LOOP-based, not periodic)
✅ Configurable cooldown between batches (0=continuous, >0=pause N minutes)
✅ Work-based execution (checks queue before running)
✅ Batch size controlled globally (MAX_CONCURRENT_OPERATIONS config)
✅ Window close time as natural duration limit (no artificial max)
✅ Resumable execution with checkpointing
✅ Better monitoring and visibility
✅ Ultra-simplified architecture (7 day columns vs 11 separate day/hour fields = 36% reduction)

### Configuration Simplifications

**Old Design** (11 fields for scheduling):
- ❌ 7 day flags: `run_on_monday`, `run_on_tuesday`, ..., `run_on_sunday`
- ❌ 4 hour fields: `weekday_start_hour`, `weekday_end_hour`, `weekend_start_hour`, `weekend_end_hour`
- ❌ Problem: Can't have different hours per day (Monday vs Friday)

**New Design** (7 fields for scheduling):
- ✅ 7 day columns: `monday_hours`, `tuesday_hours`, ..., `sunday_hours`
- ✅ Format: `'HH24:MI-HH24:MI'` (e.g., `'22:00-06:00'`)
- ✅ NULL = don't run that day
- ✅ Each day can have different hours (Monday `'22:00-06:00'`, Friday `'20:00-10:00'`, etc.)

**Also Removed** (from previous iterations):
- ❌ `weekday_interval_hours` / `weekend_interval_hours` → Replaced by `batch_cooldown_minutes`
- ❌ `max_operations_per_run` → Use global `MAX_CONCURRENT_OPERATIONS` from `dwh_ilm_config`
- ❌ `max_duration_minutes` → Window close time is natural limit
- ❌ `max_policies_per_run`, `priority_threshold`, `execution_mode` → Not needed

**Final Configuration Fields** (10 total):
- ✅ Per-day scheduling (7 fields: `monday_hours` through `sunday_hours`)
- ✅ Cooldown control (1 field: `batch_cooldown_minutes`)
- ✅ Checkpointing settings (2 fields: `enable_checkpointing`, `checkpoint_frequency`)

## 🎯 Naming Convention - NO SUFFIXES

**IMPORTANT**: All objects use clean, meaningful names without version suffixes:

✅ **DO**: `pck_dwh_ilm_execution_engine`, `execute_pending_actions`, `should_execute_now`
❌ **DON'T**: `pck_dwh_ilm_execution_engine_v2`, `execute_pending_actions_v2`, `should_execute_now_new`

**Approach**: Replace existing implementations, don't version them.

---

## 🧪 Comprehensive Testing Strategy

### Test Category 1: Unit Tests

**Test Individual Helper Functions**:

```sql
-- Test: should_run_today()
DECLARE
    v_schedule SCHEDULE_REC;
    v_result BOOLEAN;
BEGIN
    -- Setup: Monday only schedule
    v_schedule.run_on_monday := 'Y';
    v_schedule.run_on_tuesday := 'N';
    -- ... rest set to 'N'

    -- Test on Monday (should return TRUE)
    -- Test on Tuesday (should return FALSE)

    -- Assert results
    IF TO_CHAR(SYSDATE, 'DAY') = 'MONDAY   ' THEN
        ASSERT(should_run_today(v_schedule) = TRUE, 'Monday test failed');
    END IF;
END;

-- Test: is_in_execution_window()
DECLARE
    v_schedule SCHEDULE_REC;
BEGIN
    -- Setup: Window 22:00-06:00
    v_schedule.weekday_start_hour := 22;
    v_schedule.weekday_end_hour := 6;

    -- Test at 23:00 (should be TRUE)
    -- Test at 12:00 (should be FALSE)
    -- Test at 01:00 (should be TRUE - crosses midnight)
END;

-- Test: checkpoint()
-- Verify checkpoint updates execution_state correctly
-- Verify COMMIT occurs
-- Verify resume point saved
```

---

### Test Category 2: Integration Tests

**Test Complete Batch Execution Cycle**:

```sql
-- Test Scenario: End-to-End Batch Execution
PROCEDURE test_batch_execution AS
BEGIN
    -- Step 1: Setup test data
    -- Create test policy
    INSERT INTO cmr.dwh_ilm_policies (...);

    -- Create test partitions
    -- Populate evaluation queue with 20 test items

    -- Step 2: Execute batch with max_operations = 10
    execute_pending_actions(
        p_schedule_name => 'TEST_SCHEDULE'
    );

    -- Step 3: Verify results
    -- Exactly 10 operations executed
    ASSERT(get_executed_count() = 10, 'Batch size not respected');

    -- Status = 'INTERRUPTED' (10 remaining)
    ASSERT(get_batch_status() = 'INTERRUPTED', 'Status incorrect');

    -- Checkpoint saved correctly
    ASSERT(get_last_queue_id() IS NOT NULL, 'Checkpoint not saved');

    -- Step 4: Execute second batch (resume)
    execute_pending_actions(
        p_schedule_name => 'TEST_SCHEDULE'
    );

    -- Verify remaining 10 executed
    ASSERT(get_total_executed_count() = 20, 'Resume failed');

    -- Status = 'COMPLETED' (no more work)
    ASSERT(get_batch_status() = 'COMPLETED', 'Final status incorrect');
END;
```

---

### Test Category 3: Edge Case Tests

**Edge Case 1: Interrupted Batch Resumption**

```sql
PROCEDURE test_interrupted_resumption AS
    v_batch_id VARCHAR2(50);
BEGIN
    -- Setup: Start batch execution
    v_batch_id := start_test_batch(operations_count => 20);

    -- Simulate interruption after 5 operations
    EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''...,..'' IMMEDIATE';

    -- Wait for session to terminate
    DBMS_LOCK.SLEEP(5);

    -- Verify: Batch status = 'INTERRUPTED'
    ASSERT(get_batch_status(v_batch_id) = 'INTERRUPTED');

    -- Verify: Checkpoint saved (5 operations completed)
    ASSERT(get_operations_completed(v_batch_id) = 5);

    -- Execute next scheduled run
    execute_pending_actions(p_schedule_name => 'TEST_SCHEDULE');

    -- Verify: Automatically resumed from checkpoint
    -- Verify: Remaining 15 operations executed
    ASSERT(get_total_executed(v_batch_id) = 20);
    ASSERT(get_batch_status(v_batch_id) = 'COMPLETED');
END;
```

**Edge Case 2: Concurrent Execution Prevention**

```sql
PROCEDURE test_concurrent_prevention AS
    v_job_name1 VARCHAR2(100) := 'TEST_ILM_JOB_1';
    v_job_name2 VARCHAR2(100) := 'TEST_ILM_JOB_2';
BEGIN
    -- Start first job (will run for 60 seconds)
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => v_job_name1,
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN execute_pending_actions(...); END;',
        enabled => TRUE
    );

    -- Wait for first job to start
    DBMS_LOCK.SLEEP(2);

    -- Attempt to start second job (should skip)
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => v_job_name2,
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN execute_pending_actions(...); END;',
        enabled => TRUE
    );

    -- Wait for both jobs to complete
    wait_for_jobs_to_complete();

    -- Verify: Only one batch executed
    ASSERT(get_batch_count() = 1, 'Concurrent execution not prevented');

    -- Verify: Second job logged skip reason
    ASSERT(check_log_contains('Batch already running'), 'Skip not logged');
END;
```

**Edge Case 3: Max Duration Enforcement**

```sql
PROCEDURE test_max_duration_enforcement AS
    v_batch_id VARCHAR2(50);
BEGIN
    -- Setup: Schedule with max_duration = 5 minutes
    UPDATE cmr.dwh_ilm_execution_schedules
    SET max_duration_minutes = 5
    WHERE schedule_name = 'TEST_SCHEDULE';

    -- Setup: Queue with 100 operations (takes ~30 minutes normally)
    create_test_queue(operations_count => 100);

    -- Execute batch
    v_batch_id := execute_test_batch();

    -- Verify: Stopped before 5 minutes elapsed
    ASSERT(get_elapsed_minutes(v_batch_id) <= 6, 'Duration not enforced'); -- Allow 1min buffer

    -- Verify: Status = 'INTERRUPTED'
    ASSERT(get_batch_status(v_batch_id) = 'INTERRUPTED');

    -- Verify: Some operations completed (not 0, not 100)
    ASSERT(get_operations_completed(v_batch_id) BETWEEN 1 AND 99);
END;
```

**Edge Case 4: Queue Backlog Alerting**

```sql
PROCEDURE test_queue_backlog_alerts AS
BEGIN
    -- Setup: Add 1500 items to queue
    create_test_queue(operations_count => 1500);

    -- Execute health check
    check_queue_backlog();

    -- Verify: CRITICAL alert sent (backlog > 1000)
    ASSERT(get_last_alert_level() = 'CRITICAL');
    ASSERT(check_alert_contains('ILM queue backlog: 1500'));

    -- Cleanup queue to 600 items
    DELETE FROM cmr.dwh_ilm_evaluation_queue
    WHERE ROWNUM <= 900;

    -- Execute health check again
    check_queue_backlog();

    -- Verify: WARNING alert sent (backlog > 500)
    ASSERT(get_last_alert_level() = 'WARNING');
END;
```

**Edge Case 5: Priority Starvation Prevention**

```sql
PROCEDURE test_priority_starvation AS
BEGIN
    -- Setup: High priority policy with 1000 operations
    create_test_policy(priority => 100, operations => 1000);

    -- Setup: Low priority policy with 10 operations
    create_test_policy(priority => 900, operations => 10);

    -- Execute multiple batches (10 operations per batch)
    FOR i IN 1..120 LOOP  -- 120 batches = 1200 total operations
        execute_pending_actions(p_schedule_name => 'TEST_SCHEDULE');
        DBMS_LOCK.SLEEP(1);
    END LOOP;

    -- Verify: Low priority operations eventually executed
    -- (After age-based priority boost kicks in)
    ASSERT(
        get_executed_count(policy_priority => 900) > 0,
        'Low priority starved - never executed'
    );
END;
```

**Edge Case 6: Timezone/DST Handling**

```sql
PROCEDURE test_dst_transitions AS
    v_schedule SCHEDULE_REC;
BEGIN
    -- Setup: Window 22:00-06:00
    v_schedule.weekday_start_hour := 22;
    v_schedule.weekday_end_hour := 6;

    -- Test: Spring forward (2 AM → 3 AM)
    -- Simulate time = 2:30 AM on DST transition night
    -- Oracle should handle automatically via SYSTIMESTAMP

    -- Test: Fall back (2 AM → 1 AM)
    -- Simulate time = 1:30 AM (second occurrence)

    -- Verify: is_in_execution_window() returns correct result
    -- Verify: No errors or unexpected behavior
END;
```

**Edge Case 7: Long-Running Single Operation**

```sql
PROCEDURE test_long_running_operation AS
    v_batch_id VARCHAR2(50);
    v_start_time TIMESTAMP;
BEGIN
    -- Setup: Operation that takes 90 minutes
    -- Timeout set to 60 minutes
    UPDATE cmr.dwh_ilm_execution_schedules
    SET max_operation_duration_minutes = 60
    WHERE schedule_name = 'TEST_SCHEDULE';

    -- Create queue item that will take 90 minutes
    create_long_running_operation(expected_duration_min => 90);

    v_start_time := SYSTIMESTAMP;
    v_batch_id := execute_test_batch();

    -- Verify: Operation stopped at timeout (60 min + buffer)
    ASSERT(
        EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_time)) <= 65,
        'Operation timeout not enforced'
    );

    -- Verify: Operation logged as timeout/failed
    ASSERT(check_log_contains('Operation timeout after 60 minutes'));
END;
```

**Edge Case 8: Partial Batch Completion Status**

```sql
PROCEDURE test_partial_batch_status AS
    v_batch_id VARCHAR2(50);
BEGIN
    -- Setup: Queue with 20 operations, max_operations = 10
    create_test_queue(operations_count => 20);

    -- Execute first batch
    v_batch_id := execute_test_batch(max_ops => 10);

    -- Verify: Status = 'INTERRUPTED' (not COMPLETED)
    ASSERT(get_batch_status(v_batch_id) = 'INTERRUPTED');
    ASSERT(get_operations_completed(v_batch_id) = 10);

    -- Execute second batch (remaining 10)
    execute_pending_actions(...);

    -- Verify: Status = 'COMPLETED' (no more work)
    ASSERT(get_batch_status(v_batch_id) = 'COMPLETED');
    ASSERT(get_operations_completed(v_batch_id) = 20);
END;
```

---

### Test Category 4: Performance Tests

**Test: Batch Size Impact**

```sql
PROCEDURE test_batch_sizing_performance AS
    v_duration_small NUMBER;
    v_duration_medium NUMBER;
    v_duration_large NUMBER;
BEGIN
    -- Test small batch size (5 operations)
    v_duration_small := execute_and_measure(batch_size => 5);

    -- Test medium batch size (20 operations)
    v_duration_medium := execute_and_measure(batch_size => 20);

    -- Test large batch size (50 operations)
    v_duration_large := execute_and_measure(batch_size => 50);

    -- Analyze results
    DBMS_OUTPUT.PUT_LINE('Small batch (5):   ' || v_duration_small || ' sec');
    DBMS_OUTPUT.PUT_LINE('Medium batch (20): ' || v_duration_medium || ' sec');
    DBMS_OUTPUT.PUT_LINE('Large batch (50):  ' || v_duration_large || ' sec');

    -- Verify: Medium batch is optimal (balance overhead vs resumability)
END;
```

**Test: Checkpoint Frequency Impact**

```sql
PROCEDURE test_checkpoint_frequency_performance AS
BEGIN
    -- Test frequent checkpoints (every 2 operations)
    v_duration_freq := execute_and_measure(checkpoint_freq => 2);

    -- Test medium checkpoints (every 10 operations)
    v_duration_med := execute_and_measure(checkpoint_freq => 10);

    -- Test infrequent checkpoints (every 50 operations)
    v_duration_infreq := execute_and_measure(checkpoint_freq => 50);

    -- Analyze commit overhead vs resumability trade-off
END;
```

---

### Test Category 5: Disaster Recovery Tests

**Test: Database Bounce During Execution**

```sql
PROCEDURE test_db_bounce_recovery AS
BEGIN
    -- Start batch execution
    v_batch_id := start_async_batch(operations => 100);

    -- Wait for 20 operations to complete
    WAIT_FOR_OPERATIONS(20);

    -- Simulate database bounce
    -- (Manual step: Shutdown database, restart)

    -- After database restart:
    -- Execute next scheduled run
    execute_pending_actions(...);

    -- Verify: Batch resumed from checkpoint
    -- Verify: All 100 operations eventually completed
    ASSERT(get_total_executed(v_batch_id) = 100);
END;
```

**Test: Scheduler Job Kill**

```sql
PROCEDURE test_job_kill_recovery AS
BEGIN
    -- Start batch via scheduler job
    v_job_name := 'TEST_ILM_JOB';
    start_scheduler_job(v_job_name);

    -- Wait for job to start executing
    WAIT_FOR_JOB_RUNNING(v_job_name);

    -- Kill job forcefully
    DBMS_SCHEDULER.STOP_JOB(v_job_name, force => TRUE);

    -- Wait for next scheduled run
    DBMS_LOCK.SLEEP(120);  -- 2 minutes
    start_scheduler_job(v_job_name);

    -- Verify: Batch resumed automatically
    -- Verify: No duplicate operations executed
END;
```

---

### Test Category 6: Monitoring & Alerting Tests

**Test: Critical Alert Triggering**

```sql
PROCEDURE test_critical_alerts AS
BEGIN
    -- Test 1: High failure rate alert
    -- Cause 10 consecutive failures
    FOR i IN 1..10 LOOP
        create_failing_operation();
    END LOOP;

    check_critical_alerts();

    ASSERT(get_last_alert_level() = 'CRITICAL');
    ASSERT(check_alert_contains('ILM failure rate'));

    -- Test 2: Queue backlog alert
    create_test_queue(operations_count => 1500);
    check_critical_alerts();

    ASSERT(check_alert_contains('ILM queue backlog: 1500'));

    -- Test 3: Stalled batch alert
    -- Create batch stuck for > 6 hours
    create_stalled_batch(hours_old => 7);
    check_critical_alerts();

    ASSERT(check_alert_contains('ILM batch stalled > 6 hours'));
END;
```

**Test: Performance Trend Monitoring**

```sql
PROCEDURE test_performance_trending AS
BEGIN
    -- Simulate 30 days of execution data
    FOR i IN 1..30 LOOP
        -- Day 1-10: Avg 30 sec per operation
        -- Day 11-20: Avg 35 sec per operation
        -- Day 21-30: Avg 45 sec per operation (15% increase)
        create_historical_data(day => i, avg_duration => ...);
    END LOOP;

    -- Query performance trend view
    FOR rec IN (SELECT * FROM v_dwh_ilm_performance_trend) LOOP
        DBMS_OUTPUT.PUT_LINE(rec.execution_date || ': ' || rec.avg_duration_sec);
    END LOOP;

    -- Verify: Trend detection works
    -- Verify: Alert triggered for 10% increase
END;
```

---

## 📊 Test Execution Summary

| Category | Total Tests | Status | Priority |
|---|---|---|---|
| **Unit Tests** | 8 tests | Pending | High |
| **Integration Tests** | 5 tests | Pending | Critical |
| **Edge Case Tests** | 8 tests | Pending | Critical |
| **Performance Tests** | 4 tests | Pending | Medium |
| **Disaster Recovery Tests** | 3 tests | Pending | High |
| **Monitoring Tests** | 3 tests | Pending | Medium |
| **TOTAL** | **31 tests** | **Pending** | - |

**Test Execution Order**:
1. Unit Tests (fast, foundational)
2. Integration Tests (verify core functionality)
3. Edge Case Tests (production readiness)
4. Performance Tests (tuning)
5. Disaster Recovery Tests (resilience)
6. Monitoring Tests (observability)

**Success Criteria**:
- ✅ All unit tests pass (100%)
- ✅ All integration tests pass (100%)
- ✅ All edge case tests pass (100%)
- ✅ Performance within acceptable ranges (avg < 45 sec/operation)
- ✅ Disaster recovery successful (100% resumption)
- ✅ Monitoring/alerting functional (all alerts trigger correctly)

---

## 🎨 Design Evolution: Before & After Simplification

### ❌ Initial Design (Complex)

**Configuration Table**: 15 fields
```sql
CREATE TABLE dwh_ilm_execution_schedules (
    -- Core fields (3)
    schedule_id, schedule_name, enabled,

    -- Day-of-week (7)
    run_on_monday, run_on_tuesday, ..., run_on_sunday,

    -- Window hours (4)
    weekday_start_hour, weekday_end_hour,
    weekend_start_hour, weekend_end_hour,

    -- ❌ Interval control (2) - REMOVED
    weekday_interval_hours,    -- How often to run on weekdays
    weekend_interval_hours,    -- How often to run on weekends

    -- ❌ Batch limits (3) - REMOVED
    max_operations_per_run,    -- Duplicate of global config
    max_policies_per_run,      -- Not needed
    max_duration_minutes,      -- Window close is natural limit

    -- ❌ Priority filter (1) - REMOVED
    priority_threshold,        -- Not needed (execute by priority)

    -- Checkpointing (2)
    enable_checkpointing, checkpoint_frequency
);
```

**Execution Model**: Periodic (scheduler wakes every N hours, runs one batch, exits)

**Problems**:
1. Redundant configuration (max_operations duplicated)
2. Artificial limits (max_duration when window defines limit)
3. Confusing naming (interval doesn't convey cooldown purpose)
4. Periodic model requires interval calculation
5. Can't run continuously when heavy workload exists

---

### ✅ Final Design (Ultra-Simplified)

**Configuration Table**: 10 core fields (36% reduction from 11 day/hour fields)
```sql
CREATE TABLE dwh_ilm_execution_schedules (
    -- Core fields (3)
    schedule_id, schedule_name, enabled,

    -- ✅ Per-Day Scheduling (7) - ONE column per day with time window
    monday_hours VARCHAR2(11),      -- 'HH24:MI-HH24:MI' or NULL
    tuesday_hours VARCHAR2(11),
    wednesday_hours VARCHAR2(11),
    thursday_hours VARCHAR2(11),
    friday_hours VARCHAR2(11),
    saturday_hours VARCHAR2(11),
    sunday_hours VARCHAR2(11),

    -- ✅ Cooldown control (1)
    batch_cooldown_minutes,  -- 0=continuous, >0=pause N minutes

    -- Checkpointing (2)
    enable_checkpointing, checkpoint_frequency
);
```

**Execution Model**: Continuous (LOOP runs during window until queue empty or window closes)

**Benefits**:
1. ✅ Single source of truth (batch size from global config)
2. ✅ Natural limits (window close time is limit)
3. ✅ Clear naming (cooldown conveys breathing room)
4. ✅ Continuous model with optional pauses
5. ✅ Work-based execution (checks queue, not time)
6. ✅ **Different hours per day** (Monday 22:00-06:00, Friday 20:00-10:00, etc.)
7. ✅ **Simpler schema** (7 columns instead of 11 separate day/hour fields)
8. ✅ **Human-readable format** ('22:00-06:00' vs separate integers)

---

### Comparison Table

| Aspect | Before (Complex) | After (Ultra-Simplified) |
|--------|------------------|--------------------------|
| **Day/Hour Fields** | 11 fields (7 day flags + 4 hour fields) | 7 fields (one per day) |
| **Per-Day Flexibility** | No (weekday vs weekend only) | Yes (each day independent) |
| **Hour Format** | Integer hours (separate start/end) | String 'HH24:MI-HH24:MI' |
| **Config Fields Total** | 15 fields | 10 fields (-33%) |
| **Execution Model** | Periodic (every N hours) | Continuous (LOOP until done) |
| **Batch Size Control** | Per-schedule (redundant) | Global config (centralized) |
| **Duration Limit** | Artificial max_duration | Natural window close |
| **Pacing Control** | Interval hours (confusing) | Cooldown minutes (clear) |
| **Work Detection** | Time-based (run every N hours) | Work-based (check queue first) |
| **Continuous Mode** | Not supported | Native (cooldown=0) |
| **Breathing Room** | Fixed interval only | Flexible cooldown (0-N min) |
| **Priority Execution** | Filter by threshold | Always by priority (natural) |
| **Configuration Complexity** | High (many overlapping fields) | Low (minimal, focused fields) |
| **Readability** | Low (need to cross-reference fields) | High (all info in one column) |

---

### Key Insight: Why This Matters

**User's Original Requirement** (clarified mid-design):
> "What I want to achieve is this: in the weekday_start_hour/weekday_end_hour the ILM should run **continuously** - being limited by number of batch size"

**Initial Design Missed This**: Used periodic model (run every 2 hours) instead of continuous

**Final Design Matches**:
- Continuous LOOP during window
- Optional cooldown for breathing room (not mandatory interval)
- Work-based (skip if queue empty)
- Batch size from global config (centralized control)
- **Per-day scheduling** - each day can have different hours (user's additional request)
