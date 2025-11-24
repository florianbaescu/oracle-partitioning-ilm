# ILM Scheduler Enhancement - Implementation Guide

## Overview

This implementation enhances the ILM execution engine with:
- **Per-day scheduling** with custom time windows for each day
- **Continuous execution** during configured windows (LOOP-based, not periodic)
- **Configurable cooldown** between batches (0=continuous, >0=pause N minutes)
- **Work-based execution** (checks queue before running, skips if empty)
- **Checkpointing and resumability** (survive interruptions)
- **Comprehensive monitoring** views

## Installation Order

Run scripts in this order:

```sql
-- 1. Phase 0 & 1: Cleanup and Schema Setup
@scripts/scheduler_enhancement_setup.sql

-- 2. Phase 2: Execution Engine Package
@scripts/scheduler_enhancement_engine.sql

-- 3. Phase 3: Scheduler Program and Job
@scripts/scheduler_enhancement_scheduler.sql

-- 4. Phase 4: Monitoring Views
@scripts/scheduler_enhancement_monitoring.sql

-- 5. Testing (optional)
@scripts/scheduler_enhancement_test.sql
```

## Architecture

### Key Components

1. **`dwh_ilm_execution_schedules`** - Schedule configuration table
   - One row per schedule
   - 7 columns for per-day time windows (e.g., `monday_hours = '22:00-06:00'`)
   - NULL = don't run that day
   - Format: `'HH24:MI-HH24:MI'` with regex validation

2. **`dwh_ilm_execution_state`** - Batch execution state tracking
   - One row per batch execution
   - Tracks progress, checkpoints, status (RUNNING, COMPLETED, INTERRUPTED, FAILED)
   - Enables resumption after interruption

3. **`pck_dwh_ilm_execution_engine`** - Enhanced execution engine
   - Continuous execution LOOP (runs until queue empty or window closes)
   - Helper functions: `get_today_hours()`, `is_in_execution_window()`, `should_execute_now()`
   - Checkpointing support

4. **`DWH_ILM_JOB_EXECUTE`** - DBMS_SCHEDULER job
   - Runs every hour (FREQ=HOURLY; INTERVAL=1)
   - Calls `should_execute_now()` to check if should run
   - If YES: starts continuous execution
   - If NO: exits immediately (waits for next hour)

### Execution Flow

```
Hour 01:00 → Job wakes → should_execute_now() → FALSE (outside window) → Exit
Hour 02:00 → Job wakes → should_execute_now() → FALSE (outside window) → Exit
...
Hour 22:00 → Job wakes → should_execute_now() → TRUE (in window + work exists)
             ↓
          execute_pending_actions() starts
             ↓
          LOOP:
            ├─ Check window still open?
            ├─ Check work exists?
            ├─ Execute batch (up to MAX_CONCURRENT_OPERATIONS items)
            ├─ Checkpoint progress
            ├─ Cooldown (if configured)
            └─ Repeat
             ↓
          EXIT when: queue empty OR window closes
             ↓
          Job ends

Hour 23:00 → Job wakes → should_execute_now() → FALSE (already running) → Exit
...
Hour 06:00 → Previous execution exits (window closed)
Hour 07:00 → Job wakes → should_execute_now() → FALSE (outside window) → Exit
```

## Configuration

### Schedule Configuration

```sql
-- View current configuration
SELECT schedule_name, enabled, monday_hours, tuesday_hours,
       batch_cooldown_minutes, enable_checkpointing
FROM cmr.dwh_ilm_execution_schedules;

-- Update schedule times
UPDATE cmr.dwh_ilm_execution_schedules
SET monday_hours = '22:00-06:00',    -- Mon night to Tue morning
    tuesday_hours = '22:00-06:00',
    wednesday_hours = '22:00-06:00',
    thursday_hours = '22:00-06:00',
    friday_hours = '20:00-10:00',    -- Longer Friday window
    saturday_hours = '18:00-12:00',  -- Extended weekend
    sunday_hours = NULL,             -- No execution Sunday
    batch_cooldown_minutes = 5       -- 5-minute pause between batches
WHERE schedule_name = 'DEFAULT_SCHEDULE';
COMMIT;

-- Continuous execution (no cooldown)
UPDATE cmr.dwh_ilm_execution_schedules
SET batch_cooldown_minutes = 0
WHERE schedule_name = 'DEFAULT_SCHEDULE';
COMMIT;

-- Disable schedule
UPDATE cmr.dwh_ilm_execution_schedules
SET enabled = 'N'
WHERE schedule_name = 'DEFAULT_SCHEDULE';
COMMIT;
```

### Batch Size Configuration

Batch size is controlled globally (not per-schedule):

```sql
-- View current batch size
SELECT config_key, config_value
FROM cmr.dwh_ilm_config
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';

-- Update batch size
UPDATE cmr.dwh_ilm_config
SET config_value = '20'  -- Process up to 20 operations per batch
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';
COMMIT;
```

## Scheduler Control

### Enable/Disable Scheduler

```sql
-- Enable job (starts automatic execution)
EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EXECUTE');

-- Disable job (stops automatic execution)
EXEC DBMS_SCHEDULER.DISABLE('DWH_ILM_JOB_EXECUTE');

-- Check job status
SELECT job_name, enabled, state, repeat_interval,
       last_start_date, next_run_date
FROM all_scheduler_jobs
WHERE job_name = 'DWH_ILM_JOB_EXECUTE';
```

### Manual Execution

```sql
-- Run immediately (respects schedule)
EXEC DBMS_SCHEDULER.RUN_JOB('DWH_ILM_JOB_EXECUTE', use_current_session => TRUE);

-- Force run (bypass day/time checks)
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(p_force_run => TRUE);

-- Run specific schedule
EXEC pck_dwh_ilm_execution_engine.execute_pending_actions(p_schedule_name => 'DEFAULT_SCHEDULE');
```

## Monitoring

### Real-Time Monitoring

```sql
-- Check if anything is running right now
SELECT * FROM cmr.v_dwh_ilm_active_batches;

-- See current window status
SELECT * FROM cmr.v_dwh_ilm_current_window;

-- Check queue backlog
SELECT * FROM cmr.v_dwh_ilm_queue_summary;
```

### Historical Monitoring

```sql
-- Recent batch history (last 7 days)
SELECT * FROM cmr.v_dwh_ilm_recent_batches;

-- Schedule effectiveness
SELECT * FROM cmr.v_dwh_ilm_schedule_stats;

-- Detailed batch progress
SELECT * FROM cmr.v_dwh_ilm_batch_progress;
```

### Key Metrics

```sql
-- Batch completion rate
SELECT
    schedule_name,
    total_batches,
    completed_batches,
    ROUND(completed_batches * 100.0 / NULLIF(total_batches, 0), 1) AS completion_rate_pct,
    avg_ops_per_batch,
    avg_duration_minutes
FROM cmr.v_dwh_ilm_schedule_stats;

-- Operations throughput
SELECT
    execution_batch_id,
    status,
    duration_minutes,
    operations_completed,
    ops_per_minute
FROM cmr.v_dwh_ilm_recent_batches
ORDER BY start_time DESC;
```

## Troubleshooting

### Job Not Running

```sql
-- Check schedule enabled
SELECT schedule_name, enabled FROM cmr.dwh_ilm_execution_schedules;

-- Check job enabled
SELECT job_name, enabled, state FROM all_scheduler_jobs WHERE job_name = 'DWH_ILM_JOB_EXECUTE';

-- Check if in execution window
SELECT * FROM cmr.v_dwh_ilm_current_window;

-- Check if work exists
SELECT * FROM cmr.v_dwh_ilm_queue_summary;
```

### Batch Stuck

```sql
-- Check active batches
SELECT * FROM cmr.v_dwh_ilm_active_batches;

-- Check for long-running batches
SELECT execution_batch_id, elapsed_minutes, status
FROM cmr.v_dwh_ilm_active_batches
WHERE elapsed_minutes > 60;  -- Over 1 hour

-- Interrupt stuck batch (mark as INTERRUPTED so it can be resumed)
UPDATE cmr.dwh_ilm_execution_state
SET status = 'INTERRUPTED'
WHERE execution_batch_id = 'BATCH_20250124_220000_001'
AND status = 'RUNNING';
COMMIT;
```

### Check Execution Logs

```sql
-- Check scheduler job log
SELECT log_date, status, additional_info
FROM all_scheduler_job_run_details
WHERE job_name = 'DWH_ILM_JOB_EXECUTE'
ORDER BY log_date DESC;

-- Check ILM execution log
SELECT execution_start_time, policy_name, action_type,
       table_name, partition_name, status, error_message
FROM cmr.dwh_ilm_execution_log
ORDER BY execution_start_time DESC;
```

## Performance Tuning

### Adjust Batch Size

- **Too small** (< 10): More overhead, slower overall
- **Too large** (> 50): Long-running batches, harder to interrupt
- **Recommended**: 10-20 operations per batch

```sql
UPDATE cmr.dwh_ilm_config
SET config_value = '15'
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';
```

### Adjust Cooldown

- **0 minutes**: Continuous (no pause) - use when heavy backlog
- **5-10 minutes**: Moderate breathing room - default recommendation
- **15-30 minutes**: Light load, spread work over window

```sql
UPDATE cmr.dwh_ilm_execution_schedules
SET batch_cooldown_minutes = 10
WHERE schedule_name = 'DEFAULT_SCHEDULE';
```

### Checkpoint Frequency

- **Too frequent** (< 3): More commits, slower
- **Too infrequent** (> 10): More rework on interruption
- **Recommended**: 5 operations

```sql
UPDATE cmr.dwh_ilm_execution_schedules
SET checkpoint_frequency = 5
WHERE schedule_name = 'DEFAULT_SCHEDULE';
```

## Migration from Old System

If upgrading from previous ILM implementation:

1. **Backup**: Export existing `dwh_ilm_config` settings
2. **Disable old job**: Stop any existing ILM scheduler jobs
3. **Install**: Run all enhancement scripts in order
4. **Configure**: Set execution windows in `dwh_ilm_execution_schedules`
5. **Test**: Run `scheduler_enhancement_test.sql`
6. **Enable**: Start new scheduler job

Old config mappings:
- `EXECUTION_WINDOW_START` → `monday_hours`, `tuesday_hours`, etc. (start time)
- `EXECUTION_WINDOW_END` → `monday_hours`, `tuesday_hours`, etc. (end time)
- `MAX_CONCURRENT_OPERATIONS` → Kept (global config)

## Benefits Summary

### Before (Old System)
- ❌ Fixed 2-hour interval (runs every 2 hours, always)
- ❌ Processes only 10 operations per run, then exits
- ❌ No day-of-week scheduling
- ❌ No resumability (interruption = start over)
- ❌ 1000 operations = 200 hours (8+ days) to process

### After (New System)
- ✅ Continuous execution during window (runs until done)
- ✅ Per-day scheduling with custom hours each day
- ✅ Work-based (skips if queue empty - saves resources)
- ✅ Configurable cooldown (0-N minutes breathing room)
- ✅ Checkpointing and resumability
- ✅ 1000 operations = processes in one continuous session

## Support

For issues or questions:
1. Check monitoring views first
2. Review execution logs
3. Verify schedule configuration
4. Test with force run to bypass schedule checks
5. Check design document: `SCHEDULER_ENHANCEMENT_DESIGN.md`
