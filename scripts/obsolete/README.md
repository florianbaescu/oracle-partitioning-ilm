# Obsolete Scripts

This folder contains scripts that have been replaced by newer, enhanced implementations.

**These files are kept for reference only and should NOT be used in new installations.**

---

## Files in This Folder

**Total**: 5 obsolete files (replaced by enhanced implementations or consolidated)

### 1. `custom_ilm_execution_engine.sql` ❌ OBSOLETE

**Replaced By**: `../scheduler_enhancement_engine.sql`

**Reason for Obsolescence**:
- No OUT parameters (no SQL/status/error capture)
- No integration with partition utilities package
- Direct DDL execution (code duplication)
- Missing features that are now in scheduler_enhancement_engine.sql

**What Was Migrated**:
- ✅ `get_partition_size()` function → Added to scheduler_enhancement_engine.sql
- ✅ `rebuild_local_indexes()` procedure → Added to scheduler_enhancement_engine.sql
- ✅ `gather_partition_stats()` procedure → Added to scheduler_enhancement_engine.sql
- ✅ Size tracking (before/after capture) → Added to scheduler_enhancement_engine.sql
- ✅ Auto-merge after MOVE → Added to scheduler_enhancement_engine.sql
- ✅ CUSTOM action handler → Added to scheduler_enhancement_engine.sql
- ✅ All partition operations → Delegated to utilities with OUT parameters

**Date Obsoleted**: 2025-11-24

---

### 2. `scheduler_enhancement_monitoring.sql` ❌ OBSOLETE

**Replaced By**: `../schedule_conditions_monitoring.sql`

**Reason for Obsolescence**:
- References old table names (dwh_ilm_execution_schedules, dwh_ilm_execution_state)
- Missing schedule_type column in views
- No condition-specific views
- Incompatible with schedule conditions framework

**What Was Migrated**:
- ✅ `v_dwh_ilm_active_batches` → Renamed to `v_dwh_active_batches`, updated table references
- ✅ `v_dwh_ilm_schedule_stats` → Renamed to `v_dwh_schedule_stats`, added schedule_type column
- ✅ `v_dwh_ilm_batch_progress` → Renamed to `v_dwh_batch_progress`, updated table references
- ✅ `v_dwh_ilm_current_window` → Renamed to `v_dwh_current_window`, updated table references
- ✅ `v_dwh_ilm_recent_batches` → Not migrated (can be recreated if needed)
- ✅ `v_dwh_ilm_queue_summary` → Not migrated (queue-specific, still works)
- ✅ NEW: `v_dwh_schedule_conditions` → Condition monitoring
- ✅ NEW: `v_dwh_condition_failures` → Failing conditions
- ✅ NEW: `v_dwh_schedule_readiness` → Overall readiness check

**Date Obsoleted**: 2025-11-24 (schedule conditions migration)

---

### 3. `custom_ilm_scheduler.sql` ❌ OBSOLETE

**Replaced By**:
- `../scheduler_enhancement_scheduler.sql` (main execution job)
- `../scheduler_enhancement_utilities.sql` (additional jobs and helpers)

**Reason for Obsolescence**:
- Fixed 2-hour interval (inefficient)
- Processes only 10 operations per run
- No day-of-week scheduling
- No continuous execution
- No resumability/checkpointing
- 1000 operations = 8+ days to process

**What Was Migrated**:
- ✅ DWH_ILM_JOB_REFRESH_ACCESS → scheduler_enhancement_utilities.sql
- ✅ DWH_ILM_JOB_EVALUATE → scheduler_enhancement_utilities.sql
- ✅ DWH_ILM_JOB_CLEANUP → scheduler_enhancement_utilities.sql
- ✅ DWH_ILM_JOB_MONITOR_FAILURES → scheduler_enhancement_utilities.sql
- ✅ Helper procedures → scheduler_enhancement_utilities.sql
- ✅ DWH_ILM_JOB_EXECUTE → Replaced with enhanced continuous execution version

**Alert/Notification System**: Not yet migrated (copy from this file if needed)

**Date Obsoleted**: 2025-11-24 (marked in file header)

---

## New System Architecture

For new installations or migrations, use the **scheduler_enhancement_*** system:

```sql
-- Installation order:
@scripts/custom_ilm_setup.sql                    -- Core schema (still needed)
@scripts/custom_ilm_policy_engine.sql            -- Policy evaluation (still needed)
@scripts/custom_ilm_validation.sql               -- Validation (still needed)

@scripts/scheduler_enhancement_setup.sql         -- Enhanced scheduler schema
@scripts/scheduler_enhancement_engine.sql        -- Enhanced execution engine (REPLACES custom_ilm_execution_engine.sql)
@scripts/scheduler_enhancement_scheduler.sql     -- Main job (REPLACES custom_ilm_scheduler.sql)
@scripts/scheduler_enhancement_utilities.sql     -- Additional jobs
@scripts/scheduler_enhancement_monitoring.sql    -- Monitoring views

@scripts/scheduler_enhancement_test.sql          -- Testing (optional)
```

---

## Key Improvements in New System

| Feature | Old System | New System |
|---------|-----------|------------|
| **Execution** | Fixed 2-hour interval | Continuous during window |
| **Scheduling** | Single time window | Per-day scheduling (Mon-Sun) |
| **Work Detection** | Always runs | Checks queue, skips if empty |
| **Resumability** | None | Checkpointing and resume |
| **Throughput** | 10 ops every 2 hours | Continuous until done |
| **Size Tracking** | ✅ Yes | ✅ Yes (migrated) |
| **Auto-Merge** | ✅ Yes | ✅ Yes (migrated) |
| **OUT Parameters** | ❌ No | ✅ Yes |
| **SQL Audit Trail** | ❌ No | ✅ Yes (CLOB) |
| **Utilities Integration** | ❌ No | ✅ Yes |
| **Custom Actions** | ✅ Yes | ✅ Yes (migrated) |

---

## Migration Guide

If upgrading from old system:

1. **Backup** existing configuration:
   ```sql
   CREATE TABLE dwh_ilm_config_backup AS SELECT * FROM cmr.dwh_ilm_config;
   ```

2. **Stop old jobs**:
   ```sql
   EXEC DBMS_SCHEDULER.DISABLE('DWH_ILM_JOB_EXECUTE');
   ```

3. **Install new system** (see installation order above)

4. **Configure execution windows**:
   ```sql
   UPDATE cmr.dwh_ilm_execution_schedules
   SET monday_hours = '22:00-06:00',
       tuesday_hours = '22:00-06:00',
       -- ... configure all days
   WHERE schedule_name = 'DEFAULT_SCHEDULE';
   ```

5. **Enable new job**:
   ```sql
   EXEC DBMS_SCHEDULER.ENABLE('DWH_ILM_JOB_EXECUTE');
   ```

6. **Verify** with monitoring views:
   ```sql
   SELECT * FROM cmr.v_dwh_ilm_current_window;
   SELECT * FROM cmr.v_dwh_ilm_queue_summary;
   ```

---

## Why Keep These Files?

These obsolete files are retained for:
- **Reference**: Compare old vs new implementations
- **Recovery**: Rollback if issues discovered (not recommended)
- **Documentation**: Understanding the evolution of the system
- **Alert Logic**: Alert/notification procedures not yet migrated from custom_ilm_scheduler.sql

---

## Support

For questions or issues with the new system:
1. Check documentation: `SCHEDULER_IMPLEMENTATION_README.md`
2. Check design document: `SCHEDULER_ENHANCEMENT_DESIGN.md`
3. Review monitoring views: `scheduler_enhancement_monitoring.sql`
4. Test with: `scheduler_enhancement_test.sql`

---

### 4. `schedule_conditions_migration.sql` ❌ OBSOLETE

**Replaced By**: `../scheduler_enhancement_setup.sql` (consolidated)

**Reason for Obsolescence**:
- Functionality consolidated into main setup script
- Duplicated schema migration logic
- Separate file no longer needed

**What Was Migrated**:
- ✅ All schema migration logic → Moved to scheduler_enhancement_setup.sql Phase 1-3
- ✅ Table creation (dwh_execution_schedules, dwh_schedule_conditions, dwh_execution_state)
- ✅ Data migration from old tables
- ✅ All functionality preserved

**Date Obsoleted**: 2025-11-24 (consolidated into single setup file)

---

### 5. `schedule_conditions_monitoring.sql` ❌ OBSOLETE

**Replaced By**: `../scheduler_enhancement_setup.sql` (consolidated)

**Reason for Obsolescence**:
- Functionality consolidated into main setup script
- Duplicated view creation logic
- Separate file no longer needed

**What Was Migrated**:
- ✅ All monitoring views → Moved to scheduler_enhancement_setup.sql Phase 4
- ✅ v_dwh_active_batches, v_dwh_schedule_stats, v_dwh_batch_progress
- ✅ v_dwh_current_window, v_dwh_queue_summary
- ✅ v_dwh_schedule_conditions, v_dwh_condition_failures, v_dwh_schedule_readiness
- ✅ All functionality preserved

**Date Obsoleted**: 2025-11-24 (consolidated into single setup file)

---

**Last Updated**: 2025-11-24
**Obsoleted By**: Enhanced scheduler system with complete feature parity and consolidated setup
