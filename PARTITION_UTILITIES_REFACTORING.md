# Partition Utilities Refactoring Status

## Objective
Redesign partition utility functions to be orchestrated by the ILM execution engine.
All utilities now capture execution details via OUT parameters (`p_sql_executed`, `p_status`, `p_error_message`) so results can be logged by the ILM parent job.

## Architecture Changes

### Before
- Standalone procedures with internal logging
- Called `log_operation_start()` and `log_operation_end()` directly
- Self-contained error handling with direct writes to log tables

### After
- Utilities are pure execution functions with OUT parameters
- ILM execution engine calls utilities and logs results
- Separation of concerns: utilities execute, ILM engine logs
- Consistent error handling and status reporting

## OUT Parameter Contract

All utility procedures now follow this signature pattern:

```sql
PROCEDURE utility_name(
    ... input parameters ...,
    p_sql_executed OUT CLOB,      -- SQL statements executed (for audit trail)
    p_partitions_created OUT NUMBER, -- Or other relevant metric
    p_status OUT VARCHAR2,          -- 'SUCCESS', 'WARNING', 'ERROR', 'SKIPPED'
    p_error_message OUT VARCHAR2    -- Error details if any
);
```

## Completed Procedures (WITH OUT Parameters)

### 1. precreate_hot_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 206-456
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_partitions_created, p_status, p_error_message
- **Changes**:
  - Removed `v_log_id` and all `log_operation_start/end` calls
  - Added CLOB tracking for all executed SQL
  - Returns status: SUCCESS, WARNING, ERROR, SKIPPED
  - Captures partition naming validation results

### 2. precreate_all_hot_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 458-559
- **Status**: COMPLETED
- **OUT Parameters**: p_tables_processed, p_total_partitions_created, p_status, p_error_message
- **Changes**:
  - Batch operation that calls precreate_hot_partitions for each table
  - Aggregates results from child calls
  - Collects all errors into single error message

### 3. create_future_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 692-785
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_partitions_created, p_status, p_error_message
- **Changes**:
  - Generic partition creation (non-ILM)
  - Tracks successful and failed partition creates
  - Returns WARNING if some partitions failed

### 4. split_partition ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 788-847
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_status, p_error_message
- **Changes**:
  - Captures both split DDL and index rebuild DDL
  - Returns comprehensive SQL audit trail

### 5. merge_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 850-906
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_status, p_error_message
- **Changes**:
  - Captures merge DDL and index rebuild DDL
  - Returns comprehensive SQL audit trail

### 6. exchange_partition_load ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 909-999
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_status, p_error_message
- **Changes**:
  - Captures constraint disable/enable DDL
  - Captures exchange partition DDL
  - Logs statistics gathering operation

### 7. parallel_partition_load ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1002-1062
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_rows_loaded, p_status, p_error_message
- **Changes**:
  - Captures row count via SQL%ROWCOUNT
  - Logs parallel DML session setting
  - Includes ROLLBACK in exception handler

### 8. truncate_old_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1065-1136
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_partitions_truncated, p_status, p_error_message
- **Changes**:
  - Loops through old partitions based on retention days
  - Collects errors for individual partition failures
  - Returns WARNING if some partitions failed

### 9. compress_partitions ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1139-1229
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_partitions_compressed, p_status, p_error_message
- **Changes**:
  - Captures both MOVE PARTITION and index rebuild DDL
  - Handles per-partition errors gracefully
  - Critical ILM action for tiering

### 10. move_partitions_to_tablespace ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1232-1334
- **Status**: COMPLETED
- **OUT Parameters**: p_sql_executed, p_partitions_moved, p_status, p_error_message
- **Changes**:
  - Supports optional compression during move
  - Rebuilds all local indexes in target tablespace
  - Critical ILM action for cold storage migration

### 11. gather_partition_statistics ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1337-1406
- **Status**: COMPLETED
- **OUT Parameters**: p_partitions_analyzed, p_status, p_error_message
- **Changes**:
  - Supports incremental statistics mode
  - Counts partitions analyzed
  - Fixed invalid ALTER TABLE syntax (removed broken code)

### 12. gather_stale_partition_stats ✅
- **File**: `pck_dwh_partition_utilities.sql` lines 1409-1464
- **Status**: COMPLETED
- **OUT Parameters**: p_partitions_analyzed, p_status, p_error_message
- **Changes**:
  - Identifies stale partitions automatically
  - Gathers stats only on stale partitions
  - Returns count of partitions analyzed

## Informational Procedures (No OUT Parameters Needed)

These procedures are for reporting/validation only, not execution:

- `preview_hot_partitions` - DBMS_OUTPUT only
- `check_partition_health` - DBMS_OUTPUT only
- `validate_partition_constraints` - DBMS_OUTPUT only
- `partition_size_report` - DBMS_OUTPUT only

## Helper Package Status

### pck_dwh_partition_utils_helper

**Current Functions** (All validation, no logging):
- `validate_partition_naming()` ✅ - Returns compliance status
- `detect_partition_config()` ✅ - Detects interval/tablespace/compression
- `is_framework_partition_name()` ✅ - Boolean check
- `detect_interval_from_name()` ✅ - Parse partition name pattern

**Removed Functions**:
- `log_operation_start()` - ❌ Removed (ILM engine logs)
- `log_operation_end()` - ❌ Removed (ILM engine logs)

## Integration with ILM Execution Engine

The ILM execution engine (`pck_dwh_ilm_execution_engine`) will:

1. Call utility procedure with parameters
2. Receive OUT parameters (status, SQL executed, error message)
3. Log results to `cmr.dwh_ilm_execution_log`:
   - `execution_id`
   - `policy_id`
   - `action_type` (e.g., 'COMPRESS', 'MOVE_TO_TABLESPACE')
   - `partition_owner/table_name/partition_name`
   - `sql_executed` (CLOB - from utility OUT parameter)
   - `status` (from utility OUT parameter)
   - `error_message` (from utility OUT parameter)
   - `execution_start_time` / `execution_end_time`

Example integration:

```sql
PROCEDURE execute_partition_action(
    p_action_type VARCHAR2,
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) AS
    v_sql_executed CLOB;
    v_status VARCHAR2(50);
    v_error_message VARCHAR2(4000);
    v_partitions_processed NUMBER;
BEGIN
    IF p_action_type = 'COMPRESS' THEN
        pck_dwh_partition_utilities.compress_partitions(
            p_table_name => p_table_name,
            p_compression_type => 'QUERY HIGH',
            p_age_days => 90,
            p_sql_executed => v_sql_executed,
            p_partitions_compressed => v_partitions_processed,
            p_status => v_status,
            p_error_message => v_error_message
        );

        -- Log to cmr.dwh_ilm_execution_log
        INSERT INTO cmr.dwh_ilm_execution_log (
            policy_id, action_type, table_owner, table_name,
            sql_executed, status, error_message, ...
        ) VALUES (
            ..., 'COMPRESS', p_table_owner, p_table_name,
            v_sql_executed, v_status, v_error_message, ...
        );
    END IF;
END;
```

## ✅ REFACTORING COMPLETE

All 12 utility procedures have been successfully updated with OUT parameters.

## Next Steps

1. ~~**Complete remaining utility procedures** (6-12 above)~~ ✅ DONE
2. **Test compilation** of both packages
3. **Update ILM execution engine** to call utilities with new signature
4. **Test end-to-end** with sample ILM policies
5. **Update documentation** with new calling patterns

## Summary

**Total Procedures Updated**: 12
- **Partition Pre-creation**: 2 procedures (precreate_hot_partitions, precreate_all_hot_partitions)
- **Partition Management**: 3 procedures (create_future_partitions, split_partition, merge_partitions)
- **Data Loading**: 2 procedures (exchange_partition_load, parallel_partition_load)
- **ILM Actions**: 3 procedures (compress_partitions, move_partitions_to_tablespace, truncate_old_partitions)
- **Statistics**: 2 procedures (gather_partition_statistics, gather_stale_partition_stats)

**Lines of Code**: ~1,464 lines in main package body
**New Architecture**: All procedures return execution details via OUT parameters for ILM engine logging

## Files Modified

- `scripts/pck_dwh_partition_utilities.sql` - Main utilities package (COMPLETED)
- `scripts/pck_dwh_partition_utils_helper.sql` - Helper package (COMPLETED - validation only)
- `scripts/partition_management.sql` - DELETED (converted to package)
- `scripts/partition_precreation_utility.sql` - DELETED (converted to package)

## Bug Fixes

### Issue: Non-existent `status` column in `dwh_ilm_policies`
**Problem**: Code referenced `WHERE status = 'ACTIVE'` but table uses `enabled CHAR(1)` with values 'Y'/'N'

**Fixed in**:
- `pck_dwh_partition_utilities.sql` line 492: Changed `WHERE p.status = 'ACTIVE'` to `WHERE p.enabled = 'Y'`
- `pck_dwh_partition_utils_helper.sql` line 186: Changed `AND status = 'ACTIVE'` to `AND enabled = 'Y'`

## Git Status

```
D  scripts/partition_management.sql
D  scripts/partition_precreation_utility.sql
AM scripts/pck_dwh_partition_utilities.sql
AM scripts/pck_dwh_partition_utils_helper.sql
```

Ready to commit when all procedures are updated.
