# Custom ILM Framework - Installation Guide

> **⚠️ DEPRECATION NOTICE**
> This guide has been superseded by [INSTALL_GUIDE_COMPLETE.md](INSTALL_GUIDE_COMPLETE.md) which includes:
> - Enhanced prerequisites checklist with verification scripts
> - Framework independence support (install ILM or Migration in any order)
> - Comprehensive verification procedures
> - Functional testing and troubleshooting
> - All procedure names corrected with `dwh_` prefix
>
> **This document is kept for reference only.** Please use INSTALL_GUIDE_COMPLETE.md for new installations.
>
> **Last Updated:** 2025-10-22

---

Quick installation guide for the custom PL/SQL-based ILM framework.

## Prerequisites

- Oracle Database 12c or higher (tested on 19c+)
- Standard Edition or Enterprise Edition (no ADO license required)
- DBA or schema owner privileges
- Tablespaces created for different storage tiers (optional but recommended)

## Installation Steps

### Step 1: Create Tablespaces (Optional)

If you plan to use multi-tier storage, create tablespaces first:

```sql
-- Hot tier (SSD/Flash)
CREATE TABLESPACE tbs_hot
    DATAFILE '/u01/oradata/hot/hot01.dbf' SIZE 10G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE;

-- Warm tier (Standard SAS)
CREATE TABLESPACE tbs_warm
    DATAFILE '/u02/oradata/warm/warm01.dbf' SIZE 50G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE;

-- Cold tier (SATA/NAS)
CREATE TABLESPACE tbs_cold
    DATAFILE '/u03/oradata/cold/cold01.dbf' SIZE 100G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE;

-- Archive tier (Low-cost storage)
CREATE TABLESPACE tbs_archive
    DATAFILE '/u04/oradata/archive/archive01.dbf' SIZE 500G AUTOEXTEND ON
    EXTENT MANAGEMENT LOCAL AUTOALLOCATE;
```

### Step 2: Install Framework Components

Run the following scripts in order:

```sql
-- 1. Create metadata tables and basic procedures
@scripts/custom_ilm_setup.sql

-- 2. Install policy evaluation engine
@scripts/custom_ilm_policy_engine.sql

-- 3. Install execution engine
@scripts/custom_ilm_execution_engine.sql

-- 4. Install scheduler jobs
@scripts/custom_ilm_scheduler.sql
```

### Step 3: Verify Installation

```sql
-- Check metadata tables
SELECT table_name FROM user_tables WHERE table_name LIKE 'ILM_%';
-- Expected: ILM_POLICIES, ILM_EXECUTION_LOG, ILM_PARTITION_ACCESS,
--           ILM_EVALUATION_QUEUE, ILM_CONFIG

-- Check packages
SELECT object_name, status FROM user_objects
WHERE object_name IN ('ILM_POLICY_ENGINE', 'ILM_EXECUTION_ENGINE')
AND object_type = 'PACKAGE';
-- Expected: Both should show VALID

-- Check scheduler jobs
SELECT job_name, enabled, state FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB_%';
-- Expected: 4 jobs (REFRESH_ACCESS, EVALUATE, EXECUTE, CLEANUP)
```

### Step 4: Initial Configuration

```sql
-- Set execution window (default is 22:00 to 06:00)
UPDATE cmr.dwh_ilm_config SET config_value = '23:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config SET config_value = '05:00'
WHERE config_key = 'EXECUTION_WINDOW_END';

COMMIT;

-- Review all configuration
SELECT * FROM cmr.dwh_ilm_config ORDER BY config_key;
```

### Step 5: Test Installation

```sql
-- Create a test table (if not already exists)
CREATE TABLE test_fact (
    id NUMBER,
    event_date DATE,
    data VARCHAR2(100)
)
PARTITION BY RANGE (event_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2023_01 VALUES LESS THAN (DATE '2023-02-01')
);

-- Insert some test data
INSERT INTO test_fact VALUES (1, DATE '2023-01-15', 'Test data');
COMMIT;

-- Create a test policy
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'TEST_COMPRESS', USER, 'TEST_FACT',
    'COMPRESSION', 'COMPRESS', 1,  -- 1 day for testing
    'QUERY HIGH', 100, 'Y'
);
COMMIT;

-- Run test cycle
EXEC dwh_run_ilm_cycle();

-- Check results
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE table_name = 'TEST_FACT'
ORDER BY execution_start DESC;

-- Clean up test
DELETE FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_COMPRESS';
DROP TABLE test_fact PURGE;
COMMIT;
```

## Post-Installation

### Enable Automatic Execution

```sql
-- Enable auto-execution
UPDATE cmr.dwh_ilm_config SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Ensure jobs are started
EXEC dwh_start_ilm_jobs();

-- Verify jobs are running
SELECT * FROM v_ilm_scheduler_status;
```

### Define Your First Policy

```sql
-- Example: Compress SALES_FACT partitions older than 90 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_SALES_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90,
    'QUERY HIGH', 100, 'Y'
);
COMMIT;

-- Evaluate policy (without executing)
EXEC ilm_policy_engine.evaluate_policy(
    (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name = 'COMPRESS_SALES_90D')
);

-- Check what would be executed
SELECT * FROM ilm_evaluation_queue WHERE eligible = 'Y';
```

## Monitoring

### Dashboard Queries

```sql
-- Active policies
SELECT * FROM v_ilm_active_policies;

-- Recent executions
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 7
ORDER BY execution_start DESC;

-- Space savings
SELECT * FROM v_ilm_execution_stats;

-- Scheduler status
SELECT * FROM v_ilm_scheduler_status;
```

## Troubleshooting

### Issue: Packages show INVALID status

```sql
-- Recompile packages
ALTER PACKAGE ilm_policy_engine COMPILE;
ALTER PACKAGE ilm_execution_engine COMPILE BODY;

-- Check for errors
SELECT * FROM user_errors
WHERE name IN ('ILM_POLICY_ENGINE', 'ILM_EXECUTION_ENGINE')
ORDER BY sequence;
```

### Issue: Jobs not running

```sql
-- Check job status
SELECT job_name, enabled, state, failure_count
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB_%';

-- Re-enable jobs
EXEC dwh_start_ilm_jobs();

-- Run job manually for testing
EXEC dwh_run_ilm_job_now('ILM_JOB_EVALUATE');

-- Check job log
SELECT * FROM v_ilm_job_history ORDER BY log_date DESC;
```

### Issue: No partitions being evaluated

```sql
-- Refresh access tracking manually
EXEC dwh_refresh_partition_access_tracking();

-- Check partition access data
SELECT * FROM ilm_partition_access;

-- If empty, check if partitioned tables exist
SELECT table_owner, table_name, partition_name
FROM all_tab_partitions
WHERE table_owner = USER
ORDER BY table_name, partition_position;
```

## Uninstallation

To completely remove the framework:

```sql
-- Stop all jobs
EXEC dwh_stop_ilm_jobs();

-- Drop scheduler components
BEGIN
    FOR job IN (SELECT job_name FROM user_scheduler_jobs WHERE job_name LIKE 'ILM_JOB_%') LOOP
        DBMS_SCHEDULER.DROP_JOB(job.job_name, TRUE);
    END LOOP;
    FOR prog IN (SELECT program_name FROM user_scheduler_programs WHERE program_name LIKE 'ILM_%') LOOP
        DBMS_SCHEDULER.DROP_PROGRAM(prog.program_name, TRUE);
    END LOOP;
END;
/

-- Drop packages
DROP PACKAGE ilm_execution_engine;
DROP PACKAGE ilm_policy_engine;

-- Drop procedures/functions
DROP PROCEDURE dwh_refresh_partition_access_tracking;
DROP PROCEDURE refresh_partition_access_tracking;
DROP PROCEDURE cleanup_execution_logs;
DROP PROCEDURE dwh_stop_ilm_jobs;
DROP PROCEDURE dwh_start_ilm_jobs;
DROP PROCEDURE dwh_run_ilm_job_now;
DROP PROCEDURE dwh_run_ilm_cycle;
DROP FUNCTION is_execution_window_open;
DROP FUNCTION get_dwh_ilm_config;

-- Drop views
DROP VIEW v_ilm_active_policies;
DROP VIEW v_ilm_execution_stats;
DROP VIEW v_ilm_partition_temperature;
DROP VIEW v_ilm_scheduler_status;
DROP VIEW v_ilm_job_history;

-- Drop tables (WARNING: Data loss!)
DROP TABLE ilm_evaluation_queue PURGE;
DROP TABLE cmr.dwh_ilm_execution_log PURGE;
DROP TABLE ilm_partition_access PURGE;
DROP TABLE cmr.dwh_ilm_policies PURGE;
DROP TABLE cmr.dwh_ilm_config PURGE;
```

## Next Steps

1. Read the [Custom ILM Guide](docs/custom_ilm_guide.md) for detailed usage
2. Review [examples/custom_ilm_examples.sql](examples/custom_ilm_examples.sql) for policy examples
3. Define policies for your tables
4. Monitor execution and adjust as needed

## Support

For issues or questions:
- Check the [Custom ILM Guide](docs/custom_ilm_guide.md) troubleshooting section
- Review [Quick Reference](docs/quick_reference.md) for common commands
- Check execution logs: `SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = 'FAILED'`

## License

Free to use and modify for your organization.
