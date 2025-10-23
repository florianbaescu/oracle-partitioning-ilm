# Oracle Custom ILM Framework - Operations Runbook

**Version:** 2.0
**Date:** 2025-10-22
**Target Audience:** DBAs, Operations Staff, Support Teams

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Weekly Operations](#weekly-operations)
3. [Monthly Operations](#monthly-operations)
4. [Quarterly Operations](#quarterly-operations)
5. [Emergency Procedures](#emergency-procedures)
6. [Monitoring Queries](#monitoring-queries)
7. [Troubleshooting Guide](#troubleshooting-guide)
8. [Performance Tuning](#performance-tuning)
9. [Alerting Thresholds](#alerting-thresholds)
10. [Email Notifications Setup](#email-notifications-setup)
11. [Best Practices](#best-practices)
12. [Contact Escalation](#contact-escalation)

---

## Daily Operations

### Morning Health Check (10 minutes)

Run these checks every morning to ensure ILM framework is operating correctly.

#### 1. Check Scheduler Job Status

```sql
SELECT job_name, last_start_date, next_run_date,
       failure_count, state, enabled
FROM v_ilm_scheduler_status
ORDER BY job_name;
```

**Expected:**
- All jobs: `enabled = TRUE`
- All jobs: `state = 'SCHEDULED'`
- All jobs: `failure_count = 0`

**Alert if:**
- Any job: `state = 'BROKEN'`
- Any job: `failure_count > 0`
- Any job: `enabled = FALSE` (unless intentionally disabled)

**Resolution:**
```sql
-- If job broken, restart it
EXEC dwh_stop_ilm_jobs();
EXEC dwh_start_ilm_jobs();
```

---

#### 2. Check Recent Execution Failures

```sql
SELECT execution_id, policy_name, table_name, partition_name,
       TO_CHAR(execution_start, 'YYYY-MM-DD HH24:MI') AS exec_time,
       status, error_message
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 1
ORDER BY execution_start DESC;
```

**Expected:** 0 failures in last 24 hours

**Alert if:** Any failures present

**Investigation:** See [Troubleshooting: Execution Failures](#execution-failures)

---

#### 3. Check Space Savings

```sql
SELECT table_name,
       COUNT(*) AS actions_last_24h,
       ROUND(SUM(space_saved_mb), 2) AS space_saved_mb,
       ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > SYSDATE - 1
GROUP BY table_name
ORDER BY space_saved_mb DESC;
```

**Report:** Daily space savings summary

**Action:** Forward report to capacity planning team

---

#### 4. Verify Policy Validation is Active

```sql
-- Check validation trigger is enabled
SELECT trigger_name, status, trigger_type
FROM user_triggers
WHERE trigger_name = 'TRG_VALIDATE_DWH_ILM_POLICY';
```

**Expected:** `STATUS = 'ENABLED'`

**Alert if:** `STATUS = 'DISABLED'`

**Resolution:**
```sql
ALTER TRIGGER trg_validate_dwh_ilm_policy ENABLE;
```

---

#### 5. Check Partition Access Tracking Freshness

```sql
SELECT MAX(last_updated) AS latest_tracking_update,
       ROUND((SYSDATE - MAX(last_updated)) * 24, 1) AS hours_since_update,
       COUNT(DISTINCT table_name) AS tracked_tables
FROM cmr.dwh_ilm_partition_access;
```

**Expected:** `hours_since_update < 24`

**Alert if:** `hours_since_update > 48`

**Resolution:**
```sql
EXEC dwh_refresh_partition_access_tracking();
```

---

### Investigation: Job Failures

**If jobs are failing, follow this procedure:**

#### Step 1: Check Job History

```sql
SELECT job_name, log_date, status, error#,
       SUBSTR(additional_info, 1, 200) AS error_info
FROM v_ilm_job_history
WHERE status != 'SUCCEEDED'
AND log_date > SYSDATE - 7
ORDER BY log_date DESC;
```

#### Step 2: Check Execution Window

```sql
SELECT
    CASE
        WHEN TO_CHAR(SYSDATE, 'HH24:MI') BETWEEN
            (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START') AND
            (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END')
        THEN 'OPEN'
        ELSE 'CLOSED'
    END AS window_status,
    (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START') AS window_start,
    (SELECT config_value FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END') AS window_end,
    TO_CHAR(SYSDATE, 'HH24:MI') AS current_time
FROM DUAL;
```

**Note:** Jobs may not execute if outside execution window.

#### Step 3: Check Policy Configuration

```sql
-- Are policies enabled?
SELECT COUNT(*) AS enabled_policies,
       SUM(CASE WHEN enabled = 'N' THEN 1 ELSE 0 END) AS disabled_policies
FROM cmr.dwh_ilm_policies;

-- Show enabled policies
SELECT policy_name, table_name, policy_type, action_type, enabled, priority
FROM cmr.dwh_ilm_policies
WHERE enabled = 'Y'
ORDER BY priority;
```

#### Step 4: Test Manual Execution

```sql
SET SERVEROUTPUT ON
-- Try running manually
EXEC dwh_run_ilm_cycle();

-- Check for errors
SELECT execution_id, policy_name, partition_name, status, error_message
FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 1/24  -- Last hour
ORDER BY execution_start DESC;
```

---

### Recovery: Restart Failed Job

```sql
-- Option 1: Re-run specific job
EXEC dwh_run_ilm_job_now('ILM_JOB_EXECUTE');

-- Option 2: Restart all jobs
EXEC dwh_stop_ilm_jobs();
EXEC dwh_start_ilm_jobs();

-- Option 3: Run manual cycle (bypass scheduler)
EXEC dwh_run_ilm_cycle();
```

---

## Weekly Operations

### Weekly Review (30 minutes)

Perform these checks every Monday morning.

#### 1. Compression Effectiveness Report

```sql
SELECT policy_name, table_name,
       COUNT(*) AS total_compressions,
       ROUND(AVG(compression_ratio), 2) AS avg_ratio,
       ROUND(SUM(space_saved_mb)/1024, 2) AS total_saved_gb,
       ROUND(AVG(duration_seconds/60), 2) AS avg_duration_minutes
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > SYSDATE - 7
GROUP BY policy_name, table_name
ORDER BY total_saved_gb DESC;
```

**Action Items:**
- Policies with `avg_ratio < 2.0`: Review compression type
- Policies with `avg_duration_minutes > 30`: Review parallel_degree
- Tables with low space savings: Consider policy adjustment

---

#### 2. Policy Execution Distribution

```sql
SELECT policy_name, table_name,
       COUNT(*) AS executions,
       ROUND(AVG(duration_seconds), 1) AS avg_duration_sec,
       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
       SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
       ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_rate_pct
FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 7
GROUP BY policy_name, table_name
HAVING COUNT(*) > 0
ORDER BY failures DESC, executions DESC;
```

**Action Items:**
- Policies with `success_rate_pct < 95%`: Investigate failures
- Policies with 0 executions: May be candidates for removal
- Policies with high execution counts: Consider priority adjustment

---

#### 3. Partition Temperature Distribution

```sql
SELECT temperature,
       COUNT(*) AS partition_count,
       ROUND(SUM(size_mb)/1024, 2) AS total_size_gb,
       ROUND(AVG(days_since_write), 0) AS avg_age_days
FROM cmr.dwh_ilm_partition_access
GROUP BY temperature
ORDER BY
    CASE temperature
        WHEN 'HOT' THEN 1
        WHEN 'WARM' THEN 2
        WHEN 'COLD' THEN 3
    END;
```

**Expected Distribution (varies by workload):**
- HOT: 10-20% of partitions
- WARM: 30-40% of partitions
- COLD: 40-60% of partitions

**Action Items:**
- If > 70% HOT: Review HOT_THRESHOLD_DAYS (may be too high)
- If > 70% COLD: Review access patterns, may need Heat Map integration
- Unbalanced distribution: Adjust temperature thresholds

**Adjust Thresholds:**
```sql
-- Example: Tighten HOT threshold from 90 to 60 days
UPDATE cmr.dwh_ilm_config
SET config_value = '60'
WHERE config_key = 'HOT_THRESHOLD_DAYS';
COMMIT;

-- Refresh tracking to apply new thresholds
EXEC dwh_refresh_partition_access_tracking();
```

---

#### 4. Policy Validation Errors (New in v2.0)

```sql
-- Check for recent policy validation failures
SELECT TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS error_time,
       ora_err_mesg$ AS error_message
FROM dba_errors
WHERE name = 'TRG_VALIDATE_DWH_ILM_POLICY'
AND timestamp > SYSDATE - 7
ORDER BY timestamp DESC;
```

**Expected:** No validation errors

**Action:** If validation errors found, review policies being inserted/updated

---

#### 5. ILM Template Application Review (New in v2.0)

```sql
-- Check tables migrated with ILM templates in last week
SELECT t.task_name, t.source_table, t.ilm_policy_template,
       t.status, t.execution_end,
       COUNT(p.policy_id) AS policies_created
FROM cmr.dwh_migration_tasks t
LEFT JOIN cmr.dwh_ilm_policies p
    ON p.table_owner = t.source_owner
    AND p.table_name = t.source_table
WHERE t.apply_ilm_policies = 'Y'
AND t.execution_end > SYSDATE - 7
GROUP BY t.task_name, t.source_table, t.ilm_policy_template, t.status, t.execution_end
ORDER BY t.execution_end DESC;
```

**Action:** Verify auto-detected templates were appropriate

---

## Monthly Operations

### Monthly Maintenance (1 hour)

Perform these tasks on the first Monday of each month.

#### 1. Policy Audit

Review all active policies for relevance:

```sql
SELECT policy_id, policy_name, table_name, policy_type,
       action_type, age_days, age_months, priority, enabled,
       TO_CHAR(created_date, 'YYYY-MM-DD') AS created,
       TO_CHAR(modified_date, 'YYYY-MM-DD') AS modified
FROM cmr.dwh_ilm_policies
ORDER BY priority, table_name;
```

**Review Questions:**
- Are age thresholds still appropriate?
- Are priorities correctly ordered?
- Any policies never executed? (candidates for deletion)
- Any deprecated tables still have policies?

**Check Policy Usage:**
```sql
-- Policies that haven't executed in 90 days
SELECT p.policy_name, p.table_name, p.enabled,
       MAX(e.execution_end) AS last_execution
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_execution_log e ON e.policy_id = p.policy_id
GROUP BY p.policy_name, p.table_name, p.enabled
HAVING MAX(e.execution_end) < SYSDATE - 90 OR MAX(e.execution_end) IS NULL
ORDER BY last_execution NULLS FIRST;
```

**Action:** Consider disabling or removing unused policies

---

#### 2. Capacity Planning Report

Calculate growth trends and space savings:

```sql
-- Monthly space savings trend
SELECT TO_CHAR(execution_end, 'YYYY-MM') AS month,
       COUNT(DISTINCT table_name) AS tables_processed,
       COUNT(*) AS total_actions,
       ROUND(SUM(space_saved_mb)/1024, 2) AS space_saved_gb,
       ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
GROUP BY TO_CHAR(execution_end, 'YYYY-MM')
ORDER BY month DESC
FETCH FIRST 12 MONTHS ONLY;
```

**Create Management Report:**
- Total space freed in last 12 months
- Average compression ratios achieved
- Tables with best/worst compression
- Projected savings for next quarter

---

#### 3. Log Cleanup

```sql
-- Check log table size
SELECT COUNT(*) AS log_records,
       ROUND(SUM(LENGTH(error_message))/1024/1024, 2) AS error_msg_mb,
       MIN(execution_start) AS oldest_record,
       MAX(execution_start) AS newest_record
FROM cmr.dwh_ilm_execution_log;

-- Check evaluation queue size
SELECT COUNT(*) AS queue_records,
       execution_status,
       MIN(evaluation_date) AS oldest_evaluation
FROM cmr.dwh_ilm_evaluation_queue
GROUP BY execution_status;
```

**Cleanup if needed:**
```sql
-- Archive old logs (> 90 days) before cleanup
CREATE TABLE cmr.dwh_ilm_execution_log_archive AS
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE execution_start < SYSDATE - 90;

-- Delete old logs
DELETE FROM cmr.dwh_ilm_execution_log
WHERE execution_start < SYSDATE - 90;
COMMIT;

-- Clean completed queue items (> 30 days)
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'EXECUTED'
AND evaluation_date < SYSDATE - 30;
COMMIT;

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('CMR', 'DWH_ILM_EXECUTION_LOG');
EXEC DBMS_STATS.GATHER_TABLE_STATS('CMR', 'DWH_ILM_EVALUATION_QUEUE');
```

---

#### 4. Index Maintenance

Check index health on ILM metadata tables:

```sql
SELECT index_name, table_name, status,
       TO_CHAR(last_analyzed, 'YYYY-MM-DD') AS last_analyzed,
       num_rows, distinct_keys, clustering_factor
FROM user_indexes
WHERE table_name LIKE 'DWH_ILM%'
ORDER BY table_name, index_name;
```

**Rebuild if:**
- `status = 'UNUSABLE'`
- `last_analyzed > 90 days ago`
- `clustering_factor > num_rows * 2` (fragmented)

```sql
-- Rebuild unusable or fragmented indexes
ALTER INDEX idx_ilm_exec_policy REBUILD ONLINE;
ALTER INDEX idx_ilm_exec_table REBUILD ONLINE;
ALTER INDEX idx_ilm_exec_date REBUILD ONLINE;
ALTER INDEX idx_ilm_exec_status REBUILD ONLINE;
ALTER INDEX idx_ilm_queue_status REBUILD ONLINE;
ALTER INDEX idx_ilm_queue_eligible REBUILD ONLINE;
ALTER INDEX idx_ilm_access_temp REBUILD ONLINE;
```

---

#### 5. Configuration Review

Review all configuration settings:

```sql
SELECT config_key, config_value, description,
       TO_CHAR(modified_date, 'YYYY-MM-DD') AS last_modified,
       modified_by
FROM cmr.dwh_ilm_config
ORDER BY config_key;
```

**Key Settings to Review:**
- `EXECUTION_WINDOW_START` / `EXECUTION_WINDOW_END`: Still appropriate?
- `HOT_THRESHOLD_DAYS` / `WARM_THRESHOLD_DAYS`: Aligned with business needs?
- `MAX_CONCURRENT_OPERATIONS`: Balanced for performance?
- `ALERT_EMAIL_RECIPIENT`: Current DBA team email?

---

## Quarterly Operations

### Quarterly Review (2 hours)

Perform these comprehensive reviews every quarter.

#### 1. Performance Analysis

Identify slow policy executions:

```sql
SELECT policy_name, table_name, partition_name,
       ROUND(duration_seconds/60, 2) AS duration_minutes,
       TO_CHAR(execution_start, 'YYYY-MM-DD HH24:MI') AS exec_time,
       size_before_mb, compression_type, parallel_degree
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > SYSDATE - 90
AND duration_seconds > 1800  -- Longer than 30 minutes
ORDER BY duration_seconds DESC
FETCH FIRST 20 ROWS ONLY;
```

**Optimization Actions:**
- Review `parallel_degree` for slow policies
- Consider splitting large partitions
- Review compression type selection (ARCHIVE HIGH is slowest)
- Check for resource contention during execution window

---

#### 2. Policy Optimization

Test and compare compression effectiveness:

```sql
-- Compare compression types performance
SELECT compression_type,
       COUNT(*) AS usage_count,
       ROUND(AVG(compression_ratio), 2) AS avg_ratio,
       ROUND(AVG(duration_seconds/60), 1) AS avg_duration_min,
       ROUND(SUM(space_saved_mb)/1024, 2) AS total_saved_gb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > SYSDATE - 90
GROUP BY compression_type
ORDER BY compression_type;
```

**Recommendations:**
- `QUERY LOW`: Fast compression, lower ratio (~2-3x)
- `QUERY HIGH`: Good balance (~3-5x compression)
- `ARCHIVE LOW`: Higher compression (~5-7x), slower
- `ARCHIVE HIGH`: Best compression (~7-10x), slowest

---

#### 3. Access Pattern Analysis (New in v2.0)

Review partition access patterns and temperature accuracy:

```sql
-- Temperature distribution by table
SELECT table_name,
       SUM(CASE WHEN temperature = 'HOT' THEN 1 ELSE 0 END) AS hot_partitions,
       SUM(CASE WHEN temperature = 'WARM' THEN 1 ELSE 0 END) AS warm_partitions,
       SUM(CASE WHEN temperature = 'COLD' THEN 1 ELSE 0 END) AS cold_partitions,
       COUNT(*) AS total_partitions
FROM cmr.dwh_ilm_partition_access
GROUP BY table_name
ORDER BY total_partitions DESC;
```

**Action:** If Heat Map available, sync for more accurate tracking:
```sql
EXEC dwh_sync_heatmap_to_tracking();
```

---

#### 4. Documentation Update

- Update this runbook with new issues encountered
- Document any policy changes made
- Review and update retention requirements
- Update disaster recovery procedures if needed
- Review escalation contacts

---

## Emergency Procedures

### Emergency: Stop All ILM Operations

**When:** ILM operations causing production impact (CPU/IO saturation, blocking)

**Immediate Action:**
```sql
-- Step 1: Stop scheduler jobs
EXEC dwh_stop_ilm_jobs();

-- Step 2: Disable auto-execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Step 3: Kill running sessions (if needed)
SELECT 'ALTER SYSTEM KILL SESSION ''' || sid || ',' || serial# || ''' IMMEDIATE;' AS kill_cmd
FROM v$session
WHERE module LIKE '%ILM%' OR action LIKE '%ILM%';
-- Run generated statements if necessary

-- Step 4: Verify stopped
SELECT job_name, enabled, state
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%';
-- All jobs should show enabled = FALSE
```

**Resume When Safe:**
```sql
-- Step 1: Re-enable auto-execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Step 2: Restart jobs
EXEC dwh_start_ilm_jobs();

-- Step 3: Verify running
SELECT job_name, enabled, state, last_start_date
FROM v_ilm_scheduler_status;
```

---

### Emergency: Disable Specific Policy

**When:** Single policy causing issues

```sql
-- Step 1: Disable problematic policy
UPDATE cmr.dwh_ilm_policies
SET enabled = 'N', modified_by = USER, modified_date = SYSTIMESTAMP
WHERE policy_name = 'PROBLEM_POLICY_NAME';
COMMIT;

-- Step 2: Clear pending actions for this policy
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE policy_id = (
    SELECT policy_id FROM cmr.dwh_ilm_policies
    WHERE policy_name = 'PROBLEM_POLICY_NAME'
)
AND execution_status = 'PENDING';
COMMIT;

-- Step 3: Document the issue
-- Add entry to incident log with reason and resolution
```

---

### Emergency: Uncompress Partition

**When:** Compressed partition causing query performance issues

```sql
-- Step 1: Identify partition details
SELECT table_owner, table_name, partition_name,
       compression, tablespace_name
FROM dba_tab_partitions
WHERE table_owner = 'OWNER'
AND table_name = 'TABLE_NAME'
AND partition_name = 'PARTITION_NAME';

-- Step 2: Move partition without compression
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
NOCOMPRESS;

-- Step 3: Generate index rebuild statements
SELECT 'ALTER INDEX ' || index_owner || '.' || index_name ||
       ' REBUILD PARTITION ' || partition_name || ' ONLINE;' AS rebuild_cmd
FROM dba_ind_partitions ip
WHERE ip.table_owner = 'OWNER'
AND ip.table_name = 'TABLE_NAME'
AND ip.partition_name = 'PARTITION_NAME'
AND ip.status = 'UNUSABLE';

-- Step 4: Run generated rebuild statements

-- Step 5: Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('OWNER', 'TABLE_NAME', partname => 'PARTITION_NAME', granularity => 'PARTITION');
```

---

### Emergency: Rollback Partition Move

**When:** Recently moved partition needs to go back to original tablespace

```sql
-- Step 1: Check current location
SELECT partition_name, tablespace_name
FROM dba_tab_partitions
WHERE table_owner = 'OWNER'
AND table_name = 'TABLE_NAME'
AND partition_name = 'PARTITION_NAME';

-- Step 2: Move back to original tablespace
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
TABLESPACE original_tablespace;

-- Step 3: Rebuild indexes (use same query as uncompress procedure)

-- Step 4: Update ILM execution log
UPDATE cmr.dwh_ilm_execution_log
SET error_message = 'Rolled back - moved to ' || 'ORIGINAL_TABLESPACE'
WHERE table_name = 'TABLE_NAME'
AND partition_name = 'PARTITION_NAME'
AND execution_id = (
    SELECT MAX(execution_id) FROM cmr.dwh_ilm_execution_log
    WHERE table_name = 'TABLE_NAME' AND partition_name = 'PARTITION_NAME'
);
COMMIT;
```

---

### Emergency: Clear Evaluation Queue

**When:** Queue backlog is too large or contains stale entries

```sql
-- Step 1: Check queue size
SELECT execution_status, COUNT(*) AS count
FROM cmr.dwh_ilm_evaluation_queue
GROUP BY execution_status;

-- Step 2: Clear old pending entries (> 7 days)
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'PENDING'
AND evaluation_date < SYSDATE - 7;
COMMIT;

-- Step 3: Clear all executed entries
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'EXECUTED';
COMMIT;

-- Step 4: Re-evaluate if needed
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();
```

---

## Monitoring Queries

### Real-Time Monitoring

#### Currently Running ILM Actions

```sql
SELECT s.sid, s.serial#, s.username, s.status,
       s.sql_id, s.event, s.seconds_in_wait,
       s.module, s.action,
       s.logon_time, s.last_call_et
FROM v$session s
WHERE (s.module LIKE '%ILM%' OR s.action LIKE '%ILM%')
AND s.status = 'ACTIVE'
ORDER BY s.last_call_et DESC;
```

#### Pending Actions in Queue

```sql
SELECT p.policy_name, q.table_name,
       COUNT(*) AS pending_actions,
       MIN(q.evaluation_date) AS oldest_evaluation,
       ROUND((SYSDATE - MIN(q.evaluation_date)) * 24, 1) AS hours_pending
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
WHERE q.execution_status = 'PENDING'
AND q.eligible = 'Y'
GROUP BY p.policy_name, q.table_name
ORDER BY hours_pending DESC;
```

#### Long-Running Operations

```sql
SELECT execution_id, policy_name, table_name, partition_name,
       TO_CHAR(execution_start, 'YYYY-MM-DD HH24:MI') AS start_time,
       ROUND((SYSDATE - execution_start) * 24 * 60, 1) AS running_minutes,
       action_type, compression_type
FROM cmr.dwh_ilm_execution_log
WHERE status = 'RUNNING'
ORDER BY execution_start;
```

---

### Performance Monitoring

#### Slowest Executions Today

```sql
SELECT policy_name, table_name, partition_name,
       ROUND(duration_seconds/60, 2) AS duration_minutes,
       TO_CHAR(execution_start, 'HH24:MI') AS start_time,
       TO_CHAR(execution_end, 'HH24:MI') AS end_time,
       status, compression_type
FROM cmr.dwh_ilm_execution_log
WHERE TRUNC(execution_start) = TRUNC(SYSDATE)
ORDER BY duration_seconds DESC
FETCH FIRST 10 ROWS ONLY;
```

#### Compression Ratio Trending

```sql
SELECT TO_CHAR(execution_end, 'YYYY-MM-DD') AS execution_date,
       ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio,
       ROUND(AVG(duration_seconds/60), 1) AS avg_duration_minutes,
       COUNT(*) AS compressions,
       ROUND(SUM(space_saved_mb)/1024, 2) AS total_saved_gb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_end > SYSDATE - 30
GROUP BY TO_CHAR(execution_end, 'YYYY-MM-DD')
ORDER BY execution_date;
```

#### Resource Utilization During ILM

```sql
-- Check concurrent operations
SELECT COUNT(*) AS concurrent_ilm_operations
FROM v$session
WHERE module LIKE '%ILM%' AND status = 'ACTIVE';

-- Check temp space usage
SELECT s.username, s.sid, s.serial#,
       ROUND(u.blocks * 8192/1024/1024, 2) AS temp_mb
FROM v$session s
JOIN v$tempseg_usage u ON s.saddr = u.session_addr
WHERE s.module LIKE '%ILM%'
ORDER BY temp_mb DESC;
```

---

## Troubleshooting Guide

### Problem: Policies Not Executing

**Symptoms:** No recent executions in log, queue is empty

**Diagnosis:**
```sql
-- Step 1: Check if jobs are running
SELECT job_name, state, enabled, failure_count
FROM v_ilm_scheduler_status;

-- Step 2: Check auto-execution enabled
SELECT config_value
FROM cmr.dwh_ilm_config
WHERE config_key = 'ENABLE_AUTO_EXECUTION';

-- Step 3: Check execution window
SELECT config_value
FROM cmr.dwh_ilm_config
WHERE config_key IN ('EXECUTION_WINDOW_START', 'EXECUTION_WINDOW_END');

-- Step 4: Check active policies
SELECT COUNT(*) FROM cmr.dwh_ilm_policies WHERE enabled = 'Y';
```

**Solutions:**
- Jobs broken: `EXEC dwh_start_ilm_jobs();`
- Auto-execution disabled: Update config to 'Y'
- Outside window: Wait or adjust window
- No enabled policies: Enable policies

---

### Problem: Execution Failures

**Symptoms:** Executions showing STATUS = 'FAILED'

**Diagnosis:**
```sql
-- Get recent failures with full error messages
SELECT execution_id, policy_name, table_name, partition_name,
       action_type, error_code, error_message,
       TO_CHAR(execution_start, 'YYYY-MM-DD HH24:MI') AS exec_time
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 7
ORDER BY execution_start DESC;
```

**Common Error Codes:**
- **ORA-14006:** Partition not found → Partition may have been dropped
- **ORA-01031:** Insufficient privileges → Grant needed
- **ORA-01652:** Unable to extend temp segment → Increase temp tablespace
- **ORA-14511:** Cannot perform operation on online partition → Set execution_mode to OFFLINE

**Solutions:**
```sql
-- Solution 1: Invalid partition - clear from queue
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE partition_name = 'INVALID_PARTITION';

-- Solution 2: Privilege issue - grant required privilege
GRANT ALTER ANY TABLE TO ilm_schema;

-- Solution 3: Temp space - add temp file
ALTER TABLESPACE TEMP ADD TEMPFILE SIZE 10G;

-- Solution 4: Change execution mode
UPDATE cmr.dwh_ilm_policies
SET execution_mode = 'OFFLINE'
WHERE policy_id = <policy_id>;
```

---

### Problem: Low Compression Ratios

**Symptoms:** Compression ratio < 2x consistently

**Diagnosis:**
```sql
-- Check compression effectiveness by table
SELECT table_name, compression_type,
       COUNT(*) AS compressions,
       ROUND(AVG(compression_ratio), 2) AS avg_ratio,
       ROUND(MIN(compression_ratio), 2) AS min_ratio,
       ROUND(MAX(compression_ratio), 2) AS max_ratio
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > SYSDATE - 30
GROUP BY table_name, compression_type
HAVING AVG(compression_ratio) < 2.0
ORDER BY avg_ratio;
```

**Possible Causes:**
1. Data already compressed
2. Compression type too low (QUERY LOW)
3. Data types not compressible (LOBs, encrypted)
4. Small partitions (< 100MB)

**Solutions:**
- Upgrade compression: `QUERY LOW` → `QUERY HIGH` → `ARCHIVE HIGH`
- Skip re-compression of already compressed partitions
- Add size threshold to policy to skip small partitions

---

### Problem: Queue Backlog

**Symptoms:** Large number of PENDING items in queue

**Diagnosis:**
```sql
-- Check queue size and age
SELECT execution_status, COUNT(*) AS count,
       MIN(evaluation_date) AS oldest,
       MAX(evaluation_date) AS newest,
       ROUND(AVG(SYSDATE - evaluation_date), 1) AS avg_age_days
FROM cmr.dwh_ilm_evaluation_queue
GROUP BY execution_status;
```

**Causes:**
- MAX_CONCURRENT_OPERATIONS too low
- Execution window too short
- Policies evaluating too frequently
- Operations running slowly

**Solutions:**
```sql
-- Solution 1: Increase concurrent operations
UPDATE cmr.dwh_ilm_config
SET config_value = '20'  -- Increase from default 10
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';

-- Solution 2: Extend execution window
UPDATE cmr.dwh_ilm_config
SET config_value = '20:00'  -- Start earlier
WHERE config_key = 'EXECUTION_WINDOW_START';

-- Solution 3: Clear old pending items
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE execution_status = 'PENDING'
AND evaluation_date < SYSDATE - 7;
```

---

### Problem: Policy Validation Errors (New in v2.0)

**Symptoms:** Cannot insert/update policies, validation trigger errors

**Diagnosis:**
```sql
-- Check recent validation errors
SELECT ORA_ERR_MESG$
FROM dba_errors
WHERE name = 'TRG_VALIDATE_DWH_ILM_POLICY'
AND timestamp > SYSDATE - 1;
```

**Common Validation Errors:**
- **ORA-20001:** Table doesn't exist or not partitioned
- **ORA-20002:** Target tablespace doesn't exist
- **ORA-20003:** Invalid compression type
- **ORA-20004:** Missing compression_type for COMPRESS action
- **ORA-20005:** Missing target_tablespace for MOVE action
- **ORA-20007:** No condition specified (age_days, access_pattern, etc.)
- **ORA-20010:** Priority out of range (must be 1-999)

**Solutions:** Fix policy definition based on error code (see error message reference in POLICY_VALIDATION_IMPLEMENTATION.md)

---

### Problem: Inaccurate Temperature Classification (New in v2.0)

**Symptoms:** All partitions showing same temperature, policies based on access_pattern not working

**Diagnosis:**
```sql
-- Check temperature distribution
SELECT temperature, COUNT(*) AS partition_count
FROM cmr.dwh_ilm_partition_access
GROUP BY temperature;

-- Check tracking freshness
SELECT MAX(last_updated) AS latest_update,
       ROUND((SYSDATE - MAX(last_updated)) * 24, 1) AS hours_since_update
FROM cmr.dwh_ilm_partition_access;
```

**Causes:**
1. Partition tracking not refreshed
2. Using age-based calculation (default)
3. Heat Map not available or not synced

**Solutions:**
```sql
-- Solution 1: Refresh partition tracking
EXEC dwh_refresh_partition_access_tracking();

-- Solution 2: Sync with Heat Map (if available)
EXEC dwh_sync_heatmap_to_tracking();

-- Solution 3: Adjust temperature thresholds
UPDATE cmr.dwh_ilm_config
SET config_value = '60'  -- Tighten threshold
WHERE config_key = 'HOT_THRESHOLD_DAYS';
COMMIT;

EXEC dwh_refresh_partition_access_tracking();
```

---

## Performance Tuning

### Tuning Compression Performance

**Slow Compressions:**
1. **Increase Parallel Degree**
```sql
UPDATE cmr.dwh_ilm_policies
SET parallel_degree = 8  -- Increase from 4
WHERE policy_name = 'SLOW_POLICY';
```

2. **Use Lower Compression Level**
```sql
-- Change from ARCHIVE HIGH to QUERY HIGH for faster compression
UPDATE cmr.dwh_ilm_policies
SET compression_type = 'QUERY HIGH'
WHERE compression_type = 'ARCHIVE HIGH'
AND policy_name = 'SLOW_POLICY';
```

3. **Schedule During Low Activity**
```sql
-- Adjust execution window to off-peak hours
UPDATE cmr.dwh_ilm_config
SET config_value = '23:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config
SET config_value = '04:00'
WHERE config_key = 'EXECUTION_WINDOW_END';
```

---

### Tuning Queue Processing

**Speed Up Queue Processing:**
```sql
-- Increase concurrent operations
UPDATE cmr.dwh_ilm_config
SET config_value = '15'  -- From 10
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';

-- Verify resource availability first:
SELECT *
FROM v$resource_limit
WHERE resource_name IN ('processes', 'sessions');
```

---

### Tuning Policy Evaluation

**Reduce Evaluation Overhead:**
1. **Adjust Evaluation Frequency**
   - Current: Every 30 minutes (default)
   - Reduce to hourly for less volatile workloads

2. **Optimize Policy Priority**
```sql
-- Higher priority = evaluated first
-- Assign priorities strategically:
-- 100 = Critical policies (compliance, regulatory)
-- 200 = High value (large space savings)
-- 300 = Normal policies
-- 400 = Low priority (small tables)
```

---

## Alerting Thresholds

Configure monitoring alerts based on these thresholds:

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| **Job failure count (24h)** | > 1 | > 3 | Investigate job history |
| **Execution failure rate** | > 5% | > 10% | Review error messages |
| **Average compression ratio** | < 2.0x | < 1.5x | Review compression types |
| **Job execution lag** | > 2 hours | > 4 hours | Check scheduler/window |
| **Queue backlog (pending)** | > 100 | > 500 | Increase concurrency |
| **Execution duration** | > 60 min | > 120 min | Review parallel_degree |
| **Tracking freshness** | > 24 hours | > 48 hours | Refresh tracking |
| **Policy validation errors** | > 1/day | > 5/day | Review policy changes |
| **Temp space usage** | > 70% | > 90% | Add temp files |
| **Log table size** | > 1M rows | > 5M rows | Run cleanup |

---

## Email Notifications Setup

The ILM framework includes built-in email notification support for failures and critical events.

### Prerequisites

1. **Oracle UTL_MAIL Package Access**
   ```sql
   -- Run as SYSDBA
   GRANT EXECUTE ON UTL_MAIL TO cmr;
   ```

2. **SMTP Server Configuration**
   ```sql
   -- Run as SYSDBA
   ALTER SYSTEM SET smtp_out_server='smtp.company.com:25' SCOPE=BOTH;
   ```

3. **Network Access**
   - Ensure database server can reach SMTP server
   - Check firewall rules for port 25 (or your SMTP port)

---

### Initial Configuration

#### Step 1: Configure Email Settings

```sql
-- Set recipient list (comma-separated)
UPDATE cmr.dwh_ilm_config
SET config_value = 'dba-team@company.com,datawarehouse-ops@company.com'
WHERE config_key = 'ALERT_EMAIL_RECIPIENTS';

-- Set sender email
UPDATE cmr.dwh_ilm_config
SET config_value = 'oracle-ilm@company.com'
WHERE config_key = 'ALERT_EMAIL_SENDER';

-- Set SMTP server
UPDATE cmr.dwh_ilm_config
SET config_value = 'smtp.company.com'
WHERE config_key = 'SMTP_SERVER';

COMMIT;
```

#### Step 2: Configure Alert Thresholds

```sql
-- Failure threshold (how many failures trigger an alert)
UPDATE cmr.dwh_ilm_config
SET config_value = '3'  -- Default: 3 failures
WHERE config_key = 'ALERT_FAILURE_THRESHOLD';

-- Alert interval (minimum hours between alerts to prevent spam)
UPDATE cmr.dwh_ilm_config
SET config_value = '4'  -- Default: 4 hours
WHERE config_key = 'ALERT_INTERVAL_HOURS';

COMMIT;
```

**Recommended Settings:**
- **Production Critical Tables:** Threshold = 2, Interval = 2 hours
- **Production Standard Tables:** Threshold = 3, Interval = 4 hours (default)
- **Development/Test:** Threshold = 5, Interval = 8 hours

#### Step 3: Test Email Functionality

```sql
-- Send test email
SET SERVEROUTPUT ON
BEGIN
    dwh_send_ilm_alert(
        p_alert_type => 'TEST',
        p_subject => 'ILM Email Notification Test',
        p_message => 'Test email from ILM framework on ' ||
                     SYS_CONTEXT('USERENV', 'DB_NAME')
    );
    DBMS_OUTPUT.PUT_LINE('Test alert sent successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Check SMTP configuration and UTL_MAIL grants');
END;
/
```

**Expected Result:** Team members receive test email within 1-2 minutes

**If test fails:**
1. Check SMTP server configuration: `SHOW PARAMETER smtp_out_server`
2. Verify UTL_MAIL grant: `SELECT * FROM user_tab_privs WHERE table_name = 'UTL_MAIL'`
3. Test network connectivity: `tnsping smtp.company.com`
4. Check database alert log for detailed errors

#### Step 4: Enable Email Notifications

```sql
-- Enable notifications
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;
```

#### Step 5: Enable Failure Monitoring Job

```sql
-- Enable the scheduler job that monitors for failures
BEGIN
    DBMS_SCHEDULER.ENABLE('ILM_JOB_MONITOR_FAILURES');
    DBMS_OUTPUT.PUT_LINE('Failure monitoring job enabled');
END;
/

-- Verify job is running
SELECT job_name, enabled, state, next_run_date, failure_count
FROM user_scheduler_jobs
WHERE job_name = 'ILM_JOB_MONITOR_FAILURES';
```

**Expected:** `enabled = TRUE`, `state = 'SCHEDULED'`, `failure_count = 0`

---

### Alert Types and Format

The framework sends three types of alerts:

#### 1. Failure Alerts (Automatic)
**Trigger:** When failure count exceeds threshold within monitoring window

**Subject:** `[ILM FAILURE] N ILM failures detected in last 24 hours`

**Content:**
```
ILM Failure Alert
=================

Failure Count: 5
Time Period: Last 24 hours
Alert Threshold: 3

Recent Failures:
----------------

Execution ID: 12345
  Policy: SALES_COMPRESS_90D
  Table: SALES_FACT
  Partition: P_2023_06
  Error: ORA-01555: snapshot too old
  Time: 2024-10-23 02:15:43
  Recommendation: Increase undo_retention or reduce operation duration

Execution ID: 12346
  Policy: ORDERS_ARCHIVE_1Y
  Table: ORDER_FACT
  Partition: P_2022_12
  Error: ORA-01658: unable to create initial extent
  Time: 2024-10-23 02:47:12
  Recommendation: Add datafiles to tablespace TBS_COLD

...

Action Required:
- Review error messages above
- Query: SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = 'FAILED' AND execution_start > SYSTIMESTAMP - 1
- Contact DBA team if issue persists
```

#### 2. Warning Alerts (Manual/Custom)
**Trigger:** Custom conditions detected (e.g., stuck operations)

**Subject:** `[ILM WARNING] Custom warning message`

#### 3. Test Alerts
**Trigger:** Manual test via `dwh_send_ilm_alert`

**Subject:** `[ILM TEST] Test message`

---

### Monitoring Email Notifications

#### Check Email Configuration

```sql
SELECT config_key, config_value
FROM cmr.dwh_ilm_config
WHERE config_key LIKE 'ALERT%' OR config_key LIKE 'ENABLE_EMAIL%'
ORDER BY config_key;
```

#### View Recent Failures (Alert Candidates)

```sql
SELECT
    COUNT(*) AS failure_count,
    MIN(execution_start) AS first_failure,
    MAX(execution_start) AS last_failure,
    LISTAGG(DISTINCT table_name, ', ') WITHIN GROUP (ORDER BY table_name) AS affected_tables
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSTIMESTAMP - INTERVAL '24' HOUR;
```

#### Manually Trigger Failure Check

```sql
-- Check for failures and send alert if threshold exceeded
SET SERVEROUTPUT ON
EXEC dwh_check_ilm_failures(p_hours_back => 24);
```

---

### Adjusting Alert Behavior

#### Increase Sensitivity (Production Critical)

```sql
-- Alert on 2 failures instead of 3
UPDATE cmr.dwh_ilm_config
SET config_value = '2'
WHERE config_key = 'ALERT_FAILURE_THRESHOLD';

-- Send alerts every 2 hours if failures continue
UPDATE cmr.dwh_ilm_config
SET config_value = '2'
WHERE config_key = 'ALERT_INTERVAL_HOURS';

COMMIT;
```

#### Decrease Sensitivity (Development/Test)

```sql
-- Alert on 10 failures
UPDATE cmr.dwh_ilm_config
SET config_value = '10'
WHERE config_key = 'ALERT_FAILURE_THRESHOLD';

-- Send alerts at most once per day
UPDATE cmr.dwh_ilm_config
SET config_value = '24'
WHERE config_key = 'ALERT_INTERVAL_HOURS';

COMMIT;
```

#### Temporarily Disable Alerts (Maintenance Window)

```sql
-- Disable notifications temporarily
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;

-- Perform maintenance...

-- Re-enable notifications
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
COMMIT;
```

---

### Troubleshooting Email Issues

#### Problem: No Emails Received

**Check 1: Verify Email is Enabled**
```sql
SELECT config_value FROM cmr.dwh_ilm_config
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';
-- Expected: 'Y'
```

**Check 2: Verify Failure Threshold Met**
```sql
SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSTIMESTAMP - INTERVAL '24' HOUR;
-- Must be >= ALERT_FAILURE_THRESHOLD
```

**Check 3: Verify Not in Alert Suppression Window**
```sql
-- Check when last alert was sent (stored in scheduler job log)
SELECT log_date, additional_info
FROM user_scheduler_job_run_details
WHERE job_name = 'ILM_JOB_MONITOR_FAILURES'
ORDER BY log_date DESC
FETCH FIRST 5 ROWS ONLY;
```

**Check 4: Test Email Manually**
```sql
EXEC dwh_send_ilm_alert('TEST', 'Test Subject', 'Test Message');
```

#### Problem: Too Many Emails

**Solution: Increase alert interval or threshold**
```sql
-- Send emails less frequently (every 8 hours instead of 4)
UPDATE cmr.dwh_ilm_config
SET config_value = '8'
WHERE config_key = 'ALERT_INTERVAL_HOURS';

-- Require more failures to trigger alert (5 instead of 3)
UPDATE cmr.dwh_ilm_config
SET config_value = '5'
WHERE config_key = 'ALERT_FAILURE_THRESHOLD';

COMMIT;
```

#### Problem: Emails Go to Spam

**Solution: Work with email team to:**
1. Whitelist sender address (`oracle-ilm@company.com`)
2. Add SPF record for database server IP
3. Configure proper SMTP authentication if required
4. Use internal relay server instead of external SMTP

---

### Best Practices for Email Alerts

1. **Use Distribution Lists**
   - Don't hardcode individual emails
   - Use team distribution lists (e.g., `dba-team@company.com`)
   - Easier to manage team membership changes

2. **Set Appropriate Thresholds**
   - Production: Lower threshold (2-3 failures)
   - Development: Higher threshold (5-10 failures)
   - Avoid alert fatigue from too many emails

3. **Regular Testing**
   - Test email functionality monthly
   - Verify all team members receive alerts
   - Update recipient list when team changes

4. **Document Responses**
   - Create runbook for common alert types
   - Document resolution steps
   - Track alert response times

5. **Monitor Alert Frequency**
   - Track how often alerts are sent
   - Investigate if alert frequency is increasing
   - Tune policies to prevent recurring failures

---

## Best Practices

### Policy Management

1. **Use Descriptive Names**
   - Good: `SALES_COMPRESS_90D`, `ORDERS_ARCHIVE_1Y`
   - Bad: `POLICY1`, `COMPRESS_OLD`

2. **Document Policy Intent**
   - Add comments explaining business rationale
   - Note data retention requirements
   - Document who requested the policy

3. **Test Before Production**
   - Test new policies on non-production first
   - Use `enabled = 'N'` initially
   - Validate with manual execution
   - Enable after successful test

4. **Review Policy Effectiveness**
   - Monthly review of compression ratios
   - Adjust thresholds based on results
   - Remove unused policies

---

### Operational Excellence

1. **Maintain Execution Window**
   - Schedule during off-peak hours
   - Avoid overlap with batch jobs
   - Leave buffer before morning peak

2. **Monitor Resource Usage**
   - Track temp space consumption
   - Monitor parallel execution impact
   - Balance concurrency vs. performance

3. **Regular Maintenance**
   - Weekly: Review failures and performance
   - Monthly: Audit policies and cleanup logs
   - Quarterly: Comprehensive performance review

4. **Documentation**
   - Keep runbook updated
   - Document all policy changes
   - Maintain incident log
   - Update contact information

---

### Migration Integration (New in v2.0)

1. **Use Auto-Detection**
   - Let framework detect template based on table name
   - Override only when necessary
   - Follow standard naming conventions

2. **Verify Template Application**
   - Check policies created after migration
   - Validate policies with `dwh_validate_ilm_policy()`
   - Test policy evaluation on new tables

3. **Initialize Tracking**
   - Ensure partition tracking initialized
   - Verify temperature classification
   - Refresh tracking after migration

---

## Contact Escalation

### Escalation Path

**Level 1: Operations DBA** (Daily monitoring)
- Daily health checks
- Basic troubleshooting
- Job restarts
- Policy enable/disable
- **Contact:** [dba-operations@company.com]
- **Response Time:** 1 hour (business hours)

**Level 2: Senior DBA** (Policy optimization)
- Policy performance tuning
- Compression strategy
- Configuration changes
- Advanced troubleshooting
- **Contact:** [dba-senior@company.com]
- **Response Time:** 4 hours

**Level 3: Database Architect** (Framework issues)
- Framework modifications
- Integration issues
- Performance architecture
- Escalated incidents
- **Contact:** [dba-architect@company.com]
- **Response Time:** Next business day

**Level 4: Vendor Support** (Oracle bugs)
- Oracle SR creation
- Bug escalation
- Patch management
- **Contact:** [Oracle Support]

---

## Quick Reference Commands

### Daily Operations
```sql
-- Health check
SELECT * FROM v_ilm_scheduler_status;
SELECT * FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 1 AND status = 'FAILED';

-- Space savings
SELECT table_name, ROUND(SUM(space_saved_mb)/1024,2) AS gb_saved
FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 1 GROUP BY table_name;
```

### Emergency Commands
```sql
-- Stop everything
EXEC dwh_stop_ilm_jobs();

-- Start everything
EXEC dwh_start_ilm_jobs();

-- Manual cycle
EXEC dwh_run_ilm_cycle();

-- Disable policy
UPDATE cmr.dwh_ilm_policies SET enabled = 'N' WHERE policy_name = '...';
```

### Validation Commands (New in v2.0)
```sql
-- Validate policy
EXEC dwh_validate_ilm_policy(1);

-- Refresh tracking
EXEC dwh_refresh_partition_access_tracking();

-- Sync Heat Map
EXEC dwh_sync_heatmap_to_tracking();
```

---

**Document Version:** 2.0
**Last Updated:** 2025-10-22
**Maintained By:** DBA Team
**Review Frequency:** Quarterly

---

**End of Operations Runbook**
