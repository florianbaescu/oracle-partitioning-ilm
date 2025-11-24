# Oracle Data Warehouse ILM Framework - Complete Architecture Analysis

**Analysis Date**: 2025-11-21
**Purpose**: Comprehensive analysis of ILM orchestration, prioritization, best practices, and production requirements

---

## 📊 Executive Summary

This Oracle Data Warehouse ILM (Information Lifecycle Management) framework is a **production-grade, queue-based orchestration system** that automates partition lifecycle operations across three distinct stages:

1. **Refresh Access** - Track partition heat/temperature
2. **Evaluate Policies** - Identify eligible partitions
3. **Execute Actions** - Perform ILM operations (compress, tier, purge)

The system implements intelligent **multi-level prioritization**, graceful **error handling**, and **audit trail** capabilities while remaining flexible for custom policies and actions.

---

## 🏗️ Complete Architecture Overview

### Core Components

| Component Type | Object Name | Purpose |
|---|---|---|
| **Metadata Tables** (7) | dwh_ilm_policies | Policy definitions |
| | dwh_ilm_partition_access | Partition heat map |
| | dwh_ilm_evaluation_queue | Pending actions queue |
| | dwh_ilm_execution_log | Execution history |
| | dwh_ilm_partition_merges | Merge audit trail |
| | dwh_ilm_config | Configuration parameters |
| | dwh_ilm_threshold_profiles | Temperature thresholds |
| **PL/SQL Packages** (3) | pck_dwh_ilm_policy_engine | Policy evaluation |
| | pck_dwh_ilm_execution_engine | Action execution |
| | pck_dwh_partition_utilities | Partition operations |
| **Scheduler Jobs** (4) | DWH_ILM_JOB_REFRESH_ACCESS | Daily @ 1 AM |
| | DWH_ILM_JOB_EVALUATE | Daily @ 2 AM |
| | DWH_ILM_JOB_EXECUTE | Every 2 hours |
| | DWH_ILM_JOB_CLEANUP | Weekly |

### Data Flow Architecture

```
┌──────────────────────────────────────────────────────────┐
│  STAGE 1: PARTITION ACCESS TRACKING (Daily @ 1 AM)      │
│  ─────────────────────────────────────────────────────   │
│  dwh_refresh_partition_access_tracking()                 │
│    ├─ Reads: dba_tab_partitions, dba_segments            │
│    ├─ Calculates: partition age from high_value          │
│    ├─ Classifies: HOT/WARM/COLD using threshold profiles │
│    ├─ Integrates: Oracle Heat Map (if available)         │
│    └─ Populates: dwh_ilm_partition_access                │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  STAGE 2: POLICY EVALUATION (Daily @ 2 AM)              │
│  ─────────────────────────────────────────────────────   │
│  pck_dwh_ilm_policy_engine.evaluate_all_policies()       │
│    ├─ For each enabled policy (ORDER BY priority):      │
│    │   ├─ Check partition eligibility:                  │
│    │   │   ├─ Age >= threshold                          │
│    │   │   ├─ Size >= threshold                         │
│    │   │   ├─ Temperature matches pattern               │
│    │   │   ├─ Not already in target state               │
│    │   │   └─ Custom SQL condition                      │
│    │   └─ If eligible → Insert into queue               │
│    └─ Populates: dwh_ilm_evaluation_queue                │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│  STAGE 3: ACTION EXECUTION (Every 2 hours)              │
│  ─────────────────────────────────────────────────────   │
│  pck_dwh_ilm_execution_engine.execute_pending_actions()  │
│    ├─ Check: Execution window (22:00-06:00)             │
│    ├─ For each policy (ORDER BY priority):              │
│    │   ├─ For each pending partition:                   │
│    │   │   ├─ execute_single_action()                   │
│    │   │   │   ├─ COMPRESS_PARTITION                    │
│    │   │   │   ├─ MOVE_PARTITION                        │
│    │   │   │   ├─ MAKE_READONLY                         │
│    │   │   │   ├─ DROP/TRUNCATE                         │
│    │   │   │   └─ CUSTOM (user-defined)                 │
│    │   │   ├─ Auto-merge (if enabled)                   │
│    │   │   └─ Log to execution_log                      │
│    │   └─ Respect: MAX_CONCURRENT_OPERATIONS            │
│    └─ Updates: dwh_ilm_execution_log                     │
└──────────────────────────────────────────────────────────┘
```

---

## 🎯 ILM Orchestration Steps - Detailed Flow

### STAGE 1: Partition Access Tracking

**Procedure**: `dwh_refresh_partition_access_tracking(p_table_owner, p_table_name)`

**Purpose**: Build a "heat map" of partition temperatures based on age and access patterns.

**Process**:

1. **Merge partition metadata** from Oracle data dictionary:
   ```sql
   MERGE INTO cmr.dwh_ilm_partition_access pa
   USING (
       SELECT
           tp.table_owner,
           tp.table_name,
           tp.partition_name,
           tp.high_value,
           sg.bytes / 1024 / 1024 AS size_mb
       FROM dba_tab_partitions tp
       JOIN dba_segments sg ON ...
   ) src
   ON (pa.partition_name = src.partition_name)
   ```

2. **Calculate partition age** from high_value date:
   ```sql
   days_since_write = TRUNC(SYSDATE - partition_high_value_date)
   ```

3. **Classify temperature** using threshold profiles:
   ```sql
   temperature = CASE
       WHEN days_since_write < hot_threshold_days THEN 'HOT'
       WHEN days_since_write < warm_threshold_days THEN 'WARM'
       ELSE 'COLD'
   END
   ```

4. **Threshold profiles** (configurable):
   - **DEFAULT profile**: HOT=730d (24mo), WARM=1825d (60mo), COLD=60mo+
   - **Policy-specific profiles**: Override per table/policy
   - **Custom profiles**: Define in `dwh_ilm_threshold_profiles`

5. **Optional Heat Map integration** (Enterprise Edition):
   ```sql
   EXEC dwh_sync_heatmap_to_tracking()
   -- Reads DBA_HEAT_MAP_SEGMENT for actual access data
   ```

**Scheduled**: Daily @ 1:00 AM

---

### STAGE 2: Policy Evaluation

**Procedure**: `pck_dwh_ilm_policy_engine.evaluate_all_policies()`

**Purpose**: Identify partitions eligible for ILM actions based on policy rules.

**Process**:

1. **Clear stale queue items** (older than 7 days):
   ```sql
   DELETE FROM cmr.dwh_ilm_evaluation_queue
   WHERE evaluation_date < SYSDATE - 7
   AND execution_status NOT IN ('RUNNING', 'COMPLETED');
   ```

2. **Iterate through enabled policies** by priority:
   ```sql
   FOR policy IN (
       SELECT * FROM cmr.dwh_ilm_policies
       WHERE enabled = 'Y'
       ORDER BY priority ASC, policy_id ASC
   )
   ```

3. **For each policy, evaluate all partitions**:
   ```sql
   FOR partition IN (
       SELECT * FROM cmr.dwh_ilm_partition_access
       WHERE table_owner = policy.table_owner
       AND table_name = policy.table_name
   )
   ```

4. **Eligibility checks** (via `is_partition_eligible()`):
   - ✅ **Policy enabled**: `policy.enabled = 'Y'`
   - ✅ **Age threshold**: `partition.age_days >= policy.age_days` OR `age_months >= policy.age_months`
   - ✅ **Size threshold**: `partition.size_mb >= policy.min_size_mb`
   - ✅ **Temperature match**: `partition.temperature = policy.access_pattern` (if specified)
   - ✅ **Not already in target state**:
     - For COMPRESS: `compression != 'ENABLED'`
     - For MOVE: `tablespace_name != policy.target_tablespace`
   - ✅ **Custom condition**: Evaluate `policy.custom_condition` SQL

5. **Queue eligible partitions**:
   ```sql
   INSERT INTO cmr.dwh_ilm_evaluation_queue (
       policy_id,
       partition_name,
       eligible,
       reason,
       execution_status
   ) VALUES (
       policy.policy_id,
       partition.partition_name,
       'Y',
       'Partition meets all policy criteria',
       'PENDING'
   );
   ```

**Scheduled**: Daily @ 2:00 AM (runs after refresh_access)

---

### STAGE 3: Action Execution

**Procedure**: `pck_dwh_ilm_execution_engine.execute_pending_actions(p_max_operations)`

**Purpose**: Execute ILM actions on eligible partitions.

**Process**:

1. **Check execution window**:
   ```sql
   IF NOT is_execution_window_open() THEN
       DBMS_OUTPUT.PUT_LINE('Outside execution window');
       RETURN;
   END IF;
   ```
   - Default: 22:00-06:00 (overnight)
   - Configurable via `EXECUTION_WINDOW_START/END` in `dwh_ilm_config`

2. **Retrieve pending policies** by priority:
   ```sql
   FOR policy IN (
       SELECT DISTINCT p.policy_id, p.priority
       FROM cmr.dwh_ilm_evaluation_queue q
       JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
       WHERE q.execution_status = 'PENDING'
       AND q.eligible = 'Y'
       ORDER BY p.priority, p.policy_id
   )
   ```

3. **Execute actions** per policy:
   ```sql
   v_operations_count := 0;

   FOR queue_item IN (
       SELECT * FROM cmr.dwh_ilm_evaluation_queue
       WHERE policy_id = policy.policy_id
       AND execution_status = 'PENDING'
       FOR UPDATE  -- Lock row
   ) LOOP
       -- Check max operations limit
       IF v_operations_count >= p_max_operations THEN
           EXIT;
       END IF;

       -- Execute action
       execute_single_action(queue_item.queue_id);
       v_operations_count := v_operations_count + 1;
   END LOOP;
   ```

4. **Execute single action** (`execute_single_action()`):
   ```sql
   CASE policy.action_type
       WHEN 'COMPRESS' THEN
           compress_partition(...);
       WHEN 'MOVE' THEN
           move_partition(...);
           -- Auto-merge if enabled
           IF config.AUTO_MERGE_PARTITIONS = 'Y' THEN
               attempt_partition_merge(...);
           END IF;
       WHEN 'READ_ONLY' THEN
           make_partition_readonly(...);
       WHEN 'DROP' THEN
           drop_partition(...);
       WHEN 'TRUNCATE' THEN
           truncate_partition(...);
       WHEN 'CUSTOM' THEN
           EXECUTE IMMEDIATE policy.custom_action;
   END CASE;
   ```

5. **Log execution results**:
   ```sql
   INSERT INTO cmr.dwh_ilm_execution_log (
       policy_id,
       partition_name,
       action_type,
       status,                -- SUCCESS, FAILED, SKIPPED
       execution_start_time,
       execution_end_time,
       duration_seconds,
       size_before_mb,
       size_after_mb,
       space_saved_mb,
       compression_ratio,
       error_code,
       error_message
   ) VALUES (...);
   ```

6. **Update queue status**:
   ```sql
   UPDATE cmr.dwh_ilm_evaluation_queue
   SET execution_status = 'EXECUTED',  -- or 'FAILED'
       execution_date = SYSTIMESTAMP
   WHERE queue_id = ...;
   ```

**Scheduled**: Every 2 hours (checks execution window internally)

**Concurrency Control**: Respects `MAX_CONCURRENT_OPERATIONS` (default: 4)

---

## 🎖️ Prioritization Logic

The ILM framework implements **3-level prioritization** to ensure critical operations execute first:

### Level 1: Policy Priority (Primary)

**Column**: `dwh_ilm_policies.priority` (NUMBER, lower = higher priority)

**Typical Priorities**:
- **100** - Compression (reduce storage cost quickly)
- **200** - Tiering to WARM storage
- **300** - Tiering to COLD storage
- **400** - Make read-only (compliance)
- **900** - Purge/drop old data

**Example**:
```sql
Policy A: Priority 100 (COMPRESS_OLD_PARTITIONS)
  → Executes ALL eligible partitions before any Priority 200 policies

Policy B: Priority 200 (TIER_TO_WARM_3M)
  → Executes after Policy A completes

Policy C: Priority 900 (PURGE_OLD_PARTITIONS)
  → Executes last
```

### Level 2: Evaluation Order (Secondary)

Within same policy priority, partitions ordered by:
```sql
ORDER BY evaluation_date ASC
```
- Earlier evaluations execute first
- FIFO queue per policy

### Level 3: Concurrency Limits (Tertiary)

**Max Operations Per Run**: `p_max_operations` (default: 10)
**Max Concurrent Operations**: `MAX_CONCURRENT_OPERATIONS` (default: 4)

**Behavior**:
- If limit reached during execution → EXIT loop
- Remaining partitions wait for next scheduler cycle (2 hours)
- Prevents resource exhaustion

### Prioritization Algorithm

```
operations_count = 0

FOR each policy (ORDER BY priority ASC, policy_id ASC):
    FOR each partition (ORDER BY evaluation_date ASC):
        IF operations_count >= MAX_OPERATIONS:
            EXIT ALL LOOPS

        execute_action(partition)
        operations_count++
    END FOR
END FOR
```

### Example Execution Flow

```
Hour 22:00 (First run):
  Policy 100 (COMPRESS) - 50 eligible partitions
    ├─ P_2023_01 → executes (1/10)
    ├─ P_2023_02 → executes (2/10)
    ├─ ...
    └─ P_2023_10 → executes (10/10)
    └─ EXIT (max_operations reached)

Hour 00:00 (Second run):
  Policy 100 (COMPRESS) - 40 remaining partitions
    ├─ P_2023_11 → executes (1/10)
    ├─ ...
    └─ P_2023_20 → executes (10/10)
    └─ EXIT (max_operations reached)

Hour 02:00 (Third run):
  Policy 100 (COMPRESS) - 30 remaining partitions
    ├─ P_2023_21 → executes (1/10)
    ├─ ...
    └─ P_2023_30 → executes (10/10)
    └─ EXIT (max_operations reached)

Hour 04:00 (Fourth run):
  Policy 100 (COMPRESS) - 20 remaining partitions
    ├─ P_2023_31 → executes (1/10)
    ├─ ...
    └─ P_2023_40 → executes (10/10)
    └─ EXIT (max_operations reached)

Hour 06:00 (Fifth run):
  Policy 100 (COMPRESS) - 10 remaining partitions
    ├─ P_2023_41 → executes (1/10)
    ├─ ...
    └─ P_2023_50 → executes (10/10)

  Policy 200 (TIER_TO_WARM) - NOW starts executing
```

---

## ⚙️ Action Types & Execution Details

### 1. COMPRESS Action

**Purpose**: Reduce storage cost by compressing aged data

**DDL Executed**:
```sql
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
COMPRESS FOR [QUERY HIGH | ARCHIVE LOW | ARCHIVE HIGH]
[PCTFREE n]
PARALLEL 4;

-- Rebuild local indexes
ALTER INDEX idx_name REBUILD PARTITION partition_name PARALLEL 4;
```

**Policy Configuration**:
```sql
action_type = 'COMPRESS'
compression_type = 'QUERY HIGH'  -- or 'ARCHIVE LOW', 'ARCHIVE HIGH'
pctfree = 10
age_days = 90
```

**Metrics Captured**:
- `size_before_mb`
- `size_after_mb`
- `space_saved_mb = size_before - size_after`
- `compression_ratio = size_before / size_after`

**Use Cases**:
- Compress partitions older than 90 days
- Reduce storage costs while maintaining query performance
- Typical compression ratios: 2-10x depending on data type

---

### 2. MOVE Action (Storage Tiering)

**Purpose**: Tier data to cheaper storage (HOT → WARM → COLD)

**DDL Executed**:
```sql
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
TABLESPACE target_tablespace
[COMPRESS FOR compression_type]
[PCTFREE n]
PARALLEL 4;

-- Rebuild local indexes in target tablespace
ALTER INDEX idx_name REBUILD PARTITION partition_name
TABLESPACE target_tablespace
PARALLEL 4;
```

**Policy Configuration**:
```sql
action_type = 'MOVE'
target_tablespace = 'TBS_WARM'
compression_type = 'QUERY HIGH'  -- Optional: compress during move
age_months = 3
access_pattern = 'WARM'
```

**Auto-Merge Feature**:

After moving a monthly partition, framework **automatically attempts merge** if:
- Config: `AUTO_MERGE_PARTITIONS = 'Y'`
- Partition matches: `P_YYYY_MM` format (monthly)
- Yearly partition exists: `P_YYYY` in same tablespace
- Partitions are adjacent (Oracle requirement)

**Merge DDL**:
```sql
ALTER TABLE owner.table_name
MERGE PARTITIONS yearly_partition, monthly_partition
INTO PARTITION yearly_partition
UPDATE INDEXES;
```

**Merge Logging**:
```sql
INSERT INTO cmr.dwh_ilm_partition_merges (
    table_owner,
    table_name,
    source_partition,
    target_partition,
    merge_status,    -- SUCCESS, FAILED, SKIPPED
    duration_seconds,
    rows_merged
);
```

**Important**: Merge failure does NOT fail the MOVE operation (logged separately).

---

### 3. READ_ONLY Action

**Purpose**: Make partition immutable (compliance, archival)

**DDL Executed**:
```sql
ALTER TABLE owner.table_name
MODIFY PARTITION partition_name READ ONLY;
```

**Policy Configuration**:
```sql
action_type = 'READ_ONLY'
age_months = 12
access_pattern = 'COLD'
```

**Use Cases**:
- Compliance requirements (prevent data modification)
- Archival partitions
- Data protection

---

### 4. DROP Action

**Purpose**: Purge old data

**DDL Executed**:
```sql
ALTER TABLE owner.table_name
DROP PARTITION partition_name;
```

**Policy Configuration**:
```sql
action_type = 'DROP'
age_months = 84  -- 7 years
priority = 900   -- Execute last
```

**Safety**: Typically assigned lowest priority (900) to ensure compression/tiering completes first.

---

### 5. TRUNCATE Action

**Purpose**: Clear data, preserve partition structure

**DDL Executed**:
```sql
ALTER TABLE owner.table_name
TRUNCATE PARTITION partition_name;
```

**Use Cases**:
- Testing/development environments
- Clear staging partitions after ETL

---

### 6. CUSTOM Action

**Purpose**: User-defined operations

**Execution**:
```sql
EXECUTE IMMEDIATE policy.custom_action;
```

**Example Policy**:
```sql
action_type = 'CUSTOM'
custom_action = '
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => ''SALES'',
        tabname => ''TRANSACTIONS'',
        partname => ''P_2023_01'',
        estimate_percent => 10
    );
END;'
```

---

## 🚨 Edge Cases & Error Handling

### Error Handling Strategy

The framework uses **layered exception handling** with graceful degradation:

#### Layer 1: Individual Action Execution

```sql
PROCEDURE execute_single_action(p_queue_id NUMBER) AS
    v_size_before NUMBER;
    v_size_after NUMBER;
    v_status VARCHAR2(20);
    v_error_code NUMBER;
    v_error_message VARCHAR2(4000);
BEGIN
    -- Get partition size before
    v_size_before := get_partition_size(...);

    BEGIN
        -- Execute action
        CASE action_type
            WHEN 'COMPRESS' THEN compress_partition(...);
            WHEN 'MOVE' THEN move_partition(...);
            ...
        END CASE;

        v_status := 'SUCCESS';
        v_size_after := get_partition_size(...);

    EXCEPTION
        WHEN OTHERS THEN
            v_status := 'FAILED';
            v_error_code := SQLCODE;
            v_error_message := SQLERRM;
            v_size_after := v_size_before;  -- No change
    END;

    -- ALWAYS log result (success or failure)
    INSERT INTO cmr.dwh_ilm_execution_log (...);

    -- Update queue status
    UPDATE cmr.dwh_ilm_evaluation_queue
    SET execution_status = CASE v_status
                              WHEN 'SUCCESS' THEN 'EXECUTED'
                              ELSE 'FAILED' END;

    COMMIT;  -- Commit even on failure
END;
```

**Key Points**:
- ✅ Failures logged to `dwh_ilm_execution_log`
- ✅ COMMIT even on failure (no rollback)
- ✅ Failed items remain in queue for manual review
- ✅ Execution continues to next partition

---

#### Layer 2: Policy-Level Execution

```sql
PROCEDURE execute_policy(p_policy_id NUMBER) AS
BEGIN
    FOR queue_item IN (SELECT * FROM queue WHERE policy_id = p_policy_id) LOOP
        BEGIN
            execute_single_action(queue_item.queue_id);
        EXCEPTION
            WHEN OTHERS THEN
                -- Log error, CONTINUE to next partition
                log_error(queue_item.queue_id, SQLERRM);
        END;
    END LOOP;
END;
```

**Behavior**: One partition failure does NOT stop other partitions in same policy.

---

#### Layer 3: Complete Execution Cycle

```sql
PROCEDURE execute_pending_actions(p_max_operations NUMBER) AS
BEGIN
    -- Check execution window
    IF NOT is_execution_window_open() THEN
        RETURN;
    END IF;

    FOR policy IN (SELECT * FROM policies ORDER BY priority) LOOP
        BEGIN
            execute_policy(policy.policy_id);
        EXCEPTION
            WHEN OTHERS THEN
                -- Log policy-level error
                log_policy_error(policy.policy_id, SQLERRM);
        END;
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        -- Critical error - log to execution log
        INSERT INTO dwh_ilm_execution_log (status, error_message)
        VALUES ('SYSTEM_FAILURE', SQLERRM);
        COMMIT;
END;
```

---

### Edge Case Handling

#### Edge Case 1: Action Already Applied

**Scenario**: Partition already compressed, policy tries to compress again

**Handling**:
```sql
-- In compress_partition()
SELECT compression
INTO v_current_compression
FROM all_tab_partitions
WHERE partition_name = p_partition_name;

IF v_current_compression = 'ENABLED' THEN
    -- Skip compression
    v_status := 'SKIPPED';
    v_error_message := 'Partition already compressed';
    RETURN;
END IF;
```

**Result**: Logged as SKIPPED, not FAILED

---

#### Edge Case 2: Long-Running Operations

**Scenario**: COMPRESS operation takes 3 hours, exceeds 2-hour scheduler interval

**Handling**:
- Scheduler runs every 2 hours
- Each run executes up to `p_max_operations` (default: 10)
- Long-running operation blocks that partition only
- Next scheduler run picks up next partition from queue

**No blocking** - queue-based design prevents interference

---

#### Edge Case 3: Partition Merge Adjacent Requirement

**Scenario**: Oracle requires merging **adjacent** partitions only

**Handling**:
```sql
-- In attempt_partition_merge()
-- Check if partitions are adjacent
SELECT COUNT(*) INTO v_between_count
FROM all_tab_partitions
WHERE table_owner = p_table_owner
AND table_name = p_table_name
AND partition_position BETWEEN
    (SELECT partition_position FROM all_tab_partitions WHERE partition_name = p_yearly_partition)
    AND
    (SELECT partition_position FROM all_tab_partitions WHERE partition_name = p_monthly_partition);

IF v_between_count > 2 THEN
    -- Partitions not adjacent - skip merge
    v_merge_status := 'SKIPPED';
    v_reason := 'Partitions not adjacent';
    RETURN;
END IF;
```

**Result**: Merge skipped, MOVE operation still succeeds

---

#### Edge Case 4: DDL Lock Timeout

**Scenario**: Cannot acquire DDL lock for partition merge

**Handling**:
```sql
-- Set timeout before merge
ALTER SESSION SET DDL_LOCK_TIMEOUT = 30;  -- 30 seconds

BEGIN
    -- Attempt merge
    EXECUTE IMMEDIATE 'ALTER TABLE ... MERGE PARTITIONS ...';
    v_merge_status := 'SUCCESS';
EXCEPTION
    WHEN lock_timeout THEN
        v_merge_status := 'SKIPPED';
        v_reason := 'DDL lock timeout';
END;
```

**Configuration**: `MERGE_LOCK_TIMEOUT` in `dwh_ilm_config` (default: 30 seconds)

---

#### Edge Case 5: Policy Eligibility Constantly True

**Scenario**: Custom condition always evaluates to TRUE

**Handling**:
- Partition remains in queue indefinitely
- Manual intervention required:
  ```sql
  DELETE FROM cmr.dwh_ilm_evaluation_queue
  WHERE policy_id = ...
  AND partition_name = ...;
  ```

**Prevention**: Test custom conditions thoroughly before enabling policy

---

#### Edge Case 6: Missing Threshold Profile

**Scenario**: Policy references non-existent threshold profile

**Handling**:
```sql
-- Fallback to DEFAULT profile
SELECT profile_id INTO v_profile_id
FROM cmr.dwh_ilm_threshold_profiles
WHERE profile_name = 'DEFAULT';

-- If DEFAULT missing, use hardcoded fallback
hot_threshold_days := NVL(v_hot_days, 90);
warm_threshold_days := NVL(v_warm_days, 365);
cold_threshold_days := NVL(v_cold_days, 1095);
```

---

### Failure Recovery Mechanisms

#### 1. No Built-In Retry

**Design Decision**: Failed actions remain in queue as `execution_status = 'FAILED'`

**Rationale**:
- Immediate retry likely fails again
- Manual investigation required
- Prevents infinite retry loops

**Manual Retry**:
```sql
-- Reset failed action for retry
UPDATE cmr.dwh_ilm_evaluation_queue
SET execution_status = 'PENDING',
    execution_date = NULL
WHERE queue_id = ...
AND execution_status = 'FAILED';
```

---

#### 2. Failure Alerting

**Procedure**: `dwh_check_ilm_failures()`

**Logic**:
```sql
-- Count recent failures
SELECT COUNT(*) INTO v_failure_count
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start >= SYSTIMESTAMP - INTERVAL '4' HOUR;

-- Check if exceeds threshold
IF v_failure_count >= v_alert_threshold THEN
    -- Send email alert
    dwh_send_ilm_alert(
        p_subject => 'ILM Failures Detected',
        p_message => v_failure_count || ' failures in last 4 hours'
    );
END IF;
```

**Configuration**:
- `ALERT_FAILURE_THRESHOLD` (default: 3 failures)
- `ALERT_INTERVAL_HOURS` (default: 4 hours)
- `ENABLE_EMAIL_NOTIFICATIONS` ('Y'/'N')

**Prevents alert spam** while maintaining visibility

---

#### 3. Log Retention & Cleanup

**Procedure**: `DWH_ILM_JOB_CLEANUP` (weekly)

**Logic**:
```sql
DELETE FROM cmr.dwh_ilm_execution_log
WHERE execution_start < SYSDATE - v_retention_days;

DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE evaluation_date < SYSDATE - 30
AND execution_status IN ('EXECUTED', 'FAILED');
```

**Configuration**:
- `LOG_RETENTION_DAYS` (default: 365 days)

---

## 📚 Oracle ILM Best Practices (2025)

Based on Oracle's July 2025 Version 23ai documentation and industry best practices:

### 1. Storage Tiering Architecture

**Best Practice**: Define clear storage tiers aligned with data access patterns

**Recommended Tiers**:

| Tier | Storage Type | Data Age | Access Pattern | Cost |
|---|---|---|---|---|
| **HOT** | NVMe SSD / Flash | 0-24 months | Frequent reads/writes | High |
| **WARM** | SSD / SATA | 24-60 months | Occasional reads | Medium |
| **COLD** | HDD / Object Storage | 60+ months | Rare access | Low |
| **ARCHIVE** | Tape / Cloud Glacier | 7+ years | Compliance only | Very Low |

**Implementation**:
```sql
-- HOT tier partitions: No compression, fast storage
-- WARM tier: Query High compression, SSD storage
-- COLD tier: Archive High compression, HDD storage
```

---

### 2. Compression Strategy

**Best Practice**: Use tiered compression aligned with access patterns

**Recommended Approach**:

```sql
-- HOT partitions (0-24 months): NO compression
--   → Maximum query performance
--   → Frequent inserts/updates benefit from no compression overhead

-- WARM partitions (24-60 months): COMPRESS FOR QUERY HIGH
--   → Balance between compression ratio (2-3x) and query performance
--   → Optimal for read-heavy workloads

-- COLD partitions (60+ months): COMPRESS FOR ARCHIVE HIGH
--   → Maximum compression ratio (8-15x)
--   → Acceptable decompression overhead for rare queries
--   → Hybrid Columnar Compression (HCC) if available
```

**Oracle Recommendation**:
> "If an organization doesn't have access to HCC, use only Advanced Row Compression in ADO policies"

---

### 3. Partition-Level ILM

**Best Practice**: Partition is the fundamental unit for ILM

**Oracle Guidance**:
> "Without Partitioning Enterprise Edition, ILM has much less interest as it would be difficult to implement storage tiering within same object"

**Why Partitioning is Essential**:
- Whole tables unlikely to be unaccessed for long periods
- Partitions enable granular lifecycle policies
- DDL operations (MOVE, COMPRESS) on partitions don't lock entire table
- Partition exchange enables zero-downtime data loading

---

### 4. Heat Map Integration

**Best Practice**: Use Oracle Heat Map for real access tracking (Enterprise Edition)

**Heat Map Benefits**:
- Tracks actual segment-level access (reads/writes)
- Automatically populated by database
- No performance overhead
- More accurate than age-based classification

**Integration**:
```sql
-- Sync Heat Map data to ILM access table
EXEC dwh_sync_heatmap_to_tracking();

-- Use in policy evaluation
access_pattern = 'COLD' AND read_count = 0 AND days_since_read > 90
```

---

### 5. Automatic Data Optimization (ADO)

**Oracle's Native ILM**: ADO + Heat Map + Partitioning

**ADO Policies Example**:
```sql
-- Compress partitions after 90 days of no modification
ALTER TABLE sales ILM
ADD POLICY COMPRESS FOR QUERY HIGH
SEGMENT AFTER 90 DAYS OF NO MODIFICATION;

-- Tier to low-cost storage after 12 months of no access
ALTER TABLE sales ILM
ADD POLICY MOVE TABLESPACE low_cost_storage
SEGMENT AFTER 12 MONTHS OF NO ACCESS;
```

**When to Use Custom Framework vs ADO**:

| Feature | Custom Framework (this codebase) | Oracle ADO |
|---|---|---|
| **Cost** | No licensing (Standard Edition) | Requires Enterprise Edition + Advanced Compression |
| **Flexibility** | Fully customizable policies | Fixed ADO policy types |
| **Access Tracking** | Manual or custom | Automatic Heat Map |
| **Execution Control** | Explicit scheduling, batch sizing | Automatic, less control |
| **Audit Trail** | Complete custom logging | DBA_ILMDATAMOVEMENTPOLICIES |
| **Complexity** | Higher (DIY) | Lower (Oracle-managed) |

**Recommendation**: Use custom framework for Standard Edition or when advanced control needed.

---

### 6. Batch Processing & Execution Windows

**Best Practice**: Execute ILM operations during maintenance windows

**Reasons**:
- DDL operations acquire locks (brief but disruptive)
- MOVE/COMPRESS are resource-intensive
- Minimize impact on OLTP workloads

**Recommended Windows**:
- **Weekdays**: 22:00-06:00 (overnight)
- **Weekends**: Extended windows (20:00-08:00)
- **Batch sizes**: 10-20 operations per run (configurable)

**Oracle DBMS_SCHEDULER Best Practices**:
- Use job chains for complex workflows
- Create jobs in batches for performance
- Monitor via `DBA_SCHEDULER_JOB_RUN_DETAILS`
- Keep database jobs inside database (not OS scripts)

---

### 7. Space Threshold Triggers

**Best Practice**: Trigger aggressive tiering when tablespace fills up

**Oracle ADO Feature**:
> "When a tablespace reaches the fullness threshold (85%), database automatically moves coldest table/partition(s) to target tablespace"

**Implementation**:
```sql
-- Monitor tablespace usage
SELECT tablespace_name,
       ROUND(used_percent, 2) AS used_pct
FROM dba_tablespace_usage_metrics
WHERE used_percent > 80;

-- Create emergency tiering policy
INSERT INTO cmr.dwh_ilm_policies (
    policy_name => 'EMERGENCY_TIER_ON_SPACE_PRESSURE',
    priority => 50,  -- High priority
    age_days => 60,  -- Tier anything older than 60 days
    action_type => 'MOVE',
    target_tablespace => 'TBS_WARM',
    enabled => 'N'  -- Enable manually when needed
);
```

---

### 8. Checkpointing & Resumability

**Best Practice**: Implement checkpointing for long-running batch operations

**Why Critical**:
- Database maintenance windows can be interrupted
- Scheduler jobs can fail mid-execution
- Prevents re-processing already completed operations

**Oracle Resumable Operations**:
- Oracle has built-in `RESUMABLE` feature for space allocation
- Applies to DML operations
- Not applicable to DDL (ALTER TABLE MOVE/COMPRESS)

**Custom Checkpointing** (this framework's enhancement):
```sql
-- Save state every N operations
IF operations_count MOD checkpoint_frequency = 0 THEN
    UPDATE execution_state
    SET last_queue_id = current_queue_id,
        operations_completed = operations_count;
    COMMIT;
END IF;

-- Resume from checkpoint
SELECT last_queue_id INTO v_resume_point
FROM execution_state
WHERE status = 'INTERRUPTED';

FOR queue_item IN (
    SELECT * FROM queue
    WHERE queue_id > v_resume_point
) LOOP
    ...
END LOOP;
```

---

## 🎯 Production Requirements & Recommendations

### 1. Monitoring & Observability

**Required Metrics**:
- ✅ Execution success rate (by policy, by day)
- ✅ Space saved (GB per day/week/month)
- ✅ Compression ratios achieved
- ✅ Execution duration trends
- ✅ Failure rate and error patterns
- ✅ Queue backlog size

**Recommended Views**:
```sql
-- Daily execution summary
CREATE VIEW v_ilm_daily_summary AS
SELECT
    TRUNC(execution_start) AS execution_date,
    COUNT(*) AS total_operations,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures,
    ROUND(SUM(space_saved_mb) / 1024, 2) AS space_saved_gb,
    ROUND(AVG(duration_seconds), 1) AS avg_duration_sec
FROM cmr.dwh_ilm_execution_log
GROUP BY TRUNC(execution_start)
ORDER BY execution_date DESC;
```

---

### 2. Alerting Strategy

**Critical Alerts** (Immediate Response):
- ❗ Failure rate > 20% in last 4 hours
- ❗ Execution queue backlog > 1000 items
- ❗ Tablespace > 90% full and ILM policies failing
- ❗ Scheduler jobs disabled unexpectedly

**Warning Alerts** (Next Business Day):
- ⚠️ Execution duration trending upward (>10% increase)
- ⚠️ Compression ratio below expected (< 2x)
- ⚠️ No executions in last 24 hours (scheduler issue?)

**Implementation**:
```sql
CREATE OR REPLACE PROCEDURE check_ilm_health AS
    v_failure_rate NUMBER;
    v_queue_backlog NUMBER;
BEGIN
    -- Calculate failure rate
    SELECT
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    INTO v_failure_rate
    FROM cmr.dwh_ilm_execution_log
    WHERE execution_start >= SYSTIMESTAMP - INTERVAL '4' HOUR;

    IF v_failure_rate > 20 THEN
        send_alert('CRITICAL', 'ILM failure rate ' || v_failure_rate || '%');
    END IF;

    -- Check queue backlog
    SELECT COUNT(*) INTO v_queue_backlog
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING';

    IF v_queue_backlog > 1000 THEN
        send_alert('CRITICAL', 'ILM queue backlog ' || v_queue_backlog || ' items');
    END IF;
END;
```

---

### 3. Capacity Planning

**Metrics to Track**:
- Storage growth rate (GB/day)
- ILM space savings rate (GB/day)
- Net storage growth after ILM
- Tier distribution (% HOT/WARM/COLD)

**Projection Model**:
```sql
WITH monthly_trends AS (
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
    AVG(space_saved_gb) OVER (ORDER BY month ROWS 3 PRECEDING) AS moving_avg_3m
FROM monthly_trends
ORDER BY month DESC;
```

---

### 4. Performance Tuning

**Parallel Execution**:
```sql
-- Current: ALTER TABLE ... MOVE PARTITION ... PARALLEL 4
-- Tune based on CPU cores and workload
ALTER SESSION SET PARALLEL_DEGREE_POLICY = AUTO;
```

**Batch Size Tuning**:
- Too small (< 5): Frequent scheduler overhead
- Too large (> 50): Reduces resumability granularity
- **Recommended**: 10-20 operations per batch

**Checkpoint Frequency**:
- Too frequent (every 1-2 ops): Commit overhead
- Too infrequent (every 50 ops): Risk losing progress
- **Recommended**: Every 5-10 operations

---

### 5. Disaster Recovery

**Backup Considerations**:
- Execution state tables should be backed up
- Queue can be rebuilt from policy evaluation
- Execution log is audit trail (critical to preserve)

**Recovery Scenarios**:

**Scenario 1: Scheduler Jobs Disabled**
```sql
-- Check job status
SELECT job_name, enabled, state
FROM dba_scheduler_jobs
WHERE job_name LIKE 'DWH_ILM%';

-- Re-enable if needed
EXEC dwh_start_ilm_jobs();
```

**Scenario 2: Corrupted Execution State**
```sql
-- Clear stuck executions
UPDATE cmr.dwh_ilm_execution_state
SET status = 'FAILED',
    end_time = SYSTIMESTAMP
WHERE status = 'RUNNING'
AND start_time < SYSTIMESTAMP - INTERVAL '6' HOUR;
```

**Scenario 3: Mass Re-evaluation Needed**
```sql
-- Clear queue and re-evaluate
TRUNCATE TABLE cmr.dwh_ilm_evaluation_queue;
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();
```

---

### 6. Security & Compliance

**Access Control**:
```sql
-- Grant execute on packages to DBA role only
GRANT EXECUTE ON pck_dwh_ilm_policy_engine TO dba_role;
GRANT EXECUTE ON pck_dwh_ilm_execution_engine TO dba_role;

-- Application users: read-only access to views
GRANT SELECT ON v_dwh_ilm_active_batches TO app_readonly_role;
```

**Audit Requirements**:
- All ILM actions logged with timestamps
- Execution log retained per compliance requirements
- User actions (manual policy changes) audited

**Retention Policies**:
```sql
-- Example: Regulatory requirement to keep 7 years
UPDATE cmr.dwh_ilm_config
SET config_value = '2555'  -- 7 years in days
WHERE config_key = 'LOG_RETENTION_DAYS';
```

---

## 📋 Critical Edge Cases for Scheduler Enhancement

Based on the analysis, the scheduler enhancement MUST address these edge cases:

### 1. Interrupted Batch Resumption

**Current Risk**: If scheduler job fails mid-execution, all progress lost

**Requirement**:
- Save checkpoint every N operations
- Automatically detect interrupted batches on next run
- Resume from last checkpoint, not start over

**Test Scenarios**:
- Database bounce during execution
- Scheduler job killed by admin
- Session timeout

---

### 2. Weekend vs Weekday Behavior

**Current Risk**: Fixed 2-hour interval runs 24/7, no day differentiation

**Requirement**:
- Different intervals for weekdays (2h) vs weekends (4h)
- Different execution windows (weekday: 22-6, weekend: 20-8)
- Ability to disable weekends entirely

**Test Scenarios**:
- Transition from Friday night to Saturday morning
- Holiday schedules (treat as weekends)

---

### 3. Queue Backlog Management

**Current Risk**: If queue grows faster than execution capacity, backlog grows indefinitely

**Requirement**:
- Monitor queue growth rate vs execution rate
- Alert when backlog exceeds threshold
- Allow "emergency" runs with higher batch sizes
- Priority queue: high-priority policies execute first always

**Test Scenarios**:
- Bulk evaluation adds 500 partitions to queue
- Scheduler disabled for 1 week, then re-enabled

---

### 4. Concurrent Execution Prevention

**Current Risk**: If job runs longer than interval (e.g., 3-hour job on 2-hour interval), concurrent executions

**Requirement**:
- Check for running batch before starting new batch
- Skip execution if previous batch still running
- Log skip reason

**Test Scenarios**:
- Long-running COMPRESS operation
- Multiple scheduler jobs triggered simultaneously

---

### 5. Max Duration Enforcement

**Current Risk**: No hard stop if execution window closing

**Requirement**:
- Enforce `max_duration_minutes` (e.g., 120 min)
- Stop gracefully at checkpoint before window closes
- Resume in next window

**Test Scenarios**:
- Execution started at 05:30, window closes at 06:00
- Max duration reached with 100 operations still pending

---

### 6. Policy Priority Starvation

**Current Risk**: Low-priority policies may never execute if high-priority queue never empties

**Requirement**:
- Guarantee some operations for lower-priority policies
- Optional: Age-based priority boost (waiting > 7 days → priority++)
- Optional: Round-robin policy execution mode

**Test Scenarios**:
- Priority 100 policy has 10,000 partitions
- Priority 900 policy waiting for 30 days

---

### 7. Partial Batch Completion

**Current Risk**: If batch completes only 5/10 operations before max_duration, unclear if "success"

**Requirement**:
- Distinguish between:
  - **COMPLETED**: All pending operations executed
  - **INTERRUPTED**: Stopped due to time/duration limits, more pending
  - **FAILED**: Critical error prevented execution
- Resume INTERRUPTED batches automatically

---

### 8. Timezone Handling

**Current Risk**: Execution window defined in database server timezone

**Requirement**:
- Document timezone behavior clearly
- Optional: Support explicit timezone in schedule config
- Handle daylight saving time transitions

**Test Scenarios**:
- DST spring forward (2 AM → 3 AM)
- DST fall back (2 AM → 1 AM)

---

## ✅ Summary & Recommendations

### Architecture Strengths

✅ **Well-designed 3-stage pipeline** (refresh → evaluate → execute)
✅ **Multi-level prioritization** (policy priority + queue order + concurrency)
✅ **Comprehensive error handling** (layered exceptions, graceful degradation)
✅ **Complete audit trail** (execution log, partition merges)
✅ **Flexible policy framework** (age, size, temperature, custom conditions)
✅ **Production-ready** (logging, alerting, failure monitoring)

### Current Limitations

❌ **No batch resumability** - Progress lost on interruption
❌ **No day-of-week scheduling** - Runs 24/7 regardless
❌ **Fixed intervals** - Same 2-hour interval always
❌ **No backlog management** - Queue can grow unbounded
❌ **Limited observability** - No batch-level tracking

### Scheduler Enhancement Priorities

1. **MUST HAVE** (Production Blockers):
   - ✅ Batch checkpointing & resumability
   - ✅ Day-of-week scheduling
   - ✅ Variable hour intervals (weekday vs weekend)
   - ✅ Concurrent execution prevention
   - ✅ Max duration enforcement

2. **SHOULD HAVE** (Production Best Practices):
   - ✅ Queue backlog monitoring & alerts
   - ✅ Batch-level execution tracking
   - ✅ Priority starvation prevention
   - ✅ Emergency/force run capability

3. **NICE TO HAVE** (Future Enhancements):
   - 🔄 Holiday calendar support
   - 🔄 Dynamic batch sizing based on workload
   - 🔄 Partition-level dependency tracking
   - 🔄 Predictive capacity planning

---

**Next Steps**: Enrich `SCHEDULER_ENHANCEMENT_DESIGN.md` with these findings and production requirements.
