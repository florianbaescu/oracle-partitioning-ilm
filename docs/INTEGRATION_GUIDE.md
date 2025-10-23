# ILM Framework Integration Guide

**Version:** 1.0
**Date:** 2025-10-23
**Target Audience:** DBAs, ETL Developers, Application Architects, Backup Administrators

---

## Table of Contents

1. [Introduction](#introduction)
2. [ETL Integration](#etl-integration)
3. [Application Hooks](#application-hooks)
4. [Backup Coordination](#backup-coordination)
5. [Monitoring System Integration](#monitoring-system-integration)
6. [Security and Compliance](#security-and-compliance)
7. [Troubleshooting Integration Issues](#troubleshooting-integration-issues)
8. [Reference Architectures](#reference-architectures)

---

## Introduction

### Purpose

This guide provides comprehensive patterns and best practices for integrating the Oracle Custom ILM Framework with existing data warehouse infrastructure, including:

- **ETL Processes** - Coordinating data loads with ILM operations
- **Applications** - Handling partition state changes in application logic
- **Backup Systems** - Optimizing backups with ILM tiers
- **Monitoring Tools** - Integrating ILM metrics with existing monitoring
- **Compliance Systems** - Meeting audit and retention requirements

### Integration Challenges

Common challenges when deploying ILM in existing environments:

| Challenge | Impact | Solution Section |
|-----------|--------|------------------|
| ILM compresses partitions during ETL load | Load failures, performance degradation | [ETL Integration](#etl-integration) |
| Queries slow after compression | Poor user experience, SLA violations | [Application Hooks](#application-hooks) |
| RMAN backups take forever after compression | Backup window overruns, storage costs | [Backup Coordination](#backup-coordination) |
| ILM operations conflict with maintenance windows | Failed operations, manual intervention | [ETL Integration - Scheduling](#scheduling-coordination) |
| No visibility into ILM impact on applications | Silent failures, production issues | [Monitoring Integration](#monitoring-system-integration) |

### Prerequisites

Before implementing integration patterns:

- [ ] ILM Framework installed and tested
- [ ] Baseline operational metrics established (query performance, backup times, ETL duration)
- [ ] Understanding of current ETL schedules and maintenance windows
- [ ] Documentation of application query patterns
- [ ] Current backup strategy documented

---

## ETL Integration

### Overview

ETL processes and ILM operations can conflict if not properly coordinated. This section provides patterns to safely integrate ILM with data loading workflows.

### Scheduling Coordination

#### Problem: Overlapping Operations

**Scenario:** Nightly ETL loads data from 10 PM to 6 AM. ILM compression runs at 2 AM and locks partitions, causing ETL failures.

**Solution: Configure Non-Overlapping Execution Windows**

```sql
-- Set ILM execution window to run AFTER ETL completes
UPDATE cmr.dwh_ilm_config
SET config_value = '06:00'  -- Start after ETL ends
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config
SET config_value = '08:00'  -- End before business hours
WHERE config_key = 'EXECUTION_WINDOW_END';

COMMIT;

-- Verify configuration
SELECT config_key, config_value, description
FROM cmr.dwh_ilm_config
WHERE config_key LIKE '%WINDOW%';
```

**Timeline Example:**

```
22:00 - 06:00  ETL Load Window
06:00 - 08:00  ILM Execution Window
08:00 - 09:00  RMAN Backup Window
09:00+         Business Hours (reports, queries)
```

#### Advanced: Dynamic Window Adjustment

For environments where ETL duration varies:

```sql
-- Procedure to adjust ILM window based on ETL completion
CREATE OR REPLACE PROCEDURE adjust_ilm_window AS
    v_etl_end_time VARCHAR2(5);
    v_etl_running NUMBER;
BEGIN
    -- Check if ETL is still running
    SELECT COUNT(*) INTO v_etl_running
    FROM v$session
    WHERE module LIKE '%ETL%'
    OR program LIKE '%informatica%'
    OR program LIKE '%ODI%';

    -- If ETL completed, allow ILM to start
    IF v_etl_running = 0 THEN
        -- Get current time
        SELECT TO_CHAR(SYSDATE, 'HH24:MI') INTO v_etl_end_time FROM DUAL;

        -- Update ILM window to start now
        UPDATE cmr.dwh_ilm_config
        SET config_value = v_etl_end_time
        WHERE config_key = 'EXECUTION_WINDOW_START';

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('ILM window adjusted to start at: ' || v_etl_end_time);
    ELSE
        DBMS_OUTPUT.PUT_LINE('ETL still running. ILM window not adjusted.');
    END IF;
END;
/

-- Schedule to check every 30 minutes during ETL window
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'CHECK_ETL_COMPLETION',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN adjust_ilm_window(); END;',
        start_date => TRUNC(SYSDATE) + 22/24,  -- 10 PM
        repeat_interval => 'FREQ=MINUTELY; INTERVAL=30; BYHOUR=22,23,0,1,2,3,4,5,6',
        enabled => TRUE,
        comments => 'Check if ETL completed to start ILM'
    );
END;
/
```

### Partition Locking Detection

#### Problem: ETL Fails Due to Locked Partition

**Solution: Check for Locks Before ILM Operations**

```sql
-- Function to check if partition is locked
CREATE OR REPLACE FUNCTION is_partition_locked(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN BOOLEAN AS
    v_locked_count NUMBER;
BEGIN
    -- Check for locks on partition
    SELECT COUNT(*) INTO v_locked_count
    FROM v$locked_object lo
    JOIN dba_objects o ON o.object_id = lo.object_id
    WHERE o.owner = p_table_owner
    AND o.object_name = p_table_name
    AND o.subobject_name = p_partition_name;

    RETURN (v_locked_count > 0);
END;
/

-- Enhanced policy engine check (add to pck_dwh_ilm_policy_engine)
CREATE OR REPLACE FUNCTION can_execute_on_partition(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN BOOLEAN AS
    v_etl_sessions NUMBER;
    v_is_locked BOOLEAN;
BEGIN
    -- Check 1: Is partition locked?
    v_is_locked := is_partition_locked(p_table_owner, p_table_name, p_partition_name);
    IF v_is_locked THEN
        RETURN FALSE;
    END IF;

    -- Check 2: Are there active ETL sessions on this table?
    SELECT COUNT(*) INTO v_etl_sessions
    FROM v$session s
    JOIN v$sql sq ON s.sql_id = sq.sql_id
    WHERE sq.sql_text LIKE '%' || p_table_name || '%'
    AND s.module LIKE '%ETL%'
    AND s.status = 'ACTIVE';

    IF v_etl_sessions > 0 THEN
        RETURN FALSE;
    END IF;

    -- Check 3: Is partition being actively queried?
    SELECT COUNT(*) INTO v_etl_sessions
    FROM v$sql_plan p
    WHERE p.object_name = p_table_name
    AND p.partition_start IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM v$session s
        WHERE s.sql_id = p.sql_id
        AND s.status = 'ACTIVE'
    );

    RETURN (v_etl_sessions = 0);
END;
/
```

### ETL Load Patterns

#### Pattern 1: Daily Partition Append

**Scenario:** ETL loads daily data into yesterday's partition. Compress after load completes.

```sql
-- Create post-ETL trigger policy
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    age_days,  -- Compress partitions 1 day old
    compression_type,
    priority, enabled
) VALUES (
    'SALES_COMPRESS_DAILY_POST_ETL', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS',
    1,  -- Yesterday's partition
    'QUERY HIGH',
    100, 'Y'
);
COMMIT;

-- ETL completion script calls ILM
-- Add to end of ETL job:
/*
-- After successful ETL load
EXEC pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT');
EXEC pck_dwh_ilm_execution_engine.execute_table(USER, 'SALES_FACT', p_max_operations => 1);
*/
```

**Timeline:**
```
Day 1:
  - 23:00: ETL loads data for 2024-01-15 into P_2024_01_15
  - 05:00: ETL completes successfully
  - 05:05: ILM compresses P_2024_01_14 (yesterday)

Day 2:
  - 23:00: ETL loads data for 2024-01-16 into P_2024_01_16
  - 05:00: ETL completes successfully
  - 05:05: ILM compresses P_2024_01_15
```

#### Pattern 2: Bulk Historical Load

**Scenario:** Loading 3 years of historical data. Compress in batches during load.

```sql
-- Disable ILM during bulk load
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- ETL Script:
-- 1. Load batch of partitions (e.g., 1 month)
-- 2. Compress loaded partitions
-- 3. Continue to next batch

-- Example ETL integration:
DECLARE
    v_month DATE := DATE '2021-01-01';
    v_end_date DATE := DATE '2024-01-01';
BEGIN
    WHILE v_month < v_end_date LOOP
        -- Load month of data
        DBMS_OUTPUT.PUT_LINE('Loading: ' || TO_CHAR(v_month, 'YYYY-MM'));
        -- ... your ETL logic here ...

        -- Compress loaded partition immediately
        DECLARE
            v_partition_name VARCHAR2(30);
        BEGIN
            v_partition_name := 'P_' || TO_CHAR(v_month, 'YYYY_MM');

            pck_dwh_ilm_execution_engine.compress_partition(
                p_table_owner => USER,
                p_table_name => 'SALES_FACT',
                p_partition_name => v_partition_name,
                p_compression_type => 'QUERY HIGH',
                p_rebuild_indexes => TRUE,
                p_gather_stats => TRUE
            );

            DBMS_OUTPUT.PUT_LINE('Compressed: ' || v_partition_name);
        END;

        -- Next month
        v_month := ADD_MONTHS(v_month, 1);
    END LOOP;
END;
/

-- Re-enable ILM after bulk load
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;
```

#### Pattern 3: Partition Exchange Load

**Scenario:** Using partition exchange for fast staging-to-production loads.

```sql
-- Staging table (no ILM policies - short-lived)
CREATE TABLE sales_staging (
    sale_id NUMBER(18) NOT NULL,
    sale_date DATE NOT NULL,
    -- ... other columns ...
    CONSTRAINT pk_sales_staging PRIMARY KEY (sale_id)
)
-- No partitioning, no compression for staging
TABLESPACE tbs_staging;

-- Production fact table (with ILM policies)
CREATE TABLE sales_fact (
    sale_id NUMBER(18) NOT NULL,
    sale_date DATE NOT NULL,
    -- ... other columns ...
    CONSTRAINT pk_sales_fact PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(PARTITION p_2020_01 VALUES LESS THAN (DATE '2020-02-01'))
COMPRESS FOR QUERY HIGH
ENABLE ROW MOVEMENT;

-- ETL Process:
-- 1. Load staging table (fast inserts)
INSERT INTO sales_staging SELECT * FROM external_source;
COMMIT;

-- 2. Create matching staging partition structure
CREATE TABLE sales_staging_partitioned
PARTITION BY RANGE (sale_date)
(PARTITION p_load VALUES LESS THAN (DATE '2024-02-01'))
AS SELECT * FROM sales_staging;

-- 3. Exchange partition (instant)
ALTER TABLE sales_fact
EXCHANGE PARTITION p_2024_01
WITH TABLE sales_staging_partitioned
INCLUDING INDEXES
WITHOUT VALIDATION;

-- 4. Clean up staging
TRUNCATE TABLE sales_staging;
DROP TABLE sales_staging_partitioned;

-- 5. ILM will compress this partition based on age policies (no immediate action needed)
```

### Post-ETL Workflows

#### Triggering ILM After ETL Completion

**Option 1: Direct Call from ETL**

```sql
-- Add to end of ETL script
BEGIN
    -- Evaluate and execute ILM for tables loaded today
    pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT');
    pck_dwh_ilm_execution_engine.execute_table(
        p_table_owner => USER,
        p_table_name => 'SALES_FACT',
        p_max_operations => 5  -- Limit operations
    );

    DBMS_OUTPUT.PUT_LINE('ILM operations completed');
EXCEPTION
    WHEN OTHERS THEN
        -- Log error but don't fail ETL
        DBMS_OUTPUT.PUT_LINE('ILM error (non-critical): ' || SQLERRM);
END;
/
```

**Option 2: Oracle Scheduler Chain**

```sql
-- Create chain for ETL -> ILM workflow
BEGIN
    -- Create programs
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ETL_LOAD_PROGRAM',
        program_type => 'PLSQL_BLOCK',
        program_action => 'BEGIN your_etl_procedure(); END;',
        enabled => TRUE
    );

    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_COMPRESS_PROGRAM',
        program_type => 'PLSQL_BLOCK',
        program_action => 'BEGIN dwh_run_ilm_cycle(); END;',
        enabled => TRUE
    );

    -- Create chain
    DBMS_SCHEDULER.CREATE_CHAIN(
        chain_name => 'ETL_TO_ILM_CHAIN'
    );

    -- Define chain steps
    DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
        chain_name => 'ETL_TO_ILM_CHAIN',
        step_name => 'STEP_ETL_LOAD',
        program_name => 'ETL_LOAD_PROGRAM'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
        chain_name => 'ETL_TO_ILM_CHAIN',
        step_name => 'STEP_ILM_COMPRESS',
        program_name => 'ILM_COMPRESS_PROGRAM'
    );

    -- Define chain rules (ILM runs only if ETL succeeds)
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'ETL_TO_ILM_CHAIN',
        condition => 'TRUE',
        action => 'START STEP_ETL_LOAD',
        rule_name => 'START_ETL'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'ETL_TO_ILM_CHAIN',
        condition => 'STEP_ETL_LOAD SUCCEEDED',
        action => 'START STEP_ILM_COMPRESS',
        rule_name => 'ETL_SUCCESS_START_ILM'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'ETL_TO_ILM_CHAIN',
        condition => 'STEP_ILM_COMPRESS SUCCEEDED',
        action => 'END',
        rule_name => 'ILM_DONE'
    );

    -- If ETL fails, skip ILM
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'ETL_TO_ILM_CHAIN',
        condition => 'STEP_ETL_LOAD FAILED',
        action => 'END',
        rule_name => 'ETL_FAILED_SKIP_ILM'
    );

    -- Enable chain
    DBMS_SCHEDULER.ENABLE('ETL_TO_ILM_CHAIN');

    -- Create job to run chain daily
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'NIGHTLY_ETL_ILM_JOB',
        job_type => 'CHAIN',
        job_action => 'ETL_TO_ILM_CHAIN',
        start_date => TRUNC(SYSDATE) + 22/24,  -- 10 PM
        repeat_interval => 'FREQ=DAILY; BYHOUR=22',
        enabled => TRUE
    );
END;
/
```

**Option 3: Event-Based Trigger**

```sql
-- Create Advanced Queue for ETL completion events
BEGIN
    DBMS_AQADM.CREATE_QUEUE_TABLE(
        queue_table => 'ETL_EVENT_QUEUE_TAB',
        queue_payload_type => 'SYS.AQ$_JMS_TEXT_MESSAGE'
    );

    DBMS_AQADM.CREATE_QUEUE(
        queue_name => 'ETL_COMPLETION_QUEUE',
        queue_table => 'ETL_EVENT_QUEUE_TAB'
    );

    DBMS_AQADM.START_QUEUE(
        queue_name => 'ETL_COMPLETION_QUEUE'
    );
END;
/

-- ETL publishes completion event
CREATE OR REPLACE PROCEDURE notify_etl_complete(
    p_table_name VARCHAR2,
    p_rows_loaded NUMBER
) AS
    v_enqueue_options DBMS_AQ.ENQUEUE_OPTIONS_T;
    v_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
    v_message SYS.AQ$_JMS_TEXT_MESSAGE;
    v_msgid RAW(16);
BEGIN
    v_message := SYS.AQ$_JMS_TEXT_MESSAGE.CONSTRUCT;
    v_message.SET_TEXT('ETL_COMPLETE:' || p_table_name || ':' || p_rows_loaded);

    DBMS_AQ.ENQUEUE(
        queue_name => 'ETL_COMPLETION_QUEUE',
        enqueue_options => v_enqueue_options,
        message_properties => v_message_properties,
        payload => v_message,
        msgid => v_msgid
    );
    COMMIT;
END;
/

-- ILM listens for events and triggers compression
-- (Implementation depends on your messaging infrastructure)
```

### ETL Tool-Specific Integration

#### Informatica Integration

```sql
-- Add post-session command in Informatica workflow:
-- Command: sqlplus user/pass@db @run_ilm_for_table.sql TABLE_NAME

-- run_ilm_for_table.sql script:
SET SERVEROUTPUT ON
DECLARE
    v_table_name VARCHAR2(128) := '&1';  -- Parameter from command line
BEGIN
    DBMS_OUTPUT.PUT_LINE('Running ILM for table: ' || v_table_name);

    pck_dwh_ilm_policy_engine.evaluate_table(USER, v_table_name);
    pck_dwh_ilm_execution_engine.execute_table(
        p_table_owner => USER,
        p_table_name => v_table_name,
        p_max_operations => 10
    );

    DBMS_OUTPUT.PUT_LINE('ILM completed successfully');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        -- Exit with error code to fail Informatica post-session command if needed
        -- RAISE;
END;
/
EXIT;
```

#### Oracle Data Integrator (ODI) Integration

```sql
-- ODI Procedure: ILM_COMPRESS_AFTER_LOAD

-- Step 1: Get table name from ODI context
-- #TABLE_NAME is ODI variable

-- Step 2: Execute ILM via ODI procedure
BEGIN
    pck_dwh_ilm_policy_engine.evaluate_table(
        p_table_owner => '<?= odiRef.getSchemaName("WORK_SCHEMA") ?>',
        p_table_name => '#TABLE_NAME'
    );

    pck_dwh_ilm_execution_engine.execute_table(
        p_table_owner => '<?= odiRef.getSchemaName("WORK_SCHEMA") ?>',
        p_table_name => '#TABLE_NAME',
        p_max_operations => 5
    );
END;

-- Add ILM_COMPRESS_AFTER_LOAD procedure to ODI package after load step
```

---

## Application Hooks

### Overview

Applications need to adapt to partition state changes (compression, tiering, read-only status). This section provides patterns for application-ILM integration.

### Pre/Post Operation Hooks

#### Custom Hook Framework

```sql
-- Hook registry table
CREATE TABLE cmr.dwh_ilm_hooks (
    hook_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hook_name VARCHAR2(100) NOT NULL UNIQUE,
    hook_type VARCHAR2(20) NOT NULL,  -- PRE_COMPRESS, POST_COMPRESS, PRE_MOVE, POST_MOVE, etc.
    table_pattern VARCHAR2(128),      -- NULL = all tables, or specific table pattern
    hook_procedure VARCHAR2(200) NOT NULL,  -- Procedure to call
    enabled CHAR(1) DEFAULT 'Y',
    priority NUMBER DEFAULT 100,      -- Execution order
    created_date TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Register hooks
INSERT INTO cmr.dwh_ilm_hooks (hook_name, hook_type, table_pattern, hook_procedure, priority)
VALUES ('INVALIDATE_APP_CACHE', 'POST_COMPRESS', 'SALES_FACT', 'app_cache.invalidate_partition', 100);

INSERT INTO cmr.dwh_ilm_hooks (hook_name, hook_type, table_pattern, hook_procedure, priority)
VALUES ('NOTIFY_REPORTING_SYSTEM', 'POST_MOVE', '%_FACT', 'reporting.notify_partition_moved', 200);

COMMIT;

-- Hook execution procedure
CREATE OR REPLACE PROCEDURE execute_ilm_hooks(
    p_hook_type VARCHAR2,
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2,
    p_action_details VARCHAR2 DEFAULT NULL
) AS
    v_hook_sql VARCHAR2(4000);
    v_error_count NUMBER := 0;
BEGIN
    -- Execute all matching hooks in priority order
    FOR hook IN (
        SELECT hook_name, hook_procedure, priority
        FROM cmr.dwh_ilm_hooks
        WHERE hook_type = p_hook_type
        AND enabled = 'Y'
        AND (table_pattern IS NULL
             OR p_table_name LIKE table_pattern)
        ORDER BY priority, hook_id
    ) LOOP
        BEGIN
            -- Build dynamic SQL to call hook procedure
            v_hook_sql := 'BEGIN ' || hook.hook_procedure || '(' ||
                         '''' || p_table_owner || ''', ' ||
                         '''' || p_table_name || ''', ' ||
                         '''' || p_partition_name || ''', ' ||
                         '''' || NVL(p_action_details, '') || '''' ||
                         '); END;';

            EXECUTE IMMEDIATE v_hook_sql;

            DBMS_OUTPUT.PUT_LINE('Executed hook: ' || hook.hook_name);

        EXCEPTION
            WHEN OTHERS THEN
                -- Log error but continue with other hooks
                DBMS_OUTPUT.PUT_LINE('Hook error [' || hook.hook_name || ']: ' || SQLERRM);
                v_error_count := v_error_count + 1;

                -- Log to execution log
                INSERT INTO cmr.dwh_ilm_execution_log (
                    policy_id, policy_name, table_owner, table_name, partition_name,
                    execution_start, status, error_message
                ) VALUES (
                    NULL, 'HOOK:' || hook.hook_name, p_table_owner, p_table_name, p_partition_name,
                    SYSTIMESTAMP, 'FAILED', SUBSTR(SQLERRM, 1, 4000)
                );
                COMMIT;
        END;
    END LOOP;

    IF v_error_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('WARNING: ' || v_error_count || ' hook(s) failed');
    END IF;
END;
/

-- Integrate hooks into execution engine
-- Modify pck_dwh_ilm_execution_engine.compress_partition:
/*
-- Add before compression:
execute_ilm_hooks('PRE_COMPRESS', p_table_owner, p_table_name, p_partition_name);

-- Add after compression:
execute_ilm_hooks('POST_COMPRESS', p_table_owner, p_table_name, p_partition_name,
                  'compression_type=' || p_compression_type);
*/
```

#### Example Hook: Application Cache Invalidation

```sql
-- Application cache invalidation hook
CREATE OR REPLACE PACKAGE app_cache AS
    PROCEDURE invalidate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_details VARCHAR2
    );
END app_cache;
/

CREATE OR REPLACE PACKAGE BODY app_cache AS
    PROCEDURE invalidate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_details VARCHAR2
    ) AS
        v_cache_key VARCHAR2(200);
        v_http_response CLOB;
    BEGIN
        -- Build cache key
        v_cache_key := p_table_owner || '.' || p_table_name || ':' || p_partition_name;

        DBMS_OUTPUT.PUT_LINE('Invalidating cache for: ' || v_cache_key);

        -- Option 1: Call HTTP endpoint to invalidate cache
        BEGIN
            v_http_response := UTL_HTTP.REQUEST(
                'http://app-server:8080/cache/invalidate?key=' ||
                UTL_URL.ESCAPE(v_cache_key)
            );
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('HTTP call failed: ' || SQLERRM);
        END;

        -- Option 2: Update database flag that application checks
        UPDATE app_cache_registry
        SET cache_invalid = 'Y',
            invalidated_date = SYSTIMESTAMP,
            invalidated_reason = 'ILM_OPERATION'
        WHERE table_name = p_table_name
        AND partition_name = p_partition_name;

        COMMIT;

        -- Option 3: Send message to queue for async processing
        -- (Message queue implementation here)

    END invalidate_partition;
END app_cache;
/
```

#### Example Hook: Notify Reporting System

```sql
-- Hook to notify reporting system when partitions move to cold storage
CREATE OR REPLACE PACKAGE reporting AS
    PROCEDURE notify_partition_moved(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_details VARCHAR2
    );
END reporting;
/

CREATE OR REPLACE PACKAGE BODY reporting AS
    PROCEDURE notify_partition_moved(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_details VARCHAR2
    ) AS
        v_email_body CLOB;
    BEGIN
        -- Extract target tablespace from details
        -- details format: "target_tablespace=TBS_COLD,compression_type=ARCHIVE HIGH"

        v_email_body := 'Partition moved to cold storage:' || CHR(10) ||
                       CHR(10) ||
                       'Table: ' || p_table_owner || '.' || p_table_name || CHR(10) ||
                       'Partition: ' || p_partition_name || CHR(10) ||
                       'Details: ' || p_details || CHR(10) ||
                       CHR(10) ||
                       'Action Required:' || CHR(10) ||
                       '- Update reporting queries to expect slower response times' || CHR(10) ||
                       '- Consider caching results for this time period' || CHR(10) ||
                       '- Review query execution plans';

        -- Send email notification
        UTL_MAIL.SEND(
            sender => 'oracle-ilm@company.com',
            recipients => 'reporting-team@company.com',
            subject => '[ILM] Partition moved to cold storage: ' || p_table_name,
            message => v_email_body
        );

        DBMS_OUTPUT.PUT_LINE('Notification sent to reporting team');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Failed to send notification: ' || SQLERRM);
    END notify_partition_moved;
END reporting;
/
```

### Query Routing Based on Partition Temperature

#### Pattern: Different Query Strategies for Hot vs Cold Data

```sql
-- Application query routing logic
CREATE OR REPLACE FUNCTION get_query_strategy(
    p_table_name VARCHAR2,
    p_date_from DATE,
    p_date_to DATE
) RETURN VARCHAR2 AS
    v_hot_partitions NUMBER := 0;
    v_cold_partitions NUMBER := 0;
    v_strategy VARCHAR2(20);
BEGIN
    -- Count partition temperatures for date range
    SELECT
        SUM(CASE WHEN temperature = 'HOT' THEN 1 ELSE 0 END),
        SUM(CASE WHEN temperature IN ('COLD', 'WARM') THEN 1 ELSE 0 END)
    INTO v_hot_partitions, v_cold_partitions
    FROM cmr.dwh_ilm_partition_access
    WHERE table_name = p_table_name
    AND partition_name IN (
        SELECT partition_name
        FROM user_tab_partitions
        WHERE table_name = p_table_name
        -- Partitions that overlap with date range
    );

    -- Determine strategy
    IF v_cold_partitions = 0 THEN
        v_strategy := 'FAST';  -- All hot data, use standard query
    ELSIF v_hot_partitions = 0 THEN
        v_strategy := 'CACHED';  -- All cold data, use cached/pre-aggregated results
    ELSE
        v_strategy := 'HYBRID';  -- Mix, warn user about potential slowness
    END IF;

    RETURN v_strategy;
END;
/

-- Application usage example (pseudo-code in application):
/*
strategy = get_query_strategy('SALES_FACT', date_from, date_to)

if strategy == 'FAST':
    // Run query directly on table
    execute_query(sql)

elif strategy == 'CACHED':
    // Check if cached result exists
    cached_result = get_from_cache(query_key)
    if cached_result:
        return cached_result
    else:
        result = execute_query(sql)
        store_in_cache(query_key, result, ttl=86400)  # Cache for 24 hours
        return result

elif strategy == 'HYBRID':
    // Warn user about slower query
    show_warning("Query includes historical data and may be slower")
    result = execute_query(sql)
    return result
*/
```

#### Pattern: Partition Pruning Hints for Compressed Data

```sql
-- Application generates SQL with hints based on compression status
CREATE OR REPLACE FUNCTION generate_query_with_hints(
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN VARCHAR2 AS
    v_compression VARCHAR2(30);
    v_sql VARCHAR2(4000);
BEGIN
    -- Get partition compression type
    SELECT compress_for INTO v_compression
    FROM user_tab_partitions
    WHERE table_name = p_table_name
    AND partition_name = p_partition_name;

    -- Build query with appropriate hints
    v_sql := 'SELECT /*+ ';

    IF v_compression LIKE '%ARCHIVE%' THEN
        -- Heavy compression, optimize for decompression
        v_sql := v_sql || 'PARALLEL(4) FULL(' || p_table_name || ') ';
    ELSIF v_compression LIKE '%QUERY%' THEN
        -- Query-optimized compression, standard hints
        v_sql := v_sql || 'PARALLEL(2) INDEX_FFS(' || p_table_name || ') ';
    ELSE
        -- No compression or OLTP
        v_sql := v_sql || 'INDEX(' || p_table_name || ') ';
    END IF;

    v_sql := v_sql || '*/ * FROM ' || p_table_name ||
             ' PARTITION (' || p_partition_name || ')';

    RETURN v_sql;
END;
/
```

### Read-Only Partition Handling

#### Problem: Application Attempts to Update Archived Partitions

**Solution: Detect and Handle Read-Only Partitions**

```sql
-- Function to check if partition is read-only
CREATE OR REPLACE FUNCTION is_partition_readonly(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN BOOLEAN AS
    v_readonly VARCHAR2(3);
BEGIN
    SELECT read_only INTO v_readonly
    FROM all_tab_partitions
    WHERE table_owner = p_table_owner
    AND table_name = p_table_name
    AND partition_name = p_partition_name;

    RETURN (v_readonly = 'YES');
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN FALSE;
END;
/

-- Application check before DML
CREATE OR REPLACE PROCEDURE safe_update_sales(
    p_sale_id NUMBER,
    p_new_amount NUMBER
) AS
    v_sale_date DATE;
    v_partition_name VARCHAR2(30);
    v_is_readonly BOOLEAN;
BEGIN
    -- Get sale date to determine partition
    SELECT sale_date INTO v_sale_date
    FROM sales_fact
    WHERE sale_id = p_sale_id;

    -- Determine partition name
    v_partition_name := 'P_' || TO_CHAR(v_sale_date, 'YYYY_MM');

    -- Check if partition is read-only
    v_is_readonly := is_partition_readonly(USER, 'SALES_FACT', v_partition_name);

    IF v_is_readonly THEN
        -- Partition is archived, handle appropriately
        RAISE_APPLICATION_ERROR(-20001,
            'Cannot update sale ' || p_sale_id || '. ' ||
            'Data from ' || TO_CHAR(v_sale_date, 'YYYY-MM') || ' is archived (read-only). ' ||
            'Please contact DBA team for historical data updates.');
    END IF;

    -- Proceed with update
    UPDATE sales_fact
    SET total_amount = p_new_amount,
        modified_date = SYSDATE
    WHERE sale_id = p_sale_id
    AND sale_date = v_sale_date;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/
```

#### Application Error Handling for ORA-14650

```sql
-- Wrapper to handle read-only partition errors gracefully
CREATE OR REPLACE PROCEDURE handle_dml_with_readonly(
    p_sql_statement VARCHAR2,
    p_error_action VARCHAR2 DEFAULT 'RAISE'  -- RAISE, LOG, IGNORE
) AS
    v_error_msg VARCHAR2(4000);
BEGIN
    EXECUTE IMMEDIATE p_sql_statement;

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;

        -- ORA-14650: operation not supported for partitions that are read-only
        IF SQLCODE = -14650 THEN
            CASE p_error_action
                WHEN 'RAISE' THEN
                    RAISE_APPLICATION_ERROR(-20002,
                        'Cannot modify archived data. ' ||
                        'This partition is read-only. ' ||
                        'Contact administrator if modification is required.');

                WHEN 'LOG' THEN
                    -- Log attempt to modify read-only partition
                    INSERT INTO app_error_log (
                        error_date, error_code, error_message, sql_statement
                    ) VALUES (
                        SYSTIMESTAMP, SQLCODE, v_error_msg, p_sql_statement
                    );
                    COMMIT;

                WHEN 'IGNORE' THEN
                    -- Silently ignore (use with caution!)
                    NULL;

                ELSE
                    RAISE;
            END CASE;
        ELSE
            -- Other errors, re-raise
            RAISE;
        END IF;
END;
/

-- Application usage:
/*
-- Instead of direct DML:
UPDATE sales_fact SET total_amount = 100 WHERE sale_id = 12345;

-- Use wrapper:
handle_dml_with_readonly(
    'UPDATE sales_fact SET total_amount = 100 WHERE sale_id = 12345',
    'RAISE'  -- or 'LOG' to log and continue
);
*/
```

### Application Callbacks

#### Pattern: REST API Notification

```sql
-- Callback procedure using UTL_HTTP
CREATE OR REPLACE PROCEDURE notify_application_via_http(
    p_event_type VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2,
    p_details CLOB
) AS
    v_http_req UTL_HTTP.REQ;
    v_http_resp UTL_HTTP.RESP;
    v_json_payload CLOB;
    v_response CLOB;
BEGIN
    -- Build JSON payload
    v_json_payload := '{' ||
        '"event_type": "' || p_event_type || '",' ||
        '"table_name": "' || p_table_name || '",' ||
        '"partition_name": "' || p_partition_name || '",' ||
        '"timestamp": "' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"') || '",' ||
        '"details": ' || p_details ||
    '}';

    -- POST to application endpoint
    v_http_req := UTL_HTTP.BEGIN_REQUEST(
        'http://app-server:8080/api/ilm/notifications',
        'POST',
        'HTTP/1.1'
    );

    UTL_HTTP.SET_HEADER(v_http_req, 'Content-Type', 'application/json');
    UTL_HTTP.SET_HEADER(v_http_req, 'Content-Length', LENGTH(v_json_payload));

    UTL_HTTP.WRITE_TEXT(v_http_req, v_json_payload);

    v_http_resp := UTL_HTTP.GET_RESPONSE(v_http_req);

    -- Read response
    BEGIN
        LOOP
            UTL_HTTP.READ_TEXT(v_http_resp, v_response, 32767);
        END LOOP;
    EXCEPTION
        WHEN UTL_HTTP.END_OF_BODY THEN
            NULL;
    END;

    UTL_HTTP.END_RESPONSE(v_http_resp);

    DBMS_OUTPUT.PUT_LINE('HTTP notification sent successfully');
    DBMS_OUTPUT.PUT_LINE('Response: ' || SUBSTR(v_response, 1, 200));

EXCEPTION
    WHEN OTHERS THEN
        IF UTL_HTTP.GET_RESPONSE(v_http_req) IS NOT NULL THEN
            UTL_HTTP.END_RESPONSE(v_http_resp);
        END IF;
        DBMS_OUTPUT.PUT_LINE('HTTP notification failed: ' || SQLERRM);
END;
/
```

---

## Backup Coordination

### Overview

ILM operations can significantly impact backup strategy and timing. This section provides best practices for coordinating RMAN backups with ILM lifecycle management.

### RMAN Integration Patterns

#### Problem: Compression Triggers Full Backup

**Scenario:** Compressing 100 partitions per month. Each compression marks all blocks as changed, forcing RMAN to back up all 100 partitions fully, extending backup window from 2 hours to 8 hours.

**Solution: Tiered Backup Strategy Aligned with ILM**

```sql
-- ILM Tier Definition
-- HOT (0-3 months):    Uncompressed,      TBS_HOT,    Daily Incremental
-- WARM (3-12 months):  QUERY HIGH,        TBS_WARM,   Weekly Incremental
-- COOL (12-36 months): ARCHIVE HIGH,      TBS_COLD,   Monthly Full, then Skip
-- COLD (36+ months):   ARCHIVE HIGH + RO, TBS_ARCHIVE, Skip Backups

-- RMAN Configuration Script
RMAN> CONFIGURE BACKUP OPTIMIZATION ON;
RMAN> CONFIGURE ARCHIVELOG DELETION POLICY TO BACKED UP 2 TIMES TO DISK;

-- Configure backup policies per tablespace
RMAN> CONFIGURE DATAFILE BACKUP COPIES FOR DEVICE TYPE DISK TO 2;

-- Exclude cold/archive tablespaces from regular backups
RMAN> CONFIGURE EXCLUDE FOR TABLESPACE tbs_cold;
RMAN> CONFIGURE EXCLUDE FOR TABLESPACE tbs_archive;
```

**RMAN Backup Script with ILM Awareness:**

```bash
#!/bin/bash
# rman_ilm_aware_backup.sh

rman target / <<EOF

# Daily backup script - HOT data only
RUN {
    ALLOCATE CHANNEL d1 DEVICE TYPE DISK;
    ALLOCATE CHANNEL d2 DEVICE TYPE DISK;
    ALLOCATE CHANNEL d3 DEVICE TYPE DISK;
    ALLOCATE CHANNEL d4 DEVICE TYPE DISK;

    # Incremental backup of HOT tablespace (daily)
    BACKUP INCREMENTAL LEVEL 1
        TABLESPACE tbs_hot
        FORMAT '/backup/hot_%U'
        TAG 'DAILY_HOT_INCREMENTAL';

    # Backup archive logs
    BACKUP ARCHIVELOG ALL
        DELETE ALL INPUT
        FORMAT '/backup/arch_%U';

    RELEASE CHANNEL d1;
    RELEASE CHANNEL d2;
    RELEASE CHANNEL d3;
    RELEASE CHANNEL d4;
}

# Report
LIST BACKUP SUMMARY;

EXIT;
EOF
```

**Weekly Backup Script - WARM Data:**

```bash
#!/bin/bash
# rman_weekly_warm_backup.sh

rman target / <<EOF

RUN {
    ALLOCATE CHANNEL d1 DEVICE TYPE DISK;
    ALLOCATE CHANNEL d2 DEVICE TYPE DISK;

    # Weekly incremental backup of WARM tablespace
    # Run this AFTER weekend ILM compression completes
    BACKUP INCREMENTAL LEVEL 1
        TABLESPACE tbs_warm
        FORMAT '/backup/warm_%U'
        TAG 'WEEKLY_WARM_INCREMENTAL';

    RELEASE CHANNEL d1;
    RELEASE CHANNEL d2;
}

EXIT;
EOF
```

**Monthly Backup Script - COLD Data (One-Time):**

```bash
#!/bin/bash
# rman_monthly_cold_backup.sh

rman target / <<EOF

RUN {
    ALLOCATE CHANNEL d1 DEVICE TYPE DISK;

    # Full backup of COLD tablespace (one-time after compression)
    BACKUP AS COMPRESSED BACKUPSET
        TABLESPACE tbs_cold
        FORMAT '/backup/cold_%U'
        TAG 'COLD_FULL_BACKUP';

    RELEASE CHANNEL d1;
}

# After this backup, configure to skip
CONFIGURE EXCLUDE FOR TABLESPACE tbs_cold;

EXIT;
EOF
```

### Timing Coordination

#### Problem: ILM Compresses During Backup Window

**Solution: Coordinate Schedules Using Configuration**

```sql
-- Check when backup runs
SELECT
    job_name,
    last_start_date,
    next_run_date,
    TO_CHAR(next_run_date, 'HH24:MI') AS next_run_time
FROM dba_scheduler_jobs
WHERE job_name LIKE '%BACKUP%'
ORDER BY next_run_date;

-- Adjust ILM window to run BEFORE backup
-- Example timeline:
-- 22:00 - 05:00  ETL Load
-- 05:00 - 07:00  ILM Compression (captures yesterday's data)
-- 07:00 - 09:00  RMAN Backup (backs up newly compressed data)
-- 09:00+         Business Hours

UPDATE cmr.dwh_ilm_config
SET config_value = '05:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config
SET config_value = '07:00'  -- End before backup starts
WHERE config_key = 'EXECUTION_WINDOW_END';

COMMIT;
```

#### Coordinated Scheduler Chain

```sql
-- Create comprehensive workflow chain
BEGIN
    DBMS_SCHEDULER.CREATE_CHAIN(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN'
    );

    -- Define steps
    DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        step_name => 'STEP_ETL',
        program_name => 'ETL_LOAD_PROGRAM'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        step_name => 'STEP_ILM',
        program_name => 'ILM_COMPRESS_PROGRAM'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_STEP(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        step_name => 'STEP_BACKUP',
        program_name => 'RMAN_BACKUP_PROGRAM'  -- Shell script via external job
    );

    -- Define rules (sequential execution)
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'TRUE',
        action => 'START STEP_ETL',
        rule_name => 'START_ETL'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'STEP_ETL SUCCEEDED',
        action => 'START STEP_ILM',
        rule_name => 'ETL_DONE_START_ILM'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'STEP_ILM SUCCEEDED',
        action => 'START STEP_BACKUP',
        rule_name => 'ILM_DONE_START_BACKUP'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'STEP_BACKUP SUCCEEDED',
        action => 'END',
        rule_name => 'BACKUP_DONE'
    );

    -- Handle failures
    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'STEP_ETL FAILED',
        action => 'END',  -- Skip ILM and backup if ETL fails
        rule_name => 'ETL_FAILED'
    );

    DBMS_SCHEDULER.DEFINE_CHAIN_RULE(
        chain_name => 'NIGHTLY_ETL_ILM_BACKUP_CHAIN',
        condition => 'STEP_ILM FAILED',
        action => 'START STEP_BACKUP',  -- Still run backup even if ILM fails
        rule_name => 'ILM_FAILED_CONTINUE_BACKUP'
    );

    -- Enable chain
    DBMS_SCHEDULER.ENABLE('NIGHTLY_ETL_ILM_BACKUP_CHAIN');
END;
/
```

### Block Change Tracking

#### Enable BCT to Optimize Incremental Backups

```sql
-- Check if BCT is enabled
SELECT status, filename
FROM v$block_change_tracking;

-- Enable BCT (as SYSDBA)
-- This significantly reduces time for incremental backups
ALTER DATABASE ENABLE BLOCK CHANGE TRACKING
USING FILE '/u01/oradata/BCT/block_change_tracking.bct';

-- After enabling BCT, incremental backups only scan changed blocks
-- Even after ILM compression (which changes all blocks in partition)
```

**Impact Example:**
```
Without BCT:
- RMAN scans all 500GB to find changed blocks
- Backup takes 4 hours

With BCT:
- RMAN reads BCT file, identifies 50GB of changes
- Backup takes 30 minutes
```

### Recovery Implications

#### Testing Recovery with Compressed Partitions

```sql
-- Create test recovery scenario
RMAN> RUN {
    -- Restore and recover specific compressed partition
    RESTORE DATAFILE '/u01/oradata/tbs_cold_01.dbf';
    RECOVER DATAFILE '/u01/oradata/tbs_cold_01.dbf';
}

-- Verify partition accessibility after recovery
SELECT COUNT(*) FROM sales_fact PARTITION (p_2022_01);

-- Check compression status maintained
SELECT
    tablespace_name,
    segment_name,
    partition_name,
    compression,
    compress_for
FROM user_segments
WHERE segment_name = 'SALES_FACT'
AND partition_name = 'P_2022_01';
```

#### PITR with Compressed Data

```sql
-- Point-in-time recovery for compressed tablespace
RMAN> RUN {
    SET UNTIL TIME "TO_DATE('2024-01-15 08:00:00', 'YYYY-MM-DD HH24:MI:SS')";

    RESTORE TABLESPACE tbs_warm;
    RECOVER TABLESPACE tbs_warm;

    -- Verify compression is intact
    SQL "SELECT compression, compress_for
         FROM user_tab_partitions
         WHERE tablespace_name = ''TBS_WARM''";
}

-- After PITR, may need to re-run ILM if recovered to time before compression
EXEC pck_dwh_ilm_policy_engine.evaluate_table(USER, 'SALES_FACT');
```

### Backup Space Optimization

#### Measure Backup Space Savings

```sql
-- Calculate backup space before/after ILM implementation
WITH backup_sizes AS (
    SELECT
        TO_CHAR(completion_time, 'YYYY-MM') AS backup_month,
        SUM(bytes)/1024/1024/1024 AS backup_size_gb
    FROM v$backup_set_details
    WHERE backup_type = 'D'  -- Full or incremental datafile backups
    GROUP BY TO_CHAR(completion_time, 'YYYY-MM')
)
SELECT
    backup_month,
    backup_size_gb,
    LAG(backup_size_gb) OVER (ORDER BY backup_month) AS prev_month_gb,
    backup_size_gb - LAG(backup_size_gb) OVER (ORDER BY backup_month) AS change_gb,
    ROUND(
        (backup_size_gb - LAG(backup_size_gb) OVER (ORDER BY backup_month)) /
        LAG(backup_size_gb) OVER (ORDER BY backup_month) * 100,
        2
    ) AS change_pct
FROM backup_sizes
ORDER BY backup_month DESC;
```

**Expected Results After ILM:**
```
Backup Size Reduction:
- Month 1 (before ILM): 500 GB
- Month 2 (ILM started): 550 GB (initial compression triggers full backup)
- Month 3: 400 GB (20% reduction)
- Month 6: 300 GB (40% reduction, tiered strategy in effect)
```

---

## Monitoring System Integration

### Metrics Export for External Monitoring

#### Pattern: Prometheus/Grafana Integration

```sql
-- Create view for Prometheus exporter
CREATE OR REPLACE VIEW v_ilm_metrics_prometheus AS
SELECT
    'ilm_space_saved_mb' AS metric_name,
    table_name AS label_table,
    SUM(space_saved_mb) AS metric_value,
    MAX(execution_end) AS metric_timestamp
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > SYSDATE - 30
GROUP BY table_name

UNION ALL

SELECT
    'ilm_compression_ratio' AS metric_name,
    table_name AS label_table,
    AVG(compression_ratio) AS metric_value,
    MAX(execution_end) AS metric_timestamp
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND compression_ratio IS NOT NULL
AND execution_start > SYSDATE - 30
GROUP BY table_name

UNION ALL

SELECT
    'ilm_execution_failures' AS metric_name,
    policy_name AS label_table,
    COUNT(*) AS metric_value,
    MAX(execution_start) AS metric_timestamp
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 1
GROUP BY policy_name;

-- Grant access to monitoring user
GRANT SELECT ON v_ilm_metrics_prometheus TO monitoring_user;
```

**Prometheus Exporter Query:**
```python
# Python script to export ILM metrics to Prometheus
import cx_Oracle
from prometheus_client import start_http_server, Gauge

# Define metrics
space_saved = Gauge('ilm_space_saved_mb', 'Space saved by ILM in MB', ['table'])
compression_ratio = Gauge('ilm_compression_ratio', 'Average compression ratio', ['table'])
failures = Gauge('ilm_execution_failures', 'Failed ILM executions', ['policy'])

def collect_metrics():
    conn = cx_Oracle.connect('monitoring_user', 'password', 'db_host:1521/service')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM v_ilm_metrics_prometheus")

    for row in cursor:
        metric_name, label, value, timestamp = row
        if metric_name == 'ilm_space_saved_mb':
            space_saved.labels(table=label).set(value)
        elif metric_name == 'ilm_compression_ratio':
            compression_ratio.labels(table=label).set(value)
        elif metric_name == 'ilm_execution_failures':
            failures.labels(policy=label).set(value)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    start_http_server(8000)
    while True:
        collect_metrics()
        time.sleep(60)  # Collect every minute
```

#### Pattern: CloudWatch Integration (AWS)

```sql
-- Procedure to send metrics to CloudWatch
CREATE OR REPLACE PROCEDURE send_metrics_to_cloudwatch AS
    v_metric_value NUMBER;
    v_http_req UTL_HTTP.REQ;
    v_http_resp UTL_HTTP.RESP;
    v_json_payload CLOB;
BEGIN
    -- Get space saved in last 24 hours
    SELECT NVL(SUM(space_saved_mb), 0) INTO v_metric_value
    FROM cmr.dwh_ilm_execution_log
    WHERE status = 'SUCCESS'
    AND execution_start > SYSDATE - 1;

    -- Build CloudWatch metric JSON
    v_json_payload := '{' ||
        '"Namespace": "CustomILM",' ||
        '"MetricData": [' ||
            '{' ||
                '"MetricName": "SpaceSaved",' ||
                '"Value": ' || v_metric_value || ',' ||
                '"Unit": "Megabytes",' ||
                '"Timestamp": "' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') || '"' ||
            '}' ||
        ']' ||
    '}';

    -- POST to CloudWatch API (via API Gateway or Lambda)
    v_http_req := UTL_HTTP.BEGIN_REQUEST(
        'https://your-api-gateway-url/cloudwatch-metrics',
        'POST',
        'HTTP/1.1'
    );

    UTL_HTTP.SET_HEADER(v_http_req, 'Content-Type', 'application/json');
    UTL_HTTP.WRITE_TEXT(v_http_req, v_json_payload);

    v_http_resp := UTL_HTTP.GET_RESPONSE(v_http_req);
    UTL_HTTP.END_RESPONSE(v_http_resp);

    DBMS_OUTPUT.PUT_LINE('CloudWatch metrics sent successfully');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Failed to send CloudWatch metrics: ' || SQLERRM);
END;
/

-- Schedule to run hourly
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'SEND_ILM_METRICS_TO_CLOUDWATCH',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN send_metrics_to_cloudwatch(); END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=HOURLY; INTERVAL=1',
        enabled => TRUE
    );
END;
/
```

---

## Security and Compliance

### Audit Logging

```sql
-- Enhanced audit logging for ILM operations
CREATE TABLE cmr.dwh_ilm_audit_log (
    audit_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    audit_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP,
    username VARCHAR2(128),
    operation_type VARCHAR2(50),  -- POLICY_CREATE, POLICY_MODIFY, POLICY_DELETE, PARTITION_COMPRESS, etc.
    object_owner VARCHAR2(128),
    object_name VARCHAR2(128),
    partition_name VARCHAR2(128),
    old_value CLOB,
    new_value CLOB,
    client_info VARCHAR2(256),
    os_user VARCHAR2(128),
    host VARCHAR2(128),
    terminal VARCHAR2(128),
    reason VARCHAR2(4000)
);

-- Audit trigger on policy changes
CREATE OR REPLACE TRIGGER trg_audit_ilm_policy_changes
BEFORE INSERT OR UPDATE OR DELETE ON cmr.dwh_ilm_policies
FOR EACH ROW
DECLARE
    v_operation VARCHAR2(50);
    v_old_value CLOB;
    v_new_value CLOB;
BEGIN
    IF INSERTING THEN
        v_operation := 'POLICY_CREATE';
        v_new_value := 'Policy: ' || :NEW.policy_name || ', Type: ' || :NEW.policy_type;
    ELSIF UPDATING THEN
        v_operation := 'POLICY_MODIFY';
        v_old_value := 'Enabled: ' || :OLD.enabled || ', Age: ' || :OLD.age_days || ' days';
        v_new_value := 'Enabled: ' || :NEW.enabled || ', Age: ' || :NEW.age_days || ' days';
    ELSIF DELETING THEN
        v_operation := 'POLICY_DELETE';
        v_old_value := 'Policy: ' || :OLD.policy_name;
    END IF;

    INSERT INTO cmr.dwh_ilm_audit_log (
        username, operation_type, object_owner, object_name,
        old_value, new_value, client_info, os_user, host
    ) VALUES (
        USER, v_operation, :NEW.table_owner, :NEW.table_name,
        v_old_value, v_new_value,
        SYS_CONTEXT('USERENV', 'CLIENT_INFO'),
        SYS_CONTEXT('USERENV', 'OS_USER'),
        SYS_CONTEXT('USERENV', 'HOST')
    );
END;
/
```

### Data Retention Compliance

```sql
-- Compliance check view
CREATE OR REPLACE VIEW v_ilm_compliance_status AS
SELECT
    p.table_name,
    p.policy_name,
    p.policy_type,
    CASE
        WHEN p.policy_type = 'PURGE' AND p.age_months >= 84 THEN 'COMPLIANT'  -- 7 years
        WHEN p.policy_type = 'PURGE' AND p.age_months < 84 THEN 'NON_COMPLIANT'
        ELSE 'N/A'
    END AS gdpr_compliance,
    CASE
        WHEN p.policy_type = 'PURGE' AND p.age_months >= 84 THEN 'COMPLIANT'  -- SOX: 7 years
        WHEN p.policy_type = 'PURGE' AND p.age_months < 84 THEN 'NON_COMPLIANT'
        ELSE 'N/A'
    END AS sox_compliance,
    p.age_months,
    p.enabled
FROM cmr.dwh_ilm_policies p
WHERE p.policy_type = 'PURGE';

-- Alert on non-compliant policies
SELECT * FROM v_ilm_compliance_status
WHERE gdpr_compliance = 'NON_COMPLIANT'
OR sox_compliance = 'NON_COMPLIANT';
```

---

## Troubleshooting Integration Issues

### Common Integration Problems

#### Problem 1: ETL Fails with "Partition Locked"

**Diagnosis:**
```sql
-- Check what's locking the partition
SELECT
    s.sid,
    s.serial#,
    s.username,
    s.program,
    s.module,
    o.object_name,
    o.subobject_name AS partition_name,
    l.locked_mode
FROM v$locked_object l
JOIN v$session s ON l.session_id = s.sid
JOIN dba_objects o ON l.object_id = o.object_id
WHERE o.object_name = 'SALES_FACT';
```

**Solution:**
- Adjust ILM execution window
- Implement lock detection before ILM operations
- Add retry logic to ETL process

#### Problem 2: Backup Window Overruns After ILM

**Diagnosis:**
```sql
-- Check backup size growth
SELECT
    TO_CHAR(start_time, 'YYYY-MM-DD') AS backup_date,
    SUM(bytes)/1024/1024/1024 AS backup_size_gb,
    ROUND(SUM(elapsed_seconds)/3600, 2) AS backup_hours
FROM v$backup_set
WHERE start_time > SYSDATE - 30
GROUP BY TO_CHAR(start_time, 'YYYY-MM-DD')
ORDER BY backup_date DESC;
```

**Solution:**
- Enable block change tracking
- Implement tiered backup strategy
- Exclude cold tablespaces from regular backups

#### Problem 3: Application Queries Slow After Compression

**Diagnosis:**
```sql
-- Compare query performance before/after compression
SELECT
    sql_id,
    plan_hash_value,
    executions,
    ROUND(elapsed_time/1000000/executions, 2) AS avg_elapsed_sec,
    ROUND(cpu_time/1000000/executions, 2) AS avg_cpu_sec,
    TO_CHAR(last_active_time, 'YYYY-MM-DD HH24:MI:SS') AS last_run
FROM v$sql
WHERE sql_text LIKE '%SALES_FACT%'
AND executions > 5
ORDER BY avg_elapsed_sec DESC;
```

**Solution:**
- Use QUERY HIGH instead of ARCHIVE HIGH for frequently queried data
- Implement query result caching
- Add partition pruning hints
- Review and adjust age thresholds

---

## Reference Architectures

### Architecture 1: Small Data Warehouse (< 5 TB)

**Characteristics:**
- Single ETL window (nightly)
- RMAN to disk
- Limited maintenance windows

**Integration Pattern:**
```
20:00 - 04:00  ETL Load
04:00 - 05:00  ILM Compression (yesterday's partition)
05:00 - 06:00  RMAN Incremental Backup
06:00+         Business Hours
```

**Configuration:**
```sql
-- Simple daily cycle
UPDATE cmr.dwh_ilm_config SET config_value = '04:00' WHERE config_key = 'EXECUTION_WINDOW_START';
UPDATE cmr.dwh_ilm_config SET config_value = '05:00' WHERE config_key = 'EXECUTION_WINDOW_END';
COMMIT;

-- Conservative policies
-- Compress after 90 days (keep recent data uncompressed)
-- No tiering (single tablespace)
```

### Architecture 2: Medium Data Warehouse (5-50 TB)

**Characteristics:**
- Multiple ETL workflows
- RMAN to disk + tape
- Multi-tier storage

**Integration Pattern:**
```
20:00 - 05:00  ETL Load (multiple jobs)
05:00 - 07:00  ILM Operations (compression + tiering)
07:00 - 09:00  RMAN Incremental Backup (HOT tier)
09:00 - 17:00  Business Hours
17:00 - 19:00  Weekend: WARM tier backup
```

**Configuration:**
```sql
-- Tiered compression strategy
-- Policy 1: Compress at 30 days (QUERY HIGH)
-- Policy 2: Re-compress + move at 12 months (ARCHIVE HIGH, TBS_COLD)
-- Policy 3: Read-only at 36 months

-- Backup strategy
-- Daily: HOT tier incremental
-- Weekly: WARM tier incremental
-- Monthly: COLD tier full, then exclude
```

### Architecture 3: Large Data Warehouse (50+ TB)

**Characteristics:**
- 24/7 ETL (micro-batches)
- RMAN to disk + cloud
- Advanced storage tiering

**Integration Pattern:**
```
Continuous: Micro-batch ETL loads
06:00 - 08:00: ILM window (compress yesterday -1 day to avoid conflicts)
08:00 - 10:00: RMAN incremental (HOT tier)
10:00 - 12:00: Weekend: WARM/COLD tier backups
```

**Configuration:**
```sql
-- Aggressive compression
-- Compress at 7 days (QUERY HIGH)
-- Move to WARM at 90 days (QUERY HIGH)
-- Move to COLD at 12 months (ARCHIVE HIGH)
-- Purge at 7 years

-- Advanced features
-- Oracle scheduler chains for orchestration
-- Application hooks for cache invalidation
-- Custom monitoring dashboards
-- Automated alerting
```

---

## Summary

### Integration Checklist

Before going to production:

**ETL Integration:**
- [ ] ILM execution window does not overlap with ETL
- [ ] Partition locking detection implemented
- [ ] Post-ETL ILM trigger configured
- [ ] ETL failure handling tested

**Application Integration:**
- [ ] Application hooks registered and tested
- [ ] Read-only partition handling implemented
- [ ] Query routing strategy defined
- [ ] Cache invalidation mechanism in place

**Backup Integration:**
- [ ] Tiered backup strategy documented
- [ ] RMAN scripts updated for ILM tiers
- [ ] Block change tracking enabled
- [ ] Recovery procedures tested with compressed partitions

**Monitoring:**
- [ ] ILM metrics exported to monitoring system
- [ ] Alerting thresholds configured
- [ ] Dashboard created for ILM operations
- [ ] Email notifications tested

**Compliance:**
- [ ] Audit logging enabled
- [ ] Retention policies verified against requirements
- [ ] Compliance reporting automated
- [ ] Legal team signoff obtained

### Next Steps

1. Review current environment against reference architectures
2. Select integration patterns appropriate for your scale
3. Implement in phases (ETL → Backup → Application → Monitoring)
4. Test thoroughly in non-production
5. Document runbooks for operations team
6. Deploy to production with monitoring

---

## References

- [Operations Runbook](operations/OPERATIONS_RUNBOOK.md) - Day-to-day operations
- [Policy Design Guide](POLICY_DESIGN_GUIDE.md) - Designing effective policies
- [Custom ILM Guide](custom_ilm_guide.md) - Framework usage
- [Table Migration Guide](table_migration_guide.md) - Migrating to partitioned tables

---

**Document Version:** 1.0
**Last Updated:** 2025-10-23
**Authors:** Data Warehouse Team, Integration Architects
**Status:** Production Ready
