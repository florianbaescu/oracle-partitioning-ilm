# Oracle Custom ILM Framework - Complete Installation Guide

**Version:** 2.0
**Date:** 2025-10-22
**Frameworks:** Custom ILM + Table Migration
**Status:** Production Ready

---

## Table of Contents

1. [Prerequisites Checklist](#prerequisites-checklist)
2. [Installation Steps](#installation-steps)
3. [Post-Installation](#post-installation)
4. [Troubleshooting](#troubleshooting)
5. [Uninstallation](#uninstallation)
6. [Framework Independence](#framework-independence)

---

## Prerequisites Checklist

### Oracle Database Requirements

- [ ] Oracle Database 12c or higher (12.1.0.2+)
- [ ] Oracle Database 19c or higher (**RECOMMENDED**)
- [ ] Standard Edition (ILM features work) OR Enterprise Edition
- [ ] Oracle ADO/Heat Map (**OPTIONAL** - for advanced access tracking only)

**Verify Your Version:**
```sql
SELECT version, version_full
FROM product_component_version
WHERE product LIKE 'Oracle Database%';
```

**Expected:** 12.1.0.2 or higher

---

### Privilege Requirements

The installing user needs:

- [ ] CREATE TABLE
- [ ] CREATE VIEW
- [ ] CREATE PROCEDURE
- [ ] CREATE TRIGGER
- [ ] CREATE SEQUENCE
- [ ] SELECT on DBA_TAB_PARTITIONS
- [ ] SELECT on DBA_SEGMENTS
- [ ] SELECT on DBA_TABLESPACES
- [ ] DBMS_SCHEDULER privileges (CREATE JOB, CREATE PROGRAM)

**Verify Privileges:**
```sql
SET SERVEROUTPUT ON

-- Check system privileges
SELECT privilege FROM user_sys_privs
WHERE privilege IN (
    'CREATE TABLE', 'CREATE VIEW', 'CREATE PROCEDURE',
    'CREATE TRIGGER', 'CREATE SEQUENCE'
)
ORDER BY privilege;

-- Check DBMS_SCHEDULER access
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'TEST_PRIVILEGE_CHECK',
        job_type => 'PLSQL_BLOCK',
        job_action => 'BEGIN NULL; END;',
        enabled => FALSE
    );
    DBMS_SCHEDULER.DROP_JOB('TEST_PRIVILEGE_CHECK');
    DBMS_OUTPUT.PUT_LINE('✓ DBMS_SCHEDULER privileges OK');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('✗ Missing DBMS_SCHEDULER privileges');
        RAISE;
END;
/
```

**Grant Missing Privileges (run as DBA):**
```sql
GRANT CREATE TABLE TO cmr;
GRANT CREATE VIEW TO cmr;
GRANT CREATE PROCEDURE TO cmr;
GRANT CREATE TRIGGER TO cmr;
GRANT CREATE SEQUENCE TO cmr;
GRANT CREATE JOB TO cmr;
GRANT SELECT ON dba_tab_partitions TO cmr;
GRANT SELECT ON dba_segments TO cmr;
GRANT SELECT ON dba_tablespaces TO cmr;
```

---

### Schema Configuration

**CRITICAL:** All framework objects are created with `cmr.` schema prefix.

#### Option 1: Install in CMR Schema (RECOMMENDED)

```sql
-- Connect as CMR user
CONN cmr/password@database

-- Verify you're in correct schema
SELECT USER FROM DUAL;
-- Expected: CMR

-- Proceed with installation
@scripts/custom_ilm_setup.sql
```

#### Option 2: Install in Different Schema

```sql
-- If installing in a different schema, you must:
-- 1. Search and replace ALL occurrences of 'cmr.' in scripts
-- 2. Replace with your schema name

-- Example using sed (Unix/Linux/Mac):
sed 's/cmr\./myschema\./g' scripts/*.sql > scripts_modified/

-- Or using PowerShell (Windows):
Get-ChildItem scripts/*.sql | ForEach-Object {
    (Get-Content $_) -replace 'cmr\.', 'myschema.' | Set-Content ("scripts_modified/" + $_.Name)
}
```

#### Option 3: Grant Access from CMR Schema

```sql
-- If objects exist in CMR and you want to use from another schema
GRANT SELECT, INSERT, UPDATE, DELETE ON cmr.dwh_ilm_policies TO myuser;
GRANT SELECT ON cmr.dwh_ilm_execution_log TO myuser;
GRANT SELECT ON cmr.dwh_ilm_config TO myuser;
GRANT EXECUTE ON cmr.pck_dwh_ilm_policy_engine TO myuser;
GRANT EXECUTE ON cmr.pck_dwh_ilm_execution_engine TO myuser;
GRANT EXECUTE ON cmr.dwh_run_ilm_cycle TO myuser;
GRANT EXECUTE ON cmr.dwh_start_ilm_jobs TO myuser;
GRANT EXECUTE ON cmr.dwh_stop_ilm_jobs TO myuser;
```

---

### Disk Space Requirements

- **Metadata tables:** ~10 MB initial
- **Execution logs:** 1-5 MB per month (depends on policy activity)
- **Partition access tracking:** ~1 KB per partition tracked

**Check Available Space:**
```sql
SELECT tablespace_name,
       ROUND(SUM(bytes)/1024/1024, 2) AS free_mb
FROM dba_free_space
WHERE tablespace_name = (
    SELECT default_tablespace FROM dba_users WHERE username = USER
)
GROUP BY tablespace_name;
```

**Recommended:** At least 100 MB free space in default tablespace

---

## Installation Steps

### Step 1: Pre-Installation Validation

Run this validation script to ensure all prerequisites are met:

```sql
SET SERVEROUTPUT ON

DECLARE
    v_version VARCHAR2(20);
    v_count NUMBER;
    v_error_count NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Pre-Installation Validation');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Check Oracle version
    BEGIN
        SELECT version INTO v_version
        FROM product_component_version
        WHERE product LIKE 'Oracle Database%';

        DBMS_OUTPUT.PUT_LINE('Oracle Version: ' || v_version);

        IF TO_NUMBER(SUBSTR(v_version, 1, 2)) < 12 THEN
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Oracle 12c or higher required');
            v_error_count := v_error_count + 1;
        ELSE
            DBMS_OUTPUT.PUT_LINE('✓ Version check passed');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Cannot determine Oracle version');
            v_error_count := v_error_count + 1;
    END;

    DBMS_OUTPUT.PUT_LINE('');

    -- Check CREATE privileges
    SELECT COUNT(*) INTO v_count
    FROM user_sys_privs
    WHERE privilege IN ('CREATE TABLE', 'CREATE VIEW', 'CREATE PROCEDURE');

    IF v_count < 3 THEN
        DBMS_OUTPUT.PUT_LINE('✗ ERROR: Missing CREATE privileges');
        DBMS_OUTPUT.PUT_LINE('  Found: ' || v_count || ' of 3 required');
        v_error_count := v_error_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('✓ CREATE privileges OK (' || v_count || ' found)');
    END IF;

    -- Check DBA views access
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM dba_tab_partitions
        WHERE ROWNUM = 1;

        DBMS_OUTPUT.PUT_LINE('✓ DBA_TAB_PARTITIONS access OK');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Cannot access DBA_TAB_PARTITIONS');
            v_error_count := v_error_count + 1;
    END;

    -- Check DBMS_SCHEDULER privileges
    BEGIN
        DBMS_SCHEDULER.CREATE_JOB(
            job_name => 'TEST_PRIV_CHECK',
            job_type => 'PLSQL_BLOCK',
            job_action => 'BEGIN NULL; END;',
            enabled => FALSE
        );
        DBMS_SCHEDULER.DROP_JOB('TEST_PRIV_CHECK');
        DBMS_OUTPUT.PUT_LINE('✓ DBMS_SCHEDULER access OK');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Missing DBMS_SCHEDULER privileges');
            v_error_count := v_error_count + 1;
    END;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');

    IF v_error_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ All prerequisites met');
        DBMS_OUTPUT.PUT_LINE('========================================');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ ' || v_error_count || ' error(s) found');
        DBMS_OUTPUT.PUT_LINE('========================================');
        RAISE_APPLICATION_ERROR(-20001, 'Prerequisites not met. Fix errors above before installing.');
    END IF;
END;
/
```

**Expected Output:**
```
========================================
Pre-Installation Validation
========================================

Oracle Version: 19.0.0.0.0
✓ Version check passed

✓ CREATE privileges OK (3 found)
✓ DBA_TAB_PARTITIONS access OK
✓ DBMS_SCHEDULER access OK

========================================
✓ All prerequisites met
========================================
```

---

### Step 2: Choose Your Installation

You can install one or both frameworks in any order:

- **Custom ILM Framework:** Policy-based lifecycle management for partitioned tables
- **Table Migration Framework:** Convert non-partitioned tables to partitioned with analysis

**✅ NEW:** Both frameworks are now fully independent (as of 2025-10-22)

---

### Step 2A: Install Custom ILM Framework

```sql
-- Set session parameters for clean install
SET ECHO ON
SET DEFINE OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK ON
SPOOL ilm_install.log

-- Component 1: Metadata tables and base procedures (~5 minutes)
PROMPT ========================================
PROMPT Installing ILM Metadata Tables...
PROMPT ========================================
@scripts/custom_ilm_setup.sql

-- Verify step 1:
SELECT COUNT(*) AS table_count
FROM user_tables
WHERE table_name LIKE 'DWH_ILM%';
-- Expected: 5 tables (DWH_ILM_POLICIES, DWH_ILM_EXECUTION_LOG, DWH_ILM_PARTITION_ACCESS, DWH_ILM_EVALUATION_QUEUE, DWH_ILM_CONFIG)

PAUSE Press Enter to continue with policy engine installation...

-- Component 2: Policy evaluation engine (~1 minute)
PROMPT ========================================
PROMPT Installing ILM Policy Engine...
PROMPT ========================================
@scripts/custom_ilm_policy_engine.sql

-- Verify step 2:
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name = 'PCK_DWH_ILM_POLICY_ENGINE';
-- Expected: PACKAGE and PACKAGE BODY, both VALID

PAUSE Press Enter to continue with execution engine installation...

-- Component 3: Execution engine (~1 minute)
PROMPT ========================================
PROMPT Installing ILM Execution Engine...
PROMPT ========================================
@scripts/custom_ilm_execution_engine.sql

-- Verify step 3:
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name = 'PCK_DWH_ILM_EXECUTION_ENGINE';
-- Expected: PACKAGE and PACKAGE BODY, both VALID

PAUSE Press Enter to continue with validation utilities (optional)...

-- Component 3A: Validation utilities - OPTIONAL (~30 seconds)
PROMPT ========================================
PROMPT Installing ILM Validation Utilities (Optional)...
PROMPT ========================================
@scripts/custom_ilm_validation.sql

-- Verify step 3A:
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name = 'DWH_VALIDATE_ILM_POLICY';
-- Expected: PROCEDURE, VALID

-- Note: This is an optional component for policy validation and testing
-- The dwh_validate_ilm_policy procedure can be used to validate policies
-- Example: EXEC dwh_validate_ilm_policy(1);

PAUSE Press Enter to continue with scheduler installation...

-- Component 4: Scheduler jobs (~1 minute)
PROMPT ========================================
PROMPT Installing ILM Scheduler Jobs...
PROMPT ========================================
@scripts/custom_ilm_scheduler.sql

-- Verify step 4:
SELECT job_name, enabled, state
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%'
ORDER BY job_name;
-- Expected: 4 jobs (ILM_JOB_CLEANUP, ILM_JOB_EVALUATE, ILM_JOB_EXECUTE, ILM_JOB_REFRESH_ACCESS)

SPOOL OFF

PROMPT ========================================
PROMPT ILM Framework Installation Complete!
PROMPT ========================================
```

---

### Step 2B: Install Table Migration Framework (OPTIONAL)

```sql
-- Set session parameters
SET ECHO ON
SET DEFINE OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK ON
SPOOL migration_install.log

-- Component 1: Migration metadata tables (~2 minutes)
PROMPT ========================================
PROMPT Installing Migration Metadata...
PROMPT ========================================
@scripts/table_migration_setup.sql

-- Verify:
SELECT table_name FROM user_tables
WHERE table_name LIKE '%MIGRATION%'
ORDER BY table_name;
-- Expected: DWH_MIGRATION_ANALYSIS, DWH_MIGRATION_EXECUTION_LOG, DWH_MIGRATION_ILM_TEMPLATES, DWH_MIGRATION_PROJECTS, DWH_MIGRATION_TASKS

PAUSE Press Enter to continue with analysis package...

-- Component 2: Analysis engine (~2 minutes)
PROMPT ========================================
PROMPT Installing Migration Analysis Engine...
PROMPT ========================================
@scripts/table_migration_analysis.sql

-- Verify:
SELECT object_name, status
FROM user_objects
WHERE object_name = 'PCK_DWH_TABLE_MIGRATION_ANALYZER';
-- Expected: VALID

PAUSE Press Enter to continue with execution package...

-- Component 3: Execution engine (~2 minutes)
PROMPT ========================================
PROMPT Installing Migration Execution Engine...
PROMPT ========================================
@scripts/table_migration_execution.sql

-- Verify:
SELECT object_name, status
FROM user_objects
WHERE object_name = 'PCK_DWH_TABLE_MIGRATION_EXECUTOR';
-- Expected: VALID

SPOOL OFF

PROMPT ========================================
PROMPT Migration Framework Installation Complete!
PROMPT ========================================
```

---

### Step 3: Comprehensive Verification

Run this comprehensive verification to ensure everything installed correctly:

```sql
SET SERVEROUTPUT ON

DECLARE
    v_count NUMBER;
    v_error_count NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Installation Verification');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Verify ILM tables
    SELECT COUNT(*) INTO v_count
    FROM user_tables
    WHERE table_name IN (
        'DWH_ILM_POLICIES',
        'DWH_ILM_EXECUTION_LOG',
        'DWH_ILM_PARTITION_ACCESS',
        'DWH_ILM_EVALUATION_QUEUE',
        'DWH_ILM_CONFIG'
    );

    IF v_count = 5 THEN
        DBMS_OUTPUT.PUT_LINE('✓ All 5 ILM metadata tables created');
    ELSIF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Only ' || v_count || ' of 5 ILM tables created');
        v_error_count := v_error_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('  ILM tables not found (framework not installed)');
    END IF;

    -- Verify ILM packages
    SELECT COUNT(*) INTO v_count
    FROM user_objects
    WHERE object_type IN ('PACKAGE', 'PACKAGE BODY')
    AND object_name LIKE 'PCK_DWH_ILM%'
    AND status = 'VALID';

    IF v_count = 4 THEN  -- 2 packages × 2 (spec + body)
        DBMS_OUTPUT.PUT_LINE('✓ All ILM packages compiled successfully');
    ELSIF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Only ' || v_count || ' of 4 ILM package objects valid');
        v_error_count := v_error_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('  ILM packages not found (framework not installed)');
    END IF;

    -- Verify ILM scheduler jobs
    SELECT COUNT(*) INTO v_count
    FROM user_scheduler_jobs
    WHERE job_name LIKE 'ILM_JOB%';

    IF v_count = 4 THEN
        DBMS_OUTPUT.PUT_LINE('✓ All 4 ILM scheduler jobs created');
    ELSIF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Only ' || v_count || ' of 4 ILM jobs created');
        v_error_count := v_error_count + 1;
    ELSE
        DBMS_OUTPUT.PUT_LINE('  ILM scheduler jobs not found (framework not installed)');
    END IF;

    -- Verify Migration tables
    SELECT COUNT(*) INTO v_count
    FROM user_tables
    WHERE table_name LIKE '%MIGRATION%';

    IF v_count = 5 THEN
        DBMS_OUTPUT.PUT_LINE('✓ All 5 Migration tables created');
    ELSIF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ ' || v_count || ' Migration tables created');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Migration tables not found (framework not installed)');
    END IF;

    -- Verify Migration packages
    SELECT COUNT(*) INTO v_count
    FROM user_objects
    WHERE object_type IN ('PACKAGE', 'PACKAGE BODY')
    AND object_name LIKE 'PCK_DWH_TABLE_MIGRATION%'
    AND status = 'VALID';

    IF v_count = 4 THEN  -- 2 packages × 2 (spec + body)
        DBMS_OUTPUT.PUT_LINE('✓ All Migration packages compiled successfully');
    ELSIF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ ' || v_count || ' Migration package objects valid');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Migration packages not found (framework not installed)');
    END IF;

    -- Verify config table
    SELECT COUNT(*) INTO v_count
    FROM cmr.dwh_ilm_config;

    IF v_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ Config table has ' || v_count || ' parameters');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ ERROR: Config table is empty');
        v_error_count := v_error_count + 1;
    END IF;

    -- Verify views
    SELECT COUNT(*) INTO v_count
    FROM user_views
    WHERE view_name LIKE '%ILM%' OR view_name LIKE '%MIGRATION%';

    DBMS_OUTPUT.PUT_LINE('✓ Created ' || v_count || ' monitoring views');

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('========================================');

    IF v_error_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('✓ Installation verification PASSED');
    ELSE
        DBMS_OUTPUT.PUT_LINE('⚠ Installation verification completed with ' || v_error_count || ' warning(s)');
    END IF;

    DBMS_OUTPUT.PUT_LINE('========================================');
END;
/
```

---

### Step 4: Initial Configuration

Configure the framework for your environment:

```sql
-- Configure execution window (ILM operations will only run during this window)
UPDATE cmr.dwh_ilm_config
SET config_value = '23:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config
SET config_value = '05:00'
WHERE config_key = 'EXECUTION_WINDOW_END';

-- Configure maximum concurrent operations
UPDATE cmr.dwh_ilm_config
SET config_value = '4'
WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';

-- Configure alert email (optional - requires UTL_MAIL setup)
UPDATE cmr.dwh_ilm_config
SET config_value = 'dba@yourcompany.com'
WHERE config_key = 'ALERT_EMAIL_RECIPIENT';

-- Review all configuration
SELECT config_key, config_value, description
FROM cmr.dwh_ilm_config
ORDER BY config_key;

COMMIT;
```

---

### Step 5: Functional Test

Test the ILM framework with a simple test case:

```sql
SET SERVEROUTPUT ON

-- Create test table
CREATE TABLE ilm_test_table (
    id NUMBER,
    test_date DATE,
    data VARCHAR2(100)
)
PARTITION BY RANGE (test_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2023_01 VALUES LESS THAN (DATE '2023-02-01')
);

-- Insert test data
INSERT INTO ilm_test_table
VALUES (1, DATE '2023-01-15', 'Test data');
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Test table created with 1 partition');

-- Initialize partition tracking
EXEC dwh_refresh_partition_access_tracking(USER, 'ILM_TEST_TABLE');

DBMS_OUTPUT.PUT_LINE('✓ Partition tracking initialized');

-- Create test policy (compress partition older than 1 day for testing)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'TEST_COMPRESS', USER, 'ILM_TEST_TABLE',
    'COMPRESSION', 'COMPRESS', 1,
    'QUERY HIGH', 100, 'Y'
);
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Test policy created');

-- Run test cycle
DBMS_OUTPUT.PUT_LINE('');
DBMS_OUTPUT.PUT_LINE('Running ILM test cycle...');
DBMS_OUTPUT.PUT_LINE('========================================');

EXEC dwh_run_ilm_cycle();

DBMS_OUTPUT.PUT_LINE('========================================');
DBMS_OUTPUT.PUT_LINE('');

-- Check results
SELECT execution_id, policy_name, partition_name, status,
       duration_seconds, space_saved_mb
FROM cmr.dwh_ilm_execution_log
WHERE table_name = 'ILM_TEST_TABLE'
ORDER BY execution_start DESC
FETCH FIRST 5 ROWS ONLY;

-- Cleanup test
DELETE FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_COMPRESS';
DROP TABLE ilm_test_table PURGE;
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Functional test complete - cleanup done');
```

**Expected Results:**
- Test table created
- Partition tracking initialized
- Policy created and evaluated
- Policy executed (compression attempted)
- Execution logged in dwh_ilm_execution_log

---

## Post-Installation

### Enable Automatic Execution

```sql
-- Enable auto-execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Start scheduler jobs
EXEC dwh_start_ilm_jobs();

-- Verify jobs are running
SELECT job_name, enabled, state, next_run_date
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%'
ORDER BY job_name;
```

**Expected Output:**
```
JOB_NAME                 ENABLED  STATE      NEXT_RUN_DATE
----------------------- -------- ---------- -------------------
ILM_JOB_CLEANUP         TRUE     SCHEDULED  22-OCT-25 03.00.00 AM
ILM_JOB_EVALUATE        TRUE     SCHEDULED  22-OCT-25 02.00.00 AM
ILM_JOB_EXECUTE         TRUE     SCHEDULED  22-OCT-25 12.00.00 PM
ILM_JOB_REFRESH_ACCESS  TRUE     SCHEDULED  22-OCT-25 01.00.00 AM
```

---

### Create First Production Policy

See [Custom ILM Guide](docs/custom_ilm_guide.md) for detailed examples. Basic template:

```sql
-- Compress partitions older than 90 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_MYFACT_90D', USER, 'MY_FACT_TABLE',
    'COMPRESSION', 'COMPRESS', 90,
    'QUERY HIGH', 100, 'Y'
);
COMMIT;

-- Initialize tracking for your table
EXEC dwh_refresh_partition_access_tracking(USER, 'MY_FACT_TABLE');
```

---

## Troubleshooting

### Problem: ORA-06550 identifier must be declared

**Symptom:**
```
ORA-06550: line 1, column 7:
PLS-00201: identifier 'RUN_ILM_CYCLE' must be declared
```

**Cause:** Using old procedure name from outdated documentation

**Solution:**
Use `dwh_run_ilm_cycle()` instead of `run_ilm_cycle()`

**All procedures use `dwh_` prefix:**
- ✅ `dwh_run_ilm_cycle()`
- ✅ `dwh_start_ilm_jobs()`
- ✅ `dwh_stop_ilm_jobs()`
- ✅ `dwh_run_ilm_job_now('JOB_NAME')`
- ✅ `dwh_refresh_partition_access_tracking()`

---

### Problem: ORA-00942 table or view does not exist

**Symptom:**
```
ORA-00942: table or view "CMR"."DWH_ILM_POLICIES" does not exist
```

**Cause:** Scripts are hard-coded with `cmr.` schema prefix

**Solutions:**
1. **Option A (Recommended):** Install in CMR schema
2. **Option B:** Search/replace `cmr.` with your schema name in all scripts before installation
3. **Option C:** Grant access to CMR schema objects (see Step 3)

---

### Problem: Package compiled with errors

**Symptom:**
```
Warning: Package Body created with compilation errors.
```

**Diagnosis:**
```sql
-- Show compilation errors
SHOW ERRORS PACKAGE BODY pck_dwh_ilm_policy_engine;

-- Check package status
SELECT object_name, object_type, status
FROM user_objects
WHERE object_name LIKE 'PCK_DWH_ILM%'
ORDER BY object_name, object_type;
```

**Common Causes:**
1. Missing function dependency - Usually resolved automatically as of 2025-10-22
2. Invalid schema prefix - Check you're in correct schema
3. Missing privileges - Run privilege verification script

**Solution:**
Check installation log (`ilm_install.log`), verify all previous steps completed successfully.

---

### Problem: No partitions evaluated

**Symptom:** Policy runs but finds 0 eligible partitions

**Diagnosis:**
```sql
-- Check if partition tracking initialized
SELECT COUNT(*) FROM cmr.dwh_ilm_partition_access
WHERE table_name = 'YOUR_TABLE';

-- If 0, tracking not initialized
```

**Solution:**
```sql
-- Initialize tracking for specific table
EXEC dwh_refresh_partition_access_tracking(USER, 'YOUR_TABLE');

-- Or initialize tracking for all partitioned tables
EXEC dwh_refresh_partition_access_tracking();
```

---

### Problem: Scheduler jobs not running

**Symptom:** Jobs show as DISABLED or never execute

**Diagnosis:**
```sql
SELECT job_name, enabled, state, failure_count, last_start_date
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%';
```

**Solution:**
```sql
-- Start all ILM jobs
EXEC dwh_start_ilm_jobs();

-- Or manually enable each job:
BEGIN
    DBMS_SCHEDULER.ENABLE('ILM_JOB_REFRESH_ACCESS');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_EVALUATE');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_EXECUTE');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_CLEANUP');
END;
/

-- Verify
SELECT job_name, enabled FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%';
```

---

### Problem: Jobs enabled but not executing

**Symptom:** Jobs are ENABLED but last_start_date is NULL

**Diagnosis:**
```sql
-- Check execution window configuration
SELECT config_key, config_value
FROM cmr.dwh_ilm_config
WHERE config_key LIKE '%WINDOW%';

-- Check if auto-execution is enabled
SELECT config_value
FROM cmr.dwh_ilm_config
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
```

**Solution:**
```sql
-- Enable auto-execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Manually trigger a job to test
EXEC dwh_run_ilm_job_now('ILM_JOB_EVALUATE');

-- Check job history
SELECT * FROM v_ilm_job_history
WHERE log_date > SYSDATE - 1
ORDER BY log_date DESC;
```

---

## Uninstallation

**⚠ WARNING:** This will delete all ILM policies, execution history, and metadata!

```sql
SET SERVEROUTPUT ON

-- Step 1: Stop all jobs
EXEC dwh_stop_ilm_jobs();

-- Step 2: Drop scheduler components
BEGIN
    FOR job IN (SELECT job_name FROM user_scheduler_jobs
                WHERE job_name LIKE 'ILM_JOB_%') LOOP
        DBMS_SCHEDULER.DROP_JOB(job.job_name, TRUE);
        DBMS_OUTPUT.PUT_LINE('Dropped job: ' || job.job_name);
    END LOOP;

    FOR prog IN (SELECT program_name FROM user_scheduler_programs
                 WHERE program_name LIKE 'ILM_%') LOOP
        DBMS_SCHEDULER.DROP_PROGRAM(prog.program_name, TRUE);
        DBMS_OUTPUT.PUT_LINE('Dropped program: ' || prog.program_name);
    END LOOP;
END;
/

-- Step 3: Drop packages
DROP PACKAGE pck_dwh_ilm_execution_engine;
DROP PACKAGE pck_dwh_ilm_policy_engine;
DROP PACKAGE pck_dwh_table_migration_analyzer;
DROP PACKAGE pck_dwh_table_migration_executor;

-- Step 4: Drop procedures and functions
DROP PROCEDURE dwh_refresh_partition_access_tracking;
DROP PROCEDURE dwh_run_ilm_cycle;
DROP PROCEDURE dwh_start_ilm_jobs;
DROP PROCEDURE dwh_stop_ilm_jobs;
DROP PROCEDURE dwh_run_ilm_job_now;
DROP PROCEDURE dwh_validate_ilm_policy;
DROP PROCEDURE cleanup_execution_logs;
DROP PROCEDURE refresh_partition_access_tracking;
DROP FUNCTION get_dwh_ilm_config;
DROP FUNCTION is_execution_window_open;

-- Step 5: Drop views
DROP VIEW v_ilm_active_policies;
DROP VIEW v_ilm_execution_stats;
DROP VIEW v_ilm_partition_temperature;
DROP VIEW v_ilm_scheduler_status;
DROP VIEW v_ilm_job_history;
DROP VIEW v_dwh_ilm_policy_summary;
DROP VIEW v_dwh_ilm_upcoming_actions;
DROP VIEW v_dwh_ilm_space_savings;
DROP VIEW v_dwh_ilm_execution_history;
DROP VIEW v_dwh_migration_dashboard;
DROP VIEW v_dwh_migration_task_status;
DROP VIEW v_dwh_migration_candidates;
DROP VIEW v_dwh_date_column_analysis;

-- Step 6: Drop tables (⚠ DATA LOSS!)
DROP TABLE cmr.dwh_ilm_evaluation_queue PURGE;
DROP TABLE cmr.dwh_ilm_execution_log PURGE;
DROP TABLE cmr.dwh_ilm_partition_access PURGE;
DROP TABLE cmr.dwh_ilm_policies PURGE;
DROP TABLE cmr.dwh_migration_execution_log PURGE;
DROP TABLE cmr.dwh_migration_analysis PURGE;
DROP TABLE cmr.dwh_migration_tasks PURGE;
DROP TABLE cmr.dwh_migration_ilm_templates PURGE;
DROP TABLE cmr.dwh_migration_projects PURGE;
DROP TABLE cmr.dwh_ilm_config PURGE;

DBMS_OUTPUT.PUT_LINE('✓ Uninstallation complete');
```

---

## Framework Independence

**✅ NEW (as of 2025-10-22):** Both frameworks are now fully independent!

### Supported Installation Scenarios

| Scenario | Result |
|----------|--------|
| Install ILM only | ✅ Works independently |
| Install Migration only | ✅ Works independently |
| Install ILM then Migration | ✅ Both coexist, share config table |
| Install Migration then ILM | ✅ Both coexist, share config table |

### How It Works

Both frameworks share the `cmr.dwh_ilm_config` table for configuration:

1. **ILM setup** creates the config table with ILM-specific settings
2. **Migration setup** creates the config table if it doesn't exist, then adds migration-specific settings
3. **Both frameworks** use MERGE statements (idempotent - safe to re-run)
4. **No installation order required**

### Configuration Keys by Framework

**ILM-specific:**
- `ENABLE_AUTO_EXECUTION`
- `EXECUTION_WINDOW_START`
- `EXECUTION_WINDOW_END`
- `MAX_CONCURRENT_OPERATIONS`
- `HOT_THRESHOLD_DAYS`
- `WARM_THRESHOLD_DAYS`
- `COLD_THRESHOLD_DAYS`
- `ACCESS_TRACKING_ENABLED`
- `LOG_RETENTION_DAYS`

**Migration-specific:**
- `MIGRATION_BACKUP_ENABLED`
- `MIGRATION_VALIDATE_ENABLED`
- `MIGRATION_AUTO_ILM_ENABLED`
- `MIGRATION_PARALLEL_DEGREE`
- `STORAGE_INITIAL_EXTENT`
- `STORAGE_NEXT_EXTENT`
- `STORAGE_EXTENT_MODE`
- `NULL_HANDLING_STRATEGY`
- `NULL_DEFAULT_DATE`, `NULL_DEFAULT_NUMBER`, `NULL_DEFAULT_VARCHAR`
- `INITIAL_PARTITION_BUFFER_MONTHS`
- `FALLBACK_INITIAL_PARTITION_DATE`

---

## Next Steps

After installation:

1. **Read the guides:**
   - [Custom ILM Guide](docs/custom_ilm_guide.md) - Detailed policy configuration
   - [Table Migration Guide](docs/table_migration_guide.md) - Converting tables to partitioned
   - [Partitioning Strategy](docs/partitioning_strategy.md) - Best practices

2. **Review examples:**
   - [Custom ILM Examples](examples/custom_ilm_examples.sql)
   - [Table Migration Examples](examples/table_migration_examples.sql)
   - [Complete Migration Workflow](examples/complete_migration_workflow.sql)

3. **Define policies:**
   - Identify tables for ILM
   - Determine retention requirements
   - Create and test policies

4. **Monitor execution:**
   - Check daily execution logs
   - Review space savings
   - Tune policy thresholds

---

## Support

### Documentation

- [README](README.md) - Project overview
- [Custom ILM Guide](docs/custom_ilm_guide.md) - Complete ILM documentation
- [Quick Reference](docs/quick_reference.md) - Command cheat sheet

### Logs and Diagnostics

```sql
-- Check recent execution failures
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 7
ORDER BY execution_start DESC;

-- Check job execution history
SELECT * FROM v_ilm_job_history
WHERE log_date > SYSDATE - 7
ORDER BY log_date DESC;

-- Check policy effectiveness
SELECT * FROM v_dwh_ilm_space_savings
WHERE execution_date > TRUNC(SYSDATE - 30)
ORDER BY execution_date DESC;
```

### Getting Help

1. Check [troubleshooting section](#troubleshooting) above
2. Review execution logs for specific error messages
3. Verify configuration: `SELECT * FROM cmr.dwh_ilm_config ORDER BY config_key;`
4. Check package status: `SELECT object_name, status FROM user_objects WHERE object_name LIKE 'PCK_DWH%';`

---

**Installation Guide Version 2.0**
**Last Updated:** 2025-10-22
**Changes in v2.0:**
- Updated all procedure names to use `dwh_` prefix
- Documented framework independence (no installation order required)
- Added comprehensive troubleshooting section
- Enhanced verification scripts
- Added functional testing procedure

---

**End of Installation Guide**
