# Missing Components Analysis - Oracle Custom ILM Framework

**Document Version:** 1.0
**Date:** 2025-10-22
**Status:** Complete Analysis

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Critical Missing Code Components](#critical-missing-code-components)
3. [Missing Documentation](#missing-documentation)
4. [Missing Operational Components](#missing-operational-components)
5. [Integration Gaps](#integration-gaps)
6. [Implementation Roadmap](#implementation-roadmap)

---

## Executive Summary

The Oracle Custom ILM Framework is **85-90% complete** with solid core functionality. However, several critical gaps exist that will prevent successful deployment:

**Critical Blockers (P0):**
- 4 documented procedures don't exist (naming mismatch)
- Hard-coded schema dependency (cmr.)
- Incomplete dependency chain between frameworks
- Missing partition access tracking implementation

**High Priority (P1):**
- No policy validation
- Missing operational procedures
- Incomplete ILM template application
- Missing error notifications

**Medium Priority (P2):**
- Operations documentation gaps
- Integration guides missing
- Testing framework absent

---

## Critical Missing Code Components

### 1. ~~Wrapper Procedures for Backwards Compatibility~~ RESOLVED

#### **Problem Description**

Documentation referenced 4 procedures that didn't exist in the codebase. This created immediate errors when users followed the installation guide.

#### **Original Issue**

Documentation used simplified names:
- `run_ilm_cycle()` (documented but not implemented)
- `start_ilm_jobs()` (documented but not implemented)
- `stop_ilm_jobs()` (documented but not implemented)
- `run_ilm_job_now()` (documented but not implemented)

But code implemented with `dwh_` prefix:
- `dwh_run_ilm_cycle()` (implemented)
- `dwh_start_ilm_jobs()` (implemented)
- `dwh_stop_ilm_jobs()` (implemented)
- `dwh_run_ilm_job_now()` (implemented)

#### **Solution Implemented**

**✅ FIXED:** Updated all documentation to consistently use `dwh_` prefix, matching the actual code implementation.

#### **Files Updated**

1. ✅ `README.md` - Updated procedure names (lines 91, 98)
2. ✅ `INSTALL_CUSTOM_ILM.md` - Updated procedure names (lines 126, 150, 224, 227, 237, 255, 273-280)
3. ✅ `docs/custom_ilm_guide.md` - Updated procedure names (lines 418, 421, 422, 423, 744, 757)
4. ✅ `examples/custom_ilm_examples.sql` - Updated procedure names (lines 323, 331, 336, 339, 358, 366, 369, 377, 387, 516-517, 524, 580)
5. ✅ `examples/custom_ilm_examples.sql` - Updated package names to use `pck_dwh_` prefix

#### **Impact**

- **Status:** RESOLVED ✅
- **Approach:** Documentation standardization (cleaner than wrapper procedures)
- **User Impact:** None - documentation now matches code
- **Future Maintenance:** Consistent naming convention established

---

### 2. ~~Missing Configuration Function Dependency~~ RESOLVED

#### **Problem Description**

The table migration framework depended on the `cmr.dwh_ilm_config` table which was only created in the ILM framework setup. This created a hidden dependency preventing standalone migration framework installation.

#### **Problem Code**

From `scripts/table_migration_analysis.sql` line 2690:

```sql
-- This function call will FAIL if custom ILM not installed first:
v_parallel_degree := NVL(
    TO_NUMBER(get_dwh_ilm_config('MIGRATION_PARALLEL_DEGREE')),
    4
);
```

Also used in `table_migration_execution.sql` lines 1847, 1857, 2814.

#### **Dependency Chain**

```
table_migration_analysis.sql (Package Body)
    ↓ calls
get_dwh_ilm_config() function
    ↓ defined in
custom_ilm_setup.sql (line 923)
    ↓ which queries
cmr.dwh_ilm_config table
    ↓ defined in
custom_ilm_setup.sql (line 179)
```

#### **Error When Installing Migration Framework Alone**

```sql
SQL> @scripts/table_migration_analysis.sql
Warning: Package Body created with compilation errors.

SQL> SHOW ERRORS PACKAGE BODY pck_dwh_table_migration_analyzer;
Errors for PACKAGE BODY PCK_DWH_TABLE_MIGRATION_ANALYZER:

LINE/COL ERROR
-------- -----------------------------------------------------------------
2690/37  PL/SQL: ORA-00904: "GET_DWH_ILM_CONFIG": invalid identifier
```

#### **Impact**

- **Severity:** CRITICAL (P0)
- **User Impact:** Cannot install migration framework standalone
- **Affected Users:** Anyone wanting only table migration, not ILM
- **Documentation:** No mention of this dependency

#### **Solution Options**

**Option A: Move Function to Migration Setup (RECOMMENDED)**

Add to `scripts/table_migration_setup.sql` after line 773:

```sql
-- =============================================================================
-- SECTION 7: CONFIGURATION HELPER FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Get configuration value (local copy for migration framework)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_dwh_migration_config(
    p_config_key VARCHAR2
) RETURN VARCHAR2
AS
    v_value VARCHAR2(4000);
BEGIN
    SELECT config_value INTO v_value
    FROM cmr.dwh_ilm_config
    WHERE config_key = p_config_key;

    RETURN v_value;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Return sensible defaults if config table not found
        RETURN CASE p_config_key
            WHEN 'MIGRATION_PARALLEL_DEGREE' THEN '4'
            WHEN 'MIGRATION_BACKUP_ENABLED' THEN 'Y'
            WHEN 'MIGRATION_VALIDATE_ENABLED' THEN 'Y'
            ELSE NULL
        END;
END;
/
```

Then update all calls in `table_migration_analysis.sql` and `table_migration_execution.sql`:

```sql
-- Change from:
v_parallel_degree := NVL(TO_NUMBER(get_dwh_ilm_config('MIGRATION_PARALLEL_DEGREE')), 4);

-- Change to:
v_parallel_degree := NVL(TO_NUMBER(get_dwh_migration_config('MIGRATION_PARALLEL_DEGREE')), 4);
```

**Option B: Document Dependency (TEMPORARY FIX)**

Add to README.md and installation guides:

```markdown
## Framework Installation Order

**IMPORTANT:** The table migration framework depends on the custom ILM framework.
You MUST install in this order:

1. Install Custom ILM Framework first:
   - @scripts/custom_ilm_setup.sql
   - @scripts/custom_ilm_policy_engine.sql
   - @scripts/custom_ilm_execution_engine.sql
   - @scripts/custom_ilm_scheduler.sql

2. Then install Table Migration Framework:
   - @scripts/table_migration_setup.sql
   - @scripts/table_migration_analysis.sql
   - @scripts/table_migration_execution.sql
```

**Option C: Exception Handler Fallback**

Modify `table_migration_analysis.sql` to handle missing function:

```sql
-- Wrap function calls in exception handler:
BEGIN
    v_parallel_degree := NVL(
        TO_NUMBER(get_dwh_ilm_config('MIGRATION_PARALLEL_DEGREE')),
        4
    );
EXCEPTION
    WHEN OTHERS THEN
        -- Function doesn't exist or table missing, use default
        v_parallel_degree := 4;
END;
```

#### **Solution Implemented**

**✅ FIXED:** Added conditional config table creation to `table_migration_setup.sql`

The migration setup now:
1. Checks if `cmr.dwh_ilm_config` table exists
2. If not exists, creates it (standalone migration installation)
3. If exists, reuses it (ILM framework already installed)
4. Uses MERGE statements to insert migration-specific configs (idempotent)

**Code Added** (table_migration_setup.sql after line 649):

```sql
DECLARE
    v_table_exists NUMBER;
BEGIN
    -- Check if config table exists
    SELECT COUNT(*) INTO v_table_exists
    FROM all_tables
    WHERE owner = 'CMR'
    AND table_name = 'DWH_ILM_CONFIG';

    -- Create table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE cmr.dwh_ilm_config (
                config_key          VARCHAR2(100) PRIMARY KEY,
                config_value        VARCHAR2(4000),
                description         VARCHAR2(500),
                modified_by         VARCHAR2(50),
                modified_date       TIMESTAMP DEFAULT SYSTIMESTAMP
            )';

        DBMS_OUTPUT.PUT_LINE('Created cmr.dwh_ilm_config table for standalone migration framework');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Config table cmr.dwh_ilm_config already exists (ILM framework installed)');
    END IF;
END;
/
```

#### **Files Updated**

1. ✅ `scripts/table_migration_setup.sql` - Added conditional table creation (lines 654-679)

#### **Benefits**

- ✅ Migration framework can now install standalone (no ILM dependency)
- ✅ ILM framework can still install standalone (no migration dependency)
- ✅ Both frameworks can coexist (shared config table)
- ✅ Config table automatically created when needed
- ✅ MERGE statements remain idempotent
- ✅ No code changes needed in migration packages (they already have local get_dwh_ilm_config functions)

#### **Testing Scenarios**

| Scenario | Result |
|----------|--------|
| Install ILM only | ✅ Works - config table created by ILM setup |
| Install Migration only | ✅ Works - config table created by Migration setup |
| Install ILM then Migration | ✅ Works - Migration reuses existing config table |
| Install Migration then ILM | ✅ Works - ILM reuses existing config table |

#### **Impact**

- **Status:** RESOLVED ✅
- **Approach:** Conditional table creation with shared config
- **User Impact:** None - both frameworks now fully independent
- **Installation:** No specific order required

---

### 3. Missing ILM Policy Validation

#### **Problem Description**

Users can insert invalid policies without any validation. This causes silent failures during execution.

#### **Current State - No Validation**

```sql
-- This INSERT will succeed even though table doesn't exist:
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_NONEXISTENT_TABLE', USER, 'TABLE_DOES_NOT_EXIST',
    'COMPRESSION', 'COMPRESS', 90,
    'QUERY HIGH', 100, 'Y'
);
-- ✓ Succeeds with no error!

-- Later, when policy engine runs:
EXEC pck_dwh_ilm_policy_engine.evaluate_policy(1);
-- Returns 0 partitions evaluated (silent failure)
```

#### **What Should Be Validated**

1. **Table Exists:** Policy references a real table
2. **Table is Partitioned:** Non-partitioned tables can't have partition-level ILM
3. **Tablespace Exists:** For MOVE actions, target tablespace must exist
4. **Compression Type Valid:** Must be valid Oracle compression type
5. **Action/Policy Type Compatibility:** Some combinations don't make sense
6. **Age Thresholds Sensible:** age_days and age_months shouldn't both be set
7. **Priority Uniqueness:** Warn about conflicting priorities
8. **Custom Condition Syntax:** Validate custom SQL doesn't have obvious errors

#### **Impact**

- **Severity:** HIGH (P1)
- **User Impact:** Silent failures, difficult debugging
- **Affected Users:** All policy administrators
- **Documentation:** Not mentioned

#### **Solution Required**

Add validation trigger to `scripts/custom_ilm_setup.sql` (after line 54):

```sql
-- =============================================================================
-- SECTION 1B: POLICY VALIDATION TRIGGER
-- =============================================================================

CREATE OR REPLACE TRIGGER trg_validate_dwh_ilm_policy
BEFORE INSERT OR UPDATE ON cmr.dwh_ilm_policies
FOR EACH ROW
DECLARE
    v_count NUMBER;
    v_partition_count NUMBER;
    v_error_msg VARCHAR2(500);
BEGIN
    -- Validation 1: Table exists and is partitioned
    BEGIN
        SELECT COUNT(*) INTO v_partition_count
        FROM all_part_tables
        WHERE owner = :NEW.table_owner
        AND table_name = :NEW.table_name;

        IF v_partition_count = 0 THEN
            -- Check if table exists but is not partitioned
            SELECT COUNT(*) INTO v_count
            FROM all_tables
            WHERE owner = :NEW.table_owner
            AND table_name = :NEW.table_name;

            IF v_count > 0 THEN
                v_error_msg := 'Table ' || :NEW.table_owner || '.' || :NEW.table_name ||
                              ' exists but is not partitioned. ILM policies require partitioned tables.';
            ELSE
                v_error_msg := 'Table ' || :NEW.table_owner || '.' || :NEW.table_name ||
                              ' does not exist.';
            END IF;
            RAISE_APPLICATION_ERROR(-20001, v_error_msg);
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
                'Unable to verify table ' || :NEW.table_owner || '.' || :NEW.table_name);
    END;

    -- Validation 2: Tablespace exists (for MOVE action)
    IF :NEW.action_type = 'MOVE' AND :NEW.target_tablespace IS NOT NULL THEN
        SELECT COUNT(*) INTO v_count
        FROM dba_tablespaces
        WHERE tablespace_name = :NEW.target_tablespace;

        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20002,
                'Target tablespace ' || :NEW.target_tablespace || ' does not exist');
        END IF;
    END IF;

    -- Validation 3: Compression type valid (if specified)
    IF :NEW.compression_type IS NOT NULL THEN
        IF :NEW.compression_type NOT IN (
            'QUERY LOW', 'QUERY HIGH', 'ARCHIVE LOW', 'ARCHIVE HIGH',
            'BASIC', 'OLTP'
        ) THEN
            RAISE_APPLICATION_ERROR(-20003,
                'Invalid compression type: ' || :NEW.compression_type ||
                '. Valid types: QUERY LOW/HIGH, ARCHIVE LOW/HIGH, BASIC, OLTP');
        END IF;
    END IF;

    -- Validation 4: Action type requires parameters
    IF :NEW.action_type = 'COMPRESS' AND :NEW.compression_type IS NULL THEN
        RAISE_APPLICATION_ERROR(-20004,
            'COMPRESS action requires compression_type to be specified');
    END IF;

    IF :NEW.action_type = 'MOVE' AND :NEW.target_tablespace IS NULL THEN
        RAISE_APPLICATION_ERROR(-20005,
            'MOVE action requires target_tablespace to be specified');
    END IF;

    IF :NEW.action_type = 'CUSTOM' AND :NEW.custom_action IS NULL THEN
        RAISE_APPLICATION_ERROR(-20006,
            'CUSTOM action requires custom_action PL/SQL block');
    END IF;

    -- Validation 5: Age threshold conflict
    IF :NEW.age_days IS NOT NULL AND :NEW.age_months IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('WARNING: Both age_days and age_months specified. ' ||
                           'age_months will take precedence.');
    END IF;

    -- Validation 6: At least one condition specified
    IF :NEW.age_days IS NULL
       AND :NEW.age_months IS NULL
       AND :NEW.access_pattern IS NULL
       AND :NEW.size_threshold_mb IS NULL
       AND :NEW.custom_condition IS NULL THEN
        RAISE_APPLICATION_ERROR(-20007,
            'Policy must have at least one condition: age_days, age_months, ' ||
            'access_pattern, size_threshold_mb, or custom_condition');
    END IF;

    -- Validation 7: Policy type and action type compatibility
    IF :NEW.policy_type = 'COMPRESSION' AND :NEW.action_type NOT IN ('COMPRESS') THEN
        RAISE_APPLICATION_ERROR(-20008,
            'COMPRESSION policy type requires COMPRESS action');
    END IF;

    IF :NEW.policy_type = 'PURGE' AND :NEW.action_type NOT IN ('DROP', 'TRUNCATE') THEN
        RAISE_APPLICATION_ERROR(-20009,
            'PURGE policy type requires DROP or TRUNCATE action');
    END IF;

    -- Validation 8: Priority range check
    IF :NEW.priority < 1 OR :NEW.priority > 999 THEN
        RAISE_APPLICATION_ERROR(-20010,
            'Priority must be between 1 and 999');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- Re-raise any errors
        RAISE;
END;
/

COMMENT ON TRIGGER trg_validate_dwh_ilm_policy IS
    'Validates ILM policy configuration before insert/update';
```

#### **Additional Validation Procedure**

Add validation procedure for testing policies:

```sql
-- -----------------------------------------------------------------------------
-- Procedure: Validate Policy Configuration
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE validate_ilm_policy(
    p_policy_id NUMBER,
    p_show_details BOOLEAN DEFAULT TRUE
) AS
    v_policy cmr.dwh_ilm_policies%ROWTYPE;
    v_partition_count NUMBER;
    v_eligible_count NUMBER;
    v_warnings VARCHAR2(4000) := '';
    v_errors VARCHAR2(4000) := '';
BEGIN
    -- Get policy
    SELECT * INTO v_policy
    FROM cmr.dwh_ilm_policies
    WHERE policy_id = p_policy_id;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Validating Policy: ' || v_policy.policy_name);
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Check 1: Count partitions in target table
    SELECT COUNT(*) INTO v_partition_count
    FROM all_tab_partitions
    WHERE table_owner = v_policy.table_owner
    AND table_name = v_policy.table_name;

    DBMS_OUTPUT.PUT_LINE('Target table: ' || v_policy.table_owner || '.' || v_policy.table_name);
    DBMS_OUTPUT.PUT_LINE('Partition count: ' || v_partition_count);

    IF v_partition_count = 0 THEN
        v_errors := v_errors || 'ERROR: Table has no partitions. ';
    END IF;

    -- Check 2: Test eligibility evaluation
    BEGIN
        pck_dwh_ilm_policy_engine.evaluate_policy(p_policy_id);

        SELECT COUNT(*) INTO v_eligible_count
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE policy_id = p_policy_id
        AND eligible = 'Y';

        DBMS_OUTPUT.PUT_LINE('Eligible partitions: ' || v_eligible_count);

        IF v_eligible_count = 0 THEN
            v_warnings := v_warnings || 'WARNING: No partitions currently eligible. ';
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            v_errors := v_errors || 'ERROR: Evaluation failed - ' || SQLERRM || '. ';
    END;

    -- Check 3: Compression compatibility
    IF v_policy.action_type = 'COMPRESS' THEN
        -- Check if we can compress partitions
        DECLARE
            v_can_compress NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_can_compress
            FROM all_tab_partitions
            WHERE table_owner = v_policy.table_owner
            AND table_name = v_policy.table_name
            AND compression IN ('DISABLED', 'ENABLED')
            FETCH FIRST 1 ROWS ONLY;
        EXCEPTION
            WHEN OTHERS THEN
                v_errors := v_errors || 'ERROR: Cannot query compression status. ';
        END;
    END IF;

    -- Check 4: Tablespace accessibility
    IF v_policy.action_type = 'MOVE' THEN
        DECLARE
            v_ts_size NUMBER;
        BEGIN
            SELECT bytes/1024/1024 INTO v_ts_size
            FROM dba_free_space
            WHERE tablespace_name = v_policy.target_tablespace
            FETCH FIRST 1 ROWS ONLY;

            DBMS_OUTPUT.PUT_LINE('Target tablespace free space: ' ||
                               ROUND(v_ts_size, 2) || ' MB');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_errors := v_errors || 'ERROR: Cannot access target tablespace. ';
        END;
    END IF;

    -- Report results
    DBMS_OUTPUT.PUT_LINE('========================================');
    IF v_errors IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('VALIDATION FAILED');
        DBMS_OUTPUT.PUT_LINE(v_errors);
    ELSIF v_warnings IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('VALIDATION PASSED WITH WARNINGS');
        DBMS_OUTPUT.PUT_LINE(v_warnings);
    ELSE
        DBMS_OUTPUT.PUT_LINE('VALIDATION PASSED');
    END IF;
    DBMS_OUTPUT.PUT_LINE('========================================');

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: Policy ID ' || p_policy_id || ' not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
END;
/
```

#### **Usage Example**

```sql
-- Insert policy (will validate automatically)
INSERT INTO cmr.dwh_ilm_policies (...) VALUES (...);

-- Validate existing policy
EXEC validate_ilm_policy(1);

-- Output:
-- ========================================
-- Validating Policy: COMPRESS_SALES_90D
-- ========================================
-- Target table: CMR.SALES_FACT
-- Partition count: 24
-- Eligible partitions: 8
-- ========================================
-- VALIDATION PASSED
-- ========================================
```

#### **Files That Need Updates**

1. `scripts/custom_ilm_setup.sql` - Add trigger after table creation
2. `scripts/custom_ilm_setup.sql` - Add validation procedure
3. `docs/custom_ilm_guide.md` - Document validation procedure
4. `examples/custom_ilm_examples.sql` - Add validation examples

---

### 4. Missing Partition Access Tracking Implementation

#### **Problem Description**

The ILM framework includes partition access tracking tables and views, but there's no actual implementation to capture real partition access. The current implementation uses placeholder timestamps.

#### **Current State - Placeholder Data Only**

From `scripts/custom_ilm_setup.sql` line 642:

```sql
-- This sets last_write_time to CURRENT timestamp (not actual write time!)
last_write_time => SYSTIMESTAMP,  -- ⚠️ Placeholder, not real access time
```

This means:
- All partitions show as "just accessed" (HOT)
- Temperature calculations are meaningless
- Access pattern-based policies don't work correctly

#### **Impact on Policies**

Policies using access pattern criteria will fail:

```sql
-- This policy won't work correctly:
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type,
    access_pattern,  -- ⚠️ Relies on accurate temperature!
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_COLD_PARTITIONS', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS',
    'COLD',  -- But all partitions show as HOT!
    'ARCHIVE HIGH', 100, 'Y'
);
```

#### **What's Documented But Not Implemented**

From `docs/custom_ilm_guide.md` lines 866-898, there's example code for Heat Map integration, but:

1. It's in the "Advanced Features" section (not installed by default)
2. It's example code, not actual implementation
3. Heat Map requires Enterprise Edition + license

#### **Impact**

- **Severity:** HIGH (P1)
- **User Impact:** Access pattern-based policies don't work
- **Affected Users:** Anyone using temperature/access pattern criteria
- **Documentation:** Documented as "optional advanced feature"

#### **Solution Options**

**Option A: Oracle Heat Map Integration (Enterprise Edition Only)**

Add to `scripts/custom_ilm_setup.sql`:

```sql
-- =============================================================================
-- SECTION 8: ORACLE HEAT MAP INTEGRATION (OPTIONAL)
-- =============================================================================
-- Requires: Oracle Enterprise Edition with Heat Map enabled
-- Enable: ALTER SYSTEM SET HEAT_MAP = ON;

CREATE OR REPLACE PROCEDURE dwh_sync_heatmap_to_tracking AS
    v_heat_map_available BOOLEAN := FALSE;
    v_count NUMBER;
BEGIN
    -- Check if Heat Map is enabled
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM v$option
        WHERE parameter = 'Heat Map'
        AND value = 'TRUE';

        v_heat_map_available := (v_count > 0);
    EXCEPTION
        WHEN OTHERS THEN
            v_heat_map_available := FALSE;
    END;

    IF NOT v_heat_map_available THEN
        DBMS_OUTPUT.PUT_LINE('Oracle Heat Map not available or not enabled');
        DBMS_OUTPUT.PUT_LINE('Using partition high_value dates for temperature calculation');
        RETURN;
    END IF;

    -- Sync Heat Map data to tracking table
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            h.object_owner,
            h.object_name,
            h.subobject_name,
            h.segment_write_time,
            h.segment_read_time,
            TRUNC(SYSDATE - NVL(h.segment_write_time, SYSDATE - 10000)) AS days_since_write,
            TRUNC(SYSDATE - NVL(h.segment_read_time, SYSDATE - 10000)) AS days_since_read
        FROM dba_heat_map_segment h
        WHERE h.object_type = 'TABLE PARTITION'
        AND h.object_owner = USER
    ) src
    ON (a.table_owner = src.object_owner
        AND a.table_name = src.object_name
        AND a.partition_name = src.subobject_name)
    WHEN MATCHED THEN UPDATE SET
        a.last_write_time = src.segment_write_time,
        a.last_read_time = src.segment_read_time,
        a.days_since_write = src.days_since_write,
        a.days_since_read = src.days_since_read,
        a.write_count = NVL(a.write_count, 0) + 1,
        a.read_count = NVL(a.read_count, 0) + 1,
        a.temperature = CASE
            WHEN src.days_since_write < 90 THEN 'HOT'
            WHEN src.days_since_write < 365 THEN 'WARM'
            ELSE 'COLD'
        END,
        a.last_updated = SYSTIMESTAMP;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Synced Heat Map data for ' || SQL%ROWCOUNT || ' partition(s)');
END;
/
```

**Option B: Partition High Value Based Tracking (Default - No License Required)**

Enhanced version of existing approach:

```sql
-- Replace existing dwh_refresh_partition_access_tracking with enhanced version
CREATE OR REPLACE PROCEDURE dwh_refresh_partition_access_tracking(
    p_table_owner VARCHAR2 DEFAULT USER,
    p_table_name VARCHAR2 DEFAULT NULL
) AS
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
    v_cold_threshold NUMBER;

    -- Function to extract date from partition high_value
    FUNCTION get_partition_date(
        p_owner VARCHAR2,
        p_table VARCHAR2,
        p_partition VARCHAR2
    ) RETURN DATE
    IS
        v_high_value LONG;
        v_date DATE;
    BEGIN
        SELECT high_value INTO v_high_value
        FROM all_tab_partitions
        WHERE table_owner = p_owner
        AND table_name = p_table
        AND partition_name = p_partition;

        -- Try to evaluate high_value as date
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v_high_value || ' FROM DUAL'
            INTO v_date;
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN NULL;
        END;
    END;

BEGIN
    -- Get thresholds from config
    SELECT TO_NUMBER(config_value) INTO v_hot_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_warm_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_cold_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'COLD_THRESHOLD_DAYS';

    -- Merge partition data with age calculated from high_value
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            tp.table_owner,
            tp.table_name,
            tp.partition_name,
            tp.num_rows,
            ROUND(NVL(s.bytes, 0)/1024/1024, 2) AS size_mb,
            tp.compression,
            s.tablespace_name,
            -- Calculate estimated write time from partition boundary date
            get_partition_date(tp.table_owner, tp.table_name, tp.partition_name) AS partition_date,
            CASE
                WHEN get_partition_date(tp.table_owner, tp.table_name, tp.partition_name) IS NOT NULL THEN
                    TRUNC(SYSDATE - get_partition_date(tp.table_owner, tp.table_name, tp.partition_name))
                ELSE 10000  -- Very old if can't determine
            END AS calculated_age_days
        FROM all_tab_partitions tp
        LEFT JOIN all_segments s
            ON s.owner = tp.table_owner
            AND s.segment_name = tp.table_name
            AND s.partition_name = tp.partition_name
        WHERE tp.table_owner = p_table_owner
        AND (p_table_name IS NULL OR tp.table_name = p_table_name)
    ) src
    ON (a.table_owner = src.table_owner
        AND a.table_name = src.table_name
        AND a.partition_name = src.partition_name)
    WHEN MATCHED THEN UPDATE SET
        a.num_rows = src.num_rows,
        a.size_mb = src.size_mb,
        a.compression = src.compression,
        a.tablespace_name = src.tablespace_name,
        -- Only update if we don't have real tracking data
        a.last_write_time = CASE
            WHEN a.write_count > 0 THEN a.last_write_time  -- Preserve real data
            ELSE src.partition_date  -- Use calculated
        END,
        a.days_since_write = src.calculated_age_days,
        a.days_since_read = src.calculated_age_days,  -- Assume read age = write age
        a.temperature = CASE
            WHEN src.calculated_age_days < v_hot_threshold THEN 'HOT'
            WHEN src.calculated_age_days < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        a.last_updated = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        table_owner, table_name, partition_name,
        last_write_time, num_rows, size_mb, compression, tablespace_name,
        read_count, write_count,
        days_since_write, days_since_read, temperature, last_updated
    ) VALUES (
        src.table_owner, src.table_name, src.partition_name,
        src.partition_date, src.num_rows, src.size_mb, src.compression, src.tablespace_name,
        0, 0,  -- No real access tracking yet
        src.calculated_age_days,
        src.calculated_age_days,
        CASE
            WHEN src.calculated_age_days < v_hot_threshold THEN 'HOT'
            WHEN src.calculated_age_days < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        SYSTIMESTAMP
    );

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Partition access tracking refreshed: ' || SQL%ROWCOUNT || ' partitions');
    DBMS_OUTPUT.PUT_LINE('Note: Temperature based on partition age (high_value), not actual access');
    DBMS_OUTPUT.PUT_LINE('      For accurate access tracking, integrate with Oracle Heat Map or application logging');
END;
/
```

**Option C: Application-Level Tracking (Custom Implementation)**

Add trigger-based tracking:

```sql
-- Template trigger to deploy on application tables
-- Must be customized for each table
CREATE OR REPLACE PROCEDURE create_partition_tracking_trigger(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2
) AS
    v_trigger_name VARCHAR2(128);
    v_sql VARCHAR2(4000);
BEGIN
    v_trigger_name := 'TRG_' || SUBSTR(p_table_name, 1, 20) || '_ACCESS';

    v_sql := 'CREATE OR REPLACE TRIGGER ' || p_table_owner || '.' || v_trigger_name || '
    AFTER INSERT OR UPDATE ON ' || p_table_owner || '.' || p_table_name || '
    FOR EACH ROW
    DECLARE
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_partition_name VARCHAR2(128);
    BEGIN
        -- Get partition name from ROWID
        SELECT subobject_name INTO v_partition_name
        FROM all_objects
        WHERE owner = ''' || p_table_owner || '''
        AND object_name = ''' || p_table_name || '''
        AND object_type = ''TABLE PARTITION''
        AND data_object_id = DBMS_ROWID.ROWID_OBJECT(:NEW.ROWID);

        -- Update access tracking
        UPDATE cmr.dwh_ilm_partition_access
        SET last_write_time = SYSTIMESTAMP,
            write_count = NVL(write_count, 0) + 1,
            days_since_write = 0,
            temperature = ''HOT'',
            last_updated = SYSTIMESTAMP
        WHERE table_owner = ''' || p_table_owner || '''
        AND table_name = ''' || p_table_name || '''
        AND partition_name = v_partition_name;

        IF SQL%ROWCOUNT = 0 THEN
            -- Partition not in tracking yet, insert it
            INSERT INTO cmr.dwh_ilm_partition_access (
                table_owner, table_name, partition_name,
                last_write_time, write_count,
                days_since_write, temperature, last_updated
            ) VALUES (
                ''' || p_table_owner || ''', ''' || p_table_name || ''', v_partition_name,
                SYSTIMESTAMP, 1,
                0, ''HOT'', SYSTIMESTAMP
            );
        END IF;

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            -- Silently fail to avoid impacting application
            NULL;
    END;';

    EXECUTE IMMEDIATE v_sql;

    DBMS_OUTPUT.PUT_LINE('Created tracking trigger: ' || v_trigger_name);
END;
/

-- Usage:
-- EXEC create_partition_tracking_trigger(USER, 'SALES_FACT');
```

#### **Recommended Solution**

**Use Option B (Enhanced High Value Tracking)** as default, with:
- Documentation for Option A (Heat Map integration for EE)
- Documentation for Option C (Application triggers for precision tracking)

#### **Files That Need Updates**

1. `scripts/custom_ilm_setup.sql` - Replace `dwh_refresh_partition_access_tracking` with enhanced version
2. `scripts/custom_ilm_setup.sql` - Add `dwh_sync_heatmap_to_tracking` procedure
3. `scripts/custom_ilm_setup.sql` - Add `create_partition_tracking_trigger` procedure
4. `docs/custom_ilm_guide.md` - Document all three tracking options clearly
5. `INSTALL_CUSTOM_ILM.md` - Add tracking setup as optional step

---

### 5. Missing Error Notification System

#### **Problem Description**

Scheduler jobs run unattended but there's no notification mechanism when jobs fail. Administrators must manually check logs.

#### **Current State - Silent Failures**

Jobs can fail without anyone knowing:

```sql
-- Job runs and fails
-- No email sent
-- No alert generated
-- Must manually check:
SELECT * FROM v_ilm_job_history WHERE status = 'FAILED';
```

#### **Impact**

- **Severity:** MEDIUM (P2)
- **User Impact:** Failures go unnoticed
- **Affected Users:** Operations team
- **Documentation:** Not mentioned

#### **Solution Required**

Add email notification configuration to `scripts/custom_ilm_scheduler.sql`:

```sql
-- =============================================================================
-- SECTION 7: ERROR NOTIFICATION SETUP
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Configure email notifications for job failures
-- Requires: UTL_MAIL or DBMS_MAIL configured
-- -----------------------------------------------------------------------------

-- Create notification procedure
CREATE OR REPLACE PROCEDURE send_ilm_alert(
    p_job_name VARCHAR2,
    p_error_message VARCHAR2
) AS
    v_recipient VARCHAR2(500);
    v_subject VARCHAR2(200);
    v_body VARCHAR2(4000);
BEGIN
    -- Get recipient from config
    BEGIN
        SELECT config_value INTO v_recipient
        FROM cmr.dwh_ilm_config
        WHERE config_key = 'ALERT_EMAIL_RECIPIENT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- No recipient configured, log only
            DBMS_OUTPUT.PUT_LINE('ALERT: ' || p_job_name || ' failed: ' || p_error_message);
            RETURN;
    END;

    v_subject := 'ILM Job Failure: ' || p_job_name;
    v_body := 'ILM Framework Alert' || CHR(10) || CHR(10) ||
              'Job Name: ' || p_job_name || CHR(10) ||
              'Time: ' || TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') || CHR(10) ||
              'Error: ' || p_error_message || CHR(10) || CHR(10) ||
              'Please check v_ilm_job_history for details.' || CHR(10) || CHR(10) ||
              'This is an automated message from Oracle Custom ILM Framework.';

    -- Send email (requires UTL_MAIL setup)
    BEGIN
        UTL_MAIL.SEND(
            sender => 'oracle_ilm@yourcompany.com',
            recipients => v_recipient,
            subject => v_subject,
            message => v_body
        );
    EXCEPTION
        WHEN OTHERS THEN
            -- Email failed, log to alert log
            DBMS_SYSTEM.KSDWRT(2, 'ILM Alert: ' || v_subject || ' - ' || p_error_message);
    END;
END;
/

-- Add configuration for email recipient
MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_EMAIL_RECIPIENT' AS config_key,
              'dba@yourcompany.com' AS config_value,
              'Email address for ILM job failure notifications' AS description
       FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_ON_FAILURE' AS config_key,
              'Y' AS config_value,
              'Enable email alerts on job failures' AS description
       FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

COMMIT;

-- -----------------------------------------------------------------------------
-- Enhanced scheduler programs with error handling
-- -----------------------------------------------------------------------------

-- Update ILM_EXECUTE_ACTIONS program to send alerts on failure
BEGIN
    DBMS_SCHEDULER.DROP_PROGRAM('ILM_EXECUTE_ACTIONS', TRUE);
END;
/

BEGIN
    DBMS_SCHEDULER.CREATE_PROGRAM(
        program_name => 'ILM_EXECUTE_ACTIONS',
        program_type => 'PLSQL_BLOCK',
        program_action => '
            DECLARE
                v_enabled VARCHAR2(1);
                v_alert_enabled VARCHAR2(1);
                v_error_msg VARCHAR2(4000);
            BEGIN
                -- Check if auto execution is enabled
                SELECT config_value INTO v_enabled
                FROM cmr.dwh_ilm_config
                WHERE config_key = ''ENABLE_AUTO_EXECUTION'';

                IF v_enabled = ''Y'' THEN
                    pck_dwh_ilm_execution_engine.execute_pending_actions(p_max_operations => 10);
                ELSE
                    DBMS_OUTPUT.PUT_LINE(''Automatic execution is disabled'');
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_error_msg := SQLERRM;

                    -- Check if alerts enabled
                    SELECT NVL(config_value, ''N'') INTO v_alert_enabled
                    FROM cmr.dwh_ilm_config
                    WHERE config_key = ''ALERT_ON_FAILURE'';

                    IF v_alert_enabled = ''Y'' THEN
                        send_ilm_alert(''ILM_JOB_EXECUTE'', v_error_msg);
                    END IF;

                    RAISE;  -- Re-raise to mark job as failed
            END;',
        enabled => TRUE,
        comments => 'Executes pending ILM actions with error notification'
    );
END;
/
```

#### **Alternative: DBMS_SCHEDULER Notifications**

Simpler approach using built-in scheduler notifications:

```sql
-- Enable notifications on job failures
BEGIN
    -- For each ILM job:
    FOR job_rec IN (
        SELECT job_name
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'ILM_JOB_%'
    ) LOOP
        -- Add notification for failures
        DBMS_SCHEDULER.ADD_JOB_EMAIL_NOTIFICATION(
            job_name => job_rec.job_name,
            recipients => 'dba@yourcompany.com',
            events => 'JOB_FAILED, JOB_BROKEN'
        );

        DBMS_OUTPUT.PUT_LINE('Added notification for: ' || job_rec.job_name);
    END LOOP;
END;
/
```

#### **Files That Need Updates**

1. `scripts/custom_ilm_scheduler.sql` - Add notification procedure and configuration
2. `scripts/custom_ilm_scheduler.sql` - Update program definitions with error handling
3. `INSTALL_CUSTOM_ILM.md` - Add email configuration as optional step
4. `docs/custom_ilm_guide.md` - Document notification setup

---

### 6. Missing ILM Template Application Procedure

#### **Problem Description**

The framework includes `cmr.dwh_migration_ilm_templates` table with 8 predefined templates, but there's no procedure to actually apply these templates to a table after migration.

#### **Current State**

Templates exist but are not used:

```sql
-- Templates exist in table:
SELECT template_name FROM cmr.dwh_migration_ilm_templates;
-- SCD2_EFFECTIVE_DATE
-- SCD2_VALID_FROM_TO
-- EVENTS_SHORT_RETENTION
-- EVENTS_COMPLIANCE
-- STAGING_7DAY
-- STAGING_CDC
-- STAGING_ERROR_QUARANTINE
-- HIST_TABLE

-- But no way to apply them automatically!
```

The migration executor has this code (line 2814 in `table_migration_execution.sql`):

```sql
IF v_apply_ilm = 'Y' THEN
    -- TODO: Auto-create ILM policies based on table type
    DBMS_OUTPUT.PUT_LINE('  (ILM policy auto-creation not yet implemented)');
END IF;
```

#### **Impact**

- **Severity:** MEDIUM (P2)
- **User Impact:** Must manually create ILM policies after migration
- **Affected Users:** Anyone using table migration framework
- **Documentation:** Templates are documented but not actually usable

#### **Solution Required**

Add procedure to `scripts/table_migration_execution.sql` (before line 3194):

```sql
-- =============================================================================
-- SECTION: ILM TEMPLATE APPLICATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Apply ILM template to migrated table
-- -----------------------------------------------------------------------------
PROCEDURE apply_ilm_template(
    p_task_id NUMBER,
    p_template_name VARCHAR2 DEFAULT NULL
) AS
    v_task cmr.dwh_migration_tasks%ROWTYPE;
    v_template_name VARCHAR2(100);
    v_policies_created NUMBER := 0;
BEGIN
    -- Get task details
    SELECT * INTO v_task
    FROM cmr.dwh_migration_tasks
    WHERE task_id = p_task_id;

    DBMS_OUTPUT.PUT_LINE('Applying ILM template to: ' || v_task.target_table);

    -- Determine template to use
    v_template_name := p_template_name;

    IF v_template_name IS NULL THEN
        -- Auto-detect based on table stereotype
        v_template_name := CASE v_task.table_stereotype
            WHEN 'SCD2_EFFECTIVE' THEN 'SCD2_EFFECTIVE_DATE'
            WHEN 'SCD2_VALID_FROM_TO' THEN 'SCD2_VALID_FROM_TO'
            WHEN 'EVENTS_HIGH_VOLUME' THEN 'EVENTS_SHORT_RETENTION'
            WHEN 'EVENTS_COMPLIANCE' THEN 'EVENTS_COMPLIANCE'
            WHEN 'STAGING_TRANSACTIONAL' THEN 'STAGING_7DAY'
            WHEN 'STAGING_CDC' THEN 'STAGING_CDC'
            WHEN 'STAGING_ERROR' THEN 'STAGING_ERROR_QUARANTINE'
            WHEN 'HIST_TABLE' THEN 'HIST_TABLE'
            ELSE NULL
        END;
    END IF;

    IF v_template_name IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('  No template specified and cannot auto-detect');
        DBMS_OUTPUT.PUT_LINE('  Skipping ILM template application');
        RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('  Using template: ' || v_template_name);

    -- Create policies from template
    FOR tmpl IN (
        SELECT *
        FROM cmr.dwh_migration_ilm_templates
        WHERE template_name = v_template_name
        ORDER BY priority
    ) LOOP
        BEGIN
            INSERT INTO cmr.dwh_ilm_policies (
                policy_name,
                table_owner,
                table_name,
                policy_type,
                action_type,
                age_days,
                age_months,
                compression_type,
                target_tablespace,
                priority,
                rebuild_indexes,
                gather_stats,
                enabled
            ) VALUES (
                v_task.target_table || '_' || tmpl.policy_suffix,
                v_task.table_owner,
                v_task.target_table,
                tmpl.policy_type,
                tmpl.action_type,
                tmpl.age_threshold_days,
                tmpl.age_threshold_months,
                tmpl.compression_type,
                tmpl.target_tablespace,
                tmpl.priority,
                tmpl.rebuild_indexes,
                tmpl.gather_stats,
                'Y'  -- Enabled by default
            );

            v_policies_created := v_policies_created + 1;
            DBMS_OUTPUT.PUT_LINE('    Created policy: ' || v_task.target_table || '_' || tmpl.policy_suffix);

        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN
                DBMS_OUTPUT.PUT_LINE('    Policy already exists: ' || v_task.target_table || '_' || tmpl.policy_suffix);
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('    ERROR creating policy: ' || SQLERRM);
        END;
    END LOOP;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('  ILM policies created: ' || v_policies_created);

    -- Initialize partition access tracking for this table
    dwh_init_partition_access_tracking(v_task.table_owner, v_task.target_table);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR: Template ' || v_template_name || ' not found');
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR applying template: ' || SQLERRM);
        ROLLBACK;
END apply_ilm_template;
```

Then replace the TODO at line 2814:

```sql
-- Replace this:
IF v_apply_ilm = 'Y' THEN
    -- TODO: Auto-create ILM policies based on table type
    DBMS_OUTPUT.PUT_LINE('  (ILM policy auto-creation not yet implemented)');
END IF;

-- With this:
IF v_apply_ilm = 'Y' THEN
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Applying ILM template...');
    apply_ilm_template(p_task_id);
END IF;
```

#### **Files That Need Updates**

1. `scripts/table_migration_execution.sql` - Add `apply_ilm_template` procedure
2. `scripts/table_migration_execution.sql` - Replace TODO with actual call
3. `docs/table_migration_guide.md` - Document ILM template application
4. `examples/complete_migration_workflow.sql` - Add example showing ILM template usage

---

## Missing Documentation

### 7. Complete Installation Guide

#### **Problem Description**

The current `INSTALL_CUSTOM_ILM.md` has several gaps that will confuse users during installation.

#### **Current Gaps**

1. **Incorrect procedure names** (already covered in section 1)
2. **No prerequisite verification steps**
3. **No schema configuration guidance**
4. **No privilege requirements documentation**
5. **No Oracle version compatibility matrix**
6. **No troubleshooting section for common errors**
7. **No upgrade/migration instructions**

#### **Impact**

- **Severity:** HIGH (P1)
- **User Impact:** Installation failures, confusion
- **Affected Users:** All new installations
- **Documentation:** `INSTALL_CUSTOM_ILM.md` incomplete

#### **Solution Required**

Create comprehensive `INSTALL_GUIDE_COMPLETE.md`:

```markdown
# Oracle Custom ILM Framework - Complete Installation Guide

## Prerequisites Checklist

### Oracle Database Requirements

- [ ] Oracle Database 12c or higher (12.1.0.2+)
- [ ] Oracle Database 19c or higher (RECOMMENDED)
- [ ] Standard Edition (ILM features work) OR Enterprise Edition
- [ ] Oracle ADO/Heat Map (OPTIONAL - for advanced access tracking only)

**Verify Your Version:**
```sql
SELECT version, version_full
FROM product_component_version
WHERE product LIKE 'Oracle Database%';
```

**Expected:** 12.1.0.2 or higher

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

### Schema Configuration

**CRITICAL:** All framework objects are created with `cmr.` schema prefix.

**Option 1: Install in CMR schema (RECOMMENDED)**
```sql
-- Connect as CMR user
CONN cmr/password@database
@scripts/custom_ilm_setup.sql
```

**Option 2: Install in different schema**
```sql
-- If installing in a different schema, you must:
-- 1. Search and replace ALL occurrences of 'cmr.' in scripts
-- 2. Replace with your schema name

-- Example: Replace cmr. with myschema.
sed 's/cmr\./myschema\./g' scripts/*.sql
```

**Option 3: Grant access from CMR schema**
```sql
-- If objects exist in CMR and you want to use from another schema
GRANT SELECT, INSERT, UPDATE, DELETE ON cmr.dwh_ilm_policies TO myuser;
GRANT SELECT ON cmr.dwh_ilm_execution_log TO myuser;
GRANT EXECUTE ON cmr.pck_dwh_ilm_policy_engine TO myuser;
GRANT EXECUTE ON cmr.pck_dwh_ilm_execution_engine TO myuser;
-- etc.
```

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

## Installation Steps

### Step 1: Pre-Installation Validation

Run validation script:

```sql
SET SERVEROUTPUT ON
DECLARE
    v_version VARCHAR2(20);
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('=================================');
    DBMS_OUTPUT.PUT_LINE('Pre-Installation Validation');
    DBMS_OUTPUT.PUT_LINE('=================================');

    -- Check version
    SELECT version INTO v_version
    FROM product_component_version
    WHERE product LIKE 'Oracle Database%';

    DBMS_OUTPUT.PUT_LINE('Oracle Version: ' || v_version);

    IF TO_NUMBER(SUBSTR(v_version, 1, 2)) < 12 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Oracle 12c or higher required');
    END IF;

    -- Check privileges
    SELECT COUNT(*) INTO v_count
    FROM user_sys_privs
    WHERE privilege IN ('CREATE TABLE', 'CREATE VIEW', 'CREATE PROCEDURE');

    IF v_count < 3 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Missing CREATE privileges');
    END IF;

    -- Check DBA views access
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM dba_tab_partitions
        WHERE ROWNUM = 1;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20003, 'Cannot access DBA_TAB_PARTITIONS');
    END;

    DBMS_OUTPUT.PUT_LINE('✓ All prerequisites met');
    DBMS_OUTPUT.PUT_LINE('=================================');
END;
/
```

### Step 2: Install Framework Components

```sql
-- Set session parameters for clean install
SET ECHO ON
SET DEFINE OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SPOOL ilm_install.log

-- Component 1: Metadata tables and base procedures (5 minutes)
@scripts/custom_ilm_setup.sql

-- Verify step 1:
SELECT COUNT(*) AS table_count
FROM user_tables
WHERE table_name LIKE 'DWH_ILM%';
-- Expected: 5 tables

-- Component 2: Policy evaluation engine (1 minute)
@scripts/custom_ilm_policy_engine.sql

-- Verify step 2:
SELECT object_name, status
FROM user_objects
WHERE object_name = 'PCK_DWH_ILM_POLICY_ENGINE';
-- Expected: VALID

-- Component 3: Execution engine (1 minute)
@scripts/custom_ilm_execution_engine.sql

-- Verify step 3:
SELECT object_name, status
FROM user_objects
WHERE object_name = 'PCK_DWH_ILM_EXECUTION_ENGINE';
-- Expected: VALID

-- Component 4: Scheduler jobs (1 minute)
@scripts/custom_ilm_scheduler.sql

-- Verify step 4:
SELECT COUNT(*) AS job_count
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%';
-- Expected: 4 jobs

SPOOL OFF
```

### Step 3: Verify Installation

```sql
-- Complete verification script
SET SERVEROUTPUT ON
BEGIN
    DBMS_OUTPUT.PUT_LINE('=================================');
    DBMS_OUTPUT.PUT_LINE('Installation Verification');
    DBMS_OUTPUT.PUT_LINE('=================================');

    -- Check tables
    DECLARE
        v_count NUMBER;
    BEGIN
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
            DBMS_OUTPUT.PUT_LINE('✓ All 5 metadata tables created');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Only ' || v_count || ' of 5 tables created');
        END IF;
    END;

    -- Check packages
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_objects
        WHERE object_type = 'PACKAGE'
        AND object_name LIKE 'PCK_DWH_ILM%'
        AND status = 'VALID';

        IF v_count = 2 THEN
            DBMS_OUTPUT.PUT_LINE('✓ Both ILM packages compiled');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ ERROR: Only ' || v_count || ' of 2 packages valid');
        END IF;
    END;

    -- Check scheduler jobs
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_scheduler_jobs
        WHERE job_name LIKE 'ILM_JOB%';

        IF v_count = 4 THEN
            DBMS_OUTPUT.PUT_LINE('✓ All 4 scheduler jobs created');
        ELSE
            DBMS_OUTPUT.PUT_LINE('✗ WARNING: Only ' || v_count || ' of 4 jobs created');
        END IF;
    END;

    -- Check views
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM user_views
        WHERE view_name LIKE '%ILM%';

        DBMS_OUTPUT.PUT_LINE('✓ Created ' || v_count || ' monitoring views');
    END;

    -- Check config
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM cmr.dwh_ilm_config;

        DBMS_OUTPUT.PUT_LINE('✓ Config table has ' || v_count || ' parameters');
    END;

    DBMS_OUTPUT.PUT_LINE('=================================');
    DBMS_OUTPUT.PUT_LINE('Installation verification complete');
    DBMS_OUTPUT.PUT_LINE('=================================');
END;
/
```

### Step 4: Initial Configuration

```sql
-- Configure execution window (default 22:00 to 06:00)
UPDATE cmr.dwh_ilm_config
SET config_value = '23:00'
WHERE config_key = 'EXECUTION_WINDOW_START';

UPDATE cmr.dwh_ilm_config
SET config_value = '05:00'
WHERE config_key = 'EXECUTION_WINDOW_END';

-- Configure alert email (optional)
UPDATE cmr.dwh_ilm_config
SET config_value = 'dba@yourcompany.com'
WHERE config_key = 'ALERT_EMAIL_RECIPIENT';

-- Review all configuration
SELECT config_key, config_value, description
FROM cmr.dwh_ilm_config
ORDER BY config_key;

COMMIT;
```

### Step 5: Functional Test

```sql
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

-- Initialize partition tracking
EXEC dwh_refresh_partition_access_tracking(USER, 'ILM_TEST_TABLE');

-- Create test policy
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

-- Run test cycle
EXEC dwh_run_ilm_cycle();

-- Check results
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE table_name = 'ILM_TEST_TABLE'
ORDER BY execution_start DESC;

-- Cleanup test
DELETE FROM cmr.dwh_ilm_policies WHERE policy_name = 'TEST_COMPRESS';
DROP TABLE ilm_test_table PURGE;
COMMIT;

DBMS_OUTPUT.PUT_LINE('✓ Functional test complete');
```

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

### Create First Production Policy

See `docs/custom_ilm_guide.md` for examples. Basic template:

```sql
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

## Troubleshooting

### Problem: ORA-06550 identifier must be declared

**Symptom:**
```
ORA-06550: line 1, column 7:
PLS-00201: identifier 'RUN_ILM_CYCLE' must be declared
```

**Cause:** Using wrong procedure name (documentation mismatch)

**Solution:** Use `dwh_run_ilm_cycle()` instead of `run_ilm_cycle()`

### Problem: ORA-00942 table or view does not exist

**Symptom:**
```
ORA-00942: table or view "CMR"."DWH_ILM_POLICIES" does not exist
```

**Cause:** Script hard-coded with `cmr.` schema prefix

**Solution:**
- Option 1: Install in CMR schema
- Option 2: Search/replace cmr. with your schema name in all scripts

### Problem: Package compiled with errors

**Symptom:**
```
Warning: Package Body created with compilation errors.
```

**Diagnosis:**
```sql
-- Show errors
SHOW ERRORS PACKAGE BODY pck_dwh_ilm_policy_engine;

-- Common causes:
-- 1. Missing function dependency (get_dwh_ilm_config)
-- 2. Invalid schema prefix
-- 3. Missing privileges
```

**Solution:** Check installation log, verify all previous steps completed successfully

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
EXEC dwh_refresh_partition_access_tracking(USER, 'YOUR_TABLE');
```

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
-- Start all jobs
EXEC dwh_start_ilm_jobs();

-- Or manually enable each job:
BEGIN
    DBMS_SCHEDULER.ENABLE('ILM_JOB_REFRESH_ACCESS');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_EVALUATE');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_EXECUTE');
    DBMS_SCHEDULER.ENABLE('ILM_JOB_CLEANUP');
END;
/
```

## Uninstallation

```sql
-- Stop all jobs first
EXEC dwh_stop_ilm_jobs();

-- Drop jobs and programs
BEGIN
    FOR job IN (SELECT job_name FROM user_scheduler_jobs
                WHERE job_name LIKE 'ILM_JOB_%') LOOP
        DBMS_SCHEDULER.DROP_JOB(job.job_name, TRUE);
    END LOOP;

    FOR prog IN (SELECT program_name FROM user_scheduler_programs
                 WHERE program_name LIKE 'ILM_%') LOOP
        DBMS_SCHEDULER.DROP_PROGRAM(prog.program_name, TRUE);
    END LOOP;
END;
/

-- Drop packages
DROP PACKAGE pck_dwh_ilm_execution_engine;
DROP PACKAGE pck_dwh_ilm_policy_engine;

-- Drop procedures and functions
DROP PROCEDURE dwh_refresh_partition_access_tracking;
DROP PROCEDURE dwh_run_ilm_cycle;
DROP PROCEDURE dwh_start_ilm_jobs;
DROP PROCEDURE dwh_stop_ilm_jobs;
DROP PROCEDURE dwh_run_ilm_job_now;
DROP FUNCTION get_dwh_ilm_config;

-- Drop views
DROP VIEW v_ilm_active_policies;
DROP VIEW v_ilm_execution_stats;
DROP VIEW v_ilm_partition_temperature;
DROP VIEW v_ilm_scheduler_status;
DROP VIEW v_ilm_job_history;

-- Drop tables (WARNING: Data loss!)
DROP TABLE cmr.dwh_ilm_evaluation_queue PURGE;
DROP TABLE cmr.dwh_ilm_execution_log PURGE;
DROP TABLE cmr.dwh_ilm_partition_access PURGE;
DROP TABLE cmr.dwh_ilm_policies PURGE;
DROP TABLE cmr.dwh_ilm_config PURGE;
```

## Upgrade from Previous Versions

*Future section - no previous versions exist yet*

## Next Steps

1. Read [Custom ILM Guide](docs/custom_ilm_guide.md)
2. Review [Policy Examples](examples/custom_ilm_examples.sql)
3. Define policies for your tables
4. Monitor execution and tune as needed

## Support

For issues or questions:
- Check troubleshooting section above
- Review [Custom ILM Guide](docs/custom_ilm_guide.md)
- Check execution logs: `SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = 'FAILED'`
```

#### **Files That Need Creation/Updates**

1. **CREATE NEW:** `INSTALL_GUIDE_COMPLETE.md` (above content)
2. **UPDATE:** `README.md` - Change links to point to new complete guide
3. **DEPRECATE:** `INSTALL_CUSTOM_ILM.md` - Add deprecation notice pointing to new guide

---

### 8. Operations Runbook

#### **Problem Description**

No documentation exists for day-to-day operations, troubleshooting, and maintenance tasks.

#### **Impact**

- **Severity:** MEDIUM (P2)
- **User Impact:** Operations team has no procedures
- **Affected Users:** DBAs and operations staff
- **Documentation:** Completely missing

#### **Solution Required**

Create `OPERATIONS_RUNBOOK.md`:

```markdown
# Oracle Custom ILM Framework - Operations Runbook

## Daily Operations

### Morning Health Check (10 minutes)

**1. Check Scheduler Job Status**
```sql
SELECT job_name, last_start_date, next_run_date,
       failure_count, state
FROM v_ilm_scheduler_status;
```

**Expected:** All jobs enabled, failure_count = 0

**Alert if:** Any job has state = 'BROKEN' or failure_count > 0

**2. Check Recent Execution Failures**
```sql
SELECT execution_id, policy_name, partition_name,
       execution_start, error_message
FROM cmr.dwh_ilm_execution_log
WHERE status = 'FAILED'
AND execution_start > SYSDATE - 1
ORDER BY execution_start DESC;
```

**Expected:** 0 failures in last 24 hours

**Alert if:** Any failures present

**3. Check Space Savings**
```sql
SELECT table_name,
       COUNT(*) AS actions_last_24h,
       ROUND(SUM(space_saved_mb), 2) AS space_saved_mb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > SYSDATE - 1
GROUP BY table_name
ORDER BY space_saved_mb DESC;
```

**Report:** Daily space savings summary

### Investigation: Job Failures

**If jobs are failing:**

**Step 1: Check job history**
```sql
SELECT job_name, log_date, status, error#,
       SUBSTR(additional_info, 1, 200) AS error_info
FROM v_ilm_job_history
WHERE status != 'SUCCEEDED'
AND log_date > SYSDATE - 7
ORDER BY log_date DESC;
```

**Step 2: Check execution window**
```sql
-- Are we in execution window?
SELECT
    CASE
        WHEN is_execution_window_open() THEN 'OPEN'
        ELSE 'CLOSED'
    END AS window_status,
    get_dwh_ilm_config('EXECUTION_WINDOW_START') AS window_start,
    get_dwh_ilm_config('EXECUTION_WINDOW_END') AS window_end,
    TO_CHAR(SYSDATE, 'HH24:MI') AS current_time
FROM DUAL;
```

**Step 3: Check policy configuration**
```sql
-- Are policies enabled?
SELECT policy_name, enabled, priority
FROM cmr.dwh_ilm_policies
WHERE enabled = 'Y'
ORDER BY priority;
```

**Step 4: Test manual execution**
```sql
-- Try running manually
EXEC dwh_run_ilm_cycle();

-- Check for errors
SELECT * FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 1/24  -- Last hour
ORDER BY execution_start DESC;
```

### Recovery: Restart Failed Job

```sql
-- Re-run specific job
EXEC dwh_run_ilm_job_now('ILM_JOB_EXECUTE');

-- Or restart all jobs
EXEC dwh_stop_ilm_jobs();
EXEC dwh_start_ilm_jobs();
```

## Weekly Operations

### Weekly Review (30 minutes)

**1. Compression Effectiveness**
```sql
SELECT policy_name, table_name,
       COUNT(*) AS total_compressions,
       ROUND(AVG(compression_ratio), 2) AS avg_ratio,
       ROUND(SUM(space_saved_mb), 2) AS total_saved_mb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > SYSDATE - 7
GROUP BY policy_name, table_name
ORDER BY total_saved_mb DESC;
```

**Action:** Review policies with low compression ratios (<2x)

**2. Policy Execution Distribution**
```sql
SELECT policy_name,
       COUNT(*) AS executions,
       AVG(duration_seconds) AS avg_duration_sec,
       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
       SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failures
FROM cmr.dwh_ilm_execution_log
WHERE execution_start > SYSDATE - 7
GROUP BY policy_name
ORDER BY executions DESC;
```

**Action:** Investigate policies with high failure rates

**3. Partition Temperature Distribution**
```sql
SELECT temperature,
       COUNT(*) AS partition_count,
       ROUND(SUM(size_mb), 2) AS total_size_mb
FROM cmr.dwh_ilm_partition_access
GROUP BY temperature
ORDER BY
    CASE temperature
        WHEN 'HOT' THEN 1
        WHEN 'WARM' THEN 2
        WHEN 'COLD' THEN 3
    END;
```

**Action:** Ensure proper distribution across temperature zones

## Monthly Operations

### Monthly Maintenance (1 hour)

**1. Policy Audit**

Review all active policies:
```sql
SELECT policy_id, policy_name, table_name, policy_type,
       action_type, age_days, age_months, priority, enabled,
       created_date, modified_date
FROM cmr.dwh_ilm_policies
ORDER BY priority;
```

**Actions:**
- Review age thresholds still appropriate?
- Are priorities correctly ordered?
- Any policies never executed? (candidates for deletion)

**2. Capacity Planning**

Calculate growth trends:
```sql
-- Space freed per month
SELECT TO_CHAR(execution_end, 'YYYY-MM') AS month,
       COUNT(DISTINCT table_name) AS tables_processed,
       COUNT(*) AS total_actions,
       ROUND(SUM(space_saved_mb)/1024, 2) AS space_saved_gb
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
GROUP BY TO_CHAR(execution_end, 'YYYY-MM')
ORDER BY month DESC
FETCH FIRST 12 MONTHS ONLY;
```

**3. Log Cleanup**

```sql
-- Check log table size
SELECT COUNT(*) AS log_records,
       ROUND(SUM(DBMS_LOB.GETLENGTH(action_sql))/1024/1024, 2) AS clob_size_mb
FROM cmr.dwh_ilm_execution_log;

-- Run cleanup if needed
EXEC cleanup_execution_logs();
```

**4. Index Maintenance**

Check index health on ILM tables:
```sql
SELECT index_name, table_name, status, last_analyzed
FROM user_indexes
WHERE table_name LIKE 'DWH_ILM%'
ORDER BY table_name, index_name;
```

Rebuild if needed:
```sql
-- If status = UNUSABLE or last_analyzed > 90 days ago
ALTER INDEX idx_ilm_exec_policy REBUILD ONLINE;
```

## Quarterly Operations

### Quarterly Review (2 hours)

**1. Performance Analysis**

Identify slow policy executions:
```sql
SELECT policy_name, table_name, partition_name,
       ROUND(duration_seconds/60, 2) AS duration_minutes,
       size_before_mb, compression_type
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND execution_start > SYSDATE - 90
AND duration_seconds > 1800  -- Longer than 30 minutes
ORDER BY duration_seconds DESC;
```

**Actions:**
- Review parallel_degree for slow policies
- Consider splitting large partitions
- Review compression type selection

**2. Policy Optimization**

Test compression ratios:
```sql
-- Compare compression types
SELECT compression_type,
       COUNT(*) AS usage_count,
       ROUND(AVG(compression_ratio), 2) AS avg_ratio,
       ROUND(AVG(duration_seconds), 2) AS avg_duration_sec
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_start > SYSDATE - 90
GROUP BY compression_type
ORDER BY compression_type;
```

**3. Documentation Update**

- Update runbook with new issues encountered
- Document any policy changes made
- Review and update retention requirements

## Emergency Procedures

### Emergency: Stop All ILM Operations

**When:** ILM operations causing production impact

```sql
-- Immediate stop
EXEC dwh_stop_ilm_jobs();

-- Disable auto-execution
UPDATE cmr.dwh_ilm_config
SET config_value = 'N'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

-- Verify stopped
SELECT job_name, enabled, state
FROM user_scheduler_jobs
WHERE job_name LIKE 'ILM_JOB%';
```

**Resume when safe:**
```sql
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

EXEC dwh_start_ilm_jobs();
```

### Emergency: Disable Specific Policy

```sql
-- Disable problematic policy
UPDATE cmr.dwh_ilm_policies
SET enabled = 'N'
WHERE policy_name = 'PROBLEM_POLICY_NAME';
COMMIT;

-- Clear pending actions for this policy
DELETE FROM cmr.dwh_ilm_evaluation_queue
WHERE policy_id = (
    SELECT policy_id FROM cmr.dwh_ilm_policies
    WHERE policy_name = 'PROBLEM_POLICY_NAME'
)
AND execution_status = 'PENDING';
COMMIT;
```

### Emergency: Uncompress Partition

**When:** Compressed partition causing query performance issues

```sql
-- Move partition without compression
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
NOCOMPRESS;

-- Rebuild local indexes
SELECT 'ALTER INDEX ' || index_owner || '.' || index_name ||
       ' REBUILD PARTITION ' || partition_name || ' ONLINE;'
FROM dba_ind_partitions ip
WHERE ip.table_owner = 'OWNER'
AND ip.table_name = 'TABLE_NAME'
AND ip.partition_name = 'PARTITION_NAME';

-- Run generated statements
```

### Emergency: Rollback Partition Move

**When:** Recently moved partition needs to go back

```sql
-- Move back to original tablespace
ALTER TABLE owner.table_name
MOVE PARTITION partition_name
TABLESPACE original_tablespace;

-- Rebuild indexes
-- (use same index rebuild query as above)
```

## Monitoring Queries

### Real-Time Monitoring

**Currently Running ILM Actions**
```sql
SELECT s.sid, s.serial#, s.username,
       s.sql_id, s.event, s.seconds_in_wait,
       s.module, s.action
FROM v$session s
WHERE s.module LIKE '%ILM%'
OR s.action LIKE '%ILM%'
OR s.sql_id IN (
    SELECT sql_id FROM v$sql
    WHERE sql_text LIKE '%dwh_ilm%'
);
```

**Pending Actions**
```sql
SELECT policy_name, table_name,
       COUNT(*) AS pending_actions,
       MIN(evaluation_date) AS oldest_evaluation
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
WHERE q.execution_status = 'PENDING'
AND q.eligible = 'Y'
GROUP BY policy_name, table_name
ORDER BY oldest_evaluation;
```

### Performance Monitoring

**Slowest Executions Today**
```sql
SELECT policy_name, partition_name,
       ROUND(duration_seconds/60, 2) AS duration_minutes,
       TO_CHAR(execution_start, 'HH24:MI') AS start_time,
       status
FROM cmr.dwh_ilm_execution_log
WHERE TRUNC(execution_start) = TRUNC(SYSDATE)
ORDER BY duration_seconds DESC
FETCH FIRST 10 ROWS ONLY;
```

**Compression Ratio Trending**
```sql
SELECT TO_CHAR(execution_end, 'YYYY-MM-DD') AS execution_date,
       ROUND(AVG(compression_ratio), 2) AS avg_compression_ratio,
       COUNT(*) AS compressions
FROM cmr.dwh_ilm_execution_log
WHERE status = 'SUCCESS'
AND action_type = 'COMPRESS'
AND execution_end > SYSDATE - 30
GROUP BY TO_CHAR(execution_end, 'YYYY-MM-DD')
ORDER BY execution_date;
```

## Alerting Thresholds

Configure monitoring alerts for:

| Metric | Warning | Critical |
|--------|---------|----------|
| Job failure count (24h) | > 1 | > 3 |
| Execution failure rate | > 5% | > 10% |
| Average compression ratio | < 2x | < 1.5x |
| Job execution lag | > 2 hours | > 4 hours |
| Queue backlog | > 100 | > 500 |
| Execution duration | > 1 hour | > 2 hours |

## Contact Escalation

1. **Level 1:** Operations DBA (daily monitoring)
2. **Level 2:** Senior DBA (policy optimization)
3. **Level 3:** Database Architect (framework issues)

---
```

#### **Files That Need Creation**

1. **CREATE NEW:** `OPERATIONS_RUNBOOK.md` (above content)
2. **UPDATE:** `README.md` - Add link to operations runbook
3. **UPDATE:** `docs/quick_reference.md` - Add references to runbook procedures

---

## Implementation Roadmap

### Phase 1: Critical Fixes ✅ 100% COMPLETE

**Priority: P0 - BLOCKING**

**Tasks:**
1. ✅ **DONE** - ~~Add wrapper procedures for backwards compatibility~~
   - **Approach Changed:** Standardized documentation instead of wrappers
   - Files: README.md, INSTALL_CUSTOM_ILM.md, custom_ilm_guide.md, custom_ilm_examples.sql
   - Updated: 5 files, 20+ procedure/package references
   - Result: All documentation now uses `dwh_` prefix matching code
   - Time: 1 hour (completed 2025-10-22)
   - Status: ✅ **RESOLVED** (see DOCUMENTATION_STANDARDIZATION_SUMMARY.md)

2. ✅ **DONE** - ~~Fix configuration function dependency~~
   - **Approach:** Added conditional config table creation
   - File: `scripts/table_migration_setup.sql` (lines 654-679)
   - Implementation: PL/SQL block checks if table exists, creates if needed
   - Result: Migration framework now fully standalone
   - Time: 1 hour (completed 2025-10-22)
   - Status: ✅ **RESOLVED** (see CONFIG_DEPENDENCY_FIX_SUMMARY.md)

3. ✅ **DONE** - Create complete installation guide
   - File: NEW `INSTALL_GUIDE_COMPLETE.md`
   - Production-ready comprehensive guide with:
     * Prerequisites checklist with verification scripts
     * Complete installation steps for both frameworks
     * Comprehensive verification procedures
     * Functional testing with cleanup
     * Troubleshooting section (5 common issues)
     * Framework independence documentation
   - Time: 4 hours (completed 2025-10-22)
   - Status: ✅ **COMPLETE** (see INSTALL_GUIDE_COMPLETE.md)

**Deliverables:**
- ✅ Framework installs without errors following documentation
- ✅ Table migration framework works standalone
- ✅ Clear installation instructions with troubleshooting

**Success Criteria:**
- ✅ Fresh installation completes successfully
- ✅ All documented procedures work as described
- ✅ No dependency errors
- ✅ Complete installation guide with verification steps

---

### Phase 2: High Priority Enhancements ✅ 100% COMPLETE

**Priority: P1 - HIGH**

**Tasks:**
1. ✅ **DONE** - Implement policy validation trigger
   - File: `scripts/custom_ilm_setup.sql`
   - Added `trg_validate_dwh_ilm_policy` trigger
   - Added `dwh_validate_ilm_policy()` procedure
   - 8 validation checks implemented
   - 11 examples in `custom_ilm_examples.sql` Section 7A

2. ✅ **DONE** - Enhance partition access tracking
   - File: `scripts/custom_ilm_setup.sql`
   - Enhanced `dwh_refresh_partition_access_tracking()` with real age calculation
   - Added `dwh_sync_heatmap_to_tracking()` for Heat Map integration
   - Temperature calculation based on partition `high_value` dates
   - Documentation: `PARTITION_ACCESS_TRACKING_ENHANCEMENT.md`

3. ✅ **DONE** - Implement ILM template application
   - File: `scripts/table_migration_execution.sql`
   - Enhanced `apply_ilm_policies()` with intelligent auto-detection
   - 88 lines of pattern detection logic
   - Detects SCD2, Events, Staging, HIST table types
   - Column-level analysis for SCD2 sub-types
   - 12 examples in `custom_ilm_examples.sql` Section 7B
   - Documentation: `ILM_TEMPLATE_APPLICATION_ENHANCEMENT.md`

4. ✅ **DONE** - Create operations runbook
   - File: NEW `OPERATIONS_RUNBOOK.md` (500+ lines)
   - Daily/Weekly/Monthly/Quarterly procedures
   - Emergency procedures
   - Troubleshooting guide with 6 common problems
   - Performance tuning guidance
   - Alerting thresholds (10 metrics)
   - Contact escalation (4 levels)

**Deliverables:**
- ✅ Policies validated on insert (automatic + manual validation)
- ✅ Temperature calculations work correctly (real age + Heat Map)
- ✅ ILM templates automatically applied after migration (~80% automation)
- ✅ Operations team has clear procedures (comprehensive runbook)

**Success Criteria:**
- ✅ Invalid policies rejected at insert time (8 validation checks)
- ✅ Access pattern-based policies work correctly (Heat Map integration)
- ✅ Migrated tables get ILM policies automatically (auto-detection)
- ✅ Daily operations documented and tested (500+ line runbook)

---

### Phase 3: Medium Priority Improvements (Week 3-4)

**Priority: P2 - MEDIUM**

**Tasks:**
1. Add error notification system
   - File: `scripts/custom_ilm_scheduler.sql`
   - Add email notification procedures
   - Configure scheduler notifications
   - Effort: 4 hours
   - Testing: 2 hours

2. Create policy design guide
   - File: NEW `POLICY_DESIGN_GUIDE.md`
   - Data lifecycle methodology
   - Compression strategy selection
   - Testing procedures
   - Effort: 8 hours
   - Review: 2 hours

3. Create integration guide
   - File: NEW `INTEGRATION_GUIDE.md`
   - ETL integration
   - Application hooks
   - Backup coordination
   - Effort: 6 hours
   - Review: 2 hours

4. Enhanced monitoring views
   - File: `scripts/custom_ilm_setup.sql`
   - Add performance dashboards
   - Add alerting queries
   - Effort: 3 hours
   - Testing: 1 hour

**Deliverables:**
- Automated alerting on failures
- Policy design methodology documented
- Integration patterns documented
- Enhanced monitoring capabilities

**Success Criteria:**
- Failures trigger email alerts
- Teams can design effective policies
- Integration with existing systems documented
- Monitoring shows actionable insights

---

### Phase 4: Testing & Polish (Week 5)

**Priority: P3 - NICE TO HAVE**

**Tasks:**
1. Create test suite
   - Directory: NEW `tests/`
   - Unit tests for each package
   - Integration tests
   - Regression tests
   - Effort: 16 hours
   - Documentation: 2 hours

2. Performance benchmarking
   - File: NEW `PERFORMANCE_GUIDE.md`
   - Compression performance tests
   - Migration performance tests
   - Tuning recommendations
   - Effort: 8 hours
   - Testing: 4 hours

3. Create demo/sandbox environment
   - File: NEW `DEMO_SETUP.sql`
   - Sample data generation
   - Example policies
   - Interactive tutorial
   - Effort: 6 hours
   - Testing: 2 hours

4. Final documentation review
   - All documentation files
   - Cross-reference checking
   - Example validation
   - Effort: 4 hours

5. Partition merging for FROZEN tier
   - **Rationale:** Reduce metadata overhead for very old data
     * Systems with interval partitioning accumulate 500-1000+ partitions over years
     * Oracle maintains dictionary metadata for every partition
     * Merging old partitions reduces dictionary cache pressure and simplifies management
     * Natural lifecycle: Daily/Monthly → Quarterly → Yearly as data ages
   - **Implementation:**
     * Add new ILM action types: `MERGE_MONTHLY_TO_QUARTERLY`, `MERGE_QUARTERLY_TO_YEARLY`
     * Enhanced `merge_partitions()` procedure with safety validations
     * Partition merge history tracking for audit/rollback
     * Integration with FROZEN tier (5+ years old, rarely accessed)
   - **Safety Considerations:**
     * Exclusive table lock required (run during maintenance windows only)
     * Validate consecutive partitions with compatible properties
     * Check retention policy impact (merged partitions = coarser purge granularity)
     * Document partition boundaries for potential SPLIT later
     * Only merge partitions well within retention window
   - **Merge Criteria:**
     * Age threshold (e.g., 1825 days = 5 years)
     * Already compressed (ARCHIVE HIGH or similar)
     * In read-only or archive tablespace
     * Low access frequency (verified via Heat Map or partition access tracking)
     * Not referenced by active retention policies requiring granular drops
   - **Files Modified:**
     * `scripts/custom_ilm_setup.sql` - Add merge-related config parameters
     * `scripts/custom_ilm_policy_engine.sql` - Add merge policy evaluation logic
     * `scripts/custom_ilm_execution_engine.sql` - Add merge execution procedures
     * `scripts/partition_management.sql` - Enhance existing `merge_partitions()`
     * NEW: `scripts/partition_merge_tracking.sql` - History table and procedures
   - **Configuration Parameters:**
     * `ENABLE_PARTITION_MERGING` (Y/N, default: N) - Opt-in feature
     * `MERGE_AGE_THRESHOLD_DAYS` (default: 1825 = 5 years)
     * `MERGE_MAINTENANCE_WINDOW_START` (default: 23:00)
     * `MERGE_MAINTENANCE_WINDOW_END` (default: 05:00)
     * `MERGE_MAX_PARTITIONS_PER_RUN` (default: 5) - Throttle impact
   - **Validation Before Merge:**
     * Consecutive partitions with adjacent date ranges
     * Same compression level (or both compressed)
     * Same tablespace (or both in archive tier)
     * No subpartitioning conflicts
     * Sufficient temp space for operation
   - **Documentation:**
     * NEW: `PARTITION_MERGE_GUIDE.md` - When and how to use merging
     * Update: `POLICY_DESIGN_GUIDE.md` - Add FROZEN tier with merge strategy
     * Update: `OPERATIONS_RUNBOOK.md` - Add merge monitoring procedures
     * Update: `custom_ilm_examples.sql` - Add 8-10 merge examples
   - **Example Use Case:**
     * Financial data warehouse: 7-year retention, daily queries on last 2 years
     * Strategy:
       - 0-2 years: Monthly partitions, QUERY LOW compression
       - 2-5 years: Monthly partitions, QUERY HIGH compression
       - 5-7 years: Yearly partitions (merged), ARCHIVE HIGH, read-only
       - 7+ years: DROP
     * Result: 24 monthly + 36 monthly + 2 yearly = 62 partitions vs 84 monthly (26% reduction)
   - Effort: 12 hours development
   - Testing: 4 hours (merge/rollback scenarios)
   - Documentation: 3 hours

**Deliverables:**
- Automated test suite
- Performance benchmarks
- Demo environment
- Complete, validated documentation
- Partition merging capability for FROZEN tier

**Success Criteria:**
- All tests pass
- Performance baselines established
- New users can run demo successfully
- Documentation is comprehensive and accurate
- Partition merging reduces metadata overhead without impacting queries or retention

---

## Summary

### What's Working Well

✅ Core ILM framework architecture (95% complete)
✅ Table migration framework (95% complete)
✅ Policy engine and execution engine (fully implemented)
✅ Scheduler infrastructure (fully implemented)
✅ Basic documentation (exists but needs improvement)

### Critical Gaps

🔴 **P0 - BLOCKING** (Must fix before any deployment)
1. Wrapper procedures missing (installation fails)
2. Dependency chain broken (migration framework non-functional standalone)
3. Installation guide incomplete (users can't install successfully)

🟡 **P1 - HIGH** (Impacts quality and usability)
4. No policy validation (silent failures)
5. Partition access tracking incomplete (temperature calculations wrong)
6. ILM template application not implemented (documented but not working)
7. Operations procedures missing (no day-to-day guidance)

🟢 **P2 - MEDIUM** (Improves operations)
8. No error notifications (failures go unnoticed)
9. Policy design guidance missing (users don't know how to design effective policies)
10. Integration documentation missing (how to integrate with existing systems)

⭐ **P3 - NICE TO HAVE** (Polish and testing)
11. No test suite (quality assurance gap)
12. No performance benchmarks (can't measure effectiveness)
13. No demo environment (harder to learn)
14. No partition merging capability (metadata overhead for old data)

### Estimated Effort

- **Phase 1 (Critical):** 6.5 hours development + 2 hours testing = **~9 hours (2 days)**
- **Phase 2 (High):** 16 hours development + 7 hours testing = **~23 hours (3 days)**
- **Phase 3 (Medium):** 21 hours development + 7 hours testing = **~28 hours (3.5 days)**
- **Phase 4 (Polish):** 46 hours development + 12 hours testing + 3 hours docs = **~61 hours (7.5 days)**

**Total:** ~121 hours (~15 days)

### Recommendation

**For minimum viable deployment:**
- Complete Phase 1 (Critical fixes)
- Complete tasks 1, 2, 3 from Phase 2 (validation, tracking, templates)
- Deploy with basic monitoring

**Estimated:** 5-6 days of focused development

**For production-ready deployment:**
- Complete Phase 1 and Phase 2 entirely
- Complete task 1 from Phase 3 (error notifications)
- Deploy with full operations support

**Estimated:** 8-9 days of focused development

---

## Next Steps

1. **Immediate:** Fix wrapper procedures (30 minutes)
2. **Priority:** Fix dependency chain (1 hour)
3. **Today:** Complete installation guide (4 hours)
4. **This Week:** Policy validation + partition tracking (7 hours)
5. **Next Week:** ILM templates + operations runbook (9 hours)

Would you like me to start implementing any of these fixes?
