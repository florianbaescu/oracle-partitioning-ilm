# ILM Threshold Profiles - Implementation Plan

**Feature:** Configurable threshold profiles for flexible temperature-based ILM policies
**Date:** 2025-10-23
**Version:** v3.1
**Status:** PLANNING

---

## Executive Summary

Add support for reusable threshold profiles that define HOT/WARM/COLD age boundaries. Policies can reference a profile for custom thresholds or fall back to global defaults.

**Current State:**
- Global thresholds in `dwh_ilm_config` (HOT=90, WARM=365, COLD=1095 days)
- All partitions classified using same thresholds
- Policies filter by pre-calculated temperature via `access_pattern` field

**Target State:**
- Global thresholds remain as defaults
- New `dwh_ilm_threshold_profiles` table stores reusable profiles
- Policies can reference a profile for custom thresholds
- Temperature calculation respects policy-specific or global thresholds

---

## Phase 1: Database Schema Changes

### 1.1 Create New Table: `dwh_ilm_threshold_profiles`

**Location:** `scripts/custom_ilm_setup.sql` (after dwh_ilm_config table)

```sql
-- -----------------------------------------------------------------------------
-- ILM Threshold Profiles
-- -----------------------------------------------------------------------------
-- Reusable threshold profiles for temperature-based ILM classification
-- Policies can reference a profile or fall back to global config
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_threshold_profiles (
    profile_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    profile_name        VARCHAR2(100) NOT NULL UNIQUE,
    description         VARCHAR2(500),

    -- Temperature thresholds in days
    hot_threshold_days  NUMBER NOT NULL,
    warm_threshold_days NUMBER NOT NULL,
    cold_threshold_days NUMBER NOT NULL,

    -- Audit fields
    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
    modified_by         VARCHAR2(50),
    modified_date       TIMESTAMP,

    -- Validation: ensure thresholds are in ascending order
    CONSTRAINT chk_profile_thresholds CHECK (
        hot_threshold_days < warm_threshold_days
        AND warm_threshold_days < cold_threshold_days
    )
);

CREATE INDEX idx_ilm_profiles_name ON cmr.dwh_ilm_threshold_profiles(profile_name);

COMMENT ON TABLE cmr.dwh_ilm_threshold_profiles IS
    'Reusable threshold profiles for ILM temperature-based classification';
```

### 1.2 Insert Default Profiles

```sql
-- Insert commonly used threshold profiles
MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'DEFAULT' AS profile_name,
              'Standard aging profile (matches global config)' AS description,
              90 AS hot_threshold_days,
              365 AS warm_threshold_days,
              1095 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'FAST_AGING' AS profile_name,
              'Fast aging for transactional data (sales, orders)' AS description,
              30 AS hot_threshold_days,
              90 AS warm_threshold_days,
              180 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'SLOW_AGING' AS profile_name,
              'Slow aging for reference/master data' AS description,
              180 AS hot_threshold_days,
              730 AS warm_threshold_days,
              1825 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'AGGRESSIVE_ARCHIVE' AS profile_name,
              'Aggressive archival for high-volume data' AS description,
              14 AS hot_threshold_days,
              30 AS warm_threshold_days,
              90 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);
```

### 1.3 Alter `dwh_ilm_policies` Table

```sql
-- Add foreign key to threshold profiles
ALTER TABLE cmr.dwh_ilm_policies
ADD (
    threshold_profile_id NUMBER,
    CONSTRAINT fk_ilm_policy_profile
        FOREIGN KEY (threshold_profile_id)
        REFERENCES cmr.dwh_ilm_threshold_profiles(profile_id)
);

CREATE INDEX idx_ilm_policies_profile ON cmr.dwh_ilm_policies(threshold_profile_id);
```

**Migration Note:** Existing policies will have `threshold_profile_id = NULL`, which means use global config (backwards compatible).

---

## Phase 2: Core Logic Updates

### 2.1 Create Helper Function: `get_policy_thresholds`

**Location:** `scripts/custom_ilm_setup.sql` (SECTION 4: UTILITY FUNCTIONS)

```sql
-- -----------------------------------------------------------------------------
-- Get Threshold Values for a Policy
-- -----------------------------------------------------------------------------
-- Returns HOT, WARM, COLD thresholds for a given policy
-- Uses policy's profile if specified, otherwise global config
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_policy_thresholds(
    p_policy_id NUMBER,
    p_threshold_type VARCHAR2 -- 'HOT', 'WARM', or 'COLD'
) RETURN NUMBER
AS
    v_profile_id NUMBER;
    v_threshold NUMBER;
BEGIN
    -- Get profile_id for this policy
    SELECT threshold_profile_id INTO v_profile_id
    FROM cmr.dwh_ilm_policies
    WHERE policy_id = p_policy_id;

    IF v_profile_id IS NOT NULL THEN
        -- Use profile thresholds
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_id = v_profile_id;
    ELSE
        -- Use global config
        SELECT TO_NUMBER(config_value) INTO v_threshold
        FROM cmr.dwh_ilm_config
        WHERE config_key = p_threshold_type || '_THRESHOLD_DAYS';
    END IF;

    RETURN v_threshold;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Fallback to global config
        SELECT TO_NUMBER(config_value) INTO v_threshold
        FROM cmr.dwh_ilm_config
        WHERE config_key = p_threshold_type || '_THRESHOLD_DAYS';
        RETURN v_threshold;
    WHEN OTHERS THEN
        -- Default fallback values
        RETURN CASE p_threshold_type
            WHEN 'HOT' THEN 90
            WHEN 'WARM' THEN 365
            WHEN 'COLD' THEN 1095
        END;
END get_policy_thresholds;
/
```

### 2.2 Update Policy Evaluation Logic

**Location:** `scripts/custom_ilm_policy_engine.sql`

**Function:** `is_partition_eligible` - Update temperature matching logic

**Change:** When checking `access_pattern`, calculate temperature using policy-specific thresholds

```sql
-- Current code (line ~230):
IF v_policy.access_pattern IS NOT NULL THEN
    IF v_temperature != v_policy.access_pattern THEN
        -- Skip
    END IF;
END IF;

-- New code:
IF v_policy.access_pattern IS NOT NULL THEN
    -- Calculate temperature using policy-specific thresholds
    v_hot_threshold := get_policy_thresholds(v_policy.policy_id, 'HOT');
    v_warm_threshold := get_policy_thresholds(v_policy.policy_id, 'WARM');

    v_policy_temperature := CASE
        WHEN v_age_days < v_hot_threshold THEN 'HOT'
        WHEN v_age_days < v_warm_threshold THEN 'WARM'
        ELSE 'COLD'
    END;

    IF v_policy_temperature != v_policy.access_pattern THEN
        -- Skip: partition temperature doesn't match policy requirement
        v_eligible := FALSE;
        v_reason := v_reason || 'Partition temperature (' || v_policy_temperature ||
                   ') does not match required ' || v_policy.access_pattern;
        RETURN;
    END IF;
END IF;
```

### 2.3 Update Partition Access Tracking

**Location:** `scripts/custom_ilm_setup.sql`

**Procedures to update:**
- `dwh_refresh_partition_access_tracking` - Keep using global thresholds for generic classification
- `dwh_init_partition_access_tracking` - Keep using global thresholds

**Note:** The `temperature` field in `dwh_ilm_partition_access` table remains calculated using global thresholds. This is for monitoring/display. Policy evaluation recalculates temperature using policy-specific thresholds.

---

## Phase 3: View Updates

### 3.1 Views Requiring Updates

**No changes needed** - Views use global thresholds for monitoring/display purposes:
- `dwh_v_ilm_upcoming_actions` - Generic recommendations
- `dwh_v_ilm_partition_lifecycle` - Generic lifecycle classification

### 3.2 New View: Policy Threshold Overview

**Location:** `scripts/custom_ilm_setup.sql`

```sql
-- -----------------------------------------------------------------------------
-- ILM Policy Threshold View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_policy_thresholds AS
SELECT
    p.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    p.threshold_profile_id,
    prof.profile_name,
    -- Show effective thresholds
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.hot_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS')
    END AS effective_hot_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.warm_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS')
    END AS effective_warm_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN prof.cold_threshold_days
        ELSE (SELECT TO_NUMBER(config_value) FROM cmr.dwh_ilm_config WHERE config_key = 'COLD_THRESHOLD_DAYS')
    END AS effective_cold_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN 'CUSTOM'
        ELSE 'GLOBAL'
    END AS threshold_source
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_threshold_profiles prof
    ON prof.profile_id = p.threshold_profile_id;

COMMENT ON TABLE cmr.dwh_v_ilm_policy_thresholds IS
    'Shows effective threshold values for each ILM policy';
```

---

## Phase 4: Migration Script

**New file:** `scripts/migration/upgrade_to_v3.1_threshold_profiles.sql`

```sql
-- =============================================================================
-- Migration Script: Upgrade to v3.1 - Threshold Profiles
-- =============================================================================
-- Adds threshold profiles feature to existing ILM installation
-- Safe to run multiple times (idempotent)
-- =============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED

PROMPT ========================================
PROMPT Upgrading to v3.1: Threshold Profiles
PROMPT ========================================
PROMPT

-- Step 1: Create threshold profiles table
PROMPT Step 1: Creating dwh_ilm_threshold_profiles table...

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_ilm_threshold_profiles (
            profile_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            profile_name        VARCHAR2(100) NOT NULL UNIQUE,
            description         VARCHAR2(500),
            hot_threshold_days  NUMBER NOT NULL,
            warm_threshold_days NUMBER NOT NULL,
            cold_threshold_days NUMBER NOT NULL,
            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
            modified_by         VARCHAR2(50),
            modified_date       TIMESTAMP,
            CONSTRAINT chk_profile_thresholds CHECK (
                hot_threshold_days < warm_threshold_days
                AND warm_threshold_days < cold_threshold_days
            )
        )';
    DBMS_OUTPUT.PUT_LINE('✓ Created table dwh_ilm_threshold_profiles');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Table dwh_ilm_threshold_profiles already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 2: Create index
PROMPT Step 2: Creating indexes...

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_profiles_name ON cmr.dwh_ilm_threshold_profiles(profile_name)';
    DBMS_OUTPUT.PUT_LINE('✓ Created index idx_ilm_profiles_name');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Index idx_ilm_profiles_name already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 3: Insert default profiles
PROMPT Step 3: Inserting default threshold profiles...

-- [Insert MERGE statements from Phase 1.2]

PROMPT ✓ Inserted 4 default profiles

-- Step 4: Alter policies table
PROMPT Step 4: Adding threshold_profile_id column to dwh_ilm_policies...

DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM user_tab_columns
    WHERE table_name = 'DWH_ILM_POLICIES'
    AND column_name = 'THRESHOLD_PROFILE_ID';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_policies ADD (threshold_profile_id NUMBER)';
        DBMS_OUTPUT.PUT_LINE('✓ Added column threshold_profile_id');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Column threshold_profile_id already exists');
    END IF;
END;
/

-- Step 5: Create foreign key
PROMPT Step 5: Creating foreign key constraint...

BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE cmr.dwh_ilm_policies
                       ADD CONSTRAINT fk_ilm_policy_profile
                       FOREIGN KEY (threshold_profile_id)
                       REFERENCES cmr.dwh_ilm_threshold_profiles(profile_id)';
    DBMS_OUTPUT.PUT_LINE('✓ Created foreign key fk_ilm_policy_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -2275 THEN
            DBMS_OUTPUT.PUT_LINE('  Foreign key fk_ilm_policy_profile already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 6: Create index on FK
PROMPT Step 6: Creating index on foreign key...

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_policies_profile ON cmr.dwh_ilm_policies(threshold_profile_id)';
    DBMS_OUTPUT.PUT_LINE('✓ Created index idx_ilm_policies_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('  Index idx_ilm_policies_profile already exists');
        ELSE
            RAISE;
        END IF;
END;
/

-- Step 7: Create helper function
PROMPT Step 7: Creating get_policy_thresholds function...

@@ -- Execute function creation from Phase 2.1

PROMPT ✓ Created function get_policy_thresholds

-- Step 8: Create new view
PROMPT Step 8: Creating dwh_v_ilm_policy_thresholds view...

@@ -- Execute view creation from Phase 3.2

PROMPT ✓ Created view dwh_v_ilm_policy_thresholds

-- Step 9: Update packages
PROMPT Step 9: Recompiling ILM packages with threshold profile support...

@scripts/custom_ilm_policy_engine.sql

PROMPT ✓ Recompiled pck_dwh_ilm_policy_engine

PROMPT
PROMPT ========================================
PROMPT Migration to v3.1 Complete!
PROMPT ========================================
PROMPT
PROMPT Summary:
PROMPT   - Added dwh_ilm_threshold_profiles table
PROMPT   - Added 4 default profiles (DEFAULT, FAST_AGING, SLOW_AGING, AGGRESSIVE_ARCHIVE)
PROMPT   - Extended dwh_ilm_policies with threshold_profile_id
PROMPT   - Created get_policy_thresholds() function
PROMPT   - Created dwh_v_ilm_policy_thresholds view
PROMPT   - Updated policy evaluation logic
PROMPT
PROMPT Next Steps:
PROMPT   1. Review default profiles: SELECT * FROM cmr.dwh_ilm_threshold_profiles;
PROMPT   2. View effective thresholds: SELECT * FROM cmr.dwh_v_ilm_policy_thresholds;
PROMPT   3. Optionally assign profiles to existing policies:
PROMPT      UPDATE cmr.dwh_ilm_policies SET threshold_profile_id = X WHERE ...;
PROMPT
```

---

## Phase 5: Testing Strategy

### 5.1 Unit Tests

**Test file:** `tests/unit_tests_threshold_profiles.sql`

```sql
-- Test 1: Create threshold profile
-- Test 2: Policy with NULL profile uses global config
-- Test 3: Policy with profile uses profile values
-- Test 4: Profile constraint validation (hot < warm < cold)
-- Test 5: get_policy_thresholds function returns correct values
-- Test 6: Policy evaluation respects profile thresholds
-- Test 7: View shows effective thresholds correctly
```

### 5.2 Integration Tests

**Test scenarios:**

1. **Scenario: Fast Aging Profile**
   - Create FAST_AGING profile (30/90/180)
   - Create policy with profile and access_pattern='WARM'
   - Create partition that is 100 days old
   - Verify: Partition matches (100 > 90, temperature=COLD per profile)

2. **Scenario: Global Fallback**
   - Create policy without profile (NULL)
   - Verify: Uses global config (90/365/1095)

3. **Scenario: Profile Change**
   - Update profile thresholds
   - Verify: All policies using profile reflect changes

### 5.3 Performance Tests

- Measure query performance of views with threshold subqueries
- Verify get_policy_thresholds() function performance
- Test with 1000+ policies using different profiles

---

## Phase 6: Documentation Updates

### 6.1 Update Existing Docs

**File:** `docs/custom_ilm_guide.md`

**Sections to update:**

1. **Policy Configuration** - Add threshold profile explanation
2. **Temperature-Based Policies** - Explain profile vs global thresholds
3. **Configuration Reference** - Add threshold profiles section

**File:** `README.md`

**Section:** Features - Add "Configurable threshold profiles"

**File:** `CHANGELOG.md`

**Add:** v3.1 release notes

### 6.2 New Documentation

**File:** `docs/guides/THRESHOLD_PROFILES_GUIDE.md`

```markdown
# ILM Threshold Profiles Guide

## Overview
Threshold profiles allow you to define reusable HOT/WARM/COLD age boundaries
for different data lifecycle patterns.

## When to Use Profiles

### Use Global Config When:
- All data ages at similar rates
- Simple, uniform lifecycle management
- Single organizational standard

### Use Threshold Profiles When:
- Different tables have different aging characteristics
- Transactional data vs reference data
- Regulatory requirements vary by data type
- Multi-tenant with different SLAs

## Default Profiles

### DEFAULT (90/365/1095)
Standard aging profile matching global config defaults.
Use for general-purpose data.

### FAST_AGING (30/90/180)
For high-velocity transactional data:
- Sales transactions
- Order history
- Clickstream data
- IOT sensor data

### SLOW_AGING (180/730/1825)
For stable reference/master data:
- Customer master data
- Product catalogs
- Configuration tables
- Slowly changing dimensions

### AGGRESSIVE_ARCHIVE (14/30/90)
For high-volume data requiring rapid archival:
- Log data
- Audit trails
- Temporary staging data

## Creating Custom Profiles

```sql
INSERT INTO cmr.dwh_ilm_threshold_profiles (
    profile_name,
    description,
    hot_threshold_days,
    warm_threshold_days,
    cold_threshold_days
) VALUES (
    'FINANCIAL_COMPLIANCE',
    'Financial data - aggressive archival after 90 days',
    30,   -- HOT: 0-30 days
    90,   -- WARM: 31-90 days
    365   -- COLD: 91+ days
);
```

## Assigning Profiles to Policies

### New Policy with Profile
```sql
INSERT INTO cmr.dwh_ilm_policies (
    policy_name,
    table_name,
    age_days,
    access_pattern,
    threshold_profile_id,
    action_type,
    compression_type
) VALUES (
    'COMPRESS_SALES_WARM',
    'SALES_FACT',
    100,
    'WARM',
    (SELECT profile_id FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'FAST_AGING'),
    'COMPRESS',
    'QUERY HIGH'
);
```

### Update Existing Policy
```sql
UPDATE cmr.dwh_ilm_policies
SET threshold_profile_id = (
    SELECT profile_id
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'FAST_AGING'
)
WHERE policy_name = 'COMPRESS_SALES_WARM';
```

### Revert to Global Config
```sql
UPDATE cmr.dwh_ilm_policies
SET threshold_profile_id = NULL
WHERE policy_name = 'COMPRESS_SALES_WARM';
```

## Monitoring

### View Effective Thresholds
```sql
SELECT
    policy_name,
    table_name,
    profile_name,
    threshold_source,
    effective_hot_threshold_days,
    effective_warm_threshold_days,
    effective_cold_threshold_days
FROM dwh_v_ilm_policy_thresholds
ORDER BY policy_name;
```

### Profile Usage Report
```sql
SELECT
    prof.profile_name,
    prof.description,
    COUNT(pol.policy_id) AS policies_using_profile,
    prof.hot_threshold_days || '/' ||
    prof.warm_threshold_days || '/' ||
    prof.cold_threshold_days AS thresholds
FROM cmr.dwh_ilm_threshold_profiles prof
LEFT JOIN cmr.dwh_ilm_policies pol ON pol.threshold_profile_id = prof.profile_id
GROUP BY prof.profile_name, prof.description,
         prof.hot_threshold_days, prof.warm_threshold_days, prof.cold_threshold_days
ORDER BY policies_using_profile DESC;
```

## Best Practices

1. **Start with defaults** - Use global config initially
2. **Create profiles for patterns** - Identify common aging patterns
3. **Reuse profiles** - Don't create unique profiles per table
4. **Document rationale** - Add meaningful descriptions
5. **Test before production** - Validate profile behavior
6. **Monitor effectiveness** - Review policy success rates

## Troubleshooting

### Profile Not Applied
Check policy configuration:
```sql
SELECT policy_name, threshold_profile_id
FROM cmr.dwh_ilm_policies
WHERE policy_name = 'YOUR_POLICY';
```

### Unexpected Temperature Classification
Verify effective thresholds:
```sql
SELECT * FROM dwh_v_ilm_policy_thresholds
WHERE policy_name = 'YOUR_POLICY';
```

### Profile Constraint Violation
Ensure hot < warm < cold:
```sql
-- This will fail:
INSERT INTO cmr.dwh_ilm_threshold_profiles
VALUES ('BAD_PROFILE', 'Invalid', 100, 50, 200);
-- Error: hot (100) must be < warm (50)
```
```

### 6.3 Example Updates

**File:** `examples/custom_ilm_examples.sql`

Add new examples:
```sql
-- Example 11: Using Threshold Profiles
-- Example 12: Creating Custom Profiles
-- Example 13: Profile-Based Policy Patterns
```

---

## Phase 7: Validation & Rollback

### 7.1 Pre-Upgrade Validation

**Checklist:**
- [ ] Backup dwh_ilm_policies table
- [ ] Backup dwh_ilm_config table
- [ ] Document current policy configurations
- [ ] Verify all existing policies are working
- [ ] Test migration script in non-prod environment

### 7.2 Post-Upgrade Validation

**Verification queries:**

```sql
-- 1. Verify table created
SELECT COUNT(*) FROM cmr.dwh_ilm_threshold_profiles;
-- Expected: 4 (default profiles)

-- 2. Verify column added
SELECT threshold_profile_id FROM cmr.dwh_ilm_policies WHERE ROWNUM = 1;
-- Expected: No error, NULL value

-- 3. Verify function works
SELECT get_policy_thresholds(policy_id, 'HOT') FROM cmr.dwh_ilm_policies WHERE ROWNUM = 1;
-- Expected: 90 (or profile value if assigned)

-- 4. Verify view works
SELECT * FROM dwh_v_ilm_policy_thresholds WHERE ROWNUM = 1;
-- Expected: Shows effective thresholds

-- 5. Run policy evaluation
EXEC pck_dwh_ilm_policy_engine.evaluate_all_policies();
-- Expected: No errors
```

### 7.3 Rollback Plan

**If upgrade fails:**

```sql
-- Step 1: Drop new objects
DROP VIEW dwh_v_ilm_policy_thresholds;
DROP FUNCTION get_policy_thresholds;

-- Step 2: Remove FK
ALTER TABLE cmr.dwh_ilm_policies DROP CONSTRAINT fk_ilm_policy_profile;

-- Step 3: Remove column
ALTER TABLE cmr.dwh_ilm_policies DROP COLUMN threshold_profile_id;

-- Step 4: Drop table
DROP TABLE cmr.dwh_ilm_threshold_profiles PURGE;

-- Step 5: Restore old packages
@scripts/custom_ilm_policy_engine.sql (from backup)

-- Step 6: Verify rollback
SELECT * FROM cmr.dwh_ilm_policies WHERE ROWNUM = 1;
-- Should not show threshold_profile_id column
```

---

## Phase 8: Implementation Checklist

### Pre-Implementation
- [ ] Review and approve plan
- [ ] Create feature branch: `feature/threshold-profiles`
- [ ] Set up test environment

### Implementation Tasks

#### Database Schema (Phase 1)
- [ ] Add table creation to custom_ilm_setup.sql
- [ ] Add default profile inserts
- [ ] Add ALTER TABLE statement
- [ ] Update setup script cleanup section (rerunnable)
- [ ] Test setup script on clean database

#### Core Logic (Phase 2)
- [ ] Create get_policy_thresholds function
- [ ] Update custom_ilm_policy_engine.sql
- [ ] Update policy evaluation logic
- [ ] Test function with various scenarios
- [ ] Test policy evaluation with profiles

#### Views (Phase 3)
- [ ] Create dwh_v_ilm_policy_thresholds view
- [ ] Add to custom_ilm_setup.sql
- [ ] Test view with sample data

#### Migration (Phase 4)
- [ ] Create migration script
- [ ] Test migration on copy of production schema
- [ ] Verify idempotency (run twice)
- [ ] Document manual steps if any

#### Testing (Phase 5)
- [ ] Create unit test file
- [ ] Write 7 unit tests
- [ ] Create integration test scenarios
- [ ] Run performance tests
- [ ] Update test_suite_runner.sql
- [ ] All tests pass

#### Documentation (Phase 6)
- [ ] Update custom_ilm_guide.md
- [ ] Update README.md features
- [ ] Create THRESHOLD_PROFILES_GUIDE.md
- [ ] Update CHANGELOG.md (v3.1)
- [ ] Update examples/custom_ilm_examples.sql
- [ ] Update INSTALL_GUIDE_COMPLETE.md
- [ ] Update QUICK_REFERENCE.md
- [ ] Review all docs for consistency

#### Code Review & Testing
- [ ] Peer review of all changes
- [ ] Run complete test suite
- [ ] Test upgrade path from v3.0
- [ ] Test rollback procedure
- [ ] Performance benchmarks

#### Deployment
- [ ] Merge to main branch
- [ ] Tag release: v3.1
- [ ] Update CHANGELOG.md with release date
- [ ] Create release notes
- [ ] Notify users of upgrade path

---

## Impact Analysis

### Files to Modify

**Core Scripts:**
1. `scripts/custom_ilm_setup.sql` - Add table, function, views, alter
2. `scripts/custom_ilm_policy_engine.sql` - Update policy evaluation
3. `scripts/custom_ilm_validation.sql` - No changes needed

**New Files:**
1. `scripts/migration/upgrade_to_v3.1_threshold_profiles.sql`

**Test Files:**
1. `tests/unit_tests_threshold_profiles.sql` (NEW)
2. `tests/test_suite_runner.sql` - Add new test file

**Documentation:**
1. `docs/custom_ilm_guide.md` - Update
2. `docs/guides/THRESHOLD_PROFILES_GUIDE.md` (NEW)
3. `docs/installation/INSTALL_GUIDE_COMPLETE.md` - Update
4. `docs/QUICK_REFERENCE.md` - Add profile commands
5. `README.md` - Update features
6. `CHANGELOG.md` - Add v3.1

**Examples:**
1. `examples/custom_ilm_examples.sql` - Add 3 examples

### Backward Compatibility

**✅ Fully Backward Compatible:**
- Existing policies continue working (threshold_profile_id = NULL)
- NULL means use global config (current behavior)
- No data migration required
- No breaking API changes

**Migration Path:**
- Optional: Gradually assign profiles to policies
- Can rollback column to NULL to revert behavior

### Estimated Effort

- **Development:** 8-12 hours
- **Testing:** 4-6 hours
- **Documentation:** 4-6 hours
- **Review & Deployment:** 2-3 hours

**Total:** ~20-25 hours

---

## Success Criteria

### Functional Requirements
- [ ] Threshold profiles can be created, updated, deleted
- [ ] Policies can reference profiles or use global config
- [ ] Policy evaluation respects profile thresholds
- [ ] Views display effective thresholds correctly
- [ ] Migration script works on existing installations

### Non-Functional Requirements
- [ ] No performance degradation in policy evaluation
- [ ] All existing policies continue working
- [ ] Documentation complete and accurate
- [ ] All tests pass (>95% coverage)
- [ ] Code review approved

### User Acceptance
- [ ] Users can create custom profiles
- [ ] Users can assign profiles to policies
- [ ] Users can monitor effective thresholds
- [ ] Migration path is clear and documented

---

## Version History

| Version | Date       | Author | Changes |
|---------|------------|--------|---------|
| 1.0     | 2025-10-23 | Claude | Initial plan |

---

## Appendix

### A. SQL Object Naming Conventions

- Table: `dwh_ilm_threshold_profiles`
- View: `dwh_v_ilm_policy_thresholds`
- Function: `get_policy_thresholds`
- Constraint: `chk_profile_thresholds`, `fk_ilm_policy_profile`
- Index: `idx_ilm_profiles_name`, `idx_ilm_policies_profile`

### B. Data Dictionary

**dwh_ilm_threshold_profiles:**
- `profile_id` - Surrogate key, auto-generated
- `profile_name` - Unique business key (e.g., 'FAST_AGING')
- `description` - Human-readable explanation
- `hot_threshold_days` - Days before partition becomes WARM
- `warm_threshold_days` - Days before partition becomes COLD
- `cold_threshold_days` - Days before partition becomes FROZEN

**dwh_ilm_policies (new column):**
- `threshold_profile_id` - FK to dwh_ilm_threshold_profiles, nullable

### C. Related Design Decisions

**Q: Why not add threshold columns directly to dwh_ilm_policies?**
A: Reusability. Multiple policies can share same profile. Easier to manage patterns.

**Q: Why keep global config?**
A: Default fallback, backwards compatibility, simple cases don't need profiles.

**Q: Why recalculate temperature per policy?**
A: Each policy may have different thresholds, generic temperature in tracking table is for monitoring only.

**Q: Should we cache threshold values?**
A: Future optimization. Start with direct queries, profile if needed.

---

**END OF PLAN**
