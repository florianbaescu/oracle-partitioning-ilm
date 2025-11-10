# ILM-Aware Initial Partitioning Implementation Plan

**Date:** 2025-11-10
**Status:** Planning Phase - Revised Architecture
**Priority:** High

---

## Executive Summary

The current table migration framework creates partitioned tables with uniform intervals (e.g., all monthly partitions). For tables with extensive historical data (e.g., 12 years), this approach is inefficient:

- Creates 144 monthly partitions for 12 years of data
- All partitions initially in same tablespace with same compression
- Requires extensive post-migration ILM processing (100+ partition moves)

**Proposed Solution:** Implement ILM-aware initial partitioning that creates age-stratified partitions during migration:
- **HOT tier (recent 1 year):** Monthly partitions, no compression, TBS_HOT
- **WARM tier (1-3 years):** Yearly partitions, BASIC compression, TBS_WARM
- **COLD tier (3-7 years):** Yearly partitions, OLTP compression, TBS_COLD

**Result:** 23 partitions instead of 144, data lands in correct tier during migration, minimal post-migration ILM work.

**Architecture Decision:** Store tier configuration in templates (JSON), read at execution time. **No schema changes to dwh_migration_tasks required.**

---

## Understanding tier_config vs policies

**CRITICAL DISTINCTION:** Templates with tiered partitioning have TWO components that work together:

### **tier_config** - ONE-TIME Initial Partition Creation (During Migration)

Controls how partitions are created **at migration time** (one-time event):

```
Migration Day: November 10, 2025
┌─────────────────────────────────────────────────────────────┐
│ tier_config analyzes existing data and creates partitions: │
│                                                             │
│ COLD tier (2013-2021): 9 yearly partitions                 │
│   → Already old data placed in TBS_COLD, OLTP compression  │
│                                                             │
│ WARM tier (2022-2024): 3 yearly partitions                 │
│   → Middle-aged data placed in TBS_WARM, BASIC compression │
│                                                             │
│ HOT tier (Nov 2024 - Nov 2025): 12 monthly partitions      │
│   → Recent data placed in TBS_HOT, no compression          │
│                                                             │
│ INTERVAL: Future partitions auto-created monthly in HOT    │
└─────────────────────────────────────────────────────────────┘

Result: 24 partitions created, data lands in correct tier immediately
        No post-migration moves required for existing data
```

### **policies** - ONGOING Lifecycle Management (Post-Migration)

Controls what happens to partitions **after migration** as they age (continuous evaluation):

```
Ongoing: ILM Scheduler evaluates EVERY partition daily/weekly
┌─────────────────────────────────────────────────────────────┐
│ December 2025 partition (created by INTERVAL):              │
│   → Day 0 (Dec 1, 2025): Created in TBS_HOT                │
│   → 12 months later (Dec 2026): MOVE to TBS_WARM           │
│   → 36 months later (Dec 2028): MOVE to TBS_COLD           │
│   → 84 months later (Dec 2032): READ_ONLY or DROP          │
│                                                             │
│ November 2025 partition (created during migration):         │
│   → Day 0 (Nov 10, 2025): Created in TBS_HOT               │
│   → 12 months later (Nov 2026): MOVE to TBS_WARM           │
│   → 36 months later (Nov 2028): MOVE to TBS_COLD           │
│   → 84 months later (Nov 2032): READ_ONLY or DROP          │
└─────────────────────────────────────────────────────────────┘

Result: New partitions automatically managed through lifecycle
        Policies apply uniformly to all partitions (old and new)
```

### **Why Both? Age Thresholds Must Align**

Looking at `FACT_TABLE_STANDARD_TIERED` template:

```json
{
    "tier_config": {
        "hot":  {"age_months": 12, "interval": "MONTHLY", "tablespace": "TBS_HOT", "compression": "NONE"},
        "warm": {"age_months": 36, "interval": "YEARLY",  "tablespace": "TBS_WARM", "compression": "BASIC"},
        "cold": {"age_months": 84, "interval": "YEARLY",  "tablespace": "TBS_COLD", "compression": "OLTP"}
    },
    "policies": [
        {"age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC"},
        {"age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP"},
        {"age_months": 84, "action": "READ_ONLY"}
    ]
}
```

**The age thresholds (12, 36, 84 months) intentionally match:**

| Age Threshold | tier_config (Migration Time) | policies (Ongoing) |
|---------------|------------------------------|-------------------|
| **12 months** | "Data from Nov 2024 goes into WARM tier" | "Move Dec 2025 partition to WARM in Dec 2026" |
| **36 months** | "Data from Nov 2021 goes into COLD tier" | "Move Dec 2025 partition to COLD in Dec 2028" |
| **84 months** | "Data from Nov 2018 goes into COLD tier" | "Make Dec 2025 partition READ_ONLY in Dec 2032" |

### **Complete Timeline Example**

**Migration Day: November 10, 2025** (12 years of data: 2013-2025)

`tier_config` creates 24 partitions:
- 2013-2021 data (9 years): **9 yearly partitions in COLD tier** (TBS_COLD, OLTP)
- 2022-2024 data (3 years): **3 yearly partitions in WARM tier** (TBS_WARM, BASIC)
- Nov 2024 - Nov 2025 (1 year): **12 monthly partitions in HOT tier** (TBS_HOT, uncompressed)

**Post-migration moves needed: ZERO** ✅

**December 1, 2025** (+21 days)
- INTERVAL creates Dec 2025 partition in HOT tier (TBS_HOT)
- `policies` evaluate: partition is 0 months old → no action

**November 1, 2026** (+1 year)
- Nov 2025 partition (created during migration) is now 12 months old
- `policies` **MOVE**: TBS_HOT → TBS_WARM, apply BASIC compression

**December 1, 2026** (+1 year, 21 days)
- Dec 2025 partition is now 12 months old
- `policies` **MOVE**: TBS_HOT → TBS_WARM, apply BASIC compression

**November 1, 2028** (+3 years)
- Nov 2025 partition is now 36 months old
- `policies` **MOVE**: TBS_WARM → TBS_COLD, apply OLTP compression

**November 1, 2032** (+7 years)
- Nov 2025 partition is now 84 months old
- `policies` **READ_ONLY** or **DROP** (based on retention policy)

### **Comparison: With vs Without tier_config**

**WITHOUT tier_config (old approach):**
```
Migration creates: 144 monthly partitions, all in TBS_HOT, all uncompressed

Day 1 post-migration:
  - 132 partitions are already "old" but in wrong tier
  - ILM must immediately move 132 partitions (expensive!)
  - High I/O, locks partitions, rebuilds indexes
  - May take days/weeks to complete all moves
```

**WITH tier_config (new approach):**
```
Migration creates: 24 partitions, correctly tiered from day 1

Day 1 post-migration:
  - 0 partition moves needed
  - Data already in optimal tier/tablespace/compression
  - Policies only manage NEW partitions going forward
  - Minimal post-migration ILM work
```

### **Summary**

| Aspect | tier_config | policies |
|--------|-------------|----------|
| **Purpose** | Place existing historical data | Manage new partitions as they age |
| **When** | Migration day (one-time) | Every day (continuous) |
| **Applies To** | Data that exists at migration time | Partitions created after migration |
| **Example** | "Put 2013-2021 data in COLD tier now" | "Move future partitions to COLD at 36 months" |
| **Benefit** | Immediate optimal placement | Automated ongoing lifecycle |
| **Required** | No (backward compatible) | Recommended for all partitioned tables |

**Key Insight:** Without aligned thresholds, you'd have inconsistency where historical data lands in one tier but future data moves to different tiers at different ages. The alignment ensures **uniform lifecycle treatment** for all data, regardless of when it entered the table.

---

## Current Architecture Analysis

### 1. Migration Flow

```
┌─────────────────┐
│ Create Task     │
│ (PENDING)       │
│ ilm_policy_     │
│ template = 'X'  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Analyze Table   │
│ (ANALYZING →    │
│  ANALYZED)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Apply           │
│ Recommendations │
│ (READY)         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Execute         │
│ Migration       │
│ (RUNNING →      │
│  COMPLETED)     │
│                 │
│ Reads template  │
│ Parses tier cfg │
│ Builds tiered   │
│ partition DDL   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Apply ILM       │
│ Policies        │
│ (POST-MIGRATION)│
└─────────────────┘
```

### 2. Current Partition Creation Logic

**File:** `scripts/table_migration_execution.sql`
**Procedure:** `build_partition_ddl()` (lines 161-459)

**Current Behavior:**
```sql
-- Example output for monthly interval
CREATE TABLE sales_fact_part (...)
PARTITION BY RANGE (sale_date)
INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_initial VALUES LESS THAN (TO_DATE('2013-01-01','YYYY-MM-DD'))
)
TABLESPACE TBS_HOT
COMPRESS FOR QUERY HIGH;
```

**Result:**
- One initial partition
- All future partitions auto-created with same interval (monthly)
- All partitions inherit same tablespace (TBS_HOT)
- All partitions inherit same compression (QUERY HIGH)

**For 12 years of data:**
- 1 initial partition + 144 monthly interval partitions = 145 partitions
- All in TBS_HOT, all with QUERY HIGH compression
- ILM must then process each partition individually

### 3. Existing Template Pattern

**Current Implementation:** `apply_ilm_policies()` (lines 2561-2710)

```sql
-- Task only stores template NAME
dwh_migration_tasks.ilm_policy_template = 'FACT_TABLE_STANDARD'

-- Template is read at execution time
SELECT * INTO v_template
FROM cmr.dwh_migration_ilm_templates
WHERE template_name = v_detected_template;

-- JSON is parsed to get policies
FOR policy_rec IN (
    SELECT ... FROM JSON_TABLE(v_template.policies_json, '$[*]' ...)
) LOOP
    -- Create policies
END LOOP;
```

**Key Pattern:**
- ✅ Template name stored in task
- ✅ Full template read at execution time
- ✅ JSON parsed on-demand
- ✅ No duplication of template data in task table

**We will use the SAME pattern for tier configuration!**

### 4. Key Limitations Identified

**❌ Limitation 1: Uniform Interval Constraint**

Oracle's `INTERVAL` clause applies uniformly to all auto-generated partitions. Cannot mix monthly and yearly intervals.

**❌ Limitation 2: Cannot Change Partition Granularity Post-Creation**

ILM policies can compress and move partitions, but cannot merge monthly partitions into yearly partitions automatically.

**❌ Limitation 3: Inefficient Initial Data Placement**

For 12-year table:
- Migration creates 144 monthly partitions in TBS_HOT
- ILM must move 132 partitions post-migration
- Each move locks partition, moves data, rebuilds indexes

---

## Oracle Database Capabilities

### What Oracle DOES Support

**✅ Explicit Partition Definitions with Different Properties**

```sql
CREATE TABLE sales_fact_part (...)
PARTITION BY RANGE (sale_date)
INTERVAL(NUMTOYMINTERVAL(1,'MONTH'))  -- Only applies to future partitions
(
    -- COLD tier: Yearly partitions (7-9 years ago)
    PARTITION p_2016 VALUES LESS THAN (TO_DATE('2017-01-01','YYYY-MM-DD'))
        TABLESPACE TBS_COLD COMPRESS FOR OLTP,
    PARTITION p_2017 VALUES LESS THAN (TO_DATE('2018-01-01','YYYY-MM-DD'))
        TABLESPACE TBS_COLD COMPRESS FOR OLTP,

    -- WARM tier: Yearly partitions (1-3 years ago)
    PARTITION p_2022 VALUES LESS THAN (TO_DATE('2023-01-01','YYYY-MM-DD'))
        TABLESPACE TBS_WARM COMPRESS FOR BASIC,
    PARTITION p_2023 VALUES LESS THAN (TO_DATE('2024-01-01','YYYY-MM-DD'))
        TABLESPACE TBS_WARM COMPRESS FOR BASIC,

    -- HOT tier: Monthly partitions (recent 1 year)
    PARTITION p_2024_01 VALUES LESS THAN (TO_DATE('2024-02-01','YYYY-MM-DD'))
        TABLESPACE TBS_HOT,
    PARTITION p_2024_02 VALUES LESS THAN (TO_DATE('2024-03-01','YYYY-MM-DD'))
        TABLESPACE TBS_HOT,
    -- ... monthly partitions through p_2024_12
)
ENABLE ROW MOVEMENT;
-- Future monthly partitions automatically created in TBS_HOT
```

**Key Benefits:**
1. ✅ Different partition intervals (yearly for old, monthly for recent)
2. ✅ Different tablespaces per tier
3. ✅ Different compression per tier
4. ✅ INTERVAL clause creates future HOT partitions automatically
5. ✅ Data lands in correct tier during initial migration

---

## Proposed Solution

### Architecture Overview

**ILM-Aware Initial Partitioning with Template-Driven Configuration**

```
┌──────────────────────────────────────────────────────────────┐
│ STEP 1: Template Definition (ONE TIME SETUP)                 │
│ - Enhanced JSON in dwh_migration_ilm_templates               │
│ - Add tier_config section to policies_json                   │
│ - Define HOT/WARM/COLD boundaries, intervals, storage        │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 2: Task Creation                                        │
│ - Specify ilm_policy_template = 'FACT_TABLE_STANDARD'        │
│ - NO tier configuration stored in task                       │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 3: Analysis Phase                                       │
│ - Analyze table and detect date range                        │
│ - Store date range in dwh_migration_analysis                 │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 4: Apply Recommendations                                │
│ - Copy recommended strategy to task                          │
│ - NO tier configuration copying needed                       │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 5: Build Partition DDL (NEW LOGIC)                     │
│ - Check if task.ilm_policy_template is set                   │
│ - IF YES:                                                     │
│   • SELECT template FROM dwh_migration_ilm_templates          │
│   • Parse policies_json for tier_config                      │
│   • IF tier_config exists:                                   │
│     - Call build_tiered_partition_ddl()                      │
│     - Generate explicit COLD/WARM/HOT partitions             │
│   • ELSE: Call build_uniform_partition_ddl()                 │
│ - IF NO: Call build_uniform_partition_ddl()                  │
└─────────────────────────┬────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ STEP 6: Execute Migration                                    │
│ - Create partitioned table with tiered partitions            │
│ - Data automatically lands in correct tier                   │
└──────────────────────────────────────────────────────────────┘
```

### Design Principles

**1. Single Source of Truth**
- Tier configuration stored ONLY in `dwh_migration_ilm_templates.policies_json`
- Task table stores only template name (reference)
- Template read at execution time

**2. Consistent with Existing Pattern**
- Same pattern as ILM policy management
- No new columns in `dwh_migration_tasks`
- JSON parsing at execution time

**3. Template Versioning Ready**
- Can add `template_version` field later
- Can support multiple versions of same template
- Easy to update tier configuration globally

**4. Backward Compatible**
- If no template specified: use uniform interval (existing behavior)
- If template has no `tier_config`: use uniform interval
- If template has `tier_config`: use tiered partitions
- **Zero breaking changes**

---

## Code Standards and Patterns

### LOB Handling Best Practices

Based on existing codebase patterns in `table_migration_analysis.sql`, all CLOB operations must follow these guidelines:

**1. LOB Initialization:**
```sql
-- Always use SESSION duration to survive across DECLARE blocks
DBMS_LOB.CREATETEMPORARY(v_clob_var, TRUE, DBMS_LOB.SESSION);
```

**2. LOB Cleanup (Success Path):**
```sql
-- Always check if temporary before freeing
IF DBMS_LOB.ISTEMPORARY(v_clob_var) = 1 THEN
    DBMS_LOB.FREETEMPORARY(v_clob_var);
END IF;
```

**3. LOB Cleanup (Exception Handler):**
```sql
EXCEPTION
    WHEN OTHERS THEN
        -- Clean up LOBs before re-raising error
        BEGIN
            IF DBMS_LOB.ISTEMPORARY(v_clob_var) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_clob_var);
            END IF;
        EXCEPTION WHEN OTHERS THEN NULL; END;  -- Suppress cleanup errors

        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
```

**4. Building CLOB Content:**
```sql
-- Use DBMS_LOB.APPEND for all content building
DBMS_LOB.APPEND(v_clob_var, 'some text');
DBMS_LOB.APPEND(v_clob_var, CHR(10));  -- newline
```

**Why This Matters:**
- **Memory Leaks:** Temporary LOBs not freed cause session memory leaks
- **SESSION Duration:** Required when LOBs cross DECLARE block boundaries
- **Exception Safety:** LOBs must be freed even if errors occur
- **Performance:** Proper cleanup prevents PGA memory exhaustion

### Logging Approach

The framework uses a **dual logging strategy** combining console output and persistent table logging:

#### 1. Console Logging (DBMS_OUTPUT.PUT_LINE)

Used for immediate feedback during development, testing, and interactive execution:

**Section Headers:**
```sql
DBMS_OUTPUT.PUT_LINE('========================================');
DBMS_OUTPUT.PUT_LINE('Section Title');
DBMS_OUTPUT.PUT_LINE('========================================');
```

**Structured Messages:**
```sql
-- Error messages
DBMS_OUTPUT.PUT_LINE('ERROR: Description of error: ' || SQLERRM);

-- Warning messages
DBMS_OUTPUT.PUT_LINE('WARNING: Description of warning');

-- Info messages
DBMS_OUTPUT.PUT_LINE('INFO: Description of information');

-- Progress messages (no prefix)
DBMS_OUTPUT.PUT_LINE('  Processing step 1...');
DBMS_OUTPUT.PUT_LINE('    Generated 23 partitions');
```

**Indentation Convention:**
```sql
-- Main level (no indent)
DBMS_OUTPUT.PUT_LINE('Main operation');

-- Sub-level (2 spaces)
DBMS_OUTPUT.PUT_LINE('  Sub-operation');

-- Detail level (4 spaces)
DBMS_OUTPUT.PUT_LINE('    Detail information');
```

#### 2. Persistent Table Logging (dwh_migration_execution_log)

Used for audit trail, monitoring, and troubleshooting in production:

**Table Structure:**
```sql
CREATE TABLE cmr.dwh_migration_execution_log (
    log_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id        NUMBER NOT NULL,      -- Links all steps in single migration run
    task_id             NUMBER NOT NULL,
    step_number         NUMBER,               -- Sequential step number within execution
    step_name           VARCHAR2(200),        -- Human-readable step description
    step_type           VARCHAR2(50),         -- DDL_GENERATION, BACKUP, CTAS, VALIDATION, etc.
    sql_statement       CLOB,                 -- SQL executed in this step
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    NUMBER,
    status              VARCHAR2(20),         -- RUNNING, SUCCESS, FAILED
    rows_processed      NUMBER,
    error_code          NUMBER,
    error_message       VARCHAR2(4000)
);
```

**Logging Procedure:**
```sql
-- Private helper procedure in pck_dwh_table_migration_executor
PROCEDURE log_step(
    p_task_id NUMBER,
    p_step_number NUMBER,
    p_step_name VARCHAR2,
    p_step_type VARCHAR2,
    p_sql CLOB,
    p_status VARCHAR2,
    p_start_time TIMESTAMP,
    p_end_time TIMESTAMP,
    p_error_code NUMBER DEFAULT NULL,
    p_error_message VARCHAR2 DEFAULT NULL
) AS
    v_duration NUMBER;
BEGIN
    v_duration := EXTRACT(SECOND FROM (p_end_time - p_start_time)) +
                 EXTRACT(MINUTE FROM (p_end_time - p_start_time)) * 60 +
                 EXTRACT(HOUR FROM (p_end_time - p_start_time)) * 3600;

    INSERT INTO cmr.dwh_migration_execution_log (
        execution_id, task_id, step_number, step_name, step_type, sql_statement,
        start_time, end_time, duration_seconds, status,
        error_code, error_message
    ) VALUES (
        g_execution_id, p_task_id, p_step_number, p_step_name, p_step_type, p_sql,
        p_start_time, p_end_time, v_duration, p_status,
        p_error_code, p_error_message
    );

    COMMIT;
END log_step;
```

**Usage Pattern:**
```sql
-- Log step start
v_step_start := SYSTIMESTAMP;
DBMS_OUTPUT.PUT_LINE('  Starting operation...');

-- Perform operation
-- [operation code here]

-- Log step completion
log_step(
    p_task_id => p_task.task_id,
    p_step_number => v_step_number,
    p_step_name => 'Build Tiered Partitions',
    p_step_type => 'DDL_GENERATION',
    p_sql => p_ddl,  -- Generated DDL
    p_status => 'SUCCESS',
    p_start_time => v_step_start,
    p_end_time => SYSTIMESTAMP
);

DBMS_OUTPUT.PUT_LINE('  Operation completed');
```

**Error Logging:**
```sql
EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;

        -- Log to table
        log_step(
            p_task_id => p_task.task_id,
            p_step_number => v_step_number,
            p_step_name => 'Build Tiered Partitions',
            p_step_type => 'DDL_GENERATION',
            p_sql => NULL,
            p_status => 'FAILED',
            p_start_time => v_step_start,
            p_end_time => SYSTIMESTAMP,
            p_error_code => SQLCODE,
            p_error_message => v_error_msg
        );

        -- Log to console
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_error_msg);
        RAISE;
END;
```

#### Benefits of Dual Logging:

**Console Output (DBMS_OUTPUT):**
- Immediate feedback during interactive sessions
- Easy to read and follow progress
- Captured by SQL*Plus, SQLcl, and Oracle tools
- Useful for development and testing

**Table Logging:**
- Persistent audit trail
- Searchable and queryable
- Tracks performance metrics (duration_seconds)
- Links all steps via execution_id
- Essential for production monitoring
- Enables historical analysis and troubleshooting
- Can trigger alerts based on status/duration

---

## Implementation Components

### Component 1: Enhanced Template JSON Structure

**File:** `scripts/table_migration_setup.sql`

**Current Template:**
```sql
INSERT (template_name, description, table_type, policies_json)
VALUES (
    'FACT_TABLE_STANDARD',
    'Standard ILM policies for fact tables',
    'FACT',
    '[
        {"policy_name": "{TABLE}_COMPRESS_90D", "age_days": 90, "action": "COMPRESS", "compression": "QUERY HIGH"},
        {"policy_name": "{TABLE}_TIER_WARM_12M", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM"},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD"}
    ]'
);
```

**Enhanced Template with Tier Configuration:**
```sql
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'FACT_TABLE_STANDARD_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'FACT_TABLE_STANDARD_TIERED',
        'ILM-aware partitioning with age-based tiers (HOT=1y monthly, WARM=3y yearly, COLD=7y yearly)',
        'FACT',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 12,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE"
                },
                "warm": {
                    "age_months": 36,
                    "interval": "YEARLY",
                    "tablespace": "TBS_WARM",
                    "compression": "BASIC"
                },
                "cold": {
                    "age_months": 84,
                    "interval": "YEARLY",
                    "tablespace": "TBS_COLD",
                    "compression": "OLTP"
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "priority": 300},
                {"policy_name": "{TABLE}_READONLY", "age_months": 84, "action": "READ_ONLY", "priority": 400}
            ]
        }'
    );
```

**JSON Schema:**
```json
{
  "tier_config": {
    "enabled": true,              // Flag to enable tiered partitioning
    "hot": {
      "age_months": 12,            // Data newer than this is HOT
      "interval": "MONTHLY",       // Partition interval for HOT tier
      "tablespace": "TBS_HOT",     // Tablespace for HOT partitions
      "compression": "NONE"        // Compression for HOT partitions
    },
    "warm": {
      "age_months": 36,            // Data between hot_months and this is WARM
      "interval": "YEARLY",        // Partition interval for WARM tier
      "tablespace": "TBS_WARM",
      "compression": "BASIC"
    },
    "cold": {
      "age_months": 84,            // Data between warm_months and this is COLD
      "interval": "YEARLY",
      "tablespace": "TBS_COLD",
      "compression": "OLTP"
    }
  },
  "policies": [...]              // Existing ILM policies for ongoing lifecycle
}
```

**No Schema Changes Required!**
- Uses existing `policies_json` CLOB column
- Backward compatible with existing templates
- Can coexist with non-tiered templates

### Component 2: Modified `build_partition_ddl()` Procedure

**File:** `scripts/table_migration_execution.sql`

**Add template reading and routing logic:**

```sql
PROCEDURE build_partition_ddl(
    p_task dwh_migration_tasks%ROWTYPE,
    p_ddl OUT CLOB
) AS
    v_template dwh_migration_ilm_templates%ROWTYPE;
    v_template_json JSON_OBJECT_T;
    v_tier_config JSON_OBJECT_T;
    v_tier_enabled BOOLEAN := FALSE;
BEGIN
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Building Partition DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Table: ' || p_task.source_owner || '.' || p_task.source_table);

    -- Check if task uses ILM template
    IF p_task.ilm_policy_template IS NOT NULL THEN
        BEGIN
            -- Read template (same pattern as apply_ilm_policies)
            SELECT * INTO v_template
            FROM cmr.dwh_migration_ilm_templates
            WHERE template_name = p_task.ilm_policy_template;

            DBMS_OUTPUT.PUT_LINE('ILM template: ' || v_template.template_name);

            -- Parse JSON
            IF v_template.policies_json IS NOT NULL THEN
                v_template_json := JSON_OBJECT_T.PARSE(v_template.policies_json);

                -- Check for tier_config
                IF v_template_json.has('tier_config') THEN
                    v_tier_config := TREAT(v_template_json.get('tier_config') AS JSON_OBJECT_T);

                    -- Check if tier partitioning is enabled
                    IF v_tier_config.has('enabled') AND
                       v_tier_config.get_boolean('enabled') = TRUE THEN
                        v_tier_enabled := TRUE;
                        DBMS_OUTPUT.PUT_LINE('  Tier partitioning: ENABLED');
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('  Tier partitioning: DISABLED in template');
                    END IF;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('  Tier configuration: NOT FOUND in template');
                END IF;
            END IF;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('WARNING: Template not found: ' || p_task.ilm_policy_template);
                DBMS_OUTPUT.PUT_LINE('  Falling back to uniform interval partitioning');
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('WARNING: Error reading template: ' || SQLERRM);
                DBMS_OUTPUT.PUT_LINE('  Falling back to uniform interval partitioning');
        END;
    ELSE
        DBMS_OUTPUT.PUT_LINE('No ILM template specified');
    END IF;

    -- Route to appropriate builder
    IF v_tier_enabled THEN
        DBMS_OUTPUT.PUT_LINE('Using tiered partition builder');
        DBMS_OUTPUT.PUT_LINE('');
        build_tiered_partitions(p_task, v_tier_config, p_ddl);
    ELSE
        DBMS_OUTPUT.PUT_LINE('Using uniform interval partition builder');
        DBMS_OUTPUT.PUT_LINE('');
        build_uniform_partitions(p_task, p_ddl);
    END IF;

    DBMS_OUTPUT.PUT_LINE('========================================');

END build_partition_ddl;
```

### Component 3: New Helper - `build_tiered_partitions()`

**File:** `scripts/table_migration_execution.sql`

**New procedure to generate tiered partition DDL:**

```sql
PROCEDURE build_tiered_partitions(
    p_task dwh_migration_tasks%ROWTYPE,
    p_tier_config JSON_OBJECT_T,
    p_ddl OUT CLOB
) AS
    -- Tier configuration
    v_hot_config JSON_OBJECT_T;
    v_warm_config JSON_OBJECT_T;
    v_cold_config JSON_OBJECT_T;

    v_hot_months NUMBER;
    v_hot_interval VARCHAR2(20);
    v_hot_tablespace VARCHAR2(128);
    v_hot_compression VARCHAR2(50);

    v_warm_months NUMBER;
    v_warm_interval VARCHAR2(20);
    v_warm_tablespace VARCHAR2(128);
    v_warm_compression VARCHAR2(50);

    v_cold_months NUMBER;
    v_cold_interval VARCHAR2(20);
    v_cold_tablespace VARCHAR2(128);
    v_cold_compression VARCHAR2(50);

    -- Date ranges
    v_min_date DATE;
    v_max_date DATE;
    v_current_date DATE := SYSDATE;
    v_hot_cutoff DATE;
    v_warm_cutoff DATE;
    v_cold_cutoff DATE;

    -- Partition generation
    v_partition_list CLOB;
    v_partition_name VARCHAR2(128);
    v_partition_date DATE;
    v_next_date DATE;
    v_columns CLOB;

    v_cold_count NUMBER := 0;
    v_warm_count NUMBER := 0;
    v_hot_count NUMBER := 0;

    -- Logging
    v_step_start TIMESTAMP;
    v_step_number NUMBER := 10;  -- Adjust based on workflow
    v_error_msg VARCHAR2(4000);

BEGIN
    -- Log step start
    v_step_start := SYSTIMESTAMP;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Building Tiered Partition DDL');
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Initialize partition list CLOB with SESSION duration
    -- IMPORTANT: Use DBMS_LOB.SESSION to survive across DECLARE blocks
    DBMS_LOB.CREATETEMPORARY(v_partition_list, TRUE, DBMS_LOB.SESSION);

    -- Parse tier configurations
    v_hot_config := TREAT(p_tier_config.get('hot') AS JSON_OBJECT_T);
    v_hot_months := v_hot_config.get_number('age_months');
    v_hot_interval := v_hot_config.get_string('interval');
    v_hot_tablespace := v_hot_config.get_string('tablespace');
    v_hot_compression := v_hot_config.get_string('compression');

    v_warm_config := TREAT(p_tier_config.get('warm') AS JSON_OBJECT_T);
    v_warm_months := v_warm_config.get_number('age_months');
    v_warm_interval := v_warm_config.get_string('interval');
    v_warm_tablespace := v_warm_config.get_string('tablespace');
    v_warm_compression := v_warm_config.get_string('compression');

    v_cold_config := TREAT(p_tier_config.get('cold') AS JSON_OBJECT_T);
    v_cold_months := v_cold_config.get_number('age_months');
    v_cold_interval := v_cold_config.get_string('interval');
    v_cold_tablespace := v_cold_config.get_string('tablespace');
    v_cold_compression := v_cold_config.get_string('compression');

    -- Calculate tier boundary dates
    v_hot_cutoff := ADD_MONTHS(v_current_date, -v_hot_months);
    v_warm_cutoff := ADD_MONTHS(v_current_date, -v_warm_months);
    v_cold_cutoff := ADD_MONTHS(v_current_date, -v_cold_months);

    DBMS_OUTPUT.PUT_LINE('Tier boundaries:');
    DBMS_OUTPUT.PUT_LINE('  COLD: < ' || TO_CHAR(v_cold_cutoff, 'YYYY-MM-DD') ||
                         ' (' || v_cold_interval || ' partitions)');
    DBMS_OUTPUT.PUT_LINE('  WARM: ' || TO_CHAR(v_cold_cutoff, 'YYYY-MM-DD') ||
                         ' to ' || TO_CHAR(v_warm_cutoff, 'YYYY-MM-DD') ||
                         ' (' || v_warm_interval || ' partitions)');
    DBMS_OUTPUT.PUT_LINE('  HOT:  > ' || TO_CHAR(v_hot_cutoff, 'YYYY-MM-DD') ||
                         ' (' || v_hot_interval || ' partitions)');

    -- Get source data date range from analysis
    BEGIN
        SELECT partition_boundary_min_date, partition_boundary_max_date
        INTO v_min_date, v_max_date
        FROM cmr.dwh_migration_analysis
        WHERE task_id = p_task.task_id;

        DBMS_OUTPUT.PUT_LINE('Source data range: ' ||
                           TO_CHAR(v_min_date, 'YYYY-MM-DD') || ' to ' ||
                           TO_CHAR(v_max_date, 'YYYY-MM-DD'));
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Clean up LOB before raising error
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20001,
                'No analysis data found for task_id: ' || p_task.task_id ||
                '. Run analyze_table() first.');
    END;

    -- ========================================================================
    -- COLD TIER: Yearly partitions for data older than warm_cutoff
    -- ========================================================================
    IF v_min_date < v_cold_cutoff THEN
        DBMS_OUTPUT.PUT_LINE('Generating COLD tier partitions...');

        v_partition_date := TRUNC(v_min_date, 'YYYY');  -- Start of first year

        WHILE v_partition_date < v_cold_cutoff LOOP
            v_next_date := ADD_MONTHS(v_partition_date, 12);  -- Next year
            v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');

            DBMS_LOB.APPEND(v_partition_list,
                '    PARTITION ' || v_partition_name ||
                ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                ' TABLESPACE ' || v_cold_tablespace);

            IF v_cold_compression != 'NONE' THEN
                DBMS_LOB.APPEND(v_partition_list, ' COMPRESS FOR ' || v_cold_compression);
            END IF;

            DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));

            v_cold_count := v_cold_count + 1;
            v_partition_date := v_next_date;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('  Generated ' || v_cold_count || ' COLD partitions');
    END IF;

    -- ========================================================================
    -- WARM TIER: Yearly partitions between cold_cutoff and hot_cutoff
    -- ========================================================================
    IF v_cold_cutoff < v_warm_cutoff THEN
        DBMS_OUTPUT.PUT_LINE('Generating WARM tier partitions...');

        v_partition_date := TRUNC(GREATEST(v_min_date, v_cold_cutoff), 'YYYY');

        WHILE v_partition_date < v_hot_cutoff LOOP
            v_next_date := ADD_MONTHS(v_partition_date, 12);
            v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');

            DBMS_LOB.APPEND(v_partition_list,
                '    PARTITION ' || v_partition_name ||
                ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                ' TABLESPACE ' || v_warm_tablespace);

            IF v_warm_compression != 'NONE' THEN
                DBMS_LOB.APPEND(v_partition_list, ' COMPRESS FOR ' || v_warm_compression);
            END IF;

            DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));

            v_warm_count := v_warm_count + 1;
            v_partition_date := v_next_date;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('  Generated ' || v_warm_count || ' WARM partitions');
    END IF;

    -- ========================================================================
    -- HOT TIER: Monthly partitions for recent data (< hot_cutoff)
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('Generating HOT tier partitions...');

    v_partition_date := TRUNC(GREATEST(v_min_date, v_hot_cutoff), 'MM');  -- Start of month

    WHILE v_partition_date <= TRUNC(v_current_date, 'MM') LOOP
        v_next_date := ADD_MONTHS(v_partition_date, 1);  -- Next month
        v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

        DBMS_LOB.APPEND(v_partition_list,
            '    PARTITION ' || v_partition_name ||
            ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
            ' TABLESPACE ' || v_hot_tablespace);

        IF v_hot_compression != 'NONE' THEN
            DBMS_LOB.APPEND(v_partition_list, ' COMPRESS FOR ' || v_hot_compression);
        END IF;

        DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));

        v_hot_count := v_hot_count + 1;
        v_partition_date := v_next_date;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('  Generated ' || v_hot_count || ' HOT partitions');

    -- Remove trailing comma
    v_partition_list := RTRIM(v_partition_list, ',' || CHR(10));

    -- ========================================================================
    -- Build complete DDL
    -- ========================================================================

    -- Get column definitions (reuse existing logic from build_uniform_partitions)
    -- Note: v_columns CLOB is managed by get_column_definitions procedure
    -- which handles its own LOB initialization
    get_column_definitions(p_task, v_columns);

    -- Build DDL
    p_ddl := 'CREATE TABLE ' || p_task.source_owner || '.' || p_task.source_table || '_PART' || CHR(10);
    p_ddl := p_ddl || '(' || CHR(10) || v_columns || CHR(10) || ')' || CHR(10);
    p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);

    -- INTERVAL creates future HOT partitions automatically
    p_ddl := p_ddl || 'INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))' || CHR(10);

    -- Add explicit partition list
    p_ddl := p_ddl || '(' || CHR(10);
    p_ddl := p_ddl || v_partition_list || CHR(10);
    p_ddl := p_ddl || ')' || CHR(10);

    -- Add parallel clause if specified
    IF p_task.parallel_degree > 1 THEN
        p_ddl := p_ddl || 'PARALLEL ' || p_task.parallel_degree || CHR(10);
    END IF;

    -- Add row movement
    IF p_task.enable_row_movement = 'Y' THEN
        p_ddl := p_ddl || 'ENABLE ROW MOVEMENT';
    END IF;

    -- Clean up temporary LOB
    IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
        DBMS_LOB.FREETEMPORARY(v_partition_list);
    END IF;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Tiered Partition DDL Summary:');
    DBMS_OUTPUT.PUT_LINE('  COLD tier: ' || v_cold_count || ' yearly partitions');
    DBMS_OUTPUT.PUT_LINE('  WARM tier: ' || v_warm_count || ' yearly partitions');
    DBMS_OUTPUT.PUT_LINE('  HOT tier: ' || v_hot_count || ' monthly partitions');
    DBMS_OUTPUT.PUT_LINE('  Total: ' || (v_cold_count + v_warm_count + v_hot_count) || ' partitions');
    DBMS_OUTPUT.PUT_LINE('  Future partitions: INTERVAL monthly in ' || v_hot_tablespace);
    DBMS_OUTPUT.PUT_LINE('========================================');

    -- Log step completion
    log_step(
        p_task_id => p_task.task_id,
        p_step_number => v_step_number,
        p_step_name => 'Build Tiered Partitions',
        p_step_type => 'DDL_GENERATION',
        p_sql => p_ddl,
        p_status => 'SUCCESS',
        p_start_time => v_step_start,
        p_end_time => SYSTIMESTAMP
    );

EXCEPTION
    WHEN OTHERS THEN
        v_error_msg := SQLERRM;

        -- Log failure to table
        BEGIN
            log_step(
                p_task_id => p_task.task_id,
                p_step_number => v_step_number,
                p_step_name => 'Build Tiered Partitions',
                p_step_type => 'DDL_GENERATION',
                p_sql => NULL,
                p_status => 'FAILED',
                p_start_time => v_step_start,
                p_end_time => SYSTIMESTAMP,
                p_error_code => SQLCODE,
                p_error_message => v_error_msg
            );
        EXCEPTION
            WHEN OTHERS THEN NULL;  -- Suppress logging errors
        END;

        -- Clean up LOB in case of error
        BEGIN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
        EXCEPTION WHEN OTHERS THEN NULL; END;

        -- Log to console
        DBMS_OUTPUT.PUT_LINE('ERROR: Failed to build tiered partition DDL: ' || v_error_msg);
        RAISE;
END build_tiered_partitions;
```

### Component 4: Helper - `build_uniform_partitions()`

**File:** `scripts/table_migration_execution.sql`

**Extract existing uniform partition logic into helper:**

```sql
PROCEDURE build_uniform_partitions(
    p_task dwh_migration_tasks%ROWTYPE,
    p_ddl OUT CLOB
) AS
BEGIN
    -- This is the EXISTING build_partition_ddl logic (lines 161-459)
    -- Just extract it into this helper procedure for cleaner routing

    -- Existing code for:
    -- - Column definitions
    -- - Storage clause
    -- - Single INTERVAL clause
    -- - Single initial partition
    -- - etc.

    DBMS_OUTPUT.PUT_LINE('Using uniform interval partitioning (existing logic)');

    -- [Existing build_partition_ddl code goes here]

END build_uniform_partitions;
```

---

## Implementation Plan

### Phase 1: Template Enhancement (Week 1)

**Tasks:**
1. ✅ Add new tiered templates to `table_migration_setup.sql`
   - FACT_TABLE_STANDARD_TIERED
   - EVENTS_SHORT_RETENTION_TIERED
   - SCD2_VALID_FROM_TO_TIERED
2. ✅ Document JSON schema for tier_config
3. ✅ Test JSON parsing with sample data
4. ✅ Validate backward compatibility (existing templates still work)

**Deliverables:**
- Updated `table_migration_setup.sql` with tiered templates
- Template JSON schema documentation
- No schema changes required!

**Validation:**
```sql
-- Verify tiered templates exist
SELECT template_name,
       JSON_VALUE(policies_json, '$.tier_config.enabled') as tier_enabled,
       JSON_VALUE(policies_json, '$.tier_config.hot.age_months') as hot_months
FROM cmr.dwh_migration_ilm_templates
WHERE JSON_EXISTS(policies_json, '$.tier_config');

-- Test JSON parsing
DECLARE
    v_template cmr.dwh_migration_ilm_templates%ROWTYPE;
    v_json JSON_OBJECT_T;
    v_tier JSON_OBJECT_T;
BEGIN
    SELECT * INTO v_template
    FROM cmr.dwh_migration_ilm_templates
    WHERE template_name = 'FACT_TABLE_STANDARD_TIERED';

    v_json := JSON_OBJECT_T.PARSE(v_template.policies_json);
    v_tier := TREAT(v_json.get('tier_config') AS JSON_OBJECT_T);

    DBMS_OUTPUT.PUT_LINE('HOT months: ' || v_tier.get_object('hot').get_number('age_months'));
    DBMS_OUTPUT.PUT_LINE('WARM tablespace: ' || v_tier.get_object('warm').get_string('tablespace'));
END;
/
```

### Phase 2: Core Logic Implementation (Week 2)

**Tasks:**
1. ✅ Refactor `build_partition_ddl()` to route based on template
2. ✅ Extract existing logic to `build_uniform_partitions()` helper
3. ✅ Implement `build_tiered_partitions()` procedure
4. ✅ Implement tier boundary calculation logic
5. ✅ Add dual logging strategy:
   - Console logging (DBMS_OUTPUT.PUT_LINE) for immediate feedback
   - Table logging (dwh_migration_execution_log) for persistent audit trail
   - Use existing log_step() procedure for table logging
6. ✅ Implement proper LOB handling (SESSION duration, cleanup in success and exception paths)
7. ✅ Handle edge cases (no analysis data, invalid template, etc.)

**Deliverables:**
- Updated `table_migration_execution.sql` with tiered partition logic
- Dual logging implementation (console + table)
- Unit tests for tier boundary calculations
- DDL generation tests
- LOB memory leak tests
- Execution log query examples

**Validation:**
```sql
-- Test 1: Tier boundary calculation
DECLARE
    v_hot_cutoff DATE := ADD_MONTHS(SYSDATE, -12);
    v_warm_cutoff DATE := ADD_MONTHS(SYSDATE, -36);
    v_cold_cutoff DATE := ADD_MONTHS(SYSDATE, -84);
BEGIN
    DBMS_OUTPUT.PUT_LINE('Current date: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('HOT tier cutoff: ' || TO_CHAR(v_hot_cutoff, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('WARM tier cutoff: ' || TO_CHAR(v_warm_cutoff, 'YYYY-MM-DD'));
    DBMS_OUTPUT.PUT_LINE('COLD tier cutoff: ' || TO_CHAR(v_cold_cutoff, 'YYYY-MM-DD'));
END;
/

-- Test 2: LOB cleanup verification
-- Check session temporary LOB usage before and after procedure calls
SELECT * FROM v$temporary_lobs WHERE sid = SYS_CONTEXT('USERENV', 'SID');
-- Run build_tiered_partitions
-- Check again - should be 0 temporary LOBs remaining
SELECT * FROM v$temporary_lobs WHERE sid = SYS_CONTEXT('USERENV', 'SID');

-- Test 3: Exception handling with LOB cleanup
-- Force error in build_tiered_partitions (e.g., invalid task_id)
-- Verify LOBs are cleaned up in exception handler
-- Check: SELECT * FROM v$temporary_lobs WHERE sid = SYS_CONTEXT('USERENV', 'SID');

-- Test 4: Execution log validation
-- Query execution logs for a migration run
SELECT
    log_id,
    step_number,
    step_name,
    step_type,
    status,
    duration_seconds,
    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI:SS') as started_at,
    error_message
FROM cmr.dwh_migration_execution_log
WHERE task_id = v_task_id
  AND execution_id = g_execution_id
ORDER BY step_number;

-- Test 5: Performance metrics from logs
-- Analyze step durations across multiple migrations
SELECT
    step_name,
    COUNT(*) as executions,
    ROUND(AVG(duration_seconds), 2) as avg_duration_sec,
    ROUND(MIN(duration_seconds), 2) as min_duration_sec,
    ROUND(MAX(duration_seconds), 2) as max_duration_sec,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failures
FROM cmr.dwh_migration_execution_log
WHERE step_name = 'Build Tiered Partitions'
  AND start_time >= SYSDATE - 30  -- Last 30 days
GROUP BY step_name;

-- Test 6: Audit trail for specific task
-- Complete execution history with DDL
SELECT
    t.task_name,
    t.source_owner || '.' || t.source_table as source_table,
    l.step_number,
    l.step_name,
    l.status,
    l.duration_seconds,
    SUBSTR(l.sql_statement, 1, 100) as sql_preview,
    l.error_message
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE l.task_id = v_task_id
ORDER BY l.execution_id DESC, l.step_number;
```

### Phase 3: Integration Testing (Week 3)

**Test Cases:**

#### Test Case 1: Small Dataset (3 years)
```sql
-- Create test table
CREATE TABLE test_sales_3y AS
SELECT
    ROWNUM as sale_id,
    TRUNC(SYSDATE) - LEVEL as sale_date,
    DBMS_RANDOM.STRING('A', 50) as product_name,
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 1095;  -- 3 years of daily data

-- Create migration task with tiered template
DECLARE
    v_task_id NUMBER;
BEGIN
    INSERT INTO cmr.dwh_migration_tasks (
        task_name, source_owner, source_table,
        ilm_policy_template, status
    ) VALUES (
        'Test 3Y Tiered', USER, 'TEST_SALES_3Y',
        'FACT_TABLE_STANDARD_TIERED', 'PENDING'
    ) RETURNING task_id INTO v_task_id;

    -- Run analysis
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    -- Apply recommendations
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

    -- Preview DDL
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);
END;
/

-- Expected Result:
-- WARM tier: 2 yearly partitions (2022, 2023)
-- HOT tier: 12 monthly partitions (2024-01 through 2024-12)
-- Total: 14 partitions vs 36 monthly
```

#### Test Case 2: Large Dataset (12 years)
```sql
-- Create test table with 12 years of data
CREATE TABLE test_sales_12y AS
SELECT
    ROWNUM as sale_id,
    TO_DATE('2013-01-01', 'YYYY-MM-DD') + LEVEL as sale_date,
    DBMS_RANDOM.STRING('A', 50) as product_name,
    ROUND(DBMS_RANDOM.VALUE(10, 1000), 2) as amount
FROM dual
CONNECT BY LEVEL <= 4380;  -- 12 years of daily data

-- Create migration task
DECLARE
    v_task_id NUMBER;
BEGIN
    INSERT INTO cmr.dwh_migration_tasks (
        task_name, source_owner, source_table,
        ilm_policy_template, status
    ) VALUES (
        'Test 12Y Tiered', USER, 'TEST_SALES_12Y',
        'FACT_TABLE_STANDARD_TIERED', 'PENDING'
    ) RETURNING task_id INTO v_task_id;

    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);
END;
/

-- Expected Result:
-- COLD tier: 6 yearly partitions (2013-2018)
-- WARM tier: 3 yearly partitions (2019-2021)
-- HOT tier: 12 monthly partitions (2024-01 through 2024-12)
-- Total: 21 partitions vs 144 monthly (85% reduction)
```

#### Test Case 3: Backward Compatibility (No Template)
```sql
-- Create task without template (existing behavior)
DECLARE
    v_task_id NUMBER;
BEGIN
    INSERT INTO cmr.dwh_migration_tasks (
        task_name, source_owner, source_table,
        partition_type, partition_key, interval_clause, status
    ) VALUES (
        'Test Standard', USER, 'TEST_SALES_3Y',
        'RANGE(sale_date)', 'sale_date',
        'NUMTOYMINTERVAL(1,''MONTH'')', 'PENDING'
    ) RETURNING task_id INTO v_task_id;

    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);
END;
/

-- Expected Result: Uses build_uniform_partitions() (36 monthly partitions)
-- Existing behavior preserved
```

#### Test Case 4: Non-Tiered Template
```sql
-- Create task with template that has NO tier_config
DECLARE
    v_task_id NUMBER;
BEGIN
    INSERT INTO cmr.dwh_migration_tasks (
        task_name, source_owner, source_table,
        ilm_policy_template, status
    ) VALUES (
        'Test Non-Tiered Template', USER, 'TEST_SALES_3Y',
        'FACT_TABLE_STANDARD', 'PENDING'  -- Original template without tier_config
    ) RETURNING task_id INTO v_task_id;

    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);
    pck_dwh_table_migration_executor.apply_recommendations(v_task_id);
    pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);
END;
/

-- Expected Result: Uses build_uniform_partitions()
-- Backward compatible with existing templates
```

**Deliverables:**
- Test scripts for all scenarios
- Test results documentation
- Performance comparison report

### Phase 4: Documentation (Week 4)

**Documentation Updates:**
1. ✅ User guide for tiered partitioning
2. ✅ Template configuration guide
3. ✅ Tier design best practices
4. ✅ Troubleshooting guide
5. ✅ Migration from uniform to tiered

**Deliverables:**
- Updated `docs/table_migration_guide.md`
- New `docs/ilm_tiered_partitioning.md`
- Updated `README.md` with examples
- Template catalog with tier configurations

### Phase 5: Production Rollout (Week 5)

**Pre-Deployment:**
1. ✅ Code review with DBA team
2. ✅ Performance testing on production-like dataset
3. ✅ Rollback plan (disable tiered templates)
4. ✅ Deployment runbook

**Deployment Steps:**
1. Deploy to DEV environment (Day 1)
2. Create tiered templates in DEV
3. Test with sample migrations
4. Deploy to UAT environment (Day 3)
5. User acceptance testing (Days 4-5)
6. Deploy to PROD during maintenance window (Weekend)
7. Post-deployment validation (Day 8)

**Rollback Plan:**
- Simply stop using tiered templates
- Existing non-tiered templates continue to work
- Zero breaking changes
- Can disable specific tiered templates by removing tier_config from JSON

---

## Benefits Analysis

### Partition Count Reduction

| Scenario | Data Years | Current (Monthly) | ILM-Aware (Tiered) | Reduction |
|----------|-----------|-------------------|-------------------|-----------|
| Small | 3 years | 36 partitions | 14 partitions | 61% |
| Medium | 7 years | 84 partitions | 18 partitions | 79% |
| Large | 12 years | 144 partitions | 23 partitions | 84% |
| X-Large | 20 years | 240 partitions | 31 partitions | 87% |

### Post-Migration ILM Work Reduction

**12-Year Table Example:**

| Approach | Initial Partitions | Post-Migration Moves | Total Operations |
|----------|-------------------|---------------------|------------------|
| Current | 144 in TBS_HOT | 132 partition moves | 276 operations |
| ILM-Aware | 23 pre-tiered | 0 partition moves | 23 operations |
| **Improvement** | **84% fewer** | **100% fewer** | **92% fewer** |

### Simplified Architecture Benefits

| Aspect | Previous Plan | Revised Plan | Benefit |
|--------|--------------|--------------|---------|
| Schema Changes | 12 new columns in tasks | 0 schema changes | No migration risk |
| Data Duplication | Tier config copied to each task | Template reference only | Single source of truth |
| Maintenance | Update each task individually | Update template once | Centralized management |
| Template Versioning | Complex task updates | Simple template versioning | Easy evolution |
| Backward Compatibility | New columns nullable | No schema change | Zero breaking changes |
| Logging Strategy | Console only | Dual (console + table) | Production monitoring + audit trail |

---

## Risk Analysis

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| JSON parsing errors | Medium | Medium | Schema validation, error handling, fallback |
| Incorrect tier boundary calculation | Medium | High | Extensive testing, validation checks, logging |
| Template misconfiguration | Medium | Medium | Template validation procedure, examples |
| Performance impact of large DDL | Low | Medium | Test with production-size datasets |

### Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| User confusion with templates | Medium | Low | Clear documentation, training, examples |
| Wrong tier boundaries chosen | Low | Medium | Best practice guidelines, tier calculator tool |
| Tablespace not existing | Medium | Low | Validation before migration, clear error messages |

### Reduced Risks vs Original Plan

| Risk (Original Plan) | Status in Revised Plan |
|---------------------|------------------------|
| Schema migration failure | ❌ Eliminated - no schema changes |
| Data duplication sync issues | ❌ Eliminated - no duplication |
| Task update complexity | ❌ Eliminated - template only |
| Backward compatibility issues | ❌ Eliminated - zero breaking changes |

---

## Success Metrics

### Technical Metrics

1. **Partition Count Reduction**
   - Target: 80%+ reduction for tables > 5 years
   - Measure: Compare partition counts before/after

2. **Migration Performance**
   - Target: No degradation vs current approach
   - Measure: Migration duration for 100GB, 500GB, 1TB tables

3. **Query Performance**
   - Target: 10%+ improvement in queries with date filters
   - Measure: Execution plans, elapsed time

4. **Post-Migration ILM Work**
   - Target: 90%+ reduction in partition moves
   - Measure: Count of ILM operations in first 30 days

### Operational Metrics

1. **Adoption Rate**
   - Target: 50%+ of new migrations use tiered templates within 3 months
   - Measure: Task creation with tiered templates

2. **Storage Savings**
   - Target: Immediate compression for 70%+ of historical data
   - Measure: Compare storage before/after migration

3. **Template Usage**
   - Target: 5+ tiered templates created for different use cases
   - Measure: Template catalog growth

4. **Observability and Monitoring**
   - Target: 100% of migrations logged to execution log table
   - Measure: Execution log entries per migration, average step duration tracking
   - Benefit: Complete audit trail for compliance and troubleshooting

---

## Appendix A: Template Examples

### Template 1: Standard Fact Table (7-Year Retention)

```sql
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'FACT_TABLE_7Y_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'FACT_TABLE_7Y_TIERED',
        'Standard fact table with 7-year retention (HOT=1y monthly, WARM=3y yearly, COLD=7y yearly)',
        'FACT',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 12,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE"
                },
                "warm": {
                    "age_months": 36,
                    "interval": "YEARLY",
                    "tablespace": "TBS_WARM",
                    "compression": "BASIC"
                },
                "cold": {
                    "age_months": 84,
                    "interval": "YEARLY",
                    "tablespace": "TBS_COLD",
                    "compression": "OLTP"
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "priority": 300},
                {"policy_name": "{TABLE}_READONLY", "age_months": 84, "action": "READ_ONLY", "priority": 400},
                {"policy_name": "{TABLE}_PURGE", "age_months": 84, "action": "DROP", "priority": 900}
            ]
        }'
    );
```

### Template 2: High-Volume Events (90-Day Retention)

```sql
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'EVENTS_90D_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'EVENTS_90D_TIERED',
        'High-volume event table with 90-day retention (HOT=7d daily, WARM=30d weekly, COLD=90d monthly)',
        'EVENTS',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_days": 7,
                    "interval": "DAILY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE"
                },
                "warm": {
                    "age_days": 30,
                    "interval": "WEEKLY",
                    "tablespace": "TBS_WARM",
                    "compression": "QUERY HIGH"
                },
                "cold": {
                    "age_days": 90,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_COLD",
                    "compression": "ARCHIVE HIGH"
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_days": 7, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_days": 30, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 300},
                {"policy_name": "{TABLE}_PURGE", "age_days": 90, "action": "DROP", "priority": 900}
            ]
        }'
    );
```

### Template 3: SCD2 Historical (Permanent Retention)

```sql
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'SCD2_PERMANENT_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'SCD2_PERMANENT_TIERED',
        'SCD2 with permanent retention (HOT=1y monthly, WARM=5y yearly, COLD=permanent yearly)',
        'SCD2',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 12,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE"
                },
                "warm": {
                    "age_months": 60,
                    "interval": "YEARLY",
                    "tablespace": "TBS_WARM",
                    "compression": "QUERY HIGH"
                },
                "cold": {
                    "age_months": null,
                    "interval": "YEARLY",
                    "tablespace": "TBS_COLD",
                    "compression": "ARCHIVE HIGH"
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 300},
                {"policy_name": "{TABLE}_READONLY", "age_months": 60, "action": "READ_ONLY", "priority": 400}
            ]
        }'
    );
```

---

## Appendix B: Usage Examples

### Example 1: Migrating a 12-Year Fact Table

```sql
-- Step 1: Create task with tiered template
DECLARE
    v_task_id NUMBER;
BEGIN
    INSERT INTO cmr.dwh_migration_tasks (
        task_name,
        source_owner,
        source_table,
        ilm_policy_template,  -- Template reference only!
        status
    ) VALUES (
        'Migrate Sales Fact',
        'DWH',
        'SALES_FACT',
        'FACT_TABLE_7Y_TIERED',  -- Points to template
        'PENDING'
    ) RETURNING task_id INTO v_task_id;

    DBMS_OUTPUT.PUT_LINE('Created task: ' || v_task_id);
END;
/

-- Step 2: Analyze table
EXEC pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

-- Step 3: Review recommendations
SELECT
    task_name,
    recommended_strategy,
    recommended_method,
    complexity_score,
    estimated_downtime_minutes
FROM cmr.dwh_v_migration_task_status
WHERE task_id = v_task_id;

-- Step 4: Apply recommendations
EXEC pck_dwh_table_migration_executor.apply_recommendations(v_task_id);

-- Step 5: Preview DDL (simulation mode)
EXEC pck_dwh_table_migration_executor.execute_migration(v_task_id, p_simulate => TRUE);

-- Review generated DDL in logs
-- Will show:
--   COLD tier: 6 yearly partitions (2013-2018)
--   WARM tier: 3 yearly partitions (2019-2021)
--   HOT tier: 12 monthly partitions (2024-01 to 2024-12)
--   Total: 21 partitions instead of 144

-- Step 6: Execute migration
EXEC pck_dwh_table_migration_executor.execute_migration(v_task_id);

-- Step 7: Validate
SELECT
    task_name,
    status,
    source_rows,
    target_rows,
    validation_status,
    space_saved_mb
FROM cmr.dwh_v_migration_task_status
WHERE task_id = v_task_id;

-- Step 8: Apply ILM policies (for ongoing lifecycle management)
EXEC pck_dwh_table_migration_executor.apply_ilm_policies(v_task_id);
```

### Example 2: Querying Partition Structure

```sql
-- View partition distribution after tiered migration
SELECT
    partition_name,
    high_value,
    tablespace_name,
    compression,
    compress_for,
    num_rows,
    ROUND(bytes/1024/1024, 2) as size_mb,
    CASE
        WHEN partition_name LIKE 'P_____' THEN 'COLD/WARM'
        WHEN partition_name LIKE 'P_____\__%' ESCAPE '\' THEN 'HOT'
        ELSE 'OTHER'
    END as tier
FROM dba_tab_partitions
WHERE table_owner = 'DWH'
  AND table_name = 'SALES_FACT_PART'
ORDER BY partition_position;

-- Expected output:
-- P_2013      | TBS_COLD | ENABLED | OLTP       | COLD/WARM
-- P_2014      | TBS_COLD | ENABLED | OLTP       | COLD/WARM
-- ...
-- P_2022      | TBS_WARM | ENABLED | BASIC      | COLD/WARM
-- P_2023      | TBS_WARM | ENABLED | BASIC      | COLD/WARM
-- P_2024_01   | TBS_HOT  | DISABLED| NULL       | HOT
-- P_2024_02   | TBS_HOT  | DISABLED| NULL       | HOT
-- ...
```

### Example 3: Monitoring Migration Execution Progress

```sql
-- Real-time monitoring of ongoing migration
-- Shows all steps for the current execution
SELECT
    step_number,
    step_name,
    step_type,
    status,
    ROUND(duration_seconds, 2) as duration_sec,
    TO_CHAR(start_time, 'HH24:MI:SS') as started_at,
    TO_CHAR(end_time, 'HH24:MI:SS') as ended_at,
    error_message
FROM cmr.dwh_migration_execution_log
WHERE execution_id = (
    -- Get most recent execution ID
    SELECT MAX(execution_id)
    FROM cmr.dwh_migration_execution_log
    WHERE task_id = :task_id
)
ORDER BY step_number;

-- Expected output during tiered partitioning:
-- STEP_NUMBER | STEP_NAME                 | STATUS  | DURATION_SEC
-- 1           | Create backup table       | SUCCESS | 15.23
-- 2           | Validate tablespaces      | SUCCESS | 0.45
-- 10          | Build Tiered Partitions   | SUCCESS | 2.87
-- 11          | Create partitioned table  | SUCCESS | 45.12
-- 12          | Insert data (CTAS)        | SUCCESS | 320.45
-- 13          | Validate row counts       | SUCCESS | 5.67

-- Historical performance analysis
-- Compare tiered vs uniform partition generation
SELECT
    t.task_name,
    t.ilm_policy_template,
    l.step_name,
    l.duration_seconds,
    l.start_time,
    CASE
        WHEN t.ilm_policy_template LIKE '%TIERED%' THEN 'Tiered'
        ELSE 'Uniform'
    END as partition_type
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE l.step_name = 'Build Tiered Partitions'
  AND l.status = 'SUCCESS'
  AND l.start_time >= SYSDATE - 90
ORDER BY l.start_time DESC;

-- Error analysis
-- Find all failed tiered partition generations
SELECT
    t.task_name,
    t.source_owner || '.' || t.source_table as source_table,
    l.error_code,
    l.error_message,
    l.start_time,
    l.duration_seconds
FROM cmr.dwh_migration_execution_log l
JOIN cmr.dwh_migration_tasks t ON t.task_id = l.task_id
WHERE l.step_name = 'Build Tiered Partitions'
  AND l.status = 'FAILED'
ORDER BY l.start_time DESC;

-- Audit complete migration with all DDL
SELECT
    l.step_number,
    l.step_name,
    l.step_type,
    l.status,
    l.duration_seconds,
    l.sql_statement,  -- Full DDL
    l.error_message
FROM cmr.dwh_migration_execution_log l
WHERE l.task_id = :task_id
  AND l.execution_id = :execution_id
ORDER BY l.step_number;
```

### Example 4: Creating Custom Tiered Template

```sql
-- Create custom template for specific business requirements
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'CUSTOM_FINANCIAL_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'CUSTOM_FINANCIAL_TIERED',
        'Custom financial data template (HOT=6m weekly, WARM=2y monthly, COLD=10y yearly)',
        'CUSTOM',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 6,
                    "interval": "WEEKLY",
                    "tablespace": "TBS_FINANCIAL_HOT",
                    "compression": "NONE"
                },
                "warm": {
                    "age_months": 24,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_FINANCIAL_WARM",
                    "compression": "OLTP"
                },
                "cold": {
                    "age_months": 120,
                    "interval": "YEARLY",
                    "tablespace": "TBS_FINANCIAL_COLD",
                    "compression": "ARCHIVE HIGH"
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 6, "action": "MOVE", "tablespace": "TBS_FINANCIAL_WARM", "compression": "OLTP", "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 24, "action": "MOVE", "tablespace": "TBS_FINANCIAL_COLD", "compression": "ARCHIVE HIGH", "priority": 300},
                {"policy_name": "{TABLE}_READONLY", "age_months": 120, "action": "READ_ONLY", "priority": 400}
            ]
        }'
    );
/

COMMIT;
```

---

## Appendix C: Comparison with Original Plan

### Architectural Simplification

| Component | Original Plan | Revised Plan | Change Rationale |
|-----------|--------------|--------------|------------------|
| **Schema Changes** | Add 12 columns to dwh_migration_tasks | None | No duplication, single source of truth |
| **Data Storage** | Copy tier config to each task | Store only template name | Consistent with existing ILM pattern |
| **Configuration Updates** | Update each task individually | Update template once | Centralized management |
| **apply_recommendations()** | Parse template, copy to task columns | No changes needed | Simpler implementation |
| **build_partition_ddl()** | Read tier config from task columns | Read template, parse tier_config | Direct template access |
| **Backward Compatibility** | New nullable columns | No schema changes | Zero breaking changes |
| **Template Versioning** | Complex task migration | Simple template versioning | Future-proof |

### Implementation Effort Reduction

| Phase | Original Estimate | Revised Estimate | Savings |
|-------|------------------|------------------|---------|
| Phase 1: Schema Changes | 3 days | 1 day (templates only) | 67% |
| Phase 2: Core Logic | 5 days | 4 days | 20% |
| Phase 3: Testing | 5 days | 4 days | 20% |
| Phase 4: Documentation | 3 days | 2 days | 33% |
| Phase 5: Rollout | 5 days | 3 days | 40% |
| **Total** | **21 days** | **14 days** | **33% faster** |

### Risk Reduction

| Risk Category | Original Plan | Revised Plan |
|--------------|---------------|--------------|
| Schema migration failure | Medium risk | Eliminated |
| Data synchronization issues | Medium risk | Eliminated |
| Backward compatibility issues | Medium risk | Eliminated |
| Configuration drift | High risk | Eliminated |
| Template versioning complexity | High risk | Low risk |

---

## Next Steps

**Immediate Actions:**
1. ✅ Review and approve this revised plan
2. ✅ Allocate resources (1 senior developer)
3. ✅ Set up development environment
4. ✅ Create Jira epic and stories

**Week 1 Priorities:**
1. Add tiered templates to table_migration_setup.sql
2. Test JSON parsing with sample templates
3. Document template JSON schema

**Dependencies:**
- None - fully backward compatible
- Optional: Create TBS_HOT, TBS_WARM, TBS_COLD tablespaces in test environments

**Go/No-Go Decision Point:**
- End of Phase 2 (Week 2)
- Criteria: DDL generation tests passing, peer review approval

---

**END OF DOCUMENT**
