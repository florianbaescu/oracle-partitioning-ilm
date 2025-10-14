# AUTOMATIC LIST Partitioning - Simplified Implementation Plan

## Approach

**Manual Configuration Only** - No automatic analysis or recommendations
- Users explicitly specify AUTOMATIC LIST when needed
- Framework supports execution but doesn't suggest it
- Simpler implementation, faster delivery

## Requirements

### 1. Support AUTOMATIC LIST Syntax
```sql
CREATE TABLE sales_by_region (...)
PARTITION BY LIST (region) AUTOMATIC
(
    PARTITION p_xdef VALUES (DEFAULT)
)
ENABLE ROW MOVEMENT;
```

### 2. Support Multiple Columns
```sql
PARTITION BY LIST (region, country) AUTOMATIC
(
    PARTITION p_xdef VALUES (DEFAULT)
)
```

### 3. Always Use P_XDEF for Default Partition
- Consistent naming convention: `P_XDEF` (partition exchange default)
- Makes it easy to identify the default partition
- Easier partition management

---

## Implementation Steps

### Step 1: Schema Changes

**Add columns to dwh_migration_tasks:**
```sql
ALTER TABLE cmr.dwh_migration_tasks ADD (
    automatic_list CHAR(1) DEFAULT 'N',
    list_default_values VARCHAR2(4000),  -- Comma-separated default values for P_XDEF partition
    CONSTRAINT chk_automatic_list CHECK (automatic_list IN ('Y', 'N'))
);

COMMENT ON COLUMN cmr.dwh_migration_tasks.automatic_list
IS 'Enable AUTOMATIC LIST partitioning (Oracle 12.2+). Creates partitions automatically for new values.';

COMMENT ON COLUMN cmr.dwh_migration_tasks.list_default_values
IS 'Default values for P_XDEF partition (e.g., ''NAV'' for VARCHAR, -1 for NUMBER, DATE ''5999-12-31'' for DATE). If NULL, framework determines based on data type.';
```

**Use existing columns:**
- `partition_type` field: `'LIST(region)'` or `'LIST(region, country)'`
- `partition_key` field: `'region'` or `'region, country'`

### Step 2: Type-Aware Default Values

**Default Value Constants by Data Type:**

```plsql
-- Constants for default partition values
C_DEFAULT_VARCHAR   CONSTANT VARCHAR2(100) := '''NAV''';
C_DEFAULT_CHAR      CONSTANT VARCHAR2(100) := '''NAV''';
C_DEFAULT_NUMBER    CONSTANT VARCHAR2(100) := '-1';
C_DEFAULT_DATE      CONSTANT VARCHAR2(100) := 'DATE ''5999-12-31''';
C_DEFAULT_TIMESTAMP CONSTANT VARCHAR2(100) := 'TO_TIMESTAMP(''5999-12-31 23:59:59'',''YYYY-MM-DD HH24:MI:SS'')';
```

**Rationale:**
- **VARCHAR2/CHAR:** `'NAV'` - standard "Not Available" placeholder for missing categorical data
- **NUMBER:** `-1` - sentinel value for missing/unknown numeric categories
- **DATE:** `DATE '5999-12-31'` - far future date to represent "no expiry" or "permanent" data
- **TIMESTAMP:** `TIMESTAMP '5999-12-31 23:59:59'` - far future timestamp for similar use cases

**Function to determine default values:**

```plsql
FUNCTION get_list_default_values(
    p_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_key VARCHAR2,  -- Can be 'region' or 'region, country'
    p_user_defaults VARCHAR2 DEFAULT NULL
) RETURN VARCHAR2
AS
    v_columns SYS.ODCIVARCHAR2LIST;
    v_column_name VARCHAR2(128);
    v_data_type VARCHAR2(128);
    v_default_values VARCHAR2(4000);
    v_values_list VARCHAR2(4000) := '';
    v_pos NUMBER;
BEGIN
    -- If user provided defaults, validate and return them
    IF p_user_defaults IS NOT NULL THEN
        -- Validation happens in validate_list_defaults function
        RETURN p_user_defaults;
    END IF;

    -- Parse partition key (handle single or multiple columns)
    v_columns := SYS.ODCIVARCHAR2LIST();
    FOR col IN (
        SELECT TRIM(REGEXP_SUBSTR(p_partition_key, '[^,]+', 1, LEVEL)) AS column_name
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(p_partition_key, ',') + 1
    ) LOOP
        v_columns.EXTEND;
        v_columns(v_columns.COUNT) := col.column_name;
    END LOOP;

    -- Get data types and build default values for each column
    FOR i IN 1..v_columns.COUNT LOOP
        v_column_name := v_columns(i);

        -- Get data type
        BEGIN
            SELECT data_type INTO v_data_type
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND column_name = v_column_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20610,
                    'Column ' || v_column_name || ' not found in table ' || p_owner || '.' || p_table_name);
        END;

        -- Determine default based on data type
        IF v_data_type IN ('VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
            v_default_values := C_DEFAULT_VARCHAR;
        ELSIF v_data_type IN ('NUMBER', 'INTEGER', 'FLOAT', 'BINARY_INTEGER') THEN
            v_default_values := C_DEFAULT_NUMBER;
        ELSIF v_data_type = 'DATE' THEN
            v_default_values := C_DEFAULT_DATE;
        ELSIF v_data_type LIKE 'TIMESTAMP%' THEN
            v_default_values := C_DEFAULT_TIMESTAMP;
        ELSE
            RAISE_APPLICATION_ERROR(-20611,
                'Unsupported data type for LIST partitioning: ' || v_data_type || ' (column: ' || v_column_name || ')');
        END IF;

        -- For multi-column, we need to create tuples
        -- For now, use first column's defaults
        -- TODO: Enhancement for multi-column tuple defaults
        IF i = 1 THEN
            v_values_list := v_default_values;
        END IF;
    END LOOP;

    RETURN v_values_list;
END get_list_default_values;
```

**Function to validate user-provided defaults:**

```plsql
FUNCTION validate_list_defaults(
    p_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_key VARCHAR2,
    p_user_defaults VARCHAR2
) RETURN BOOLEAN
AS
    v_columns SYS.ODCIVARCHAR2LIST;
    v_column_name VARCHAR2(128);
    v_data_type VARCHAR2(128);
    v_test_sql VARCHAR2(4000);
    v_dummy NUMBER;
BEGIN
    -- Parse partition key columns
    v_columns := SYS.ODCIVARCHAR2LIST();
    FOR col IN (
        SELECT TRIM(REGEXP_SUBSTR(p_partition_key, '[^,]+', 1, LEVEL)) AS column_name
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(p_partition_key, ',') + 1
    ) LOOP
        v_columns.EXTEND;
        v_columns(v_columns.COUNT) := col.column_name;
    END LOOP;

    -- Get data type of first column (primary validation)
    SELECT data_type INTO v_data_type
    FROM dba_tab_columns
    WHERE owner = p_owner
    AND table_name = p_table_name
    AND column_name = v_columns(1);

    -- Parse each default value and validate against data type
    FOR val IN (
        SELECT TRIM(REGEXP_SUBSTR(p_user_defaults, '[^,]+', 1, LEVEL)) AS default_value
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(p_user_defaults, ',') + 1
    ) LOOP
        -- Build test SQL to validate the value can be used in LIST VALUES clause
        BEGIN
            IF UPPER(val.default_value) = 'NULL' THEN
                -- NULL is always valid
                CONTINUE;
            END IF;

            -- Test if value is compatible with data type
            IF v_data_type IN ('VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
                -- Check if value is a valid string literal (should be quoted)
                IF val.default_value NOT LIKE '''%''' THEN
                    RAISE_APPLICATION_ERROR(-20612,
                        'String values must be quoted: ' || val.default_value || '. Example: ''UNKNOWN''');
                END IF;
            ELSIF v_data_type IN ('NUMBER', 'INTEGER', 'FLOAT') THEN
                -- Try to convert to number
                v_test_sql := 'SELECT TO_NUMBER(' || val.default_value || ') FROM DUAL';
                EXECUTE IMMEDIATE v_test_sql INTO v_dummy;
            ELSIF v_data_type = 'DATE' THEN
                -- Should be a valid DATE expression or literal
                IF val.default_value NOT LIKE 'TO_DATE(%' AND val.default_value NOT LIKE 'DATE%' THEN
                    RAISE_APPLICATION_ERROR(-20613,
                        'DATE values must use TO_DATE() or DATE literal: ' || val.default_value);
                END IF;
            ELSIF v_data_type LIKE 'TIMESTAMP%' THEN
                -- Should be a valid TIMESTAMP expression
                IF val.default_value NOT LIKE 'TO_TIMESTAMP(%' AND val.default_value NOT LIKE 'TIMESTAMP%' THEN
                    RAISE_APPLICATION_ERROR(-20614,
                        'TIMESTAMP values must use TO_TIMESTAMP() or TIMESTAMP literal: ' || val.default_value);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20615,
                    'Invalid default value (' || val.default_value || ') for data type ' || v_data_type || ': ' || SQLERRM);
        END;
    END LOOP;

    RETURN TRUE;
END validate_list_defaults;
```

### Step 3: DDL Generation

**Modify `build_partition_ddl` procedure:**

```plsql
PROCEDURE build_partition_ddl(
    p_task dwh_migration_tasks%ROWTYPE,
    p_ddl OUT CLOB
) AS
    v_partition_clause VARCHAR2(4000);
    v_default_values VARCHAR2(4000);
BEGIN
    -- ... existing column and storage clause logic ...

    -- Build partition clause
    IF p_task.automatic_list = 'Y' THEN
        -- Get default values (user-provided or type-based)
        v_default_values := get_list_default_values(
            p_owner => p_task.source_owner,
            p_table_name => p_task.source_table,
            p_partition_key => p_task.partition_key,
            p_user_defaults => p_task.list_default_values
        );

        -- Validate if user provided custom defaults
        IF p_task.list_default_values IS NOT NULL THEN
            IF NOT validate_list_defaults(
                p_owner => p_task.source_owner,
                p_table_name => p_task.source_table,
                p_partition_key => p_task.partition_key,
                p_user_defaults => p_task.list_default_values
            ) THEN
                RAISE_APPLICATION_ERROR(-20616,
                    'Invalid list_default_values: ' || p_task.list_default_values);
            END IF;
        END IF;

        -- AUTOMATIC LIST partitioning
        v_partition_clause := 'PARTITION BY ' || p_task.partition_type || ' AUTOMATIC' || CHR(10);
        v_partition_clause := v_partition_clause || '(' || CHR(10);
        v_partition_clause := v_partition_clause || '    PARTITION p_xdef VALUES (' || v_default_values || ')' || CHR(10);
        v_partition_clause := v_partition_clause || ')';

        p_ddl := p_ddl || v_partition_clause || CHR(10);

        DBMS_OUTPUT.PUT_LINE('Using default partition values: ' || v_default_values);
    ELSE
        -- Existing logic for INTERVAL/RANGE/HASH
        p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);

        IF p_task.interval_clause IS NOT NULL THEN
            -- INTERVAL partitioning logic
            -- ... existing code ...
        ELSE
            -- Regular partitioning logic
            -- ... existing code ...
        END IF;
    END IF;

    -- ... rest of DDL (storage clause, row movement) ...
END build_partition_ddl;
```

### Step 3: Validation (Optional but Recommended)

**Add Oracle version check in execute_migration:**

```plsql
-- Check if AUTOMATIC LIST is used
IF v_task.automatic_list = 'Y' THEN
    -- Verify Oracle version
    DECLARE
        v_version VARCHAR2(100);
        v_major NUMBER;
        v_minor NUMBER;
    BEGIN
        SELECT version INTO v_version FROM v$instance;

        -- Extract major.minor version (e.g., "19.3.0.0.0" -> 19.3)
        v_major := TO_NUMBER(SUBSTR(v_version, 1, INSTR(v_version, '.') - 1));
        v_minor := TO_NUMBER(SUBSTR(v_version, INSTR(v_version, '.') + 1,
                                    INSTR(v_version, '.', 1, 2) - INSTR(v_version, '.') - 1));

        IF v_major < 12 OR (v_major = 12 AND v_minor < 2) THEN
            RAISE_APPLICATION_ERROR(-20600,
                'AUTOMATIC LIST partitioning requires Oracle 12.2 or higher. Current version: ' || v_version);
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Cannot determine version - proceed with warning
            DBMS_OUTPUT.PUT_LINE('WARNING: Cannot verify Oracle version for AUTOMATIC LIST support');
    END;
END IF;
```

### Step 4: Partition Renaming After Cutover

**Rename system-generated partition names based on HIGH_VALUE:**

After the cutover completes, Oracle will have created partitions with system-generated names like `SYS_P12345`. We want to rename these to human-readable names based on their partition values.

**Add new procedure: `rename_list_partitions`**

```plsql
PROCEDURE rename_list_partitions(
    p_task_id NUMBER,
    p_owner VARCHAR2,
    p_table_name VARCHAR2
) AS
    v_partition_name VARCHAR2(128);
    v_high_value LONG;
    v_high_value_str VARCHAR2(4000);
    v_new_partition_name VARCHAR2(128);
    v_sql VARCHAR2(1000);
    v_step NUMBER := 1000;  -- High step number for post-cutover operations
    v_start TIMESTAMP;
    v_renamed_count NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Renaming AUTOMATIC LIST partitions to human-readable names...');

    -- Get all partitions except P_XDEF
    FOR part IN (
        SELECT partition_name, high_value
        FROM dba_tab_partitions
        WHERE table_owner = p_owner
        AND table_name = p_table_name
        AND partition_name != 'P_XDEF'
        AND partition_name LIKE 'SYS_P%'  -- Only system-generated names
        ORDER BY partition_position
    ) LOOP
        v_start := SYSTIMESTAMP;

        -- Get high_value as string (LONG datatype requires special handling)
        BEGIN
            SELECT high_value INTO v_high_value
            FROM dba_tab_partitions
            WHERE table_owner = p_owner
            AND table_name = p_table_name
            AND partition_name = part.partition_name;

            -- Convert LONG to VARCHAR2
            v_high_value_str := SUBSTR(part.high_value, 1, 4000);

            -- Generate human-readable partition name from high_value
            v_new_partition_name := generate_partition_name_from_value(v_high_value_str);

            -- Rename partition
            v_sql := 'ALTER TABLE ' || p_owner || '.' || p_table_name ||
                    ' RENAME PARTITION ' || part.partition_name ||
                    ' TO ' || v_new_partition_name;

            EXECUTE IMMEDIATE v_sql;

            log_step(p_task_id, v_step, 'Rename partition: ' || part.partition_name || ' -> ' || v_new_partition_name,
                    'RENAME_PARTITION', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);

            DBMS_OUTPUT.PUT_LINE('  Renamed: ' || part.partition_name || ' -> ' || v_new_partition_name ||
                                ' (value: ' || SUBSTR(v_high_value_str, 1, 50) || ')');

            v_renamed_count := v_renamed_count + 1;
            v_step := v_step + 1;

        EXCEPTION
            WHEN OTHERS THEN
                -- Log error but continue with other partitions
                log_step(p_task_id, v_step, 'Rename partition: ' || part.partition_name,
                        'RENAME_PARTITION', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                        SQLCODE, SQLERRM);

                DBMS_OUTPUT.PUT_LINE('  WARNING: Failed to rename partition ' || part.partition_name || ': ' || SQLERRM);
                v_step := v_step + 1;
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Partition renaming complete: ' || v_renamed_count || ' partitions renamed');
END rename_list_partitions;


FUNCTION generate_partition_name_from_value(
    p_high_value VARCHAR2
) RETURN VARCHAR2
AS
    v_partition_name VARCHAR2(128);
    v_value_clean VARCHAR2(200);
    v_values SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
    v_name_parts VARCHAR2(200) := '';
    v_max_length NUMBER := 30;  -- Oracle partition name limit
BEGIN
    -- Parse high_value to extract actual values
    -- Example formats:
    -- Single: 'NORTH'
    -- Multiple: 'NORTH', 'USA'
    -- Number: 1
    -- Multiple numbers: 1, 2

    -- Remove leading/trailing whitespace and quotes
    v_value_clean := TRIM(p_high_value);

    -- Extract values (handle single or multiple)
    FOR val IN (
        SELECT TRIM(BOTH '''' FROM TRIM(REGEXP_SUBSTR(v_value_clean, '[^,]+', 1, LEVEL))) AS value_part
        FROM DUAL
        CONNECT BY LEVEL <= REGEXP_COUNT(v_value_clean, ',') + 1
    ) LOOP
        IF val.value_part IS NOT NULL AND val.value_part != 'NULL' THEN
            v_values.EXTEND;
            v_values(v_values.COUNT) := val.value_part;
        END IF;
    END LOOP;

    -- Build partition name from values
    IF v_values.COUNT = 0 THEN
        -- No values found, keep original name
        RETURN NULL;
    ELSIF v_values.COUNT = 1 THEN
        -- Single value: P_<VALUE>
        v_name_parts := 'P_' || UPPER(REGEXP_REPLACE(v_values(1), '[^A-Z0-9_]', '_'));
    ELSE
        -- Multiple values: P_<VALUE1>_<VALUE2>
        FOR i IN 1..LEAST(v_values.COUNT, 3) LOOP  -- Limit to 3 values
            IF i = 1 THEN
                v_name_parts := 'P_' || UPPER(REGEXP_REPLACE(v_values(i), '[^A-Z0-9_]', '_'));
            ELSE
                v_name_parts := v_name_parts || '_' || UPPER(REGEXP_REPLACE(v_values(i), '[^A-Z0-9_]', '_'));
            END IF;
        END LOOP;

        -- If more than 3 values, add suffix
        IF v_values.COUNT > 3 THEN
            v_name_parts := v_name_parts || '_ETC';
        END IF;
    END IF;

    -- Truncate to Oracle's partition name limit
    IF LENGTH(v_name_parts) > v_max_length THEN
        v_name_parts := SUBSTR(v_name_parts, 1, v_max_length);
    END IF;

    RETURN v_name_parts;

EXCEPTION
    WHEN OTHERS THEN
        -- If name generation fails, return NULL (keep original name)
        DBMS_OUTPUT.PUT_LINE('  WARNING: Could not generate partition name from: ' || p_high_value);
        RETURN NULL;
END generate_partition_name_from_value;
```

**Call `rename_list_partitions` after cutover in `migrate_using_ctas`:**

```plsql
-- After Step 9: Rename constraints
-- Add new step for AUTOMATIC LIST partition renaming

-- Step 10: Rename AUTOMATIC LIST partitions (if applicable)
IF v_task.automatic_list = 'Y' THEN
    v_step := v_step + 10;
    v_start := SYSTIMESTAMP;
    DBMS_OUTPUT.PUT_LINE('Renaming AUTOMATIC LIST partitions...');

    rename_list_partitions(
        p_task_id => p_task_id,
        p_owner => v_task.source_owner,
        p_table_name => v_task.source_table  -- Already renamed to final name
    );
END IF;
```

**Partition Naming Examples:**

| HIGH_VALUE | Generated Name | Notes |
|------------|---------------|--------|
| `'NORTH'` | `P_NORTH` | Single string value |
| `'TENANT_001'` | `P_TENANT_001` | Tenant ID |
| `1` | `P_1` | Single numeric value |
| `'NORTH', 'USA'` | `P_NORTH_USA` | Multi-column |
| `'REGION-WEST', 'COUNTRY-USA'` | `P_REGION_WEST_COUNTRY_USA` | Special chars converted to underscore |
| `'VERY_LONG_REGION_NAME_HERE'` | `P_VERY_LONG_REGION_NAME_H` | Truncated to 30 chars |
| `'A', 'B', 'C', 'D'` | `P_A_B_C_ETC` | More than 3 values |

### Step 5: Documentation

**Update examples/table_migration_examples.sql:**

```sql
-- =============================================================================
-- AUTOMATIC LIST PARTITIONING EXAMPLES (Oracle 12.2+)
-- =============================================================================

-- Example 1: Single column AUTOMATIC LIST (multi-tenant)
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate TENANT_DATA',
    USER,
    'TENANT_DATA',
    'LIST(tenant_id)',
    'tenant_id',
    'Y',  -- Enable AUTOMATIC LIST
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);

-- Resulting DDL:
-- CREATE TABLE tenant_data_part (...)
-- PARTITION BY LIST (tenant_id) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES (DEFAULT)
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Example 2: Multi-column AUTOMATIC LIST (region + country)
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate SALES_REGIONAL',
    USER,
    'SALES_REGIONAL',
    'LIST(region, country)',
    'region, country',
    'Y',
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);

-- Resulting DDL:
-- CREATE TABLE sales_regional_part (...)
-- PARTITION BY LIST (region, country) AUTOMATIC
-- (
--     PARTITION p_xdef VALUES (DEFAULT)
-- )
-- COMPRESS FOR QUERY HIGH
-- ENABLE ROW MOVEMENT;


-- Example 3: Order status partitioning
INSERT INTO cmr.dwh_migration_tasks (
    project_id,
    task_name,
    source_owner,
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    use_compression,
    enable_row_movement,
    status
) VALUES (
    (SELECT project_id FROM cmr.dwh_migration_projects WHERE project_name = 'Q1_2024_TABLE_PARTITIONING'),
    'Migrate ORDERS',
    USER,
    'ORDERS',
    'LIST(order_status)',
    'order_status',
    'Y',
    'CTAS',
    'Y',
    'Y',
    'PENDING'
);


-- Analyze and execute
DECLARE
    v_task_id NUMBER;
BEGIN
    SELECT task_id INTO v_task_id
    FROM cmr.dwh_migration_tasks
    WHERE task_name = 'Migrate TENANT_DATA';

    -- Analyze
    pck_dwh_table_migration_analyzer.analyze_table(v_task_id);

    -- Execute
    pck_dwh_table_migration_executor.execute_migration(v_task_id);
END;
/


-- Query partition information after migration
SELECT partition_name, high_value, partition_position
FROM dba_tab_partitions
WHERE table_owner = USER
AND table_name = 'TENANT_DATA'
ORDER BY partition_position;

-- Example output:
-- PARTITION_NAME  HIGH_VALUE             PARTITION_POSITION
-- P_XDEF          DEFAULT                1
-- SYS_P12345      'TENANT_001'           2
-- SYS_P12346      'TENANT_002'           3
-- SYS_P12347      'TENANT_003'           4
```

**Update docs/table_migration_guide.md:**

Add new section after "Row Movement Configuration":

```markdown
#### AUTOMATIC LIST Partitioning

**Oracle 12.2+ Feature**

AUTOMATIC LIST partitioning automatically creates new partitions when new distinct values are inserted into the partition key column(s). This is ideal for:
- Multi-tenant applications (partition by tenant_id)
- Regional data segregation (partition by region, country)
- Status-based workflows (partition by order_status, ticket_priority)
- Dynamic categories that emerge over time

**Configuration:**

```sql
INSERT INTO cmr.dwh_migration_tasks (
    source_table,
    partition_type,       -- Can be single or multiple columns
    partition_key,        -- Must match partition_type
    automatic_list,       -- 'Y' to enable AUTOMATIC LIST
    migration_method,
    enable_row_movement  -- Required for AUTOMATIC LIST
) VALUES (
    'TENANT_DATA',
    'LIST(tenant_id)',    -- Single column
    'tenant_id',
    'Y',
    'CTAS',
    'Y'
);

-- Multi-column example:
INSERT INTO cmr.dwh_migration_tasks (
    source_table,
    partition_type,
    partition_key,
    automatic_list,
    migration_method,
    enable_row_movement
) VALUES (
    'SALES_REGIONAL',
    'LIST(region, country)',  -- Multiple columns
    'region, country',
    'Y',
    'CTAS',
    'Y'
);
```

**Generated DDL:**

```sql
CREATE TABLE tenant_data_part (...)
PARTITION BY LIST (tenant_id) AUTOMATIC
(
    PARTITION p_xdef VALUES (DEFAULT)
)
ENABLE ROW MOVEMENT;
```

**Default Partition (P_XDEF):**
- Always created with name `P_XDEF` (Partition Exchange Default)
- Contains DEFAULT values
- Consistent naming for easy identification
- Used for NULL values and initial data load

**Automatic Partition Creation:**
- Oracle creates new partitions automatically when new values are inserted
- Partition names: Oracle generates names like `SYS_P12345`
- No need to pre-define partition values
- Ideal for scenarios where values are not known at design time

**Requirements:**
- Oracle Database 12.2 or higher
- Row movement must be enabled
- Works with CTAS, ONLINE, and EXCHANGE migration methods

**When to Use:**
- ✅ Multi-tenant SaaS applications
- ✅ Regional/geographic data segregation
- ✅ Status-based partitioning
- ✅ Dynamic categories (< 100 distinct values recommended)
- ❌ High cardinality columns (use HASH instead)
- ❌ Continuous values (use RANGE instead)

**Example Use Case - Multi-Tenant:**

After migration, each new tenant automatically gets their own partition:

```sql
-- Insert data for new tenants
INSERT INTO tenant_data VALUES (1, 'TENANT_001', 'Data A', SYSDATE);
-- → Oracle creates partition for 'TENANT_001'

INSERT INTO tenant_data VALUES (2, 'TENANT_002', 'Data B', SYSDATE);
-- → Oracle creates partition for 'TENANT_002'

-- Query partitions
SELECT partition_name, high_value
FROM dba_tab_partitions
WHERE table_name = 'TENANT_DATA'
ORDER BY partition_position;

-- Output:
-- P_XDEF       DEFAULT
-- SYS_P12345   'TENANT_001'
-- SYS_P12346   'TENANT_002'
```

**Partition Management:**

```sql
-- View all partitions with values
SELECT partition_name,
       high_value,
       num_rows,
       blocks
FROM dba_tab_partitions
WHERE table_owner = USER
AND table_name = 'TENANT_DATA'
ORDER BY partition_position;

-- Drop specific tenant partition (after tenant removal)
ALTER TABLE tenant_data DROP PARTITION sys_p12345;

-- Truncate specific partition
ALTER TABLE tenant_data TRUNCATE PARTITION sys_p12345;
```
```

---

## Testing Plan

### Basic Tests

1. **Single column AUTOMATIC LIST**
   ```sql
   partition_type: 'LIST(region)'
   automatic_list: 'Y'
   ```
   - Verify DDL contains `AUTOMATIC` keyword
   - Verify P_XDEF partition is created
   - Verify ENABLE ROW MOVEMENT

2. **Multi-column AUTOMATIC LIST**
   ```sql
   partition_type: 'LIST(region, country)'
   automatic_list: 'Y'
   ```
   - Verify DDL contains both columns
   - Verify syntax is correct

3. **Verify partition creation after migration**
   - Insert rows with different values
   - Confirm Oracle creates SYS_P* partitions
   - Query dba_tab_partitions to verify

4. **Oracle version validation (optional)**
   - Test on Oracle < 12.2 (should fail gracefully)
   - Test on Oracle >= 12.2 (should succeed)

### Edge Cases

1. NULL values in partition key
2. Multiple rows with same partition key value
3. CTAS, ONLINE, and EXCHANGE methods
4. With and without compression
5. With and without row movement

---

## Summary

**What We're Implementing:**
- ✅ AUTOMATIC LIST support in DDL generation
- ✅ Single and multi-column partition keys
- ✅ P_XDEF default partition (always)
- ✅ Oracle version validation (optional)
- ✅ Documentation and examples

**What We're NOT Implementing:**
- ❌ Automatic analysis/recommendation
- ❌ Categorical column detection
- ❌ Cardinality analysis
- ❌ initial_list_values configuration

**Effort:** 4-6 hours
- Schema change: 30 minutes
- DDL generation: 2 hours
- Testing: 1-2 hours
- Documentation: 1-2 hours

**Benefits:**
- Simple, focused implementation
- User has full control
- Supports advanced use cases (multi-column)
- Consistent naming (P_XDEF)
- Ready for production use

**Risk:** Low - Minimal changes, well-defined scope
