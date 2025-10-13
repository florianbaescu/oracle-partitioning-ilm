-- =============================================================================
-- Table Migration Execution Package
-- Executes table migrations from non-partitioned to partitioned
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_table_migration_executor AS
    -- Main execution procedures
    PROCEDURE execute_migration(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE  -- If TRUE, generates DDL without executing
    );

    PROCEDURE execute_all_ready_tasks(
        p_project_id NUMBER DEFAULT NULL,
        p_max_tasks NUMBER DEFAULT NULL,
        p_simulate BOOLEAN DEFAULT FALSE  -- If TRUE, simulates all migrations
    );

    -- Migration methods
    PROCEDURE migrate_using_ctas(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    );

    PROCEDURE migrate_using_online_redef(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    );

    PROCEDURE migrate_using_exchange(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    );

    -- Post-analysis: Apply recommendations
    PROCEDURE apply_recommendations(
        p_task_id NUMBER
    );

    -- Post-migration tasks
    PROCEDURE apply_ilm_policies(
        p_task_id NUMBER
    );

    PROCEDURE validate_migration(
        p_task_id NUMBER
    );

    -- Rollback
    PROCEDURE rollback_migration(
        p_task_id NUMBER
    );

END pck_dwh_table_migration_executor;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_table_migration_executor AS

    -- ==========================================================================
    -- Private Helper Function - Get Config Value
    -- ==========================================================================

    FUNCTION get_dwh_ilm_config(p_config_key VARCHAR2) RETURN VARCHAR2 AS
        v_value VARCHAR2(4000);
    BEGIN
        SELECT config_value INTO v_value
        FROM cmr.dwh_ilm_config
        WHERE config_key = p_config_key;
        RETURN v_value;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END get_dwh_ilm_config;

    -- ==========================================================================
    -- Private Logging Procedure
    -- ==========================================================================

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
            task_id, step_number, step_name, step_type, sql_statement,
            start_time, end_time, duration_seconds, status,
            error_code, error_message
        ) VALUES (
            p_task_id, p_step_number, p_step_name, p_step_type, p_sql,
            p_start_time, p_end_time, v_duration, p_status,
            p_error_code, p_error_message
        );

        COMMIT;
    END log_step;


    -- ==========================================================================
    -- Helper Procedures
    -- ==========================================================================

    PROCEDURE create_backup_table(
        p_task_id NUMBER,
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_backup_name OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        p_backup_name := p_table_name || '_BAK_' || TO_CHAR(SYSDATE, 'YYYYMMDD_HH24MISS');

        v_sql := 'CREATE TABLE ' || p_owner || '.' || p_backup_name ||
                ' AS SELECT * FROM ' || p_owner || '.' || p_table_name;

        EXECUTE IMMEDIATE v_sql;

        log_step(p_task_id, 1, 'Create backup table', 'BACKUP', v_sql,
                'SUCCESS', v_start, SYSTIMESTAMP);

        DBMS_OUTPUT.PUT_LINE('  Backup created: ' || p_backup_name);
    END create_backup_table;


    PROCEDURE build_partition_ddl(
        p_task dwh_migration_tasks%ROWTYPE,
        p_ddl OUT CLOB
    ) AS
        v_columns CLOB;
        v_constraints CLOB;
        v_storage_clause VARCHAR2(500);
        v_requires_conversion CHAR(1);
        v_date_column VARCHAR2(128);
        v_initial_partition_clause VARCHAR2(1000);
        v_min_date DATE;
        v_starting_boundary VARCHAR2(100);
    BEGIN
        -- Check if date conversion is required and get date range info
        BEGIN
            SELECT requires_conversion, date_column_name
            INTO v_requires_conversion, v_date_column
            FROM cmr.dwh_migration_analysis
            WHERE task_id = (SELECT task_id FROM cmr.dwh_migration_tasks
                            WHERE source_owner = p_task.source_owner
                            AND source_table = p_task.source_table);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_requires_conversion := 'N';
        END;

        -- Get column definitions
        IF v_requires_conversion = 'Y' AND v_date_column IS NOT NULL THEN
            -- Build column list with date conversion
            SELECT LISTAGG(
                CASE
                    WHEN column_name = v_date_column THEN
                        column_name || '_CONVERTED DATE NOT NULL'
                    ELSE
                        column_name || ' ' || data_type ||
                        CASE
                            WHEN data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR')
                                THEN '(' || data_length || ')'
                            WHEN data_type = 'NUMBER' AND data_precision IS NOT NULL
                                THEN '(' || data_precision ||
                                    CASE WHEN data_scale IS NOT NULL AND data_scale > 0
                                        THEN ',' || data_scale ELSE '' END || ')'
                            ELSE ''
                        END ||
                        CASE WHEN nullable = 'N' THEN ' NOT NULL' ELSE '' END
                END,
                ', '
            ) WITHIN GROUP (ORDER BY column_id)
            INTO v_columns
            FROM dba_tab_columns
            WHERE owner = p_task.source_owner
            AND table_name = p_task.source_table;
        ELSE
            -- Standard column list
            SELECT LISTAGG(
                column_name || ' ' || data_type ||
                CASE
                    WHEN data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR')
                        THEN '(' || data_length || ')'
                    WHEN data_type = 'NUMBER' AND data_precision IS NOT NULL
                        THEN '(' || data_precision ||
                            CASE WHEN data_scale IS NOT NULL AND data_scale > 0
                                THEN ',' || data_scale ELSE '' END || ')'
                    ELSE ''
                END ||
                CASE WHEN nullable = 'N' THEN ' NOT NULL' ELSE '' END,
                ', '
            ) WITHIN GROUP (ORDER BY column_id)
            INTO v_columns
            FROM dba_tab_columns
            WHERE owner = p_task.source_owner
            AND table_name = p_task.source_table;
        END IF;

        -- Build storage clause
        v_storage_clause := '';
        IF p_task.target_tablespace IS NOT NULL THEN
            v_storage_clause := v_storage_clause || ' TABLESPACE ' || p_task.target_tablespace;
        END IF;
        IF p_task.use_compression = 'Y' THEN
            v_storage_clause := v_storage_clause || ' COMPRESS FOR ' || p_task.compression_type;
        END IF;
        IF p_task.parallel_degree > 1 THEN
            v_storage_clause := v_storage_clause || ' PARALLEL ' || p_task.parallel_degree;
        END IF;

        -- Build DDL
        p_ddl := 'CREATE TABLE ' || p_task.source_owner || '.' || p_task.source_table || '_PART' || CHR(10);
        p_ddl := p_ddl || '(' || CHR(10) || v_columns || CHR(10) || ')' || CHR(10);
        p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);

        -- Build initial partition clause
        IF p_task.interval_clause IS NOT NULL THEN
            -- INTERVAL partitioning: Cannot use MAXVALUE
            -- Get minimum date from source table to create starting partition
            p_ddl := p_ddl || 'INTERVAL (' || p_task.interval_clause || ')' || CHR(10);

            BEGIN
                -- Get minimum date from source table
                EXECUTE IMMEDIATE 'SELECT MIN(' || p_task.partition_key || ') FROM ' ||
                    p_task.source_owner || '.' || p_task.source_table
                    INTO v_min_date;

                IF v_min_date IS NOT NULL THEN
                    -- Round down to first day of month/year depending on interval
                    IF UPPER(p_task.interval_clause) LIKE '%MONTH%' THEN
                        v_min_date := TRUNC(v_min_date, 'MM');
                    ELSIF UPPER(p_task.interval_clause) LIKE '%YEAR%' THEN
                        v_min_date := TRUNC(v_min_date, 'YYYY');
                    ELSIF UPPER(p_task.interval_clause) LIKE '%DAY%' THEN
                        v_min_date := TRUNC(v_min_date);
                    ELSE
                        -- Default: truncate to day
                        v_min_date := TRUNC(v_min_date);
                    END IF;

                    v_starting_boundary := 'TO_DATE(''' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')';
                ELSE
                    -- No data yet, use current date
                    v_starting_boundary := 'SYSDATE';
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Fallback: use current date
                    v_starting_boundary := 'SYSDATE';
            END;

            v_initial_partition_clause := '(PARTITION p_initial VALUES LESS THAN (' || v_starting_boundary || '))';

        ELSE
            -- Regular RANGE partitioning: MAXVALUE is allowed
            v_initial_partition_clause := '(PARTITION p_initial VALUES LESS THAN (MAXVALUE))';
        END IF;

        p_ddl := p_ddl || v_initial_partition_clause;
        p_ddl := p_ddl || v_storage_clause;

    END build_partition_ddl;


    -- -------------------------------------------------------------------------
    -- Recreate Indexes Using DBMS_METADATA
    -- -------------------------------------------------------------------------
    -- DESCRIPTION:
    --   Recreates all indexes from source table on target table using Oracle's
    --   DBMS_METADATA.GET_DEPENDENT_DDL for accurate DDL extraction.
    --
    -- BENEFITS:
    --   - Captures ALL index features: function-based, bitmap, descending, compression
    --   - Preserves index expressions, invisibility, unusable status
    --   - Handles composite indexes, domain indexes, etc.
    --   - More maintainable than manual DDL construction
    --
    -- PROCESS:
    --   1. Get index DDL using DBMS_METADATA
    --   2. Parse DDL and replace table references
    --   3. Make indexes LOCAL for partitioned table
    --   4. Execute modified DDL
    -- -------------------------------------------------------------------------
    PROCEDURE recreate_indexes(
        p_task_id NUMBER,
        p_source_owner VARCHAR2,
        p_source_table VARCHAR2,
        p_target_table VARCHAR2,
        p_step_offset NUMBER
    ) AS
        v_sql CLOB;
        v_index_ddl CLOB;
        v_start TIMESTAMP;
        v_step NUMBER := p_step_offset;
        v_ddl_handle NUMBER;
        v_transform_handle NUMBER;
        v_temp_index_name VARCHAR2(128);
        v_create_pos NUMBER;
    BEGIN
        -- Configure DBMS_METADATA to get clean DDL
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);

        -- Get all indexes (excluding PK indexes)
        FOR idx IN (
            SELECT index_name
            FROM dba_indexes
            WHERE table_owner = p_source_owner
            AND table_name = p_source_table
            AND index_name NOT IN (
                SELECT constraint_name FROM dba_constraints
                WHERE owner = p_source_owner
                AND table_name = p_source_table
                AND constraint_type = 'P'
            )
            ORDER BY index_name
        ) LOOP
            v_step := v_step + 1;
            v_start := SYSTIMESTAMP;

            BEGIN
                -- Get index DDL from DBMS_METADATA
                v_index_ddl := DBMS_METADATA.GET_DDL('INDEX', idx.index_name, p_source_owner);

                -- Generate temporary index name to avoid conflict with existing index
                -- (Original table still exists at this point)
                v_temp_index_name := SUBSTR(idx.index_name, 1, 124) || '_TMP';

                -- Replace index name in CREATE INDEX statement
                v_create_pos := INSTR(UPPER(v_index_ddl), 'CREATE');
                IF v_create_pos > 0 THEN
                    -- Find the index name after CREATE [UNIQUE] INDEX
                    v_sql := REGEXP_REPLACE(v_index_ddl,
                                           '"' || idx.index_name || '"',
                                           '"' || v_temp_index_name || '"',
                                           1, 1);  -- Replace only first occurrence
                ELSE
                    v_sql := v_index_ddl;
                END IF;

                -- Replace source table name with target table name
                v_sql := REPLACE(v_sql,
                                '"' || p_source_table || '"',
                                '"' || p_target_table || '"');
                v_sql := REPLACE(v_sql,
                                ' ' || p_source_table || ' ',
                                ' ' || p_target_table || ' ');

                -- Make index LOCAL for partitioned table (if not already LOCAL)
                -- Find the position after the column list closing parenthesis
                IF INSTR(UPPER(v_sql), ' LOCAL') = 0 THEN
                    DECLARE
                        v_on_pos NUMBER;
                        v_paren_count NUMBER := 0;
                        v_insert_pos NUMBER := 0;
                        v_i NUMBER;
                    BEGIN
                        -- Find "ON table_name (" position
                        v_on_pos := INSTR(UPPER(v_sql), ' ON ');

                        IF v_on_pos > 0 THEN
                            -- Find the matching closing parenthesis after ON clause
                            FOR v_i IN v_on_pos..LENGTH(v_sql) LOOP
                                IF SUBSTR(v_sql, v_i, 1) = '(' THEN
                                    v_paren_count := v_paren_count + 1;
                                ELSIF SUBSTR(v_sql, v_i, 1) = ')' THEN
                                    v_paren_count := v_paren_count - 1;
                                    IF v_paren_count = 0 THEN
                                        v_insert_pos := v_i + 1;
                                        EXIT;
                                    END IF;
                                END IF;
                            END LOOP;

                            -- Insert LOCAL after the column list
                            IF v_insert_pos > 0 THEN
                                v_sql := SUBSTR(v_sql, 1, v_insert_pos - 1) ||
                                        CHR(10) || '  LOCAL' ||
                                        SUBSTR(v_sql, v_insert_pos);
                            END IF;
                        END IF;
                    END;
                END IF;

                -- Add PARALLEL if not present
                IF INSTR(UPPER(v_sql), ' PARALLEL') = 0 THEN
                    v_sql := REPLACE(v_sql, ';',
                            CHR(10) || '  PARALLEL ' || get_dwh_ilm_config('MIGRATION_PARALLEL_DEGREE') || ';');
                END IF;

                EXECUTE IMMEDIATE v_sql;

                log_step(p_task_id, v_step, 'Recreate index: ' || idx.index_name || ' (temp: ' || v_temp_index_name || ')',
                        'INDEX', SUBSTR(v_sql, 1, 4000), 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Created index: ' || v_temp_index_name || ' (will rename to ' || idx.index_name || ' after cutover)');
            EXCEPTION
                WHEN OTHERS THEN
                    log_step(p_task_id, v_step, 'Recreate index: ' || idx.index_name,
                            'INDEX', SUBSTR(v_sql, 1, 4000), 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to recreate index ' || idx.index_name);
                    DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);

                    -- Re-raise exception to fail the migration
                    RAISE_APPLICATION_ERROR(-20500,
                        'Index recreation failed: ' || idx.index_name || ' - ' || SQLERRM);
            END;
        END LOOP;
    END recreate_indexes;


    -- -------------------------------------------------------------------------
    -- Recreate Constraints Using DBMS_METADATA
    -- -------------------------------------------------------------------------
    -- DESCRIPTION:
    --   Recreates all constraints (CHECK, UNIQUE, FK) from source table on target
    --   table using Oracle's DBMS_METADATA.GET_DEPENDENT_DDL for accurate extraction.
    --
    -- BENEFITS:
    --   - Captures ALL constraint features: deferred, ENABLE/DISABLE, RELY/NORELY
    --   - Preserves constraint states, validation options
    --   - Handles all constraint types including complex CHECK conditions
    --   - More maintainable than manual DDL construction
    --
    -- PROCESS:
    --   1. Get constraint DDL using DBMS_METADATA
    --   2. Parse and extract individual constraint statements
    --   3. Replace table references
    --   4. Execute modified DDL
    --
    -- NOTE:
    --   - PRIMARY KEY constraints are handled separately (not recreated here)
    --   - Foreign key constraints may need to be recreated after all tables migrated
    -- -------------------------------------------------------------------------
    PROCEDURE recreate_constraints(
        p_task_id NUMBER,
        p_source_owner VARCHAR2,
        p_source_table VARCHAR2,
        p_target_table VARCHAR2,
        p_step_offset NUMBER
    ) AS
        v_sql CLOB;
        v_constraint_ddl CLOB;
        v_start TIMESTAMP;
        v_step NUMBER := p_step_offset;
    BEGIN
        -- Configure DBMS_METADATA to get clean DDL
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);

        -- Get CHECK and UNIQUE constraints first (no dependencies)
        -- NOTE: Include ALL constraints including SYS_% names, as these may be:
        --   1. NOT NULL constraints (already in build_partition_ddl - will skip)
        --   2. User-created CHECK constraints without explicit names (IMPORTANT!)
        FOR con IN (
            SELECT c.constraint_name, c.constraint_type, c.search_condition
            FROM dba_constraints c
            WHERE c.owner = p_source_owner
            AND c.table_name = p_source_table
            AND c.constraint_type IN ('C', 'U')
            ORDER BY c.constraint_type, c.constraint_name
        ) LOOP
            -- Skip NOT NULL constraints (they're already in column definitions)
            -- NOT NULL constraints have search_condition like "column_name" IS NOT NULL
            IF con.constraint_type = 'C' AND
               UPPER(con.search_condition) LIKE '%IS NOT NULL%' THEN
                CONTINUE;  -- Skip NOT NULL, already in table definition
            END IF;

            v_step := v_step + 1;
            v_start := SYSTIMESTAMP;

            BEGIN
                -- Get constraint DDL from DBMS_METADATA
                v_constraint_ddl := DBMS_METADATA.GET_DDL('CONSTRAINT', con.constraint_name, p_source_owner);

                -- Replace source table name with target table name
                v_sql := REPLACE(v_constraint_ddl,
                                '"' || p_source_table || '"',
                                '"' || p_target_table || '"');
                v_sql := REPLACE(v_sql,
                                ' ' || p_source_table || ' ',
                                ' ' || p_target_table || ' ');

                EXECUTE IMMEDIATE v_sql;

                log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name,
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Recreated constraint: ' || con.constraint_name ||
                                    ' (' || con.constraint_type || ')');
            EXCEPTION
                WHEN OTHERS THEN
                    log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name,
                            'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to recreate constraint ' || con.constraint_name);
                    DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);

                    -- Re-raise exception to fail the migration (constraints are critical)
                    RAISE_APPLICATION_ERROR(-20501,
                        'Constraint recreation failed: ' || con.constraint_name || ' - ' || SQLERRM);
            END;
        END LOOP;

        -- Now get FOREIGN KEY constraints (must be last, after all other constraints)
        DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', TRUE);

        FOR con IN (
            SELECT constraint_name
            FROM dba_constraints
            WHERE owner = p_source_owner
            AND table_name = p_source_table
            AND constraint_type = 'R'
            ORDER BY constraint_name
        ) LOOP
            v_step := v_step + 1;
            v_start := SYSTIMESTAMP;

            BEGIN
                -- Get FK constraint DDL from DBMS_METADATA
                v_constraint_ddl := DBMS_METADATA.GET_DDL('REF_CONSTRAINT', con.constraint_name, p_source_owner);

                -- Replace source table name with target table name
                v_sql := REPLACE(v_constraint_ddl,
                                '"' || p_source_table || '"',
                                '"' || p_target_table || '"');
                v_sql := REPLACE(v_sql,
                                ' ' || p_source_table || ' ',
                                ' ' || p_target_table || ' ');

                EXECUTE IMMEDIATE v_sql;

                log_step(p_task_id, v_step, 'Recreate FK constraint: ' || con.constraint_name,
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Recreated FK constraint: ' || con.constraint_name);
            EXCEPTION
                WHEN OTHERS THEN
                    -- FK constraints may fail if referenced table doesn't exist yet
                    log_step(p_task_id, v_step, 'Recreate FK constraint: ' || con.constraint_name,
                            'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Failed to recreate FK constraint ' ||
                                        con.constraint_name || ': ' || SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('           (FK constraints may need manual recreation if ' ||
                                        'referenced table not yet migrated)');
            END;
        END LOOP;
    END recreate_constraints;


    -- ==========================================================================
    -- Migration Methods
    -- ==========================================================================

    -- -------------------------------------------------------------------------
    -- MIGRATION METHOD 1: CTAS (Create Table As Select)
    -- -------------------------------------------------------------------------
    -- DESCRIPTION:
    --   Creates a new partitioned table and copies all data using INSERT SELECT.
    --   This is the most straightforward migration method.
    --
    -- REQUIREMENTS:
    --   - Downtime: Required (table locked during final rename)
    --   - Duration: Seconds to minutes depending on data volume
    --   - Space: Needs 2x table space temporarily (original + new partitioned)
    --   - Privileges: CREATE TABLE, DROP TABLE, ALTER TABLE
    --
    -- USE CASES:
    --   - Standard migrations for most tables
    --   - Tables where downtime is acceptable
    --   - Initial migrations and testing
    --
    -- PROCESS:
    --   1. Build partition DDL from analysis recommendations
    --   2. Create new partitioned table (TABLE_NAME_PART)
    --   3. Copy data with APPEND + PARALLEL hints (with date conversion if needed)
    --   4. Recreate all indexes on new table
    --   5. Recreate all constraints on new table
    --   6. Gather fresh statistics
    --   7. Rename original table to TABLE_NAME_OLD (backup)
    --   8. Rename new table to original name (cutover)
    --
    -- ADVANTAGES:
    --   + Simple and reliable
    --   + Works on all Oracle editions (Standard/Enterprise)
    --   + Easy to rollback (just swap names back)
    --   + Supports date column conversion (NUMBER/VARCHAR to DATE)
    --
    -- DISADVANTAGES:
    --   - Requires downtime during rename operation
    --   - Needs extra disk space for both tables
    --   - DML operations blocked during migration
    -- -------------------------------------------------------------------------

    PROCEDURE migrate_using_ctas(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_ddl CLOB;
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_step NUMBER := 10;
        v_new_table VARCHAR2(128);
        v_old_table VARCHAR2(128);
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION MODE: CTAS Migration');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('Method: CTAS (Create Table As Select)');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Migrating using CTAS method: ' || v_task.source_table);
        END IF;

        v_new_table := v_task.source_table || '_PART';
        v_old_table := v_task.source_table || '_OLD';

        -- Step 1: Build partition DDL
        v_start := SYSTIMESTAMP;
        build_partition_ddl(v_task, v_ddl);

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 1: BUILD PARTITION DDL');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE(v_ddl);
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            log_step(p_task_id, v_step, 'Build DDL', 'PREPARE', v_ddl,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        END IF;
        v_step := v_step + 10;

        -- Step 2: Create partitioned table
        IF NOT p_simulate THEN
            v_start := SYSTIMESTAMP;
            EXECUTE IMMEDIATE v_ddl;
            log_step(p_task_id, v_step, 'Create partitioned table', 'CREATE', v_ddl,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        END IF;
        v_step := v_step + 10;

        -- Step 3: Copy data (with date conversion if needed)
        v_start := SYSTIMESTAMP;
        DECLARE
            v_requires_conversion CHAR(1);
            v_date_column VARCHAR2(128);
            v_conversion_expr VARCHAR2(500);
            v_select_list CLOB;
        BEGIN
            -- Check if date conversion is required
            SELECT requires_conversion, date_column_name, date_conversion_expr
            INTO v_requires_conversion, v_date_column, v_conversion_expr
            FROM cmr.dwh_migration_analysis
            WHERE task_id = p_task_id;

            IF v_requires_conversion = 'Y' AND v_date_column IS NOT NULL THEN
                -- Build custom SELECT list with date conversion
                SELECT LISTAGG(
                    CASE
                        WHEN column_name = v_date_column THEN
                            v_conversion_expr || ' AS ' || column_name || '_CONVERTED'
                        ELSE
                            column_name
                    END, ', '
                ) WITHIN GROUP (ORDER BY column_id)
                INTO v_select_list
                FROM dba_tab_columns
                WHERE owner = v_task.source_owner
                AND table_name = v_task.source_table;

                v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_task.parallel_degree || ') */ INTO ' ||
                        v_task.source_owner || '.' || v_new_table ||
                        ' SELECT ' || v_select_list || ' FROM ' || v_task.source_owner || '.' || v_task.source_table;

                IF p_simulate THEN
                    DBMS_OUTPUT.PUT_LINE('Step 2: COPY DATA (with date conversion)');
                    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
                    DBMS_OUTPUT.PUT_LINE('Converting column: ' || v_date_column);
                    DBMS_OUTPUT.PUT_LINE('Conversion expression: ' || v_conversion_expr);
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE(v_sql);
                    DBMS_OUTPUT.PUT_LINE('');
                ELSE
                    DBMS_OUTPUT.PUT_LINE('  Converting date column ' || v_date_column || ' during copy');
                END IF;
            ELSE
                -- Standard SELECT *
                v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_task.parallel_degree || ') */ INTO ' ||
                        v_task.source_owner || '.' || v_new_table ||
                        ' SELECT * FROM ' || v_task.source_owner || '.' || v_task.source_table;

                IF p_simulate THEN
                    DBMS_OUTPUT.PUT_LINE('Step 2: COPY DATA');
                    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
                    DBMS_OUTPUT.PUT_LINE(v_sql);
                    DBMS_OUTPUT.PUT_LINE('');
                END IF;
            END IF;

            IF NOT p_simulate THEN
                EXECUTE IMMEDIATE v_sql;
                COMMIT;
                log_step(p_task_id, v_step, 'Copy data' ||
                        CASE WHEN v_requires_conversion = 'Y' THEN ' (with date conversion)' ELSE '' END,
                        'COPY', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- No analysis record, use standard copy
                v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_task.parallel_degree || ') */ INTO ' ||
                        v_task.source_owner || '.' || v_new_table ||
                        ' SELECT * FROM ' || v_task.source_owner || '.' || v_task.source_table;

                IF p_simulate THEN
                    DBMS_OUTPUT.PUT_LINE('Step 2: COPY DATA');
                    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
                    DBMS_OUTPUT.PUT_LINE(v_sql);
                    DBMS_OUTPUT.PUT_LINE('');
                ELSE
                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;
                    log_step(p_task_id, v_step, 'Copy data', 'COPY', v_sql,
                            'SUCCESS', v_start, SYSTIMESTAMP);
                END IF;
        END;
        v_step := v_step + 10;

        -- Step 4: Recreate indexes
        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 3: RECREATE INDEXES');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('(Indexes from ' || v_task.source_table || ' will be recreated on ' || v_new_table || ')');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            v_start := SYSTIMESTAMP;
            recreate_indexes(p_task_id, v_task.source_owner, v_task.source_table, v_new_table, v_step);
        END IF;
        v_step := v_step + 100;

        -- Step 5: Recreate constraints
        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 4: RECREATE CONSTRAINTS');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('(Constraints from ' || v_task.source_table || ' will be recreated on ' || v_new_table || ')');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            recreate_constraints(p_task_id, v_task.source_owner, v_task.source_table, v_new_table, v_step);
        END IF;
        v_step := v_step + 100;

        -- Step 6: Gather statistics
        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 5: GATHER STATISTICS');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('DBMS_STATS.GATHER_TABLE_STATS(ownname => ''' || v_task.source_owner || ''',');
            DBMS_OUTPUT.PUT_LINE('                              tabname => ''' || v_new_table || ''',');
            DBMS_OUTPUT.PUT_LINE('                              cascade => TRUE,');
            DBMS_OUTPUT.PUT_LINE('                              degree => ' || v_task.parallel_degree || ');');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            v_start := SYSTIMESTAMP;
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => v_task.source_owner,
                tabname => v_new_table,
                cascade => TRUE,
                degree => v_task.parallel_degree
            );
            log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        END IF;
        v_step := v_step + 10;

        -- Step 7: Rename tables
        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                ' RENAME TO ' || v_old_table;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 6: RENAME TABLES (CUTOVER)');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE(v_sql || ';');
            v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_new_table ||
                    ' RENAME TO ' || v_task.source_table;
            DBMS_OUTPUT.PUT_LINE(v_sql || ';');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('Step 7: RENAME INDEXES');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('(Indexes with _TMP suffix will be renamed to original names)');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION COMPLETE');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('Summary:');
            DBMS_OUTPUT.PUT_LINE('  - Original table will be renamed to: ' || v_old_table);
            DBMS_OUTPUT.PUT_LINE('  - New partitioned table will become: ' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('  - Backup preserved for rollback: ' || v_old_table);
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('To execute this migration, call:');
            DBMS_OUTPUT.PUT_LINE('  EXEC pck_dwh_table_migration_executor.execute_migration(p_task_id => ' || p_task_id || ');');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            v_start := SYSTIMESTAMP;
            EXECUTE IMMEDIATE v_sql;

            v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_new_table ||
                    ' RENAME TO ' || v_task.source_table;
            EXECUTE IMMEDIATE v_sql;

            log_step(p_task_id, v_step, 'Rename tables', 'RENAME', v_sql,
                    'SUCCESS', v_start, SYSTIMESTAMP);

            -- Step 8: Rename indexes from temporary names back to original names
            v_step := v_step + 10;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('Renaming indexes to original names...');

            FOR idx IN (
                SELECT index_name, REPLACE(index_name, '_TMP', '') AS original_name
                FROM dba_indexes
                WHERE table_owner = v_task.source_owner
                AND table_name = v_task.source_table
                AND index_name LIKE '%\_TMP' ESCAPE '\'
            ) LOOP
                BEGIN
                    v_sql := 'ALTER INDEX ' || v_task.source_owner || '.' || idx.index_name ||
                            ' RENAME TO ' || idx.original_name;
                    EXECUTE IMMEDIATE v_sql;

                    DBMS_OUTPUT.PUT_LINE('  Renamed: ' || idx.index_name || ' -> ' || idx.original_name);

                    log_step(p_task_id, v_step, 'Rename index: ' || idx.original_name,
                            'RENAME_INDEX', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
                    v_step := v_step + 1;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  WARNING: Failed to rename index ' || idx.index_name || ': ' || SQLERRM);
                        log_step(p_task_id, v_step, 'Rename index: ' || idx.original_name,
                                'RENAME_INDEX', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                                SQLCODE, SQLERRM);
                        v_step := v_step + 1;
                END;
            END LOOP;

            -- Update task with backup name
            UPDATE cmr.dwh_migration_tasks
            SET backup_table_name = v_old_table,
                can_rollback = 'Y'
            WHERE task_id = p_task_id;
            COMMIT;

            DBMS_OUTPUT.PUT_LINE('Migration completed successfully');
            DBMS_OUTPUT.PUT_LINE('  Original table renamed to: ' || v_old_table);
            DBMS_OUTPUT.PUT_LINE('  New partitioned table: ' || v_task.source_table);
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_step(p_task_id, v_step, 'Migration failed', 'ERROR', NULL,
                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
            RAISE;
    END migrate_using_ctas;


    -- -------------------------------------------------------------------------
    -- MIGRATION METHOD 2: ONLINE (Online Redefinition using DBMS_REDEFINITION)
    -- -------------------------------------------------------------------------
    -- DESCRIPTION:
    --   Uses Oracle's DBMS_REDEFINITION package to migrate tables with minimal
    --   downtime. The table remains accessible during most of the migration.
    --
    -- REQUIREMENTS:
    --   - Downtime: Near-zero (only brief lock during final sync)
    --   - Duration: Longer than CTAS due to incremental syncing
    --   - Space: Needs 2x table space (original + interim table)
    --   - Privileges: EXECUTE on DBMS_REDEFINITION
    --   - License: Oracle Enterprise Edition ONLY
    --   - Primary Key: RECOMMENDED but not required (can use ROWID if no PK)
    --
    -- USE CASES:
    --   - Large tables (100GB+) where downtime is unacceptable
    --   - High-traffic tables that cannot be offline
    --   - 24/7 systems with no maintenance windows
    --
    -- PROCESS:
    --   1. Verify table can be redefined:
    --      a) Try CONS_USE_PK (Primary Key) - PREFERRED, faster
    --      b) If no PK, try CONS_USE_ROWID (ROWID-based) - works on any table
    --   2. Create interim partitioned table structure
    --   3. Start redefinition (DBMS_REDEFINITION.START_REDEF_TABLE)
    --   4. Copy dependent objects (indexes, constraints, triggers)
    --   5. Sync interim table with ongoing changes (incremental)
    --   6. Final sync and atomic swap (brief exclusive lock)
    --   7. Finish redefinition
    --
    -- ADVANTAGES:
    --   + Near-zero downtime (only final sync lock ~seconds)
    --   + Table remains accessible during migration
    --   + DML operations continue during migration
    --   + Automatic change capture and sync
    --
    -- DISADVANTAGES:
    --   - Requires Enterprise Edition license
    --   - ROWID-based slightly slower than PK-based
    --   - More complex error handling
    --   - Takes longer than CTAS
    --   - Does NOT support date column conversion (NUMBER/VARCHAR to DATE)
    --
    -- LIMITATIONS:
    --   - Date conversion not supported (use CTAS for tables requiring conversion)
    --   - Materialized view logs not supported
    --   - Some LOB configurations not supported
    -- -------------------------------------------------------------------------

    PROCEDURE migrate_using_online_redef(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_ddl CLOB;
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_step NUMBER := 10;
        v_interim_table VARCHAR2(128);
        v_requires_conversion CHAR(1);
        v_can_redef NUMBER;
        v_redef_option PLS_INTEGER;  -- CONS_USE_PK or CONS_USE_ROWID
        v_has_pk BOOLEAN := FALSE;
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Note: Simulating ONLINE redefinition (DBMS_REDEFINITION)');
            DBMS_OUTPUT.PUT_LINE('      Table will remain accessible during actual migration');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Migrating using Online Redefinition method: ' || v_task.source_table);
        END IF;

        v_interim_table := v_task.source_table || '_REDEF';

        -- Check if date conversion is required
        BEGIN
            SELECT requires_conversion
            INTO v_requires_conversion
            FROM cmr.dwh_migration_analysis
            WHERE task_id = p_task_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_requires_conversion := 'N';
        END;

        -- Online redefinition does NOT support date conversion
        IF v_requires_conversion = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Online redefinition does not support date column conversion');
            DBMS_OUTPUT.PUT_LINE('         Table requires conversion from NUMBER/VARCHAR to DATE');
            DBMS_OUTPUT.PUT_LINE('         Falling back to CTAS method...');
            migrate_using_ctas(p_task_id, p_simulate);
            RETURN;
        END IF;

        -- Step 1: Verify table can be redefined (try PK first, then ROWID)
        v_start := SYSTIMESTAMP;

        -- Try with PRIMARY KEY first
        -- CONS_USE_PK = 1, CONS_USE_ROWID = 2 (DBMS_REDEFINITION constants)
        BEGIN
            IF NOT p_simulate THEN
                EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.CAN_REDEF_TABLE(' ||
                    'uname => :1, tname => :2, options_flag => :3); END;'
                    USING v_task.source_owner, v_task.source_table, 1; -- CONS_USE_PK = 1
            END IF;
            v_has_pk := TRUE;
            v_redef_option := 1; -- DBMS_REDEFINITION.CONS_USE_PK

            IF p_simulate THEN
                DBMS_OUTPUT.PUT_LINE('Step 1: VERIFY TABLE CAN BE REDEFINED');
                DBMS_OUTPUT.PUT_LINE('----------------------------------------');
                DBMS_OUTPUT.PUT_LINE('Option: CONS_USE_PK (Primary Key-based redefinition)');
                DBMS_OUTPUT.PUT_LINE('Table has PRIMARY KEY - using PK-based redefinition');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  Table can be redefined online using PRIMARY KEY');
                log_step(p_task_id, v_step, 'Verify table can be redefined (PK)', 'VALIDATE', NULL,
                        'SUCCESS', v_start, SYSTIMESTAMP);
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                -- Check if privilege error first
                IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION.CAN_REDEF_TABLE');
                    DBMS_OUTPUT.PUT_LINE('       Required: EXECUTE privilege on DBMS_REDEFINITION package');
                    DBMS_OUTPUT.PUT_LINE('       Falling back to CTAS method...');
                    IF NOT p_simulate THEN
                        log_step(p_task_id, v_step, 'Cannot redefine - no privileges', 'VALIDATE', NULL,
                                'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                    END IF;
                    migrate_using_ctas(p_task_id, p_simulate);
                    RETURN;
                END IF;

                -- No PK or PK-based redef failed, try ROWID-based
                IF p_simulate THEN
                    DBMS_OUTPUT.PUT_LINE('Step 1: VERIFY TABLE CAN BE REDEFINED');
                    DBMS_OUTPUT.PUT_LINE('----------------------------------------');
                    DBMS_OUTPUT.PUT_LINE('Note: Table has no PRIMARY KEY');
                    DBMS_OUTPUT.PUT_LINE('Attempting CONS_USE_ROWID (ROWID-based redefinition)');
                END IF;

                BEGIN
                    IF NOT p_simulate THEN
                        EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.CAN_REDEF_TABLE(' ||
                            'uname => :1, tname => :2, options_flag => :3); END;'
                            USING v_task.source_owner, v_task.source_table, 2; -- CONS_USE_ROWID = 2
                    END IF;
                    v_has_pk := FALSE;
                    v_redef_option := 2; -- DBMS_REDEFINITION.CONS_USE_ROWID

                    IF p_simulate THEN
                        DBMS_OUTPUT.PUT_LINE('Option: CONS_USE_ROWID (ROWID-based redefinition)');
                        DBMS_OUTPUT.PUT_LINE('ROWID-based redefinition is SUPPORTED');
                        DBMS_OUTPUT.PUT_LINE('');
                        DBMS_OUTPUT.PUT_LINE('Note: ROWID-based redefinition:');
                        DBMS_OUTPUT.PUT_LINE('  - Works on tables WITHOUT primary key');
                        DBMS_OUTPUT.PUT_LINE('  - Uses Oracle internal ROWIDs');
                        DBMS_OUTPUT.PUT_LINE('  - Slightly slower than PK-based');
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('  Table can be redefined online using ROWID (no PK required)');
                        log_step(p_task_id, v_step, 'Verify table can be redefined (ROWID)', 'VALIDATE', NULL,
                                'SUCCESS', v_start, SYSTIMESTAMP);
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Both PK and ROWID failed
                        IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                            DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION');
                            DBMS_OUTPUT.PUT_LINE('       Required: EXECUTE privilege on DBMS_REDEFINITION package');
                        ELSE
                            DBMS_OUTPUT.PUT_LINE('ERROR: Table cannot be redefined online: ' || SQLERRM);
                            DBMS_OUTPUT.PUT_LINE('       Common reasons:');
                            DBMS_OUTPUT.PUT_LINE('       - Unsupported column types (LONG, BFILE, etc.)');
                            DBMS_OUTPUT.PUT_LINE('       - Not running Enterprise Edition');
                            DBMS_OUTPUT.PUT_LINE('       - Table has unsupported features');
                        END IF;
                        DBMS_OUTPUT.PUT_LINE('       Falling back to CTAS method...');

                        IF NOT p_simulate THEN
                            log_step(p_task_id, v_step, 'Cannot redefine online', 'VALIDATE', NULL,
                                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                        END IF;
                        migrate_using_ctas(p_task_id, p_simulate);
                        RETURN;
                END;
        END;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('');
        END IF;
        v_step := v_step + 10;

        -- Step 2: Create interim partitioned table
        build_partition_ddl(v_task, v_ddl);
        v_ddl := REPLACE(v_ddl, v_task.source_table || '_PART', v_interim_table);

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 2-7: ONLINE REDEFINITION PROCESS');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('The following steps would be executed:');
            DBMS_OUTPUT.PUT_LINE('  2. Create interim partitioned table: ' || v_interim_table);
            DBMS_OUTPUT.PUT_LINE('  3. DBMS_REDEFINITION.START_REDEF_TABLE');
            DBMS_OUTPUT.PUT_LINE('     (Table remains ACCESSIBLE during this phase)');
            DBMS_OUTPUT.PUT_LINE('  4. DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS');
            DBMS_OUTPUT.PUT_LINE('     (Copies indexes, constraints, triggers)');
            DBMS_OUTPUT.PUT_LINE('  5. DBMS_REDEFINITION.SYNC_INTERIM_TABLE');
            DBMS_OUTPUT.PUT_LINE('     (Captures ongoing changes)');
            DBMS_OUTPUT.PUT_LINE('  6. Gather statistics on interim table');
            DBMS_OUTPUT.PUT_LINE('  7. DBMS_REDEFINITION.FINISH_REDEF_TABLE');
            DBMS_OUTPUT.PUT_LINE('     (Final atomic swap - brief lock ~seconds)');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION COMPLETE - ONLINE Method');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Summary:');
            DBMS_OUTPUT.PUT_LINE('  - Table will remain accessible during migration');
            DBMS_OUTPUT.PUT_LINE('  - Only brief lock during final swap');
            DBMS_OUTPUT.PUT_LINE('  - Interim table: ' || v_interim_table);

            IF v_has_pk THEN
                DBMS_OUTPUT.PUT_LINE('  - Method: CONS_USE_PK (Primary Key-based, faster)');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  - Method: CONS_USE_ROWID (ROWID-based, no PK needed)');
            END IF;

            DBMS_OUTPUT.PUT_LINE('  - Requires: Enterprise Edition');
            DBMS_OUTPUT.PUT_LINE('');
            RETURN;
        ELSE
            v_start := SYSTIMESTAMP;
            EXECUTE IMMEDIATE v_ddl;
            log_step(p_task_id, v_step, 'Create interim partitioned table', 'CREATE', v_ddl,
                    'SUCCESS', v_start, SYSTIMESTAMP);
            DBMS_OUTPUT.PUT_LINE('  Created interim table: ' || v_interim_table);
        END IF;
        v_step := v_step + 10;

        -- Step 3: Start redefinition
        v_start := SYSTIMESTAMP;
        BEGIN
            EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.START_REDEF_TABLE(' ||
                'uname => :1, orig_table => :2, int_table => :3, options_flag => :4); END;'
                USING v_task.source_owner, v_task.source_table, v_interim_table, v_redef_option;

            log_step(p_task_id, v_step, 'Start online redefinition', 'REDEFINE', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);

            IF v_has_pk THEN
                DBMS_OUTPUT.PUT_LINE('  Started online redefinition using PRIMARY KEY (table remains accessible)');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  Started online redefinition using ROWID (table remains accessible)');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                    -- Insufficient privileges or PL/SQL object not found
                    DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION.START_REDEF_TABLE');
                    DBMS_OUTPUT.PUT_LINE('       Required: EXECUTE privilege on DBMS_REDEFINITION package');
                    DBMS_OUTPUT.PUT_LINE('       Falling back to CTAS method...');
                    log_step(p_task_id, v_step, 'Start online redefinition - no privileges', 'REDEFINE', NULL,
                            'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                    migrate_using_ctas(p_task_id, p_simulate);
                    RETURN;
                ELSE
                    RAISE;
                END IF;
        END;
        v_step := v_step + 10;

        -- Step 4: Copy dependent objects (indexes, constraints, triggers)
        v_start := SYSTIMESTAMP;
        DECLARE
            v_num_errors PLS_INTEGER;
        BEGIN
            -- CONS_ORIG_PARAMS = 1 (use original parameters for indexes)
            EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS(' ||
                'uname => :1, orig_table => :2, int_table => :3, ' ||
                'copy_indexes => :4, ' ||
                'copy_triggers => TRUE, copy_constraints => TRUE, copy_privileges => TRUE, ' ||
                'ignore_errors => FALSE, num_errors => :5); END;'
                USING v_task.source_owner, v_task.source_table, v_interim_table, 1, OUT v_num_errors;

            IF v_num_errors > 0 THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: ' || v_num_errors || ' errors copying dependents (check DBA_REDEFINITION_ERRORS)');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  Copied indexes, constraints, triggers, and privileges');
            END IF;

            log_step(p_task_id, v_step, 'Copy dependent objects', 'COPY', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS');
                    log_step(p_task_id, v_step, 'Copy dependent objects - no privileges', 'COPY', NULL,
                            'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                END IF;
                RAISE;
        END;
        v_step := v_step + 10;

        -- Step 5: Synchronize interim table (captures changes during migration)
        v_start := SYSTIMESTAMP;
        BEGIN
            EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.SYNC_INTERIM_TABLE(' ||
                'uname => :1, orig_table => :2, int_table => :3); END;'
                USING v_task.source_owner, v_task.source_table, v_interim_table;

            log_step(p_task_id, v_step, 'Synchronize interim table', 'SYNC', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);
            DBMS_OUTPUT.PUT_LINE('  Synchronized interim table with ongoing changes');
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION.SYNC_INTERIM_TABLE');
                    log_step(p_task_id, v_step, 'Sync interim table - no privileges', 'SYNC', NULL,
                            'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                END IF;
                RAISE;
        END;
        v_step := v_step + 10;

        -- Step 6: Gather statistics on interim table
        v_start := SYSTIMESTAMP;
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => v_task.source_owner,
            tabname => v_interim_table,
            cascade => TRUE,
            degree => v_task.parallel_degree
        );
        log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 7: Finish redefinition (final sync and atomic swap)
        v_start := SYSTIMESTAMP;
        DBMS_OUTPUT.PUT_LINE('  Starting final sync and swap (brief exclusive lock)...');
        BEGIN
            EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.FINISH_REDEF_TABLE(' ||
                'uname => :1, orig_table => :2, int_table => :3); END;'
                USING v_task.source_owner, v_task.source_table, v_interim_table;

            log_step(p_task_id, v_step, 'Finish redefinition (atomic swap)', 'FINISH', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);
            DBMS_OUTPUT.PUT_LINE('  Redefinition completed - table swapped atomically');
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: No privileges to execute DBMS_REDEFINITION.FINISH_REDEF_TABLE');
                    log_step(p_task_id, v_step, 'Finish redefinition - no privileges', 'FINISH', NULL,
                            'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
                END IF;
                RAISE;
        END;
        v_step := v_step + 10;

        -- Update task - interim table becomes the backup
        UPDATE cmr.dwh_migration_tasks
        SET backup_table_name = v_interim_table,
            can_rollback = 'Y'
        WHERE task_id = p_task_id;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Online migration completed successfully');
        DBMS_OUTPUT.PUT_LINE('  Table migrated: ' || v_task.source_table || ' (now partitioned)');
        DBMS_OUTPUT.PUT_LINE('  Interim table available: ' || v_interim_table || ' (for rollback)');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            -- Attempt to abort redefinition
            BEGIN
                EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.ABORT_REDEF_TABLE(' ||
                    'uname => :1, orig_table => :2, int_table => :3); END;'
                    USING v_task.source_owner, v_task.source_table, v_interim_table;
                DBMS_OUTPUT.PUT_LINE('Redefinition aborted due to error');
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE = -1031 OR SQLCODE = -6550 THEN
                        DBMS_OUTPUT.PUT_LINE('WARNING: No privileges to execute DBMS_REDEFINITION.ABORT_REDEF_TABLE');
                    END IF;
                    NULL; -- Already aborted or not started
            END;

            log_step(p_task_id, v_step, 'Online migration failed', 'ERROR', NULL,
                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
            RAISE;
    END migrate_using_online_redef;


    -- -------------------------------------------------------------------------
    -- MIGRATION METHOD 3: EXCHANGE (Partition Exchange)
    -- -------------------------------------------------------------------------
    -- DESCRIPTION:
    --   Uses ALTER TABLE ... EXCHANGE PARTITION to instantly swap a non-partitioned
    --   table with a partition. This is a metadata-only operation (instant).
    --
    -- REQUIREMENTS:
    --   - Downtime: Seconds (only for DDL execution)
    --   - Duration: Instant (metadata-only operation, no data copy)
    --   - Space: Minimal (no data duplication needed)
    --   - Structure: Source table and partition must match exactly
    --   - Constraints: Column definitions, indexes must be compatible
    --   - Data Range: All data must fit within partition range
    --
    -- USE CASES:
    --   - Staging tables that need to become partitions
    --   - Bulk loading data into partitioned tables
    --   - Converting pre-loaded tables into partitions
    --   - ETL scenarios with staging-to-production pattern
    --
    -- PROCESS:
    --   1. Verify table structure matches partition requirements
    --   2. Create empty partitioned table (with matching structure)
    --   3. Validate data range fits target partition
    --   4. Create compatible indexes on partitioned table
    --   5. Execute EXCHANGE PARTITION (instant metadata swap)
    --   6. Optionally drop/keep source table
    --
    -- ADVANTAGES:
    --   + Extremely fast (no data movement)
    --   + Minimal downtime (seconds)
    --   + No extra disk space required
    --   + Perfect for ETL staging-to-production loads
    --
    -- DISADVANTAGES:
    --   - Requires exact structure match (columns, types, order)
    --   - All data must fit within ONE partition range
    --   - Does NOT support date column conversion
    --   - More restrictive than other methods
    --   - Not suitable for general-purpose migrations
    --
    -- LIMITATIONS:
    --   - Only works when entire table fits into single partition
    --   - Cannot convert data types (no NUMBER/VARCHAR to DATE conversion)
    --   - Indexes must be compatible (local vs global considerations)
    --   - Constraints must match
    --
    -- TYPICAL ETL PATTERN:
    --   1. Load data into staging table (non-partitioned)
    --   2. Transform/validate data
    --   3. Exchange staging table with target partition
    --   4. Staging table becomes empty partition (can be reused or dropped)
    -- -------------------------------------------------------------------------

    PROCEDURE migrate_using_exchange(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_ddl CLOB;
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_step NUMBER := 10;
        v_part_table VARCHAR2(128);
        v_requires_conversion CHAR(1);
        v_date_column VARCHAR2(128);
        v_min_date DATE;
        v_max_date DATE;
        v_partition_name VARCHAR2(128);
        v_data_fits BOOLEAN := FALSE;
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Note: Simulating EXCHANGE PARTITION migration');
            DBMS_OUTPUT.PUT_LINE('      Instant metadata-only operation (no data copy)');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Migrating using Partition Exchange method: ' || v_task.source_table);
        END IF;

        v_part_table := v_task.source_table || '_PART';

        -- Check if date conversion is required
        BEGIN
            SELECT requires_conversion, date_column_name
            INTO v_requires_conversion, v_date_column
            FROM cmr.dwh_migration_analysis
            WHERE task_id = p_task_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_requires_conversion := 'N';
        END;

        -- Exchange partition does NOT support date conversion
        IF v_requires_conversion = 'Y' THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Exchange partition does not support date column conversion');
            DBMS_OUTPUT.PUT_LINE('         Table requires conversion from NUMBER/VARCHAR to DATE');
            DBMS_OUTPUT.PUT_LINE('         Falling back to CTAS method...');
            migrate_using_ctas(p_task_id, p_simulate);
            RETURN;
        END IF;

        -- Step 1: Verify table has date column for partitioning
        v_start := SYSTIMESTAMP;
        BEGIN
            SELECT date_column_name
            INTO v_date_column
            FROM cmr.dwh_migration_analysis
            WHERE task_id = p_task_id
            AND date_column_name IS NOT NULL;

            DBMS_OUTPUT.PUT_LINE('  Using date column for partitioning: ' || v_date_column);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('ERROR: No date column identified for partitioning');
                DBMS_OUTPUT.PUT_LINE('       Exchange partition requires a date column for RANGE partitioning');
                DBMS_OUTPUT.PUT_LINE('       Falling back to CTAS method...');
                migrate_using_ctas(p_task_id);
                RETURN;
        END;
        log_step(p_task_id, v_step, 'Verify date column exists', 'VALIDATE', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 2: Analyze data range to determine partition
        v_start := SYSTIMESTAMP;
        EXECUTE IMMEDIATE 'SELECT MIN(' || v_date_column || '), MAX(' || v_date_column || ') ' ||
                         'FROM ' || v_task.source_owner || '.' || v_task.source_table
        INTO v_min_date, v_max_date;

        DBMS_OUTPUT.PUT_LINE('  Data range: ' || TO_CHAR(v_min_date, 'YYYY-MM-DD') ||
                           ' to ' || TO_CHAR(v_max_date, 'YYYY-MM-DD'));

        -- Check if data spans multiple months (not suitable for exchange)
        IF MONTHS_BETWEEN(v_max_date, v_min_date) > 1 THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Data spans ' || ROUND(MONTHS_BETWEEN(v_max_date, v_min_date), 1) || ' months');
            DBMS_OUTPUT.PUT_LINE('         Exchange partition works best when data fits in ONE partition');
            DBMS_OUTPUT.PUT_LINE('         This table spans multiple partitions - not ideal for exchange method');
            DBMS_OUTPUT.PUT_LINE('         Falling back to CTAS method...');
            migrate_using_ctas(p_task_id);
            RETURN;
        END IF;

        -- Determine partition name (use year-month of earliest date)
        v_partition_name := 'P_' || TO_CHAR(v_min_date, 'YYYY_MM');
        DBMS_OUTPUT.PUT_LINE('  Target partition: ' || v_partition_name);

        log_step(p_task_id, v_step, 'Analyze data range', 'ANALYZE', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 3: Create partitioned table structure
        build_partition_ddl(v_task, v_ddl);

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 3-7: EXCHANGE PARTITION PROCESS');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('The following steps would be executed:');
            DBMS_OUTPUT.PUT_LINE('  3. Create partitioned table: ' || v_part_table);
            DBMS_OUTPUT.PUT_LINE('  4. Prepare source table for exchange');
            DBMS_OUTPUT.PUT_LINE('  5. ALTER TABLE ' || v_part_table || ' EXCHANGE PARTITION ' || v_partition_name);
            DBMS_OUTPUT.PUT_LINE('     WITH TABLE ' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('     INCLUDING INDEXES WITHOUT VALIDATION');
            DBMS_OUTPUT.PUT_LINE('     (Instant metadata-only operation)');
            DBMS_OUTPUT.PUT_LINE('  6. Rename tables (cutover)');
            DBMS_OUTPUT.PUT_LINE('  7. Gather statistics');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION COMPLETE - EXCHANGE Method');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Summary:');
            DBMS_OUTPUT.PUT_LINE('  - Instant operation (no data copy)');
            DBMS_OUTPUT.PUT_LINE('  - Data range: ' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || ' to ' || TO_CHAR(v_max_date, 'YYYY-MM-DD'));
            DBMS_OUTPUT.PUT_LINE('  - Target partition: ' || v_partition_name);
            DBMS_OUTPUT.PUT_LINE('  - Empty table preserved: ' || v_task.source_table || '_EMPTY');
            DBMS_OUTPUT.PUT_LINE('');
            RETURN;
        ELSE
            v_start := SYSTIMESTAMP;
            EXECUTE IMMEDIATE v_ddl;
            log_step(p_task_id, v_step, 'Create partitioned table', 'CREATE', v_ddl,
                    'SUCCESS', v_start, SYSTIMESTAMP);
            DBMS_OUTPUT.PUT_LINE('  Created partitioned table: ' || v_part_table);
        END IF;
        v_step := v_step + 10;

        -- Step 4: Recreate indexes on source table to match target
        -- (Exchange requires compatible index structures)
        v_start := SYSTIMESTAMP;
        DBMS_OUTPUT.PUT_LINE('  Preparing source table for exchange...');
        -- Note: In production, you may need to create matching indexes
        -- For now, we'll proceed assuming structures are compatible
        log_step(p_task_id, v_step, 'Prepare source for exchange', 'PREPARE', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 5: Execute partition exchange (instant metadata swap)
        v_start := SYSTIMESTAMP;
        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_part_table ||
                ' EXCHANGE PARTITION ' || v_partition_name ||
                ' WITH TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                ' INCLUDING INDEXES WITHOUT VALIDATION';

        DBMS_OUTPUT.PUT_LINE('  Executing exchange partition (instant operation)...');
        EXECUTE IMMEDIATE v_sql;

        log_step(p_task_id, v_step, 'Exchange partition', 'EXCHANGE', v_sql,
                'SUCCESS', v_start, SYSTIMESTAMP);
        DBMS_OUTPUT.PUT_LINE('  Partition exchanged successfully');
        v_step := v_step + 10;

        -- Step 6: Rename partitioned table to original name
        v_start := SYSTIMESTAMP;
        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                ' RENAME TO ' || v_task.source_table || '_EMPTY';
        EXECUTE IMMEDIATE v_sql;

        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_part_table ||
                ' RENAME TO ' || v_task.source_table;
        EXECUTE IMMEDIATE v_sql;

        log_step(p_task_id, v_step, 'Rename tables', 'RENAME', v_sql,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 7: Gather statistics
        v_start := SYSTIMESTAMP;
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => v_task.source_owner,
            tabname => v_task.source_table,
            cascade => TRUE,
            degree => v_task.parallel_degree
        );
        log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);

        -- Update task
        UPDATE cmr.dwh_migration_tasks
        SET backup_table_name = v_task.source_table || '_EMPTY',
            can_rollback = 'Y'
        WHERE task_id = p_task_id;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Exchange partition migration completed successfully');
        DBMS_OUTPUT.PUT_LINE('  Partitioned table: ' || v_task.source_table);
        DBMS_OUTPUT.PUT_LINE('  Empty table available: ' || v_task.source_table || '_EMPTY (can be dropped or reused)');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_step(p_task_id, v_step, 'Exchange migration failed', 'ERROR', NULL,
                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
            RAISE;
    END migrate_using_exchange;


    -- ==========================================================================
    -- Main Execution
    -- ==========================================================================

    PROCEDURE execute_migration(
        p_task_id NUMBER,
        p_simulate BOOLEAN DEFAULT FALSE
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_backup_name VARCHAR2(128);
        v_start_time TIMESTAMP;
        v_end_time TIMESTAMP;
        v_source_size NUMBER;
        v_target_size NUMBER;
    BEGIN
        -- Get task
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id
        FOR UPDATE;

        -- Validate status
        IF v_task.status NOT IN ('READY', 'ANALYZED') THEN
            RAISE_APPLICATION_ERROR(-20001, 'Task not ready for migration. Current status: ' || v_task.status);
        END IF;

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION MODE - No changes will be made');
            DBMS_OUTPUT.PUT_LINE('========================================');
        ELSE
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Starting Migration');
            DBMS_OUTPUT.PUT_LINE('========================================');
        END IF;

        DBMS_OUTPUT.PUT_LINE('Task ID: ' || p_task_id);
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);
        DBMS_OUTPUT.PUT_LINE('Method: ' || v_task.migration_method);
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('');

        IF NOT p_simulate THEN
            v_start_time := SYSTIMESTAMP;
            v_source_size := pck_dwh_table_migration_analyzer.get_table_size_mb(v_task.source_owner, v_task.source_table);

            -- Update status
            UPDATE cmr.dwh_migration_tasks
            SET status = 'RUNNING',
                execution_start = v_start_time
            WHERE task_id = p_task_id;
            COMMIT;

            -- Create backup if enabled
            IF get_dwh_ilm_config('MIGRATION_BACKUP_ENABLED') = 'Y' THEN
                create_backup_table(p_task_id, v_task.source_owner, v_task.source_table, v_backup_name);
            END IF;
        END IF;

        -- Execute migration based on method
        CASE v_task.migration_method
            WHEN 'CTAS' THEN
                migrate_using_ctas(p_task_id, p_simulate);
            WHEN 'ONLINE' THEN
                migrate_using_online_redef(p_task_id, p_simulate);
            WHEN 'EXCHANGE' THEN
                migrate_using_exchange(p_task_id, p_simulate);
            ELSE
                RAISE_APPLICATION_ERROR(-20002, 'Unknown migration method: ' || v_task.migration_method);
        END CASE;

        IF NOT p_simulate THEN
            v_end_time := SYSTIMESTAMP;
            v_target_size := pck_dwh_table_migration_analyzer.get_table_size_mb(v_task.source_owner, v_task.source_table);

            -- Apply ILM policies if requested
            IF v_task.apply_ilm_policies = 'Y' THEN
                apply_ilm_policies(p_task_id);
            END IF;

            -- Validate migration
            IF get_dwh_ilm_config('MIGRATION_VALIDATE_ENABLED') = 'Y' THEN
                validate_migration(p_task_id);
            END IF;

            -- Update task
            UPDATE cmr.dwh_migration_tasks
            SET status = 'COMPLETED',
                execution_end = v_end_time,
                duration_seconds = EXTRACT(SECOND FROM (v_end_time - v_start_time)) +
                                 EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                                 EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600,
                target_size_mb = v_target_size,
                space_saved_mb = v_source_size - v_target_size
            WHERE task_id = p_task_id;
            COMMIT;

            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Migration Completed Successfully');
            DBMS_OUTPUT.PUT_LINE('Duration: ' ||
                ROUND(EXTRACT(DAY FROM (v_end_time - v_start_time)) * 24 * 60 +
                      EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 60 +
                      EXTRACT(MINUTE FROM (v_end_time - v_start_time)) +
                      EXTRACT(SECOND FROM (v_end_time - v_start_time)) / 60, 2) || ' minutes');
            DBMS_OUTPUT.PUT_LINE('Source size: ' || v_source_size || ' MB');
            DBMS_OUTPUT.PUT_LINE('Target size: ' || v_target_size || ' MB');
            DBMS_OUTPUT.PUT_LINE('Space saved: ' || (v_source_size - v_target_size) || ' MB');
            DBMS_OUTPUT.PUT_LINE('========================================');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_msg VARCHAR2(4000) := SQLERRM;
                v_error_code NUMBER := SQLCODE;
                v_end_time TIMESTAMP := SYSTIMESTAMP;
            BEGIN
                -- Log error to execution log
                log_step(
                    p_task_id => p_task_id,
                    p_step_number => 999,
                    p_step_name => 'MIGRATION_FAILED',
                    p_step_type => 'ERROR',
                    p_sql => TO_CLOB('Migration failed with error'),
                    p_status => 'FAILED',
                    p_start_time => NVL(v_start_time, v_end_time),
                    p_end_time => v_end_time,
                    p_error_code => v_error_code,
                    p_error_message => v_error_msg
                );

                -- Update task status
                UPDATE cmr.dwh_migration_tasks
                SET status = 'FAILED',
                    execution_end = v_end_time,
                    error_message = v_error_msg
                WHERE task_id = p_task_id;
                COMMIT;

                -- Log error but don't raise exception
                DBMS_OUTPUT.PUT_LINE('ERROR: Migration failed for task ' || p_task_id || ' - ' || v_error_msg);
            END;
    END execute_migration;


    PROCEDURE execute_all_ready_tasks(
        p_project_id NUMBER DEFAULT NULL,
        p_max_tasks NUMBER DEFAULT NULL,
        p_simulate BOOLEAN DEFAULT FALSE
    ) AS
        v_count NUMBER := 0;
    BEGIN
        FOR task IN (
            SELECT task_id, source_table
            FROM cmr.dwh_migration_tasks
            WHERE (p_project_id IS NULL OR project_id = p_project_id)
            AND status IN ('READY', 'ANALYZED')
            AND validation_status = 'READY'
            ORDER BY task_id
        ) LOOP
            BEGIN
                execute_migration(task.task_id, p_simulate);
                v_count := v_count + 1;

                EXIT WHEN p_max_tasks IS NOT NULL AND v_count >= p_max_tasks;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR migrating task ' || task.task_id || ': ' || SQLERRM);
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Migrated ' || v_count || ' table(s)');
    END execute_all_ready_tasks;


    -- ==========================================================================
    -- Post-Analysis: Apply Recommendations
    -- ==========================================================================

    PROCEDURE apply_recommendations(
        p_task_id NUMBER
    ) AS
        v_task cmr.dwh_migration_tasks%ROWTYPE;
        v_analysis cmr.dwh_migration_analysis%ROWTYPE;
        v_partition_type VARCHAR2(100);
        v_partition_key VARCHAR2(500);
        v_interval_clause VARCHAR2(200);
        v_recommended_method VARCHAR2(30);
        v_strategy VARCHAR2(1000);
        v_pos NUMBER;
    BEGIN
        -- Get current task
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        -- Verify task has been analyzed
        IF v_task.status NOT IN ('ANALYZED', 'ANALYZING') THEN
            RAISE_APPLICATION_ERROR(-20100,
                'Task must be analyzed before applying recommendations. Current status: ' || v_task.status);
        END IF;

        -- Get analysis results
        BEGIN
            SELECT * INTO v_analysis
            FROM cmr.dwh_migration_analysis
            WHERE task_id = p_task_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20101,
                    'No analysis results found for task ' || p_task_id || '. Run analyze_table() first.');
        END;

        -- Check for blocking issues
        IF v_analysis.blocking_issues IS NOT NULL AND
           DBMS_LOB.GETLENGTH(v_analysis.blocking_issues) > 2 THEN
            DBMS_OUTPUT.PUT_LINE('WARNING: Task has blocking issues that must be resolved:');
            DBMS_OUTPUT.PUT_LINE(v_analysis.blocking_issues);
            RAISE_APPLICATION_ERROR(-20102,
                'Cannot apply recommendations: blocking issues exist. Check analysis.blocking_issues.');
        END IF;

        DBMS_OUTPUT.PUT_LINE('Applying analysis recommendations to task ' || p_task_id);
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);

        -- Parse recommended_strategy
        -- Examples:
        --   "RANGE(sale_date) INTERVAL MONTHLY"
        --   "RANGE(order_date)"
        --   "HASH(customer_id) PARTITIONS 16"
        --   "RANGE(event_date) INTERVAL DAILY SUBPARTITION BY HASH(session_id) SUBPARTITIONS 8"
        v_strategy := v_analysis.recommended_strategy;

        IF v_strategy IS NULL THEN
            RAISE_APPLICATION_ERROR(-20103,
                'No partitioning strategy recommended by analysis. Manual specification required.');
        END IF;

        DBMS_OUTPUT.PUT_LINE('  Recommended strategy: ' || v_strategy);

        -- Extract partition type (e.g., "RANGE(sale_date)" or "HASH(customer_id) PARTITIONS 16")
        IF INSTR(UPPER(v_strategy), 'INTERVAL') > 0 THEN
            -- Has INTERVAL clause - extract portion before INTERVAL
            v_pos := INSTR(UPPER(v_strategy), 'INTERVAL');
            v_partition_type := TRIM(SUBSTR(v_strategy, 1, v_pos - 1));

            -- Extract INTERVAL clause
            v_interval_clause := TRIM(SUBSTR(v_strategy, v_pos + 8)); -- Skip "INTERVAL"

            -- Remove SUBPARTITION clause from interval if present
            IF INSTR(UPPER(v_interval_clause), 'SUBPARTITION') > 0 THEN
                v_pos := INSTR(UPPER(v_interval_clause), 'SUBPARTITION');
                v_interval_clause := TRIM(SUBSTR(v_interval_clause, 1, v_pos - 1));
            END IF;

            -- Parse interval keyword (MONTHLY, DAILY, etc.) to proper syntax
            IF UPPER(v_interval_clause) = 'MONTHLY' THEN
                v_interval_clause := 'NUMTOYMINTERVAL(1,''MONTH'')';
            ELSIF UPPER(v_interval_clause) = 'DAILY' THEN
                v_interval_clause := 'NUMTODSINTERVAL(1,''DAY'')';
            ELSIF UPPER(v_interval_clause) = 'YEARLY' THEN
                v_interval_clause := 'NUMTOYMINTERVAL(1,''YEAR'')';
            ELSIF UPPER(v_interval_clause) = 'QUARTERLY' THEN
                v_interval_clause := 'NUMTOYMINTERVAL(3,''MONTH'')';
            ELSIF UPPER(v_interval_clause) = 'WEEKLY' THEN
                v_interval_clause := 'NUMTODSINTERVAL(7,''DAY'')';
            END IF;
        ELSIF INSTR(UPPER(v_strategy), 'SUBPARTITION') > 0 THEN
            -- Has SUBPARTITION but no INTERVAL
            v_pos := INSTR(UPPER(v_strategy), 'SUBPARTITION');
            v_partition_type := TRIM(SUBSTR(v_strategy, 1, v_pos - 1));
            v_interval_clause := NULL;
        ELSE
            -- Simple partitioning (no interval, no subpartition)
            v_partition_type := TRIM(v_strategy);
            v_interval_clause := NULL;
        END IF;

        -- Extract partition key from date_column_name or parse from partition_type
        IF v_analysis.date_column_name IS NOT NULL THEN
            v_partition_key := v_analysis.date_column_name;

            -- If date conversion required, use converted column name
            IF v_analysis.requires_conversion = 'Y' THEN
                v_partition_key := v_analysis.date_column_name || '_CONVERTED';
            END IF;
        ELSE
            -- Try to extract column name from partition_type (e.g., "RANGE(column_name)")
            v_pos := INSTR(v_partition_type, '(');
            IF v_pos > 0 THEN
                v_partition_key := SUBSTR(v_partition_type, v_pos + 1,
                                         INSTR(v_partition_type, ')') - v_pos - 1);
            END IF;
        END IF;

        -- Determine recommended migration method
        IF v_analysis.recommended_method IS NOT NULL THEN
            v_recommended_method := v_analysis.recommended_method;
        ELSIF v_analysis.supports_online_redef = 'Y' AND v_task.source_rows > 10000000 THEN
            v_recommended_method := 'ONLINE';  -- Large tables benefit from online redefinition
        ELSE
            v_recommended_method := 'CTAS';    -- Default to CTAS for most cases
        END IF;

        -- Apply recommendations to task
        UPDATE cmr.dwh_migration_tasks
        SET partition_type = v_partition_type,
            partition_key = v_partition_key,
            interval_clause = v_interval_clause,
            migration_method = v_recommended_method,
            status = 'READY',
            validation_status = 'READY'
        WHERE task_id = p_task_id;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('  Applied partition type: ' || v_partition_type);
        IF v_partition_key IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('  Applied partition key: ' || v_partition_key);
        END IF;
        IF v_interval_clause IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('  Applied interval clause: ' || v_interval_clause);
        END IF;
        DBMS_OUTPUT.PUT_LINE('  Applied migration method: ' || v_recommended_method);
        DBMS_OUTPUT.PUT_LINE('  Task status updated to: READY');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Task is now ready for execution. Run:');
        DBMS_OUTPUT.PUT_LINE('  EXEC pck_dwh_table_migration_executor.execute_migration(' || p_task_id || ');');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR applying recommendations: ' || SQLERRM);
            RAISE;
    END apply_recommendations;


    -- ==========================================================================
    -- Post-Migration
    -- ==========================================================================

    PROCEDURE apply_ilm_policies(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_template dwh_migration_ilm_templates%ROWTYPE;
        v_policies_json VARCHAR2(32767);
        v_policy_name VARCHAR2(100);
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF v_task.ilm_policy_template IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('No ILM policy template specified - skipping');
            RETURN;
        END IF;

        SELECT * INTO v_template
        FROM cmr.dwh_migration_ilm_templates
        WHERE template_name = v_task.ilm_policy_template;

        DBMS_OUTPUT.PUT_LINE('Applying ILM policies from template: ' || v_template.template_name);

        -- This is simplified - in production you'd parse the JSON
        -- and create actual ILM policies
        DBMS_OUTPUT.PUT_LINE('  ILM policy application would happen here');
        DBMS_OUTPUT.PUT_LINE('  Template: ' || v_template.table_type);

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Warning: ILM template not found: ' || v_task.ilm_policy_template);
    END apply_ilm_policies;


    PROCEDURE validate_migration(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_source_count NUMBER;
        v_target_count NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF v_task.backup_table_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('No backup table - skipping validation');
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('Validating migration...');

        -- Count rows in backup
        v_sql := 'SELECT COUNT(*) FROM ' || v_task.source_owner || '.' || v_task.backup_table_name;
        EXECUTE IMMEDIATE v_sql INTO v_source_count;

        -- Count rows in new table
        v_sql := 'SELECT COUNT(*) FROM ' || v_task.source_owner || '.' || v_task.source_table;
        EXECUTE IMMEDIATE v_sql INTO v_target_count;

        IF v_source_count = v_target_count THEN
            DBMS_OUTPUT.PUT_LINE('  Validation passed: ' || v_target_count || ' rows');
        ELSE
            RAISE_APPLICATION_ERROR(-20003,
                'Row count mismatch! Source: ' || v_source_count || ', Target: ' || v_target_count);
        END IF;
    END validate_migration;


    -- ==========================================================================
    -- Rollback
    -- ==========================================================================

    PROCEDURE rollback_migration(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_sql VARCHAR2(4000);
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF v_task.can_rollback != 'Y' OR v_task.backup_table_name IS NULL THEN
            RAISE_APPLICATION_ERROR(-20004, 'Cannot rollback this migration');
        END IF;

        DBMS_OUTPUT.PUT_LINE('Rolling back migration for: ' || v_task.source_table);

        -- Drop new table
        v_sql := 'DROP TABLE ' || v_task.source_owner || '.' || v_task.source_table || ' PURGE';
        EXECUTE IMMEDIATE v_sql;

        -- Restore backup
        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.backup_table_name ||
                ' RENAME TO ' || v_task.source_table;
        EXECUTE IMMEDIATE v_sql;

        -- Update task
        UPDATE cmr.dwh_migration_tasks
        SET status = 'ROLLED_BACK',
            can_rollback = 'N'
        WHERE task_id = p_task_id;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Rollback completed successfully');
    END rollback_migration;

END pck_dwh_table_migration_executor;
/

SELECT 'Table Migration Executor Package Created Successfully!' AS status FROM DUAL;
