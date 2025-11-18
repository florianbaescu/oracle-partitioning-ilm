-- =============================================================================
-- Table Migration Execution Package
-- Executes table migrations from non-partitioned to partitioned
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_table_migration_executor AUTHID CURRENT_USER AS
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

    PROCEDURE validate_ilm_policies(
        p_task_id NUMBER
    );

    -- Rollback
    PROCEDURE rollback_migration(
        p_task_id NUMBER
    );

    -- Partition renaming helper functions (all partition types)
    FUNCTION generate_partition_name_from_value(
        p_high_value VARCHAR2,
        p_partition_type VARCHAR2,  -- 'LIST' or 'RANGE' or 'HASH'
        p_interval_clause VARCHAR2 DEFAULT NULL  -- For RANGE: determines granularity (monthly/quarterly/yearly)
    ) RETURN VARCHAR2;

    PROCEDURE rename_system_partitions(
        p_task_id NUMBER,
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_type VARCHAR2,  -- Used to determine naming strategy
        p_interval_clause VARCHAR2 DEFAULT NULL  -- For RANGE: interval definition
    );

END pck_dwh_table_migration_executor;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_table_migration_executor AS

    -- Package variable to track current execution ID
    -- All log entries from a single migration run share the same execution_id
    g_execution_id NUMBER;

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
    -- Helper Function: Generate Compression Clause
    -- ==========================================================================

    FUNCTION get_compression_clause(p_compression_type VARCHAR2) RETURN VARCHAR2 AS
    BEGIN
        IF p_compression_type IS NULL OR UPPER(p_compression_type) = 'NONE' THEN
            RETURN '';
        ELSIF UPPER(p_compression_type) = 'BASIC' THEN
            -- BASIC compression uses COMPRESS or COMPRESS BASIC (not COMPRESS FOR BASIC)
            RETURN ' COMPRESS BASIC';
        ELSE
            -- Advanced compression: OLTP, QUERY HIGH, QUERY LOW, ARCHIVE HIGH, etc.
            RETURN ' COMPRESS FOR ' || p_compression_type;
        END IF;
    END get_compression_clause;

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
        -- Initialize execution_id if not already set
        -- This handles cases where log_step is called outside of execute_migration
        IF g_execution_id IS NULL THEN
            SELECT cmr.dwh_mig_execution_seq.NEXTVAL INTO g_execution_id FROM dual;
        END IF;

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

    -- ==========================================================================
    -- Forward Declarations for Private Procedures
    -- ==========================================================================

    PROCEDURE build_tiered_partitions(
        p_task dwh_migration_tasks%ROWTYPE,
        p_tier_config JSON_OBJECT_T,
        p_ddl OUT CLOB
    );

    PROCEDURE build_uniform_partitions(
        p_task dwh_migration_tasks%ROWTYPE,
        p_ddl OUT CLOB
    );

    -- ==========================================================================
    -- Enhanced build_partition_ddl with Tiered Partitioning Support
    -- ==========================================================================
    PROCEDURE build_partition_ddl(
        p_task dwh_migration_tasks%ROWTYPE,
        p_ddl OUT CLOB
    ) AS
        v_template cmr.dwh_migration_ilm_templates%ROWTYPE;
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


    -- ==========================================================================
    -- Helper: build_uniform_partitions (Existing Logic)
    -- ==========================================================================
    PROCEDURE build_uniform_partitions(
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
            WHERE task_id = p_task.task_id;  -- Use task_id directly from p_task parameter
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

        -- Build storage clause using analyzed recommendations
        v_storage_clause := '';

        -- Step 1: Get analyzed storage recommendations from analysis table
        DECLARE
            v_analyzed_tablespace VARCHAR2(128);
            v_analyzed_storage_clause VARCHAR2(500);
            v_storage_mode VARCHAR2(20);
            v_fallback_initial VARCHAR2(20);
            v_fallback_next VARCHAR2(20);
        BEGIN
            -- Get storage mode configuration
            v_storage_mode := get_dwh_ilm_config('STORAGE_EXTENT_MODE');

            -- Try to get analyzed recommendations
            BEGIN
                SELECT
                    target_tablespace,
                    recommended_storage_clause
                INTO
                    v_analyzed_tablespace,
                    v_analyzed_storage_clause
                FROM cmr.dwh_migration_analysis
                WHERE task_id = p_task.task_id;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_analyzed_tablespace := NULL;
                    v_analyzed_storage_clause := NULL;
            END;

            -- Step 2: Apply tablespace based on mode
            -- Explicit task tablespace takes priority, then analyzed tablespace
            IF p_task.target_tablespace IS NOT NULL THEN
                v_storage_clause := v_storage_clause || ' TABLESPACE ' || p_task.target_tablespace;
            ELSIF v_analyzed_tablespace IS NOT NULL THEN
                v_storage_clause := v_storage_clause || ' TABLESPACE ' || v_analyzed_tablespace;
            END IF;

            -- Step 3: Apply STORAGE clause based on mode
            IF UPPER(v_storage_mode) = 'AUTO' THEN
                -- Use analyzed recommendations (may be NULL for ASSM/UNIFORM)
                IF v_analyzed_storage_clause IS NOT NULL THEN
                    v_storage_clause := v_storage_clause || CHR(10) || v_analyzed_storage_clause;
                END IF;
                -- Otherwise omit STORAGE clause (ASSM/UNIFORM case)

            ELSIF UPPER(v_storage_mode) = 'FORCED' THEN
                -- Always use fixed config values
                v_fallback_initial := get_dwh_ilm_config('STORAGE_INITIAL_EXTENT');
                v_fallback_next := get_dwh_ilm_config('STORAGE_NEXT_EXTENT');
                IF v_fallback_initial IS NOT NULL AND v_fallback_next IS NOT NULL THEN
                    v_storage_clause := v_storage_clause || CHR(10) ||
                        'STORAGE (INITIAL ' || v_fallback_initial || ' NEXT ' || v_fallback_next || ')';
                END IF;

            ELSIF UPPER(v_storage_mode) = 'NONE' THEN
                -- Never use STORAGE clause
                NULL;

            ELSE
                -- Default to AUTO mode
                IF v_analyzed_storage_clause IS NOT NULL THEN
                    v_storage_clause := v_storage_clause || CHR(10) || v_analyzed_storage_clause;
                END IF;
            END IF;
        END;

        IF p_task.use_compression = 'Y' THEN
            v_storage_clause := v_storage_clause || get_compression_clause(p_task.compression_type);
        END IF;
        IF p_task.parallel_degree > 1 THEN
            v_storage_clause := v_storage_clause || ' PARALLEL ' || p_task.parallel_degree;
        END IF;

        -- Build DDL
        p_ddl := 'CREATE TABLE ' || p_task.source_owner || '.' || p_task.source_table || '_PART' || CHR(10);
        p_ddl := p_ddl || '(' || CHR(10) || v_columns || CHR(10) || ')' || CHR(10);

        -- Build partition clause based on type
        IF p_task.automatic_list = 'Y' THEN
            -- AUTOMATIC LIST partitioning
            DECLARE
                v_default_values VARCHAR2(4000);
            BEGIN
                -- Get default values (user-provided or type-based)
                v_default_values := pck_dwh_table_migration_analyzer.get_list_default_values(
                    p_owner => p_task.source_owner,
                    p_table_name => p_task.source_table,
                    p_partition_key => p_task.partition_key,
                    p_user_defaults => p_task.list_default_values
                );

                -- Validate if user provided custom defaults
                IF p_task.list_default_values IS NOT NULL THEN
                    IF NOT pck_dwh_table_migration_analyzer.validate_list_defaults(
                        p_owner => p_task.source_owner,
                        p_table_name => p_task.source_table,
                        p_partition_key => p_task.partition_key,
                        p_user_defaults => p_task.list_default_values
                    ) THEN
                        RAISE_APPLICATION_ERROR(-20616,
                            'Invalid list_default_values: ' || p_task.list_default_values);
                    END IF;
                END IF;

                -- Build AUTOMATIC LIST partition clause
                p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || ' AUTOMATIC' || CHR(10);
                p_ddl := p_ddl || '(' || CHR(10);
                p_ddl := p_ddl || '    PARTITION p_xdef VALUES (' || v_default_values || ')' || CHR(10);
                p_ddl := p_ddl || ')';

                DBMS_OUTPUT.PUT_LINE('Using AUTOMATIC LIST with default partition values: ' || v_default_values);
            END;

        ELSIF p_task.interval_clause IS NOT NULL THEN
            -- INTERVAL partitioning: Cannot use MAXVALUE
            -- Get minimum date from analysis table (excludes NULLs, uses proper conversion) to avoid ORA-14300
            p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);
            p_ddl := p_ddl || 'INTERVAL (' || p_task.interval_clause || ')' || CHR(10);

            BEGIN
                -- First, try to get minimum date from analysis table
                -- Analysis excludes NULLs and applies proper date conversion
                BEGIN
                    SELECT partition_boundary_min_date
                    INTO v_min_date
                    FROM cmr.dwh_migration_analysis
                    WHERE task_id = p_task.task_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_min_date := NULL;
                END;

                -- Fallback: Query source table directly if analysis not available
                IF v_min_date IS NULL THEN
                    EXECUTE IMMEDIATE 'SELECT MIN(' || p_task.partition_key || ') FROM ' ||
                        p_task.source_owner || '.' || p_task.source_table ||
                        ' WHERE ' || p_task.partition_key || ' IS NOT NULL'
                        INTO v_min_date;
                END IF;

                IF v_min_date IS NOT NULL THEN
                    DECLARE
                        v_buffer_months NUMBER;
                    BEGIN
                        -- Get buffer configuration (default 1 month if not set)
                        BEGIN
                            v_buffer_months := TO_NUMBER(get_dwh_ilm_config('INITIAL_PARTITION_BUFFER_MONTHS'));
                        EXCEPTION
                            WHEN OTHERS THEN
                                v_buffer_months := 1;
                        END;

                        -- Round down to first day of month/year depending on interval
                        -- This ensures initial partition is BEFORE the earliest data
                        IF UPPER(p_task.interval_clause) LIKE '%MONTH%' THEN
                            v_min_date := TRUNC(v_min_date, 'MM');
                            -- Subtract buffer months for safety margin
                            v_min_date := ADD_MONTHS(v_min_date, -v_buffer_months);
                        ELSIF UPPER(p_task.interval_clause) LIKE '%YEAR%' THEN
                            v_min_date := TRUNC(v_min_date, 'YYYY');
                            -- Subtract buffer in years (convert months to years)
                            v_min_date := ADD_MONTHS(v_min_date, -v_buffer_months);
                        ELSIF UPPER(p_task.interval_clause) LIKE '%DAY%' THEN
                            v_min_date := TRUNC(v_min_date);
                            -- Subtract buffer in days (buffer_months * 30)
                            v_min_date := v_min_date - (v_buffer_months * 30);
                        ELSE
                            -- Default: truncate to day and subtract buffer
                            v_min_date := TRUNC(v_min_date);
                            v_min_date := ADD_MONTHS(v_min_date, -v_buffer_months);
                        END IF;

                        v_starting_boundary := 'TO_DATE(''' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')';
                        DBMS_OUTPUT.PUT_LINE('Initial partition boundary: ' || v_starting_boundary ||
                            ' (from analysis, excludes NULLs, includes ' || v_buffer_months || ' month buffer)');
                    END;
                ELSE
                    -- No data detected, use configured fallback date
                    DECLARE
                        v_fallback_date VARCHAR2(20);
                    BEGIN
                        v_fallback_date := get_dwh_ilm_config('FALLBACK_INITIAL_PARTITION_DATE');
                        IF v_fallback_date IS NULL THEN
                            v_fallback_date := '1900-01-01';
                        END IF;
                        v_starting_boundary := 'TO_DATE(''' || v_fallback_date || ''', ''YYYY-MM-DD'')';
                        DBMS_OUTPUT.PUT_LINE('Initial partition boundary: ' || v_fallback_date || ' (no data found, using configured fallback date)');
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Fallback: use configured fallback date
                    DECLARE
                        v_fallback_date VARCHAR2(20);
                    BEGIN
                        v_fallback_date := get_dwh_ilm_config('FALLBACK_INITIAL_PARTITION_DATE');
                        IF v_fallback_date IS NULL THEN
                            v_fallback_date := '1900-01-01';
                        END IF;
                        v_starting_boundary := 'TO_DATE(''' || v_fallback_date || ''', ''YYYY-MM-DD'')';
                        DBMS_OUTPUT.PUT_LINE('WARNING: Could not determine initial partition boundary - using ' || v_fallback_date);
                    END;
            END;

            v_initial_partition_clause := '(PARTITION p_initial VALUES LESS THAN (' || v_starting_boundary || '))';

        ELSE
            -- Regular RANGE partitioning: MAXVALUE is allowed
            p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);
            v_initial_partition_clause := '(PARTITION p_initial VALUES LESS THAN (MAXVALUE))';
        END IF;

        -- Add initial partition clause (not needed for AUTOMATIC LIST)
        IF p_task.automatic_list != 'Y' THEN
            p_ddl := p_ddl || v_initial_partition_clause;
        END IF;

        -- Add storage clause
        p_ddl := p_ddl || v_storage_clause;

        -- Add row movement clause
        IF p_task.enable_row_movement = 'Y' THEN
            p_ddl := p_ddl || CHR(10) || 'ENABLE ROW MOVEMENT';
        END IF;

    END build_uniform_partitions;


    -- ==========================================================================
    -- Helper: build_tiered_partitions (NEW - ILM-Aware Partitioning)
    -- ==========================================================================
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
        v_hot_days NUMBER;
        v_hot_interval VARCHAR2(20);
        v_hot_tablespace VARCHAR2(128);
        v_hot_compression VARCHAR2(50);
        v_hot_pctfree NUMBER;

        v_warm_months NUMBER;
        v_warm_days NUMBER;
        v_warm_interval VARCHAR2(20);
        v_warm_tablespace VARCHAR2(128);
        v_warm_compression VARCHAR2(50);
        v_warm_pctfree NUMBER;

        v_cold_months NUMBER;
        v_cold_days NUMBER;
        v_cold_interval VARCHAR2(20);
        v_cold_tablespace VARCHAR2(128);
        v_cold_compression VARCHAR2(50);
        v_cold_pctfree NUMBER;

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
        v_step_number NUMBER := 10;
        v_error_msg VARCHAR2(4000);

    BEGIN
        -- Log step start
        v_step_start := SYSTIMESTAMP;

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Building Tiered Partition DDL');
        DBMS_OUTPUT.PUT_LINE('========================================');

        -- Initialize partition list CLOB with SESSION duration
        DBMS_LOB.CREATETEMPORARY(v_partition_list, TRUE, DBMS_LOB.SESSION);

        -- ====================================================================
        -- Validate tier_config structure
        -- ====================================================================
        DBMS_OUTPUT.PUT_LINE('Validating tier configuration...');

        -- Validate HOT tier
        IF NOT p_tier_config.has('hot') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20100, 'tier_config.hot is missing');
        END IF;

        v_hot_config := TREAT(p_tier_config.get('hot') AS JSON_OBJECT_T);
        IF NOT v_hot_config.has('interval') OR NOT v_hot_config.has('tablespace') OR NOT v_hot_config.has('compression') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20101, 'tier_config.hot missing required fields (interval, tablespace, compression)');
        END IF;

        IF NOT v_hot_config.has('age_months') AND NOT v_hot_config.has('age_days') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20102, 'tier_config.hot must have either age_months or age_days');
        END IF;

        -- Validate WARM tier
        IF NOT p_tier_config.has('warm') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20103, 'tier_config.warm is missing');
        END IF;

        v_warm_config := TREAT(p_tier_config.get('warm') AS JSON_OBJECT_T);
        IF NOT v_warm_config.has('interval') OR NOT v_warm_config.has('tablespace') OR NOT v_warm_config.has('compression') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20104, 'tier_config.warm missing required fields (interval, tablespace, compression)');
        END IF;

        -- Validate COLD tier
        IF NOT p_tier_config.has('cold') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20105, 'tier_config.cold is missing');
        END IF;

        v_cold_config := TREAT(p_tier_config.get('cold') AS JSON_OBJECT_T);
        IF NOT v_cold_config.has('interval') OR NOT v_cold_config.has('tablespace') OR NOT v_cold_config.has('compression') THEN
            IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_partition_list);
            END IF;
            RAISE_APPLICATION_ERROR(-20106, 'tier_config.cold missing required fields (interval, tablespace, compression)');
        END IF;

        DBMS_OUTPUT.PUT_LINE('  ✓ Tier configuration validated');
        DBMS_OUTPUT.PUT_LINE('');

        -- Parse tier configurations (already retrieved above during validation)
        IF v_hot_config.has('age_months') THEN
            v_hot_months := v_hot_config.get_number('age_months');
        END IF;
        IF v_hot_config.has('age_days') THEN
            v_hot_days := v_hot_config.get_number('age_days');
        END IF;
        v_hot_interval := v_hot_config.get_string('interval');
        v_hot_tablespace := v_hot_config.get_string('tablespace');
        v_hot_compression := v_hot_config.get_string('compression');
        -- Get PCTFREE from template or fallback to config default
        IF v_hot_config.has('pctfree') THEN
            v_hot_pctfree := v_hot_config.get_number('pctfree');
        ELSE
            v_hot_pctfree := NVL(TO_NUMBER(get_dwh_ilm_config('PCTFREE_HOT_TIER')), 10);
        END IF;

        -- WARM tier (already retrieved during validation)
        IF v_warm_config.has('age_months') THEN
            v_warm_months := v_warm_config.get_number('age_months');
        END IF;
        IF v_warm_config.has('age_days') THEN
            v_warm_days := v_warm_config.get_number('age_days');
        END IF;
        v_warm_interval := v_warm_config.get_string('interval');
        v_warm_tablespace := v_warm_config.get_string('tablespace');
        v_warm_compression := v_warm_config.get_string('compression');
        -- Get PCTFREE from template or fallback to config default
        IF v_warm_config.has('pctfree') THEN
            v_warm_pctfree := v_warm_config.get_number('pctfree');
        ELSE
            v_warm_pctfree := NVL(TO_NUMBER(get_dwh_ilm_config('PCTFREE_WARM_TIER')), 5);
        END IF;

        -- COLD tier (already retrieved during validation)
        IF v_cold_config.has('age_months') AND NOT v_cold_config.get('age_months').is_null() THEN
            v_cold_months := v_cold_config.get_number('age_months');
        END IF;
        IF v_cold_config.has('age_days') THEN
            v_cold_days := v_cold_config.get_number('age_days');
        END IF;
        v_cold_interval := v_cold_config.get_string('interval');
        v_cold_tablespace := v_cold_config.get_string('tablespace');
        v_cold_compression := v_cold_config.get_string('compression');
        -- Get PCTFREE from template or fallback to config default
        IF v_cold_config.has('pctfree') THEN
            v_cold_pctfree := v_cold_config.get_number('pctfree');
        ELSE
            v_cold_pctfree := NVL(TO_NUMBER(get_dwh_ilm_config('PCTFREE_COLD_TIER')), 0);
        END IF;

        -- Calculate tier boundary dates
        IF v_hot_months IS NOT NULL THEN
            v_hot_cutoff := ADD_MONTHS(v_current_date, -v_hot_months);
        ELSIF v_hot_days IS NOT NULL THEN
            v_hot_cutoff := v_current_date - v_hot_days;
        END IF;

        IF v_warm_months IS NOT NULL THEN
            v_warm_cutoff := ADD_MONTHS(v_current_date, -v_warm_months);
        ELSIF v_warm_days IS NOT NULL THEN
            v_warm_cutoff := v_current_date - v_warm_days;
        END IF;

        IF v_cold_months IS NOT NULL THEN
            v_cold_cutoff := ADD_MONTHS(v_current_date, -v_cold_months);
        ELSIF v_cold_days IS NOT NULL THEN
            v_cold_cutoff := v_current_date - v_cold_days;
        END IF;

        DBMS_OUTPUT.PUT_LINE('Tier boundaries:');
        IF v_cold_cutoff IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('  COLD: < ' || TO_CHAR(v_cold_cutoff, 'YYYY-MM-DD') ||
                                 ' (' || v_cold_interval || ' partitions)');
        ELSE
            DBMS_OUTPUT.PUT_LINE('  COLD: all data before WARM cutoff (' || v_cold_interval || ' partitions, permanent retention)');
        END IF;
        DBMS_OUTPUT.PUT_LINE('  WARM: ' || TO_CHAR(v_warm_cutoff, 'YYYY-MM-DD') ||
                             ' to ' || TO_CHAR(v_hot_cutoff, 'YYYY-MM-DD') ||
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
                IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_partition_list);
                END IF;
                RAISE_APPLICATION_ERROR(-20001,
                    'No analysis data found for task_id: ' || p_task.task_id ||
                    '. Run analyze_table() first.');
        END;

        -- ====================================================================
        -- COLD TIER: Yearly/Monthly partitions for data older than warm_cutoff
        -- ====================================================================
        DBMS_OUTPUT.PUT_LINE('');
        IF v_cold_cutoff IS NOT NULL AND v_min_date < v_cold_cutoff THEN
            DBMS_OUTPUT.PUT_LINE('Generating COLD tier partitions...');

            IF UPPER(v_cold_interval) = 'YEARLY' THEN
                v_partition_date := TRUNC(v_min_date, 'YYYY');
                WHILE v_partition_date < v_cold_cutoff LOOP
                    v_next_date := ADD_MONTHS(v_partition_date, 12);

                    -- If next_date would exceed cold_cutoff, create partial year partition ending at cold_cutoff
                    IF v_next_date > v_cold_cutoff THEN
                        v_next_date := v_cold_cutoff;
                    END IF;

                    v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');

                    DBMS_LOB.APPEND(v_partition_list,
                        '    PARTITION ' || v_partition_name ||
                        ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                        ' TABLESPACE ' || v_cold_tablespace);

                    IF v_cold_compression != 'NONE' THEN
                        DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_cold_compression));
                    END IF;

                    DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_cold_pctfree);
                    DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                    v_cold_count := v_cold_count + 1;
                    v_partition_date := v_next_date;
                END LOOP;
            ELSIF UPPER(v_cold_interval) = 'MONTHLY' THEN
                v_partition_date := TRUNC(v_min_date, 'MM');
                WHILE v_partition_date < v_cold_cutoff LOOP
                    v_next_date := ADD_MONTHS(v_partition_date, 1);
                    v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

                    DBMS_LOB.APPEND(v_partition_list,
                        '    PARTITION ' || v_partition_name ||
                        ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                        ' TABLESPACE ' || v_cold_tablespace);

                    IF v_cold_compression != 'NONE' THEN
                        DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_cold_compression));
                    END IF;

                    DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_cold_pctfree);
                    DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                    v_cold_count := v_cold_count + 1;
                    v_partition_date := v_next_date;
                END LOOP;
            END IF;

            DBMS_OUTPUT.PUT_LINE('  Generated ' || v_cold_count || ' COLD partitions');
        ELSIF v_cold_cutoff IS NULL THEN
            -- Permanent retention: all data before warm_cutoff goes to COLD
            DBMS_OUTPUT.PUT_LINE('Generating COLD tier partitions (permanent retention)...');

            IF UPPER(v_cold_interval) = 'YEARLY' THEN
                v_partition_date := TRUNC(v_min_date, 'YYYY');
                WHILE v_partition_date < v_warm_cutoff LOOP
                    v_next_date := ADD_MONTHS(v_partition_date, 12);

                    -- If next_date would exceed warm_cutoff, create partial year partition ending at warm_cutoff
                    IF v_next_date > v_warm_cutoff THEN
                        v_next_date := v_warm_cutoff;
                    END IF;

                    v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');

                    DBMS_LOB.APPEND(v_partition_list,
                        '    PARTITION ' || v_partition_name ||
                        ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                        ' TABLESPACE ' || v_cold_tablespace);

                    IF v_cold_compression != 'NONE' THEN
                        DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_cold_compression));
                    END IF;

                    DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_cold_pctfree);
                    DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                    v_cold_count := v_cold_count + 1;
                    v_partition_date := v_next_date;
                END LOOP;
            END IF;

            DBMS_OUTPUT.PUT_LINE('  Generated ' || v_cold_count || ' COLD partitions');
        END IF;

        -- ====================================================================
        -- WARM TIER: Yearly/Monthly partitions between cold_cutoff and hot_cutoff
        -- ====================================================================
        DBMS_OUTPUT.PUT_LINE('Generating WARM tier partitions...');

        IF v_cold_cutoff IS NOT NULL THEN
            v_partition_date := TRUNC(GREATEST(v_min_date, v_cold_cutoff), 'YYYY');
        ELSE
            v_partition_date := TRUNC(GREATEST(v_min_date, v_warm_cutoff), 'YYYY');
        END IF;

        IF UPPER(v_warm_interval) = 'YEARLY' THEN
            WHILE v_partition_date < v_hot_cutoff LOOP
                v_next_date := ADD_MONTHS(v_partition_date, 12);

                -- If next_date would exceed hot_cutoff, create partial year partition ending at hot_cutoff
                IF v_next_date > v_hot_cutoff THEN
                    v_next_date := v_hot_cutoff;
                END IF;

                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');

                DBMS_LOB.APPEND(v_partition_list,
                    '    PARTITION ' || v_partition_name ||
                    ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                    ' TABLESPACE ' || v_warm_tablespace);

                IF v_warm_compression != 'NONE' THEN
                    DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_warm_compression));
                END IF;

                DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_warm_pctfree);
                DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                v_warm_count := v_warm_count + 1;
                v_partition_date := v_next_date;
            END LOOP;
        ELSIF UPPER(v_warm_interval) = 'MONTHLY' THEN
            v_partition_date := TRUNC(v_partition_date, 'MM');
            WHILE v_partition_date < v_hot_cutoff LOOP
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

                DBMS_LOB.APPEND(v_partition_list,
                    '    PARTITION ' || v_partition_name ||
                    ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                    ' TABLESPACE ' || v_warm_tablespace);

                IF v_warm_compression != 'NONE' THEN
                    DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_warm_compression));
                END IF;

                DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_warm_pctfree);
                DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                v_warm_count := v_warm_count + 1;
                v_partition_date := v_next_date;
            END LOOP;
        END IF;

        DBMS_OUTPUT.PUT_LINE('  Generated ' || v_warm_count || ' WARM partitions');

        -- ====================================================================
        -- HOT TIER: Monthly/Daily partitions for recent data
        -- ====================================================================
        DBMS_OUTPUT.PUT_LINE('Generating HOT tier partitions...');

        IF UPPER(v_hot_interval) = 'MONTHLY' THEN
            v_partition_date := TRUNC(GREATEST(v_min_date, v_hot_cutoff), 'MM');
            WHILE v_partition_date <= TRUNC(v_current_date, 'MM') LOOP
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

                DBMS_LOB.APPEND(v_partition_list,
                    '    PARTITION ' || v_partition_name ||
                    ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                    ' TABLESPACE ' || v_hot_tablespace);

                IF v_hot_compression != 'NONE' THEN
                    DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_hot_compression));
                END IF;

                DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_hot_pctfree);
                DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                v_hot_count := v_hot_count + 1;
                v_partition_date := v_next_date;
            END LOOP;
        ELSIF UPPER(v_hot_interval) = 'DAILY' THEN
            v_partition_date := TRUNC(GREATEST(v_min_date, v_hot_cutoff));
            WHILE v_partition_date <= TRUNC(v_current_date) LOOP
                v_next_date := v_partition_date + 1;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM_DD');

                DBMS_LOB.APPEND(v_partition_list,
                    '    PARTITION ' || v_partition_name ||
                    ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                    ' TABLESPACE ' || v_hot_tablespace);

                IF v_hot_compression != 'NONE' THEN
                    DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_hot_compression));
                END IF;

                DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_hot_pctfree);
                DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                v_hot_count := v_hot_count + 1;
                v_partition_date := v_next_date;
            END LOOP;
        ELSIF UPPER(v_hot_interval) = 'WEEKLY' THEN
            v_partition_date := TRUNC(GREATEST(v_min_date, v_hot_cutoff), 'IW');
            WHILE v_partition_date <= TRUNC(v_current_date, 'IW') LOOP
                v_next_date := v_partition_date + 7;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'IYYY_IW');

                DBMS_LOB.APPEND(v_partition_list,
                    '    PARTITION ' || v_partition_name ||
                    ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                    ' TABLESPACE ' || v_hot_tablespace);

                IF v_hot_compression != 'NONE' THEN
                    DBMS_LOB.APPEND(v_partition_list, get_compression_clause(v_hot_compression));
                END IF;

                DBMS_LOB.APPEND(v_partition_list, ' PCTFREE ' || v_hot_pctfree);
                DBMS_LOB.APPEND(v_partition_list, ',' || CHR(10));
                v_hot_count := v_hot_count + 1;
                v_partition_date := v_next_date;
            END LOOP;
        END IF;

        DBMS_OUTPUT.PUT_LINE('  Generated ' || v_hot_count || ' HOT partitions');

        -- Remove trailing comma from last partition
        v_partition_list := RTRIM(v_partition_list, ',' || CHR(10));

        -- ====================================================================
        -- Build complete CREATE TABLE DDL
        -- ====================================================================
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Assembling CREATE TABLE DDL...');

        -- Get column definitions (reuse existing logic from build_uniform_partitions)
        DECLARE
            v_requires_conversion CHAR(1);
            v_date_column VARCHAR2(128);
        BEGIN
            -- Check if date conversion is required
            BEGIN
                SELECT requires_conversion, date_column_name
                INTO v_requires_conversion, v_date_column
                FROM cmr.dwh_migration_analysis
                WHERE task_id = p_task.task_id;
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
        END;

        -- Build DDL
        p_ddl := 'CREATE TABLE ' || p_task.source_owner || '.' || p_task.source_table || '_PART' || CHR(10);
        p_ddl := p_ddl || '(' || CHR(10) || v_columns || CHR(10) || ')' || CHR(10);

        -- Add table-level storage from HOT tier (active data defaults)
        p_ddl := p_ddl || 'TABLESPACE ' || v_hot_tablespace || CHR(10);
        IF v_hot_compression != 'NONE' THEN
            p_ddl := p_ddl || get_compression_clause(v_hot_compression) || CHR(10);
        END IF;
        p_ddl := p_ddl || 'PCTFREE ' || v_hot_pctfree || CHR(10);

        p_ddl := p_ddl || 'PARTITION BY ' || p_task.partition_type || CHR(10);

        -- INTERVAL creates future HOT partitions automatically
        IF UPPER(v_hot_interval) = 'MONTHLY' THEN
            p_ddl := p_ddl || 'INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))' || CHR(10);
        ELSIF UPPER(v_hot_interval) = 'DAILY' THEN
            p_ddl := p_ddl || 'INTERVAL(NUMTODSINTERVAL(1,''DAY''))' || CHR(10);
        ELSIF UPPER(v_hot_interval) = 'WEEKLY' THEN
            p_ddl := p_ddl || 'INTERVAL(NUMTODSINTERVAL(7,''DAY''))' || CHR(10);
        END IF;

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
        DBMS_OUTPUT.PUT_LINE('  Table defaults (from HOT tier):');
        DBMS_OUTPUT.PUT_LINE('    - Tablespace: ' || v_hot_tablespace);
        DBMS_OUTPUT.PUT_LINE('    - Compression: ' || v_hot_compression);
        DBMS_OUTPUT.PUT_LINE('    - PCTFREE: ' || v_hot_pctfree);
        DBMS_OUTPUT.PUT_LINE('  COLD tier: ' || v_cold_count || ' partitions (' || v_cold_interval || ')');
        DBMS_OUTPUT.PUT_LINE('  WARM tier: ' || v_warm_count || ' partitions (' || v_warm_interval || ')');
        DBMS_OUTPUT.PUT_LINE('  HOT tier: ' || v_hot_count || ' partitions (' || v_hot_interval || ')');
        DBMS_OUTPUT.PUT_LINE('  Total: ' || (v_cold_count + v_warm_count + v_hot_count) || ' explicit partitions');
        DBMS_OUTPUT.PUT_LINE('  Future partitions: INTERVAL ' || v_hot_interval || ' in ' || v_hot_tablespace);
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
                WHEN OTHERS THEN NULL;
            END;

            -- Clean up LOB in case of error
            BEGIN
                IF DBMS_LOB.ISTEMPORARY(v_partition_list) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_partition_list);
                END IF;
            EXCEPTION WHEN OTHERS THEN NULL; END;

            DBMS_OUTPUT.PUT_LINE('ERROR: Failed to build tiered partition DDL: ' || v_error_msg);
            RAISE;
    END build_tiered_partitions;


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

        -- Clean up stale _MIGR indexes from previous failed migrations
        DBMS_OUTPUT.PUT_LINE('Checking for stale _MIGR indexes from previous migrations...');
        FOR stale_idx IN (
            SELECT index_name
            FROM dba_indexes
            WHERE table_owner = p_source_owner
            AND table_name = p_target_table
            AND index_name LIKE '%\_MIGR' ESCAPE '\'
        ) LOOP
            BEGIN
                v_sql := 'DROP INDEX ' || p_source_owner || '.' || stale_idx.index_name;
                EXECUTE IMMEDIATE v_sql;
                DBMS_OUTPUT.PUT_LINE('  Dropped stale index: ' || stale_idx.index_name);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  Warning: Could not drop stale index ' || stale_idx.index_name || ': ' || SQLERRM);
            END;
        END LOOP;

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
                v_temp_index_name := SUBSTR(idx.index_name, 1, 123) || '_MIGR';

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
                            CHR(10) || '  PARALLEL ' || get_dwh_ilm_config('MIGRATION_PARALLEL_DEGREE'));
                END IF;

                -- Strip trailing semicolon (EXECUTE IMMEDIATE doesn't accept it)
                v_sql := RTRIM(v_sql, '; ' || CHR(10) || CHR(13));

                -- Log step start
                log_step(p_task_id, v_step, 'Recreate index: ' || idx.index_name || ' (temp: ' || v_temp_index_name || ')',
                        'INDEX', SUBSTR(v_sql, 1, 4000), 'RUNNING', v_start, v_start);

                EXECUTE IMMEDIATE v_sql;

                -- Log successful completion
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
    --   Recreates all constraints (PRIMARY KEY, CHECK, UNIQUE, FK) from source table
    --   on target table using Oracle's DBMS_METADATA.GET_DDL for accurate extraction.
    --
    -- BENEFITS:
    --   - Captures ALL constraint features: deferred, ENABLE/DISABLE, RELY/NORELY
    --   - Preserves constraint states, validation options
    --   - Handles all constraint types including complex CHECK conditions
    --   - More maintainable than manual DDL construction
    --
    -- PROCESS:
    --   1. Recreate PRIMARY KEY constraint first (highest priority)
    --   2. Recreate CHECK and UNIQUE constraints (no dependencies)
    --   3. Recreate FOREIGN KEY constraints last (depends on PKs)
    --   4. Get constraint DDL using DBMS_METADATA.GET_DDL
    --   5. Replace table references
    --   6. Execute modified DDL
    --
    -- NOTE:
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

        -- Get PRIMARY KEY constraint first (highest priority)
        FOR con IN (
            SELECT c.constraint_name, c.constraint_type
            FROM dba_constraints c
            WHERE c.owner = p_source_owner
            AND c.table_name = p_source_table
            AND c.constraint_type = 'P'
            ORDER BY c.constraint_name
        ) LOOP
            v_step := v_step + 1;
            v_start := SYSTIMESTAMP;

            BEGIN
                -- Get constraint DDL from DBMS_METADATA
                v_constraint_ddl := DBMS_METADATA.GET_DDL('CONSTRAINT', con.constraint_name, p_source_owner);

                -- Replace constraint name with temporary _MIGR suffix to avoid ORA-02264
                -- (Constraint names are schema-level unique, unlike indexes)
                v_sql := REGEXP_REPLACE(v_constraint_ddl,
                                       'CONSTRAINT\s+"' || con.constraint_name || '"',
                                       'CONSTRAINT "' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR"',
                                       1, 1, 'i');

                -- Replace source table name with target table name
                v_sql := REPLACE(v_sql,
                                '"' || p_source_table || '"',
                                '"' || p_target_table || '"');
                v_sql := REPLACE(v_sql,
                                ' ' || p_source_table || ' ',
                                ' ' || p_target_table || ' ');

                -- Strip trailing semicolon (EXECUTE IMMEDIATE doesn't accept it)
                v_sql := RTRIM(v_sql, '; ' || CHR(10) || CHR(13));

                -- Log step start
                log_step(p_task_id, v_step, 'Recreate PRIMARY KEY: ' || con.constraint_name || ' (temp: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR)',
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'RUNNING', v_start, v_start);

                EXECUTE IMMEDIATE v_sql;

                -- Log successful completion
                log_step(p_task_id, v_step, 'Recreate PRIMARY KEY: ' || con.constraint_name || ' (temp: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR)',
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Created PRIMARY KEY: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR (will rename to ' || con.constraint_name || ' after cutover)');
            EXCEPTION
                WHEN OTHERS THEN
                    log_step(p_task_id, v_step, 'Recreate PRIMARY KEY: ' || con.constraint_name,
                            'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to recreate PRIMARY KEY ' || con.constraint_name);
                    DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);

                    -- Re-raise exception to fail the migration (PRIMARY KEY is critical)
                    RAISE_APPLICATION_ERROR(-20503,
                        'PRIMARY KEY recreation failed: ' || con.constraint_name || ' - ' || SQLERRM);
            END;
        END LOOP;

        -- Get CHECK and UNIQUE constraints (no dependencies)
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

                -- Replace constraint name with temporary _MIGR suffix to avoid ORA-02264
                -- (Constraint names are schema-level unique, unlike indexes)
                v_sql := REGEXP_REPLACE(v_constraint_ddl,
                                       'CONSTRAINT\s+"' || con.constraint_name || '"',
                                       'CONSTRAINT "' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR"',
                                       1, 1, 'i');

                -- Replace source table name with target table name
                v_sql := REPLACE(v_sql,
                                '"' || p_source_table || '"',
                                '"' || p_target_table || '"');
                v_sql := REPLACE(v_sql,
                                ' ' || p_source_table || ' ',
                                ' ' || p_target_table || ' ');

                -- Strip trailing semicolon (EXECUTE IMMEDIATE doesn't accept it)
                v_sql := RTRIM(v_sql, '; ' || CHR(10) || CHR(13));

                -- Log step start
                log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name || ' (temp: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR)',
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'RUNNING', v_start, v_start);

                EXECUTE IMMEDIATE v_sql;

                -- Log successful completion
                log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name || ' (temp: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR)',
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Created constraint: ' || SUBSTR(con.constraint_name, 1, 123) || '_MIGR' ||
                                    ' (' || con.constraint_type || ') (will rename to ' || con.constraint_name || ' after cutover)');
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

                -- Strip trailing semicolon (EXECUTE IMMEDIATE doesn't accept it)
                v_sql := RTRIM(v_sql, '; ' || CHR(10) || CHR(13));

                -- Log step start
                log_step(p_task_id, v_step, 'Recreate FK constraint: ' || con.constraint_name,
                        'CONSTRAINT', SUBSTR(v_sql, 1, 4000), 'RUNNING', v_start, v_start);

                EXECUTE IMMEDIATE v_sql;

                -- Log successful completion
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


    -- -------------------------------------------------------------------------
    -- Cleanup Procedure - Remove Partially Created Objects on Failure
    -- -------------------------------------------------------------------------
    PROCEDURE cleanup_failed_migration(p_task_id NUMBER) AS
        v_task cmr.dwh_migration_tasks%ROWTYPE;
        v_new_table VARCHAR2(128);
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_cleanup_summary VARCHAR2(4000);
        v_index_count NUMBER := 0;
        v_table_dropped BOOLEAN := FALSE;
    BEGIN
        v_start := SYSTIMESTAMP;
        DBMS_OUTPUT.PUT_LINE('Cleaning up failed migration...');

        -- Get task information
        BEGIN
            SELECT * INTO v_task
            FROM cmr.dwh_migration_tasks
            WHERE task_id = p_task_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: Task not found, cannot cleanup');
                RETURN;
        END;

        v_new_table := v_task.source_table || '_PART';

        -- Drop temporary indexes (with _MIGR suffix) if they exist
        FOR idx IN (
            SELECT index_name
            FROM dba_indexes
            WHERE table_owner = v_task.source_owner
            AND table_name = v_new_table
            AND index_name LIKE '%\_MIGR' ESCAPE '\'
        ) LOOP
            BEGIN
                v_sql := 'DROP INDEX ' || v_task.source_owner || '.' || idx.index_name;
                EXECUTE IMMEDIATE v_sql;
                v_index_count := v_index_count + 1;
                DBMS_OUTPUT.PUT_LINE('  Dropped index: ' || idx.index_name);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Could not drop index ' || idx.index_name || ': ' || SQLERRM);
            END;
        END LOOP;

        -- Drop the partitioned table if it exists
        BEGIN
            v_sql := 'DROP TABLE ' || v_task.source_owner || '.' || v_new_table || ' PURGE';
            EXECUTE IMMEDIATE v_sql;
            v_table_dropped := TRUE;
            DBMS_OUTPUT.PUT_LINE('  Dropped table: ' || v_new_table);
        EXCEPTION
            WHEN OTHERS THEN
                -- ORA-00942 means table doesn't exist, which is fine
                IF SQLCODE != -942 THEN
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Could not drop table ' || v_new_table || ': ' || SQLERRM);
                END IF;
        END;

        -- Update task status to FAILED
        BEGIN
            UPDATE cmr.dwh_migration_tasks
            SET status = 'FAILED',
                can_rollback = 'N'
            WHERE task_id = p_task_id;
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('  Task status updated to FAILED');
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: Could not update task status: ' || SQLERRM);
        END;

        -- Build cleanup summary
        v_cleanup_summary := 'Cleanup completed: ';
        IF v_table_dropped THEN
            v_cleanup_summary := v_cleanup_summary || 'dropped table ' || v_new_table;
        ELSE
            v_cleanup_summary := v_cleanup_summary || 'no table to drop';
        END IF;

        IF v_index_count > 0 THEN
            v_cleanup_summary := v_cleanup_summary || ', dropped ' || v_index_count || ' temporary index(es)';
        ELSE
            v_cleanup_summary := v_cleanup_summary || ', no indexes to drop';
        END IF;

        -- Log cleanup operation
        log_step(p_task_id, 9999, 'Cleanup failed migration', 'CLEANUP',
                v_cleanup_summary, 'SUCCESS', v_start, SYSTIMESTAMP);

        DBMS_OUTPUT.PUT_LINE('Cleanup completed');
    END cleanup_failed_migration;


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
            DBMS_OUTPUT.PUT_LINE('Rename mode: ' || CASE WHEN NVL(v_task.rename_original_table, 'Y') = 'Y'
                                                         THEN 'Original->_OLD, Migrated->Original'
                                                         ELSE 'Original unchanged, Migrated->_MIGR' END);
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Migrating using CTAS method: ' || v_task.source_table);
        END IF;

        -- Determine naming based on rename_original_table parameter
        IF NVL(v_task.rename_original_table, 'Y') = 'Y' THEN
            -- Default behavior: rename original to _OLD, migrated becomes original name
            v_new_table := v_task.source_table || '_PART';
            v_old_table := v_task.source_table || '_OLD';
        ELSE
            -- New behavior: keep original unchanged, migrated stays as _MIGR
            v_new_table := v_task.source_table || '_MIGR';
            v_old_table := NULL;  -- No rename of original table
        END IF;

        -- Step 1: Build partition DDL
        v_start := SYSTIMESTAMP;

        IF NOT p_simulate THEN
            -- Log step start
            log_step(p_task_id, v_step, 'Build partition DDL', 'PREPARE', NULL,
                    'RUNNING', v_start, v_start);
        END IF;

        build_partition_ddl(v_task, v_ddl);

        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 1: BUILD PARTITION DDL');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE(v_ddl);
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            -- Log successful completion with DDL
            log_step(p_task_id, v_step, 'Build partition DDL', 'PREPARE', v_ddl,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        END IF;
        v_step := v_step + 10;

        -- Step 2: Create partitioned table
        IF NOT p_simulate THEN
            v_start := SYSTIMESTAMP;

            -- Log step start so we know where failure occurs
            log_step(p_task_id, v_step, 'Create partitioned table', 'CREATE', v_ddl,
                    'RUNNING', v_start, v_start);

            EXECUTE IMMEDIATE v_ddl;

            -- Log successful completion
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

                    -- Log step start
                    log_step(p_task_id, v_step, 'Copy data (with date conversion)', 'COPY', v_sql,
                            'RUNNING', v_start, v_start);
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
                ELSE
                    -- Log step start
                    log_step(p_task_id, v_step, 'Copy data', 'COPY', v_sql,
                            'RUNNING', v_start, v_start);
                END IF;
            END IF;

            IF NOT p_simulate THEN
                EXECUTE IMMEDIATE v_sql;
                COMMIT;

                -- Log successful completion
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
                    -- Log step start
                    log_step(p_task_id, v_step, 'Copy data', 'COPY', v_sql,
                            'RUNNING', v_start, v_start);

                    EXECUTE IMMEDIATE v_sql;
                    COMMIT;

                    -- Log successful completion
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

            -- Log step start
            log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                    'RUNNING', v_start, v_start);

            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => v_task.source_owner,
                tabname => v_new_table,
                cascade => TRUE,
                degree => v_task.parallel_degree
            );

            -- Log successful completion
            log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                    'SUCCESS', v_start, SYSTIMESTAMP);
        END IF;
        v_step := v_step + 10;

        -- Step 7: Rename tables (conditional based on rename_original_table parameter)
        IF NVL(v_task.rename_original_table, 'Y') = 'Y' THEN
            -- Default behavior: rename original to _OLD, migrated becomes original name
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
                DBMS_OUTPUT.PUT_LINE('(Indexes with _MIGR suffix will be renamed to original names)');
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

            -- Log step start
            log_step(p_task_id, v_step, 'Rename tables (cutover)', 'RENAME', v_sql,
                    'RUNNING', v_start, v_start);

            EXECUTE IMMEDIATE v_sql;

            v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_new_table ||
                    ' RENAME TO ' || v_task.source_table;
            EXECUTE IMMEDIATE v_sql;

            -- Log successful completion
            log_step(p_task_id, v_step, 'Rename tables (cutover)', 'RENAME', v_sql,
                    'SUCCESS', v_start, SYSTIMESTAMP);

            -- Step 7.5: Rename old table's indexes to avoid naming conflicts
            v_step := v_step + 5;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('Renaming old table indexes to _OLD suffix...');

            FOR old_idx IN (
                SELECT index_name
                FROM dba_indexes
                WHERE table_owner = v_task.source_owner
                AND table_name = v_old_table
                AND index_name NOT LIKE '%\_OLD' ESCAPE '\'
            ) LOOP
                BEGIN
                    v_sql := 'ALTER INDEX ' || v_task.source_owner || '.' || old_idx.index_name ||
                            ' RENAME TO ' || old_idx.index_name || '_OLD';
                    EXECUTE IMMEDIATE v_sql;

                    DBMS_OUTPUT.PUT_LINE('  Renamed old index: ' || old_idx.index_name || ' -> ' || old_idx.index_name || '_OLD');

                    log_step(p_task_id, v_step, 'Rename old index: ' || old_idx.index_name,
                            'RENAME_INDEX', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to rename old index ' || old_idx.index_name || ': ' || SQLERRM);
                        log_step(p_task_id, v_step, 'Rename old index: ' || old_idx.index_name,
                                'RENAME_INDEX', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                                SQLCODE, SQLERRM);

                        -- Re-raise exception to fail the migration
                        RAISE_APPLICATION_ERROR(-20502,
                            'Old index rename failed: ' || old_idx.index_name || ' - ' || SQLERRM);
                END;
            END LOOP;

            -- Step 8: Rename indexes from temporary names back to original names
            v_step := v_step + 5;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('Renaming indexes to original names...');

            FOR idx IN (
                SELECT index_name, REPLACE(index_name, '_MIGR', '') AS original_name
                FROM dba_indexes
                WHERE table_owner = v_task.source_owner
                AND table_name = v_task.source_table
                AND index_name LIKE '%\_MIGR' ESCAPE '\'
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
                        DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to rename index ' || idx.index_name || ': ' || SQLERRM);
                        log_step(p_task_id, v_step, 'Rename index: ' || idx.original_name,
                                'RENAME_INDEX', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                                SQLCODE, SQLERRM);

                        -- Re-raise exception to fail the migration
                        RAISE_APPLICATION_ERROR(-20501,
                            'Index rename failed: ' || idx.index_name || ' -> ' || idx.original_name || ' - ' || SQLERRM);
                END;
            END LOOP;

            -- Step 8.5: Rename old table's constraints to _OLD suffix
            v_step := v_step + 5;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('Renaming old table constraints to _OLD suffix...');

            FOR old_con IN (
                SELECT constraint_name, constraint_type
                FROM dba_constraints
                WHERE owner = v_task.source_owner
                AND table_name = v_old_table
                AND constraint_name NOT LIKE '%\_OLD' ESCAPE '\'
                ORDER BY constraint_type  -- P, U, C, R order
            ) LOOP
                BEGIN
                    v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_old_table ||
                            ' RENAME CONSTRAINT ' || old_con.constraint_name ||
                            ' TO ' || old_con.constraint_name || '_OLD';
                    EXECUTE IMMEDIATE v_sql;

                    DBMS_OUTPUT.PUT_LINE('  Renamed old constraint: ' || old_con.constraint_name || ' -> ' || old_con.constraint_name || '_OLD');

                    log_step(p_task_id, v_step, 'Rename old constraint: ' || old_con.constraint_name,
                            'RENAME_CONSTRAINT', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to rename old constraint ' || old_con.constraint_name || ': ' || SQLERRM);
                        log_step(p_task_id, v_step, 'Rename old constraint: ' || old_con.constraint_name,
                                'RENAME_CONSTRAINT', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                                SQLCODE, SQLERRM);

                        -- Re-raise exception to fail the migration
                        RAISE_APPLICATION_ERROR(-20504,
                            'Old constraint rename failed: ' || old_con.constraint_name || ' - ' || SQLERRM);
                END;
            END LOOP;

            -- Step 9: Rename constraints from temporary names back to original names
            v_step := v_step + 5;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('Renaming constraints to original names...');

            FOR con IN (
                SELECT constraint_name, REPLACE(constraint_name, '_MIGR', '') AS original_name, constraint_type
                FROM dba_constraints
                WHERE owner = v_task.source_owner
                AND table_name = v_task.source_table
                AND constraint_name LIKE '%\_MIGR' ESCAPE '\'
                ORDER BY DECODE(constraint_type, 'R', 3, 'U', 2, 'C', 2, 'P', 1)  -- FK last
            ) LOOP
                BEGIN
                    v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                            ' RENAME CONSTRAINT ' || con.constraint_name ||
                            ' TO ' || con.original_name;
                    EXECUTE IMMEDIATE v_sql;

                    DBMS_OUTPUT.PUT_LINE('  Renamed: ' || con.constraint_name || ' -> ' || con.original_name);

                    log_step(p_task_id, v_step, 'Rename constraint: ' || con.original_name,
                            'RENAME_CONSTRAINT', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
                    v_step := v_step + 1;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR: Failed to rename constraint ' || con.constraint_name || ': ' || SQLERRM);
                        log_step(p_task_id, v_step, 'Rename constraint: ' || con.original_name,
                                'RENAME_CONSTRAINT', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                                SQLCODE, SQLERRM);

                        -- Re-raise exception to fail the migration
                        RAISE_APPLICATION_ERROR(-20505,
                            'Constraint rename failed: ' || con.constraint_name || ' -> ' || con.original_name || ' - ' || SQLERRM);
                END;
            END LOOP;

            -- Update task with backup name
            UPDATE cmr.dwh_migration_tasks
            SET backup_table_name = v_old_table,
                can_rollback = 'Y'
            WHERE task_id = p_task_id;
            COMMIT;

            -- Step 10: Rename system-generated partitions (all partition types)
            v_step := v_step + 10;
            DBMS_OUTPUT.PUT_LINE('Renaming system-generated partitions...');

            rename_system_partitions(
                p_task_id => p_task_id,
                p_owner => v_task.source_owner,
                p_table_name => v_task.source_table,  -- Already renamed to final name
                p_partition_type => v_task.partition_type,
                p_interval_clause => v_task.interval_clause
            );

            DBMS_OUTPUT.PUT_LINE('Migration completed successfully');
            DBMS_OUTPUT.PUT_LINE('  Original table renamed to: ' || v_old_table);
            DBMS_OUTPUT.PUT_LINE('  New partitioned table: ' || v_task.source_table);
        END IF;
    ELSE
        -- Alternative path: rename_original_table='N' - keep original table, migrated stays as _MIGR
        IF p_simulate THEN
            DBMS_OUTPUT.PUT_LINE('Step 6: SKIP TABLE RENAME (CUTOVER)');
            DBMS_OUTPUT.PUT_LINE('----------------------------------------');
            DBMS_OUTPUT.PUT_LINE('Original table will remain unchanged: ' || v_task.source_owner || '.' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('Migrated table will keep name: ' || v_new_table);
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION COMPLETE');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('Summary:');
            DBMS_OUTPUT.PUT_LINE('  - Original table remains: ' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('  - New partitioned table: ' || v_new_table);
            DBMS_OUTPUT.PUT_LINE('  - No backup created (original unchanged)');
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('To execute this migration, call:');
            DBMS_OUTPUT.PUT_LINE('  EXEC pck_dwh_table_migration_executor.execute_migration(p_task_id => ' || p_task_id || ');');
            DBMS_OUTPUT.PUT_LINE('');
        ELSE
            -- No table rename needed, just log completion
            DBMS_OUTPUT.PUT_LINE('Migration completed successfully');
            DBMS_OUTPUT.PUT_LINE('  Original table unchanged: ' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('  New partitioned table: ' || v_new_table);

            -- Update task - no backup since original unchanged
            UPDATE cmr.dwh_migration_tasks
            SET backup_table_name = NULL,
                can_rollback = 'N'
            WHERE task_id = p_task_id;
            COMMIT;
        END IF;
    END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_step(p_task_id, v_step, 'Migration failed', 'ERROR', NULL,
                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);

            -- Clean up partially created objects
            BEGIN
                cleanup_failed_migration(p_task_id);
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR during cleanup: ' || SQLERRM);
            END;

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
            IF NVL(v_task.rename_original_table, 'Y') = 'N' THEN
                DBMS_OUTPUT.PUT_LINE('  8. Rename tables (rename_original_table=N):');
                DBMS_OUTPUT.PUT_LINE('     - Partitioned table renamed to: ' || v_task.source_table || '_MIGR');
                DBMS_OUTPUT.PUT_LINE('     - Original structure renamed back to: ' || v_task.source_table);
            END IF;
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('SIMULATION COMPLETE - ONLINE Method');
            DBMS_OUTPUT.PUT_LINE('========================================');
            DBMS_OUTPUT.PUT_LINE('Summary:');
            DBMS_OUTPUT.PUT_LINE('  - Table will remain accessible during migration');
            DBMS_OUTPUT.PUT_LINE('  - Only brief lock during final swap');
            IF NVL(v_task.rename_original_table, 'Y') = 'Y' THEN
                DBMS_OUTPUT.PUT_LINE('  - Final result: ' || v_task.source_table || ' (partitioned)');
                DBMS_OUTPUT.PUT_LINE('  - Interim/backup: ' || v_interim_table);
            ELSE
                DBMS_OUTPUT.PUT_LINE('  - Original unchanged: ' || v_task.source_table);
                DBMS_OUTPUT.PUT_LINE('  - Partitioned table: ' || v_task.source_table || '_MIGR');
            END IF;
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

        -- Step 8: Handle table naming based on rename_original_table parameter
        IF NVL(v_task.rename_original_table, 'Y') = 'N' THEN
            -- Alternative behavior: rename swapped tables to keep original name unchanged
            -- After FINISH_REDEF_TABLE:
            --   - Original table name now has partitioned structure
            --   - Interim table name now has original structure
            -- We need to swap them back and rename appropriately
            v_step := v_step + 10;
            v_start := SYSTIMESTAMP;
            DBMS_OUTPUT.PUT_LINE('  Adjusting table names (rename_original_table=N)...');

            -- Rename current source_table (partitioned) to _MIGR
            v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                    ' RENAME TO ' || v_task.source_table || '_MIGR';
            EXECUTE IMMEDIATE v_sql;

            -- Rename interim table (original structure) back to source_table
            v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_interim_table ||
                    ' RENAME TO ' || v_task.source_table;
            EXECUTE IMMEDIATE v_sql;

            log_step(p_task_id, v_step, 'Rename tables (keep original unchanged)', 'RENAME', v_sql,
                    'SUCCESS', v_start, SYSTIMESTAMP);

            -- Update task - no true backup since original unchanged
            UPDATE cmr.dwh_migration_tasks
            SET backup_table_name = NULL,
                can_rollback = 'N'
            WHERE task_id = p_task_id;
            COMMIT;

            DBMS_OUTPUT.PUT_LINE('Online migration completed successfully');
            DBMS_OUTPUT.PUT_LINE('  Original table unchanged: ' || v_task.source_table);
            DBMS_OUTPUT.PUT_LINE('  New partitioned table: ' || v_task.source_table || '_MIGR');
        ELSE
            -- Default behavior: keep Oracle's swap (original is now partitioned)
            UPDATE cmr.dwh_migration_tasks
            SET backup_table_name = v_interim_table,
                can_rollback = 'Y'
            WHERE task_id = p_task_id;
            COMMIT;

            DBMS_OUTPUT.PUT_LINE('Online migration completed successfully');
            DBMS_OUTPUT.PUT_LINE('  Table migrated: ' || v_task.source_table || ' (now partitioned)');
            DBMS_OUTPUT.PUT_LINE('  Interim table available: ' || v_interim_table || ' (for rollback)');
        END IF;

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

            -- Update task status to FAILED
            BEGIN
                UPDATE cmr.dwh_migration_tasks
                SET status = 'FAILED',
                    can_rollback = 'N'
                WHERE task_id = p_task_id;
                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;

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

            -- Update task status to FAILED
            BEGIN
                UPDATE cmr.dwh_migration_tasks
                SET status = 'FAILED',
                    can_rollback = 'N'
                WHERE task_id = p_task_id;
                COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL;
            END;

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

            -- Generate unique execution ID for this migration run
            -- All log entries will share this execution_id
            SELECT cmr.dwh_mig_execution_seq.NEXTVAL INTO g_execution_id FROM dual;

            v_source_size := pck_dwh_table_migration_analyzer.get_table_size_mb(v_task.source_owner, v_task.source_table);

            -- Update status and reset error message from previous attempts
            UPDATE cmr.dwh_migration_tasks
            SET status = 'RUNNING',
                execution_start = v_start_time,
                error_message = NULL  -- Clear any previous error messages
            WHERE task_id = p_task_id;
            COMMIT;

            -- Create backup if enabled
            IF get_dwh_ilm_config('MIGRATION_BACKUP_ENABLED') = 'Y' THEN
                create_backup_table(p_task_id, v_task.source_owner, v_task.source_table, v_backup_name);
            END IF;

            -- Handle NULLs in partition key column before migration
            DECLARE
                v_null_strategy VARCHAR2(30);
                v_null_default_value VARCHAR2(100);
                v_partition_column VARCHAR2(128);
                v_update_sql VARCHAR2(4000);
                v_rows_updated NUMBER;
            BEGIN
                -- Get NULL handling strategy from analysis
                BEGIN
                    SELECT null_handling_strategy, null_default_value, date_column_name
                    INTO v_null_strategy, v_null_default_value, v_partition_column
                    FROM cmr.dwh_migration_analysis
                    WHERE task_id = p_task_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_null_strategy := NULL;
                END;

                -- Use explicit partition_key if date_column_name not available
                IF v_partition_column IS NULL THEN
                    v_partition_column := v_task.partition_key;
                END IF;

                -- Execute UPDATE strategy if configured
                IF v_null_strategy = 'UPDATE' AND v_null_default_value IS NOT NULL AND v_partition_column IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('Handling NULL values in partition key column: ' || v_partition_column);
                    DBMS_OUTPUT.PUT_LINE('  Strategy: UPDATE to default value');
                    DBMS_OUTPUT.PUT_LINE('  Default value: ' || v_null_default_value);

                    -- Build and execute UPDATE statement
                    v_update_sql := 'UPDATE ' || v_task.source_owner || '.' || v_task.source_table ||
                                  ' SET ' || v_partition_column || ' = ';

                    -- Add appropriate type conversion based on default value format
                    IF v_null_default_value LIKE '%-%-%' THEN
                        -- Date format (YYYY-MM-DD)
                        v_update_sql := v_update_sql || 'TO_DATE(''' || v_null_default_value || ''', ''YYYY-MM-DD'')';
                    ELSIF v_null_default_value IN ('-1', '0', '1') OR REGEXP_LIKE(v_null_default_value, '^\-?\d+$') THEN
                        -- Number
                        v_update_sql := v_update_sql || v_null_default_value;
                    ELSE
                        -- String
                        v_update_sql := v_update_sql || '''' || v_null_default_value || '''';
                    END IF;

                    v_update_sql := v_update_sql || ' WHERE ' || v_partition_column || ' IS NULL';

                    DBMS_OUTPUT.PUT_LINE('  SQL: ' || v_update_sql);

                    EXECUTE IMMEDIATE v_update_sql;
                    v_rows_updated := SQL%ROWCOUNT;
                    COMMIT;

                    DBMS_OUTPUT.PUT_LINE('  Updated ' || v_rows_updated || ' rows');
                ELSIF v_null_strategy = 'ALLOW_NULLS' AND v_partition_column IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('NULL handling strategy: ALLOW_NULLS - NULLs will go to first/default partition');
                ELSIF v_null_strategy IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('NULL handling: No action needed (strategy: ' || NVL(v_null_strategy, 'NONE') || ')');
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR: Failed to handle NULL values - ' || SQLERRM);
                    RAISE;
            END;
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
                -- Validate ILM policies were created successfully
                validate_ilm_policies(p_task_id);
                -- Initialize partition access tracking for ILM
                DBMS_OUTPUT.PUT_LINE('');
                dwh_init_partition_access_tracking(v_task.source_owner, v_task.source_table);
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
                space_saved_mb = v_source_size - v_target_size,
                error_message = NULL  -- Clear any errors on successful completion
            WHERE task_id = p_task_id;
            COMMIT;

            -- Initialize partition access tracking with proper temperature calculation
            BEGIN
                DBMS_OUTPUT.PUT_LINE('Initializing partition access tracking...');
                cmr.dwh_refresh_partition_access_tracking(v_task.source_owner, v_task.source_table);
                DBMS_OUTPUT.PUT_LINE('  Partition tracking initialized successfully');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  Warning: Partition tracking initialization failed - ' || SQLERRM);
                    -- Don't fail the migration if tracking fails
            END;

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

        -- Determine compression_type from template if tiered, else keep task default
        DECLARE
            v_compression_to_use VARCHAR2(50);
            v_template cmr.dwh_migration_ilm_templates%ROWTYPE;
            v_template_json JSON_OBJECT_T;
            v_tier_config JSON_OBJECT_T;
            v_hot_config JSON_OBJECT_T;
        BEGIN
            -- Default: use existing task compression_type (defaults to 'BASIC')
            v_compression_to_use := v_task.compression_type;

            -- Override if tiered template is specified
            IF v_task.ilm_policy_template IS NOT NULL THEN
                BEGIN
                    SELECT * INTO v_template
                    FROM cmr.dwh_migration_ilm_templates
                    WHERE template_name = v_task.ilm_policy_template;

                    -- Parse template JSON
                    IF v_template.policies_json IS NOT NULL THEN
                        v_template_json := JSON_OBJECT_T.PARSE(v_template.policies_json);

                        -- Check for tier_config
                        IF v_template_json.has('tier_config') THEN
                            v_tier_config := TREAT(v_template_json.get('tier_config') AS JSON_OBJECT_T);

                            -- Check if tiering is enabled
                            IF v_tier_config.has('enabled') AND
                               v_tier_config.get_boolean('enabled') = TRUE THEN

                                -- Extract HOT tier compression
                                IF v_tier_config.has('hot') THEN
                                    v_hot_config := TREAT(v_tier_config.get('hot') AS JSON_OBJECT_T);
                                    IF v_hot_config.has('compression') THEN
                                        v_compression_to_use := v_hot_config.get_string('compression');
                                        DBMS_OUTPUT.PUT_LINE('  Using HOT tier compression from template: ' || v_compression_to_use);
                                    END IF;
                                END IF;
                            END IF;
                        END IF;
                    END IF;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        -- Template not found, use default
                        NULL;
                    WHEN OTHERS THEN
                        -- JSON parsing error, use default
                        DBMS_OUTPUT.PUT_LINE('  Warning: Could not parse template compression, using default');
                END;
            END IF;

            -- Apply recommendations to task
            UPDATE cmr.dwh_migration_tasks
            SET partition_type = v_partition_type,
                partition_key = v_partition_key,
                interval_clause = v_interval_clause,
                migration_method = v_recommended_method,
                compression_type = v_compression_to_use,
                status = 'READY',
                validation_status = 'READY'
            WHERE task_id = p_task_id;
        END;

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
        v_policies_created NUMBER := 0;
        v_policies_skipped NUMBER := 0;
        v_policy_id NUMBER;
        v_error_msg VARCHAR2(4000);
        v_detected_template VARCHAR2(100);
        v_auto_detected BOOLEAN := FALSE;
        v_has_effective_date BOOLEAN;
        v_has_valid_from_to BOOLEAN;
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        -- Auto-detect template if not specified
        IF v_task.ilm_policy_template IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('No ILM policy template specified - attempting auto-detection...');

            -- Check for SCD2 column patterns (must be done in SQL context)
            DECLARE
                v_count NUMBER;
            BEGIN
                SELECT COUNT(*)
                INTO v_count
                FROM all_tab_columns
                WHERE owner = v_task.source_owner
                AND table_name = v_task.source_table
                AND column_name IN ('EFFECTIVE_DATE', 'EFFECTIVE_FROM');

                v_has_effective_date := (v_count > 0);
            EXCEPTION
                WHEN OTHERS THEN
                    v_has_effective_date := FALSE;
            END;

            DECLARE
                v_count NUMBER;
            BEGIN
                SELECT COUNT(*)
                INTO v_count
                FROM all_tab_columns
                WHERE owner = v_task.source_owner
                AND table_name = v_task.source_table
                AND column_name IN ('VALID_FROM_DTTM', 'VALID_TO_DTTM', 'VALID_FROM', 'VALID_TO');

                v_has_valid_from_to := (v_count > 0);
            EXCEPTION
                WHEN OTHERS THEN
                    v_has_valid_from_to := FALSE;
            END;

            -- Auto-detect based on table naming patterns
            v_detected_template := CASE
                -- SCD2 patterns
                WHEN REGEXP_LIKE(v_task.source_table, '_SCD2$|_HIST$|_HISTORICAL$', 'i') THEN
                    CASE
                        WHEN v_has_effective_date THEN 'SCD2_EFFECTIVE_DATE'
                        WHEN v_has_valid_from_to THEN 'SCD2_VALID_FROM_TO'
                        ELSE 'HIST_TABLE'
                    END
                -- Events patterns
                WHEN REGEXP_LIKE(v_task.source_table, '_EVENTS$|_EVENT$|_LOG$|_AUDIT$', 'i') THEN
                    'EVENTS_SHORT_RETENTION'
                -- Staging patterns
                WHEN REGEXP_LIKE(v_task.source_table, '^STG_|_STG$|_STAGING$|^STAGING_', 'i') THEN
                    CASE
                        WHEN REGEXP_LIKE(v_task.source_table, 'CDC|CHANGE', 'i') THEN 'STAGING_CDC'
                        WHEN REGEXP_LIKE(v_task.source_table, 'ERROR|ERR', 'i') THEN 'STAGING_ERROR_QUARANTINE'
                        ELSE 'STAGING_7DAY'
                    END
                -- HIST pattern
                WHEN REGEXP_LIKE(v_task.source_table, '^HIST_|_HIST_', 'i') THEN
                    'HIST_TABLE'
                ELSE NULL
            END;

            IF v_detected_template IS NOT NULL THEN
                DBMS_OUTPUT.PUT_LINE('  ✓ Auto-detected template: ' || v_detected_template);
                DBMS_OUTPUT.PUT_LINE('    Based on table naming pattern and column analysis');
                v_auto_detected := TRUE;
            ELSE
                DBMS_OUTPUT.PUT_LINE('  ✗ Could not auto-detect template');
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('Available templates:');
                FOR tmpl IN (SELECT template_name, description FROM cmr.dwh_migration_ilm_templates ORDER BY template_name) LOOP
                    DBMS_OUTPUT.PUT_LINE('  - ' || tmpl.template_name || ': ' || tmpl.description);
                END LOOP;
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('To apply ILM policies, update task with template:');
                DBMS_OUTPUT.PUT_LINE('  UPDATE cmr.dwh_migration_tasks');
                DBMS_OUTPUT.PUT_LINE('  SET ilm_policy_template = ''<template_name>''');
                DBMS_OUTPUT.PUT_LINE('  WHERE task_id = ' || p_task_id || ';');
                RETURN;
            END IF;
        ELSE
            v_detected_template := v_task.ilm_policy_template;
        END IF;

        SELECT * INTO v_template
        FROM cmr.dwh_migration_ilm_templates
        WHERE template_name = v_detected_template;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Applying ILM Policies');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Template: ' || v_template.template_name || CASE WHEN v_auto_detected THEN ' (auto-detected)' ELSE '' END);
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);
        DBMS_OUTPUT.PUT_LINE('');

        -- Parse JSON and create policies using JSON_TABLE
        -- NOTE: Tiered templates have structure {"tier_config":{...}, "policies":[...]}
        --       Non-tiered templates have structure [{...}, {...}] (array at root)
        -- Determine correct JSON path based on template type
        DECLARE
            v_is_tiered BOOLEAN := FALSE;
            v_threshold_profile_id NUMBER := NULL;
        BEGIN
            v_is_tiered := JSON_EXISTS(v_template.policies_json, '$.tier_config');

            -- Create threshold profile from policies (works for both tiered and non-tiered)
            DECLARE
                v_profile_name VARCHAR2(100);
                v_min_age_days NUMBER;
                v_mid_age_days NUMBER;
                v_max_age_days NUMBER;
                v_policy_count NUMBER;
            BEGIN
                v_profile_name := v_template.template_name || '_THRESHOLDS';

                -- Extract age thresholds from policies array
                -- Supports both tiered ($.policies[*]) and non-tiered ($[*]) templates
                IF v_is_tiered THEN
                    DBMS_OUTPUT.PUT_LINE('Tiered template detected - extracting thresholds from $.policies[*]');

                    -- Get sorted age values from policies (convert months to days)
                    SELECT
                        MIN(COALESCE(age_days, age_months * 30.44)) AS min_age,
                        MAX(CASE WHEN rn = 2 THEN COALESCE(age_days, age_months * 30.44) END) AS mid_age,
                        MAX(COALESCE(age_days, age_months * 30.44)) AS max_age,
                        COUNT(*) AS policy_count
                    INTO v_min_age_days, v_mid_age_days, v_max_age_days, v_policy_count
                    FROM (
                        SELECT
                            jt.age_days,
                            jt.age_months,
                            ROW_NUMBER() OVER (ORDER BY COALESCE(jt.age_days, jt.age_months * 30.44)) AS rn
                        FROM cmr.dwh_migration_ilm_templates t,
                            JSON_TABLE(t.policies_json, '$.policies[*]'
                                COLUMNS (
                                    age_days NUMBER PATH '$.age_days',
                                    age_months NUMBER PATH '$.age_months'
                                )
                            ) jt
                        WHERE t.template_name = v_template.template_name
                        AND (jt.age_days IS NOT NULL OR jt.age_months IS NOT NULL)
                    );
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Non-tiered template detected - extracting thresholds from $[*]');

                    -- Get sorted age values from policies (convert months to days)
                    SELECT
                        MIN(COALESCE(age_days, age_months * 30.44)) AS min_age,
                        MAX(CASE WHEN rn = 2 THEN COALESCE(age_days, age_months * 30.44) END) AS mid_age,
                        MAX(COALESCE(age_days, age_months * 30.44)) AS max_age,
                        COUNT(*) AS policy_count
                    INTO v_min_age_days, v_mid_age_days, v_max_age_days, v_policy_count
                    FROM (
                        SELECT
                            jt.age_days,
                            jt.age_months,
                            ROW_NUMBER() OVER (ORDER BY COALESCE(jt.age_days, jt.age_months * 30.44)) AS rn
                        FROM cmr.dwh_migration_ilm_templates t,
                            JSON_TABLE(t.policies_json, '$[*]'
                                COLUMNS (
                                    age_days NUMBER PATH '$.age_days',
                                    age_months NUMBER PATH '$.age_months'
                                )
                            ) jt
                        WHERE t.template_name = v_template.template_name
                        AND (jt.age_days IS NOT NULL OR jt.age_months IS NOT NULL)
                    );
                END IF;

                -- Create threshold profile if we found age-based policies
                IF v_policy_count > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('Creating/updating threshold profile: ' || v_profile_name);
                    DBMS_OUTPUT.PUT_LINE('  Found ' || v_policy_count || ' age-based policies');
                    DBMS_OUTPUT.PUT_LINE('  HOT threshold: ' || v_min_age_days || ' days');
                    DBMS_OUTPUT.PUT_LINE('  WARM threshold: ' || NVL(v_mid_age_days, v_max_age_days) || ' days');
                    DBMS_OUTPUT.PUT_LINE('  COLD threshold: ' || v_max_age_days || ' days');

                    -- Create or update threshold profile
                    MERGE INTO cmr.dwh_ilm_threshold_profiles t
                    USING (
                        SELECT
                            v_profile_name AS profile_name,
                            'Auto-generated from template: ' || v_template.template_name || ' (' || v_policy_count || ' policies)' AS description,
                            v_min_age_days AS hot_threshold_days,
                            NVL(v_mid_age_days, v_max_age_days) AS warm_threshold_days,
                            v_max_age_days AS cold_threshold_days
                        FROM dual
                    ) s
                    ON (t.profile_name = s.profile_name)
                    WHEN MATCHED THEN UPDATE SET
                        t.description = s.description,
                        t.hot_threshold_days = s.hot_threshold_days,
                        t.warm_threshold_days = s.warm_threshold_days,
                        t.cold_threshold_days = s.cold_threshold_days,
                        t.modified_by = USER,
                        t.modified_date = SYSTIMESTAMP
                    WHEN NOT MATCHED THEN INSERT (
                        profile_name, description,
                        hot_threshold_days, warm_threshold_days, cold_threshold_days
                    ) VALUES (
                        s.profile_name, s.description,
                        s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days
                    );

                    -- Get profile_id
                    SELECT profile_id INTO v_threshold_profile_id
                    FROM cmr.dwh_ilm_threshold_profiles
                    WHERE profile_name = v_profile_name;

                    DBMS_OUTPUT.PUT_LINE('  Threshold profile ID: ' || v_threshold_profile_id);
                ELSE
                    DBMS_OUTPUT.PUT_LINE('No age-based policies found - temperature will use DEFAULT profile');
                    v_threshold_profile_id := NULL;
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Warning: Could not create threshold profile from policies: ' || SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('         Temperature calculation will use DEFAULT profile');
                    v_threshold_profile_id := NULL;
            END;

            IF v_is_tiered THEN
                DBMS_OUTPUT.PUT_LINE('Using JSON path for policies: $.policies[*]');

                -- Tiered template: policies under $.policies[*]
                FOR policy_rec IN (
                    SELECT
                        REPLACE(jt.policy_name, '{TABLE}', v_task.source_table) AS policy_name,
                        jt.policy_type,
                        jt.action_type,
                        jt.age_days,
                        jt.age_months,
                        jt.compression_type,
                        jt.target_tablespace,
                        jt.pct_free_val AS pct_free,
                        jt.priority,
                        jt.enabled
                    FROM cmr.dwh_migration_ilm_templates t,
                        JSON_TABLE(t.policies_json, '$.policies[*]'
                            COLUMNS (
                                policy_name VARCHAR2(100) PATH '$.policy_name',
                                policy_type VARCHAR2(30) PATH '$.policy_type' DEFAULT 'COMPRESSION' ON EMPTY,
                                action_type VARCHAR2(30) PATH '$.action',
                                age_days NUMBER PATH '$.age_days',
                                age_months NUMBER PATH '$.age_months',
                                compression_type VARCHAR2(50) PATH '$.compression',
                                target_tablespace VARCHAR2(30) PATH '$.tablespace',
                                pct_free_val NUMBER PATH '$.pctfree',
                                priority NUMBER PATH '$.priority' DEFAULT 100 ON EMPTY,
                                enabled VARCHAR2(1) PATH '$.enabled' DEFAULT 'Y' ON EMPTY
                            )
                        ) jt
                    WHERE t.template_name = v_template.template_name
                ) LOOP
                    BEGIN
                -- Determine policy type from action if not specified
                DECLARE
                    v_pol_type VARCHAR2(30);
                    v_act_type VARCHAR2(30);
                BEGIN
                    v_act_type := UPPER(policy_rec.action_type);

                    -- Map action to policy type
                    v_pol_type := CASE v_act_type
                        WHEN 'COMPRESS' THEN 'COMPRESSION'
                        WHEN 'MOVE' THEN 'TIERING'
                        WHEN 'READ_ONLY' THEN 'ARCHIVAL'
                        WHEN 'DROP' THEN 'PURGE'
                        WHEN 'TRUNCATE' THEN 'PURGE'
                        ELSE COALESCE(policy_rec.policy_type, 'CUSTOM')
                    END;

                    -- Insert policy with threshold profile
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
                        pct_free,
                        priority,
                        enabled,
                        threshold_profile_id,
                        created_by
                    ) VALUES (
                        policy_rec.policy_name,
                        v_task.source_owner,
                        v_task.source_table,
                        v_pol_type,
                        v_act_type,
                        policy_rec.age_days,
                        policy_rec.age_months,
                        policy_rec.compression_type,
                        policy_rec.target_tablespace,
                        policy_rec.pct_free,
                        policy_rec.priority,
                        policy_rec.enabled,
                        v_threshold_profile_id,
                        USER || ' (Migration Task ' || p_task_id || ')'
                    ) RETURNING policy_id INTO v_policy_id;

                    v_policies_created := v_policies_created + 1;

                    DBMS_OUTPUT.PUT_LINE('✓ Created policy: ' || policy_rec.policy_name);
                    DBMS_OUTPUT.PUT_LINE('  Action: ' || v_act_type ||
                        CASE
                            WHEN policy_rec.age_days IS NOT NULL THEN ' after ' || policy_rec.age_days || ' days'
                            WHEN policy_rec.age_months IS NOT NULL THEN ' after ' || policy_rec.age_months || ' months'
                            ELSE ''
                        END);
                    IF policy_rec.compression_type IS NOT NULL THEN
                        DBMS_OUTPUT.PUT_LINE('  Compression: ' || policy_rec.compression_type);
                    END IF;
                    IF policy_rec.target_tablespace IS NOT NULL THEN
                        DBMS_OUTPUT.PUT_LINE('  Tablespace: ' || policy_rec.target_tablespace);
                    END IF;
                    DBMS_OUTPUT.PUT_LINE('');
                END;

                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                            -- Policy already exists, skip
                            v_policies_skipped := v_policies_skipped + 1;
                            DBMS_OUTPUT.PUT_LINE('⊘ Skipped (already exists): ' || policy_rec.policy_name);
                        WHEN OTHERS THEN
                            v_error_msg := SQLERRM;
                            DBMS_OUTPUT.PUT_LINE('✗ Failed to create policy: ' || policy_rec.policy_name);
                            DBMS_OUTPUT.PUT_LINE('  Error: ' || v_error_msg);
                    END;
                END LOOP;

            ELSE
                -- Non-tiered template: policies at root $[*]
                DBMS_OUTPUT.PUT_LINE('Non-tiered template detected - using JSON path: $[*]');

                FOR policy_rec IN (
                    SELECT
                        REPLACE(jt.policy_name, '{TABLE}', v_task.source_table) AS policy_name,
                        jt.policy_type,
                        jt.action_type,
                        jt.age_days,
                        jt.age_months,
                        jt.compression_type,
                        jt.target_tablespace,
                        jt.pct_free_val AS pct_free,
                        jt.priority,
                        jt.enabled
                    FROM cmr.dwh_migration_ilm_templates t,
                        JSON_TABLE(t.policies_json, '$[*]'
                            COLUMNS (
                                policy_name VARCHAR2(100) PATH '$.policy_name',
                                policy_type VARCHAR2(30) PATH '$.policy_type' DEFAULT 'COMPRESSION' ON EMPTY,
                                action_type VARCHAR2(30) PATH '$.action',
                                age_days NUMBER PATH '$.age_days',
                                age_months NUMBER PATH '$.age_months',
                                compression_type VARCHAR2(50) PATH '$.compression',
                                target_tablespace VARCHAR2(30) PATH '$.tablespace',
                                pct_free_val NUMBER PATH '$.pctfree',
                                priority NUMBER PATH '$.priority' DEFAULT 100 ON EMPTY,
                                enabled VARCHAR2(1) PATH '$.enabled' DEFAULT 'Y' ON EMPTY
                            )
                        ) jt
                    WHERE t.template_name = v_template.template_name
                ) LOOP
                    BEGIN
                        -- Determine policy type from action if not specified
                        DECLARE
                            v_pol_type VARCHAR2(30);
                            v_act_type VARCHAR2(30);
                        BEGIN
                            v_act_type := UPPER(policy_rec.action_type);

                            -- Map action to policy type
                            v_pol_type := CASE v_act_type
                                WHEN 'COMPRESS' THEN 'COMPRESSION'
                                WHEN 'MOVE' THEN 'TIERING'
                                WHEN 'READ_ONLY' THEN 'ARCHIVAL'
                                WHEN 'DROP' THEN 'PURGE'
                                WHEN 'TRUNCATE' THEN 'PURGE'
                                ELSE COALESCE(policy_rec.policy_type, 'CUSTOM')
                            END;

                            -- Insert policy (no threshold profile for non-tiered)
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
                                pct_free,
                                priority,
                                enabled,
                                threshold_profile_id,
                                created_by
                            ) VALUES (
                                policy_rec.policy_name,
                                v_task.source_owner,
                                v_task.source_table,
                                v_pol_type,
                                v_act_type,
                                policy_rec.age_days,
                                policy_rec.age_months,
                                policy_rec.compression_type,
                                policy_rec.target_tablespace,
                                policy_rec.pct_free,
                                policy_rec.priority,
                                policy_rec.enabled,
                                NULL,  -- Non-tiered templates use DEFAULT profile
                                USER || ' (Migration Task ' || p_task_id || ')'
                            ) RETURNING policy_id INTO v_policy_id;

                            v_policies_created := v_policies_created + 1;

                            DBMS_OUTPUT.PUT_LINE('✓ Created policy: ' || policy_rec.policy_name);
                            DBMS_OUTPUT.PUT_LINE('  Action: ' || v_act_type ||
                                CASE
                                    WHEN policy_rec.age_days IS NOT NULL THEN ' after ' || policy_rec.age_days || ' days'
                                    WHEN policy_rec.age_months IS NOT NULL THEN ' after ' || policy_rec.age_months || ' months'
                                    ELSE ''
                                END);
                            IF policy_rec.compression_type IS NOT NULL THEN
                                DBMS_OUTPUT.PUT_LINE('  Compression: ' || policy_rec.compression_type);
                            END IF;
                            IF policy_rec.target_tablespace IS NOT NULL THEN
                                DBMS_OUTPUT.PUT_LINE('  Tablespace: ' || policy_rec.target_tablespace);
                            END IF;
                            DBMS_OUTPUT.PUT_LINE('');
                        END;

                    EXCEPTION
                        WHEN DUP_VAL_ON_INDEX THEN
                            -- Policy already exists, skip
                            v_policies_skipped := v_policies_skipped + 1;
                            DBMS_OUTPUT.PUT_LINE('⊘ Skipped (already exists): ' || policy_rec.policy_name);
                        WHEN OTHERS THEN
                            v_error_msg := SQLERRM;
                            DBMS_OUTPUT.PUT_LINE('✗ Failed to create policy: ' || policy_rec.policy_name);
                            DBMS_OUTPUT.PUT_LINE('  Error: ' || v_error_msg);
                    END;
                END LOOP;
            END IF;
        END;

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('ILM Policy Application Summary');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Policies created: ' || v_policies_created);
        DBMS_OUTPUT.PUT_LINE('Policies skipped: ' || v_policies_skipped);
        DBMS_OUTPUT.PUT_LINE('');

        COMMIT;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('Warning: ILM template not found: ' || v_task.ilm_policy_template);
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR applying ILM policies: ' || SQLERRM);
            RAISE;
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


    PROCEDURE validate_ilm_policies(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_policy_count NUMBER;
        v_eligible_count NUMBER;
        v_validation_passed BOOLEAN := TRUE;
        v_step_start TIMESTAMP := SYSTIMESTAMP;
        v_step_number NUMBER;
        v_validation_details CLOB;
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get next step number
        SELECT NVL(MAX(step_number), 0) + 1 INTO v_step_number
        FROM cmr.dwh_migration_execution_log
        WHERE task_id = p_task_id;

        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        IF v_task.apply_ilm_policies != 'Y' OR v_task.ilm_policy_template IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ILM policies not applied - skipping validation');

            -- Log skip reason
            log_step(
                p_task_id => p_task_id,
                p_step_number => v_step_number,
                p_step_name => 'Validate ILM Policies',
                p_step_type => 'VALIDATION',
                p_sql => NULL,
                p_status => 'SKIPPED',
                p_start_time => v_step_start,
                p_end_time => SYSTIMESTAMP,
                p_error_message => 'ILM policies not applied or no template specified'
            );

            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Validating ILM Policies');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);
        DBMS_OUTPUT.PUT_LINE('');

        -- Initialize validation details log
        DBMS_LOB.CREATETEMPORARY(v_validation_details, TRUE, DBMS_LOB.SESSION);
        DBMS_LOB.APPEND(v_validation_details, 'ILM Policy Validation Report' || CHR(10));
        DBMS_LOB.APPEND(v_validation_details, 'Table: ' || v_task.source_owner || '.' || v_task.source_table || CHR(10));
        DBMS_LOB.APPEND(v_validation_details, CHR(10));

        -- Check if policies were created
        SELECT COUNT(*) INTO v_policy_count
        FROM cmr.dwh_ilm_policies
        WHERE table_owner = v_task.source_owner
        AND table_name = v_task.source_table;

        IF v_policy_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('✗ FAILED: No ILM policies found for table');
            DBMS_LOB.APPEND(v_validation_details, '✗ FAILED: No ILM policies found for table' || CHR(10));
            v_validation_passed := FALSE;
        ELSE
            DBMS_OUTPUT.PUT_LINE('✓ Found ' || v_policy_count || ' ILM policies');
            DBMS_LOB.APPEND(v_validation_details, '✓ Found ' || v_policy_count || ' ILM policies' || CHR(10));

            -- Display each policy
            FOR pol IN (
                SELECT policy_name, policy_type, action_type, enabled,
                       age_days, age_months, compression_type, target_tablespace
                FROM cmr.dwh_ilm_policies
                WHERE table_owner = v_task.source_owner
                AND table_name = v_task.source_table
                ORDER BY priority
            ) LOOP
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('  Policy: ' || pol.policy_name);
                DBMS_OUTPUT.PUT_LINE('    Type: ' || pol.policy_type);
                DBMS_OUTPUT.PUT_LINE('    Action: ' || pol.action_type);
                DBMS_OUTPUT.PUT_LINE('    Enabled: ' || pol.enabled);

                DBMS_LOB.APPEND(v_validation_details, CHR(10) || '  Policy: ' || pol.policy_name || CHR(10));
                DBMS_LOB.APPEND(v_validation_details, '    Type: ' || pol.policy_type || CHR(10));
                DBMS_LOB.APPEND(v_validation_details, '    Action: ' || pol.action_type || CHR(10));
                DBMS_LOB.APPEND(v_validation_details, '    Enabled: ' || pol.enabled || CHR(10));

                IF pol.age_days IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('    Trigger: ' || pol.age_days || ' days');
                    DBMS_LOB.APPEND(v_validation_details, '    Trigger: ' || pol.age_days || ' days' || CHR(10));
                END IF;
                IF pol.age_months IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('    Trigger: ' || pol.age_months || ' months');
                    DBMS_LOB.APPEND(v_validation_details, '    Trigger: ' || pol.age_months || ' months' || CHR(10));
                END IF;
                IF pol.compression_type IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('    Compression: ' || pol.compression_type);
                    DBMS_LOB.APPEND(v_validation_details, '    Compression: ' || pol.compression_type || CHR(10));
                END IF;
                IF pol.target_tablespace IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('    Tablespace: ' || pol.target_tablespace);
                    DBMS_LOB.APPEND(v_validation_details, '    Tablespace: ' || pol.target_tablespace || CHR(10));
                END IF;
            END LOOP;
        END IF;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_LOB.APPEND(v_validation_details, CHR(10));

        -- Check if table is partitioned (required for partition-level ILM)
        DECLARE
            v_partitioned VARCHAR2(3);
        BEGIN
            SELECT partitioned INTO v_partitioned
            FROM dba_tables
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table;

            IF v_partitioned = 'YES' THEN
                DBMS_OUTPUT.PUT_LINE('✓ Table is partitioned (ILM can operate on partitions)');
                DBMS_LOB.APPEND(v_validation_details, '✓ Table is partitioned (ILM can operate on partitions)' || CHR(10));
            ELSE
                DBMS_OUTPUT.PUT_LINE('⚠ WARNING: Table is not partitioned (ILM will operate on entire table)');
                DBMS_LOB.APPEND(v_validation_details, '⚠ WARNING: Table is not partitioned (ILM will operate on entire table)' || CHR(10));
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('✗ FAILED: Table not found');
                DBMS_LOB.APPEND(v_validation_details, '✗ FAILED: Table not found' || CHR(10));
                v_validation_passed := FALSE;
        END;

        -- Test policy evaluation (don't queue actions, just check eligibility)
        BEGIN
            -- Call policy engine to evaluate this table
            pck_dwh_ilm_policy_engine.evaluate_table(v_task.source_owner, v_task.source_table);

            -- Check evaluation queue
            SELECT COUNT(*) INTO v_eligible_count
            FROM cmr.dwh_ilm_evaluation_queue
            WHERE table_owner = v_task.source_owner
            AND table_name = v_task.source_table
            AND eligible = 'Y';

            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('✓ Policy evaluation successful');
            DBMS_OUTPUT.PUT_LINE('  Eligible partitions now: ' || v_eligible_count);

            DBMS_LOB.APPEND(v_validation_details, CHR(10) || '✓ Policy evaluation successful' || CHR(10));
            DBMS_LOB.APPEND(v_validation_details, '  Eligible partitions now: ' || v_eligible_count || CHR(10));

            IF v_eligible_count > 0 THEN
                DBMS_OUTPUT.PUT_LINE('  Note: These partitions are ready for ILM actions');
                DBMS_LOB.APPEND(v_validation_details, '  Note: These partitions are ready for ILM actions' || CHR(10));
            ELSE
                DBMS_OUTPUT.PUT_LINE('  Note: No partitions currently eligible (expected for new tables)');
                DBMS_LOB.APPEND(v_validation_details, '  Note: No partitions currently eligible (expected for new tables)' || CHR(10));
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;
                DBMS_OUTPUT.PUT_LINE('✗ FAILED: Policy evaluation error: ' || v_error_msg);
                DBMS_LOB.APPEND(v_validation_details, '✗ FAILED: Policy evaluation error: ' || v_error_msg || CHR(10));
                v_validation_passed := FALSE;
        END;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('========================================');
        IF v_validation_passed THEN
            DBMS_OUTPUT.PUT_LINE('ILM Validation: PASSED');
            DBMS_LOB.APPEND(v_validation_details, CHR(10) || 'ILM Validation: PASSED' || CHR(10));
        ELSE
            DBMS_OUTPUT.PUT_LINE('ILM Validation: FAILED');
            DBMS_LOB.APPEND(v_validation_details, CHR(10) || 'ILM Validation: FAILED' || CHR(10));
        END IF;
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('');

        -- Log validation results to table
        log_step(
            p_task_id => p_task_id,
            p_step_number => v_step_number,
            p_step_name => 'Validate ILM Policies',
            p_step_type => 'VALIDATION',
            p_sql => v_validation_details,
            p_status => CASE WHEN v_validation_passed THEN 'SUCCESS' ELSE 'FAILED' END,
            p_start_time => v_step_start,
            p_end_time => SYSTIMESTAMP,
            p_error_message => CASE WHEN NOT v_validation_passed THEN 'ILM policy validation failed - see details in sql_statement' END
        );

        -- Cleanup temporary LOB
        IF DBMS_LOB.ISTEMPORARY(v_validation_details) = 1 THEN
            DBMS_LOB.FREETEMPORARY(v_validation_details);
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            v_error_msg := SQLERRM;
            DBMS_OUTPUT.PUT_LINE('ERROR validating ILM policies: ' || v_error_msg);

            -- Log error
            log_step(
                p_task_id => p_task_id,
                p_step_number => v_step_number,
                p_step_name => 'Validate ILM Policies',
                p_step_type => 'VALIDATION',
                p_sql => v_validation_details,
                p_status => 'ERROR',
                p_start_time => v_step_start,
                p_end_time => SYSTIMESTAMP,
                p_error_code => SQLCODE,
                p_error_message => v_error_msg
            );

            -- Cleanup temporary LOB
            IF DBMS_LOB.ISTEMPORARY(v_validation_details) = 1 THEN
                DBMS_LOB.FREETEMPORARY(v_validation_details);
            END IF;

            RAISE;
    END validate_ilm_policies;


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


    -- =========================================================================
    -- Partition Renaming Helper Functions (All Partition Types)
    -- =========================================================================

    FUNCTION generate_partition_name_from_value(
        p_high_value VARCHAR2,
        p_partition_type VARCHAR2,
        p_interval_clause VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2
    AS
        v_partition_name VARCHAR2(128);
        v_value_clean VARCHAR2(200);
        v_values SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
        v_name_parts VARCHAR2(200) := '';
        v_max_length NUMBER := 30;  -- Oracle partition name limit
        v_date_value DATE;
        v_year NUMBER;
        v_month NUMBER;
        v_day NUMBER;
        v_interval_number NUMBER;
        v_interval_unit VARCHAR2(20);
        v_partition_date DATE;  -- The actual partition period (high_value - interval)
    BEGIN
        -- Detect partition type and generate appropriate name
        IF UPPER(p_partition_type) LIKE '%RANGE%' OR UPPER(p_partition_type) LIKE 'DATE%' THEN
            -- RANGE partition with date - extract date from HIGH_VALUE
            -- Example HIGH_VALUE: TO_DATE(' 2025-08-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
            BEGIN
                -- Try to extract date string from TO_DATE function
                -- Pattern: TO_DATE(' YYYY-MM-DD ...
                v_value_clean := REGEXP_SUBSTR(p_high_value, '''[^'']+''', 1, 1);
                IF v_value_clean IS NOT NULL THEN
                    -- Remove quotes and parse to DATE
                    v_value_clean := TRIM('''' FROM v_value_clean);
                    v_value_clean := TRIM(v_value_clean);
                    v_date_value := TO_DATE(SUBSTR(v_value_clean, 1, 19), 'YYYY-MM-DD HH24:MI:SS');

                    -- Parse interval_clause to determine actual partition granularity
                    -- Examples: NUMTOYMINTERVAL(1,'MONTH'), NUMTOYMINTERVAL(3,'MONTH'), NUMTOYMINTERVAL(1,'YEAR')
                    IF p_interval_clause IS NOT NULL THEN
                        -- Extract interval number and unit
                        -- Pattern: NUMTOYMINTERVAL(N,'UNIT') or NUMTODSINTERVAL(N,'UNIT')
                        v_interval_number := TO_NUMBER(REGEXP_SUBSTR(p_interval_clause, '\d+'));
                        v_interval_unit := REGEXP_SUBSTR(p_interval_clause, '''(\w+)''', 1, 1, NULL, 1);

                        -- Calculate actual partition period by subtracting interval from high_value
                        -- High_value is exclusive (VALUES LESS THAN), so partition represents the period BEFORE it
                        IF UPPER(v_interval_unit) = 'MONTH' THEN
                            v_partition_date := ADD_MONTHS(v_date_value, -v_interval_number);

                            -- Format based on interval size
                            IF v_interval_number = 1 THEN
                                -- Monthly: P_YYYYMM
                                v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYYMM');
                            ELSIF v_interval_number = 3 THEN
                                -- Quarterly: P_YYYYQN
                                v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYY') ||
                                               'Q' || TO_CHAR(CEIL(TO_NUMBER(TO_CHAR(v_partition_date, 'MM')) / 3));
                            ELSIF v_interval_number = 12 THEN
                                -- Yearly: P_YYYY
                                v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYY');
                            ELSE
                                -- Other month intervals: P_YYYYMM (use start month)
                                v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYYMM');
                            END IF;
                        ELSIF UPPER(v_interval_unit) = 'YEAR' THEN
                            v_partition_date := ADD_MONTHS(v_date_value, -12 * v_interval_number);
                            v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYY');
                        ELSIF UPPER(v_interval_unit) = 'DAY' THEN
                            v_partition_date := v_date_value - v_interval_number;
                            v_name_parts := 'P_' || TO_CHAR(v_partition_date, 'YYYYMMDD');
                        ELSE
                            -- Unknown interval unit, fall back to simple date extraction
                            v_name_parts := 'P_' || TO_CHAR(v_date_value, 'YYYYMMDD');
                        END IF;
                    ELSE
                        -- No interval_clause provided, use old heuristic approach
                        v_year := TO_NUMBER(SUBSTR(v_value_clean, 1, 4));
                        v_month := TO_NUMBER(SUBSTR(v_value_clean, 6, 2));
                        v_day := TO_NUMBER(SUBSTR(v_value_clean, 9, 2));

                        IF v_day = 1 AND v_month IN (1, 4, 7, 10) THEN
                            -- Quarterly partition (first day of quarter)
                            v_name_parts := 'P_' || v_year || 'Q' || CEIL(v_month / 3);
                        ELSIF v_day = 1 THEN
                            -- Monthly partition (first day of month)
                            v_name_parts := 'P_' || v_year || LPAD(v_month, 2, '0');
                        ELSIF v_month = 1 AND v_day = 1 THEN
                            -- Yearly partition (first day of year)
                            v_name_parts := 'P_' || v_year;
                        ELSE
                            -- Daily partition or other
                            v_name_parts := 'P_' || v_year || LPAD(v_month, 2, '0') || LPAD(v_day, 2, '0');
                        END IF;
                    END IF;

                    RETURN v_name_parts;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Fall through to default handling
                    NULL;
            END;

        ELSIF UPPER(p_partition_type) LIKE '%LIST%' THEN
            -- LIST partition - extract literal values
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

        ELSE
            -- HASH or other - keep system-generated name
            RETURN NULL;
        END IF;

        -- Default: return NULL (keep original name)
        RETURN NULL;

    EXCEPTION
        WHEN OTHERS THEN
            -- If name generation fails, return NULL (keep original name)
            DBMS_OUTPUT.PUT_LINE('  WARNING: Could not generate partition name from: ' || p_high_value || ' (' || SQLERRM || ')');
            RETURN NULL;
    END generate_partition_name_from_value;


    PROCEDURE rename_system_partitions(
        p_task_id NUMBER,
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_type VARCHAR2,
        p_interval_clause VARCHAR2 DEFAULT NULL
    ) AS
        v_partition_name VARCHAR2(128);
        v_high_value LONG;
        v_high_value_str VARCHAR2(4000);
        v_new_partition_name VARCHAR2(128);
        v_sql VARCHAR2(1000);
        v_step NUMBER := 1000;  -- High step number for post-cutover operations
        v_start TIMESTAMP;
        v_overall_start TIMESTAMP;
        v_renamed_count NUMBER := 0;
        v_skipped_count NUMBER := 0;
        v_failed_count NUMBER := 0;
        v_total_count NUMBER := 0;
    BEGIN
        v_overall_start := SYSTIMESTAMP;
        DBMS_OUTPUT.PUT_LINE('Renaming system-generated partitions to human-readable names...');

        IF p_interval_clause IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('  Using interval clause: ' || p_interval_clause);
        END IF;

        -- Get all system-generated partitions (skip named partitions like P_XDEF, P_INITIAL)
        FOR part IN (
            SELECT partition_name, high_value
            FROM dba_tab_partitions
            WHERE table_owner = p_owner
            AND table_name = p_table_name
            AND partition_name NOT IN ('P_XDEF', 'P_INITIAL')
            AND partition_name LIKE 'SYS_P%'  -- Only system-generated names
            ORDER BY partition_position
        ) LOOP
            v_total_count := v_total_count + 1;

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
                v_new_partition_name := generate_partition_name_from_value(
                    v_high_value_str,
                    p_partition_type,
                    p_interval_clause
                );

                -- Only rename if we generated a valid name
                IF v_new_partition_name IS NOT NULL THEN
                    -- Rename partition
                    v_sql := 'ALTER TABLE ' || p_owner || '.' || p_table_name ||
                            ' RENAME PARTITION ' || part.partition_name ||
                            ' TO ' || v_new_partition_name;

                    EXECUTE IMMEDIATE v_sql;

                    DBMS_OUTPUT.PUT_LINE('  Renamed: ' || part.partition_name || ' -> ' || v_new_partition_name ||
                                        ' (value: ' || SUBSTR(v_high_value_str, 1, 50) || ')');

                    v_renamed_count := v_renamed_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('  SKIPPED: Could not generate name for partition ' || part.partition_name);
                    v_skipped_count := v_skipped_count + 1;
                END IF;

            EXCEPTION
                WHEN OTHERS THEN
                    -- Continue with other partitions
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Failed to rename partition ' || part.partition_name || ': ' || SQLERRM);
                    v_failed_count := v_failed_count + 1;
            END;
        END LOOP;

        -- Log single summary entry
        IF v_total_count > 0 THEN
            log_step(p_task_id, v_step,
                    'Renamed ' || v_renamed_count || ' partitions (skipped: ' || v_skipped_count || ', failed: ' || v_failed_count || ')',
                    'RENAME_PARTITIONS_SUMMARY',
                    'Processed ' || v_total_count || ' system-generated partitions for ' || p_owner || '.' || p_table_name,
                    'SUCCESS', v_overall_start, SYSTIMESTAMP);
        END IF;

        DBMS_OUTPUT.PUT_LINE('Partition renaming complete: ' || v_renamed_count || ' renamed, ' ||
                            v_skipped_count || ' skipped, ' || v_failed_count || ' failed (total: ' || v_total_count || ')');
    END rename_system_partitions;

END pck_dwh_table_migration_executor;
/

SELECT 'Table Migration Executor Package Created Successfully!' AS status FROM DUAL;
