-- =============================================================================
-- Table Migration Execution Package
-- Executes table migrations from non-partitioned to partitioned
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_table_migration_executor AS
    -- Main execution procedures
    PROCEDURE execute_migration(
        p_task_id NUMBER
    );

    PROCEDURE execute_all_ready_tasks(
        p_project_id NUMBER DEFAULT NULL,
        p_max_tasks NUMBER DEFAULT NULL
    );

    -- Migration methods
    PROCEDURE migrate_using_ctas(
        p_task_id NUMBER
    );

    PROCEDURE migrate_using_online_redef(
        p_task_id NUMBER
    );

    PROCEDURE migrate_using_exchange(
        p_task_id NUMBER
    );

    -- Post-migration tasks
    PROCEDURE apply_dwh_ilm_policies(
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
    BEGIN
        -- Check if date conversion is required
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

        IF p_task.interval_clause IS NOT NULL THEN
            p_ddl := p_ddl || 'INTERVAL (' || p_task.interval_clause || ')' || CHR(10);
        END IF;

        p_ddl := p_ddl || '(PARTITION p_initial VALUES LESS THAN (MAXVALUE))';
        p_ddl := p_ddl || v_storage_clause;

    END build_partition_ddl;


    PROCEDURE recreate_indexes(
        p_task_id NUMBER,
        p_source_owner VARCHAR2,
        p_source_table VARCHAR2,
        p_target_table VARCHAR2,
        p_step_offset NUMBER
    ) AS
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_step NUMBER := p_step_offset;
    BEGIN
        FOR idx IN (
            SELECT index_name, uniqueness, index_type, tablespace_name
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

            -- Get index columns
            DECLARE
                v_columns VARCHAR2(4000);
            BEGIN
                SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position)
                INTO v_columns
                FROM dba_ind_columns
                WHERE index_owner = p_source_owner
                AND index_name = idx.index_name;

                v_sql := 'CREATE ';
                IF idx.uniqueness = 'UNIQUE' THEN
                    v_sql := v_sql || 'UNIQUE ';
                END IF;

                v_sql := v_sql || 'INDEX ' || p_source_owner || '.' || idx.index_name ||
                        ' ON ' || p_source_owner || '.' || p_target_table ||
                        '(' || v_columns || ') LOCAL PARALLEL ' ||
                        get_ilm_config('MIGRATION_PARALLEL_DEGREE');

                EXECUTE IMMEDIATE v_sql;

                log_step(p_task_id, v_step, 'Recreate index: ' || idx.index_name,
                        'INDEX', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);

                DBMS_OUTPUT.PUT_LINE('  Recreated index: ' || idx.index_name);
            EXCEPTION
                WHEN OTHERS THEN
                    log_step(p_task_id, v_step, 'Recreate index: ' || idx.index_name,
                            'INDEX', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
            END;
        END LOOP;
    END recreate_indexes;


    PROCEDURE recreate_constraints(
        p_task_id NUMBER,
        p_source_owner VARCHAR2,
        p_source_table VARCHAR2,
        p_target_table VARCHAR2,
        p_step_offset NUMBER
    ) AS
        v_sql VARCHAR2(4000);
        v_start TIMESTAMP;
        v_step NUMBER := p_step_offset;
    BEGIN
        FOR con IN (
            SELECT constraint_name, constraint_type, search_condition, r_constraint_name
            FROM dba_constraints
            WHERE owner = p_source_owner
            AND table_name = p_source_table
            AND constraint_type IN ('C', 'U', 'R')
            ORDER BY constraint_type, constraint_name
        ) LOOP
            v_step := v_step + 1;
            v_start := SYSTIMESTAMP;

            BEGIN
                v_sql := 'ALTER TABLE ' || p_source_owner || '.' || p_target_table ||
                        ' ADD CONSTRAINT ' || con.constraint_name;

                IF con.constraint_type = 'C' THEN
                    v_sql := v_sql || ' CHECK (' || con.search_condition || ')';
                ELSIF con.constraint_type = 'U' THEN
                    DECLARE
                        v_columns VARCHAR2(4000);
                    BEGIN
                        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY position)
                        INTO v_columns
                        FROM dba_cons_columns
                        WHERE owner = p_source_owner
                        AND constraint_name = con.constraint_name;

                        v_sql := v_sql || ' UNIQUE (' || v_columns || ')';
                    END;
                ELSIF con.constraint_type = 'R' THEN
                    DECLARE
                        v_columns VARCHAR2(4000);
                        v_r_owner VARCHAR2(30);
                        v_r_table VARCHAR2(128);
                    BEGIN
                        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY position)
                        INTO v_columns
                        FROM dba_cons_columns
                        WHERE owner = p_source_owner
                        AND constraint_name = con.constraint_name;

                        SELECT owner, table_name
                        INTO v_r_owner, v_r_table
                        FROM dba_constraints
                        WHERE constraint_name = con.r_constraint_name;

                        v_sql := v_sql || ' FOREIGN KEY (' || v_columns || ')' ||
                                ' REFERENCES ' || v_r_owner || '.' || v_r_table;
                    END;
                END IF;

                EXECUTE IMMEDIATE v_sql;

                log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name,
                        'CONSTRAINT', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);

            EXCEPTION
                WHEN OTHERS THEN
                    log_step(p_task_id, v_step, 'Recreate constraint: ' || con.constraint_name,
                            'CONSTRAINT', v_sql, 'FAILED', v_start, SYSTIMESTAMP,
                            SQLCODE, SQLERRM);
            END;
        END LOOP;
    END recreate_constraints;


    -- ==========================================================================
    -- Migration Methods
    -- ==========================================================================

    PROCEDURE migrate_using_ctas(
        p_task_id NUMBER
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

        DBMS_OUTPUT.PUT_LINE('Migrating using CTAS method: ' || v_task.source_table);

        v_new_table := v_task.source_table || '_PART';
        v_old_table := v_task.source_table || '_OLD';

        -- Step 1: Build partition DDL
        v_start := SYSTIMESTAMP;
        build_partition_ddl(v_task, v_ddl);
        log_step(p_task_id, v_step, 'Build DDL', 'PREPARE', v_ddl,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 2: Create partitioned table
        v_start := SYSTIMESTAMP;
        EXECUTE IMMEDIATE v_ddl;
        log_step(p_task_id, v_step, 'Create partitioned table', 'CREATE', v_ddl,
                'SUCCESS', v_start, SYSTIMESTAMP);
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

                DBMS_OUTPUT.PUT_LINE('  Converting date column ' || v_date_column || ' during copy');
            ELSE
                -- Standard SELECT *
                v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_task.parallel_degree || ') */ INTO ' ||
                        v_task.source_owner || '.' || v_new_table ||
                        ' SELECT * FROM ' || v_task.source_owner || '.' || v_task.source_table;
            END IF;

            EXECUTE IMMEDIATE v_sql;
            COMMIT;

            log_step(p_task_id, v_step, 'Copy data' ||
                    CASE WHEN v_requires_conversion = 'Y' THEN ' (with date conversion)' ELSE '' END,
                    'COPY', v_sql, 'SUCCESS', v_start, SYSTIMESTAMP);
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- No analysis record, use standard copy
                v_sql := 'INSERT /*+ APPEND PARALLEL(' || v_task.parallel_degree || ') */ INTO ' ||
                        v_task.source_owner || '.' || v_new_table ||
                        ' SELECT * FROM ' || v_task.source_owner || '.' || v_task.source_table;
                EXECUTE IMMEDIATE v_sql;
                COMMIT;
                log_step(p_task_id, v_step, 'Copy data', 'COPY', v_sql,
                        'SUCCESS', v_start, SYSTIMESTAMP);
        END;
        v_step := v_step + 10;

        -- Step 4: Recreate indexes
        v_start := SYSTIMESTAMP;
        recreate_indexes(p_task_id, v_task.source_owner, v_task.source_table, v_new_table, v_step);
        v_step := v_step + 100;

        -- Step 5: Recreate constraints
        recreate_constraints(p_task_id, v_task.source_owner, v_task.source_table, v_new_table, v_step);
        v_step := v_step + 100;

        -- Step 6: Gather statistics
        v_start := SYSTIMESTAMP;
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => v_task.source_owner,
            tabname => v_new_table,
            cascade => TRUE,
            degree => v_task.parallel_degree
        );
        log_step(p_task_id, v_step, 'Gather statistics', 'STATS', NULL,
                'SUCCESS', v_start, SYSTIMESTAMP);
        v_step := v_step + 10;

        -- Step 7: Rename tables
        v_start := SYSTIMESTAMP;
        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_task.source_table ||
                ' RENAME TO ' || v_old_table;
        EXECUTE IMMEDIATE v_sql;

        v_sql := 'ALTER TABLE ' || v_task.source_owner || '.' || v_new_table ||
                ' RENAME TO ' || v_task.source_table;
        EXECUTE IMMEDIATE v_sql;

        log_step(p_task_id, v_step, 'Rename tables', 'RENAME', v_sql,
                'SUCCESS', v_start, SYSTIMESTAMP);

        -- Update task with backup name
        UPDATE cmr.dwh_migration_tasks
        SET backup_table_name = v_old_table,
            can_rollback = 'Y'
        WHERE task_id = p_task_id;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Migration completed successfully');
        DBMS_OUTPUT.PUT_LINE('  Original table renamed to: ' || v_old_table);
        DBMS_OUTPUT.PUT_LINE('  New partitioned table: ' || v_task.source_table);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_step(p_task_id, v_step, 'Migration failed', 'ERROR', NULL,
                    'FAILED', v_start, SYSTIMESTAMP, SQLCODE, SQLERRM);
            RAISE;
    END migrate_using_ctas;


    PROCEDURE migrate_using_online_redef(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_ddl CLOB;
        v_start TIMESTAMP;
    BEGIN
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id;

        DBMS_OUTPUT.PUT_LINE('Online redefinition not yet implemented');
        DBMS_OUTPUT.PUT_LINE('Using CTAS method instead...');

        migrate_using_ctas(p_task_id);
    END migrate_using_online_redef;


    PROCEDURE migrate_using_exchange(
        p_task_id NUMBER
    ) AS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Exchange partition method not yet implemented');
        DBMS_OUTPUT.PUT_LINE('Using CTAS method instead...');

        migrate_using_ctas(p_task_id);
    END migrate_using_exchange;


    -- ==========================================================================
    -- Main Execution
    -- ==========================================================================

    PROCEDURE execute_migration(
        p_task_id NUMBER
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

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Starting Migration');
        DBMS_OUTPUT.PUT_LINE('Task ID: ' || p_task_id);
        DBMS_OUTPUT.PUT_LINE('Table: ' || v_task.source_owner || '.' || v_task.source_table);
        DBMS_OUTPUT.PUT_LINE('Method: ' || v_task.migration_method);
        DBMS_OUTPUT.PUT_LINE('========================================');

        v_start_time := SYSTIMESTAMP;
        v_source_size := pck_dwh_table_migration_analyzer.get_table_size_mb(v_task.source_owner, v_task.source_table);

        -- Update status
        UPDATE cmr.dwh_migration_tasks
        SET status = 'RUNNING',
            execution_start = v_start_time
        WHERE task_id = p_task_id;
        COMMIT;

        -- Create backup if enabled
        IF get_ilm_config('MIGRATION_BACKUP_ENABLED') = 'Y' THEN
            create_backup_table(p_task_id, v_task.source_owner, v_task.source_table, v_backup_name);
        END IF;

        -- Execute migration based on method
        CASE v_task.migration_method
            WHEN 'CTAS' THEN
                migrate_using_ctas(p_task_id);
            WHEN 'ONLINE' THEN
                migrate_using_online_redef(p_task_id);
            WHEN 'EXCHANGE' THEN
                migrate_using_exchange(p_task_id);
            WHEN 'OFFLINE' THEN
                migrate_using_ctas(p_task_id);
            ELSE
                RAISE_APPLICATION_ERROR(-20002, 'Unknown migration method: ' || v_task.migration_method);
        END CASE;

        v_end_time := SYSTIMESTAMP;
        v_target_size := pck_dwh_table_migration_analyzer.get_table_size_mb(v_task.source_owner, v_task.source_table);

        -- Apply ILM policies if requested
        IF v_task.apply_dwh_ilm_policies = 'Y' THEN
            apply_dwh_ilm_policies(p_task_id);
        END IF;

        -- Validate migration
        IF get_ilm_config('MIGRATION_VALIDATE_ENABLED') = 'Y' THEN
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
        DBMS_OUTPUT.PUT_LINE('Duration: ' || ROUND((v_end_time - v_start_time) * 24 * 60, 2) || ' minutes');
        DBMS_OUTPUT.PUT_LINE('Source size: ' || v_source_size || ' MB');
        DBMS_OUTPUT.PUT_LINE('Target size: ' || v_target_size || ' MB');
        DBMS_OUTPUT.PUT_LINE('Space saved: ' || (v_source_size - v_target_size) || ' MB');
        DBMS_OUTPUT.PUT_LINE('========================================');

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
        p_max_tasks NUMBER DEFAULT NULL
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
                execute_migration(task.task_id);
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
    -- Post-Migration
    -- ==========================================================================

    PROCEDURE apply_dwh_ilm_policies(
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
    END apply_dwh_ilm_policies;


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
