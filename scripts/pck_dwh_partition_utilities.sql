-- =============================================================================
-- Package: pck_dwh_partition_utilities
-- Description: Partition utility procedures for ILM framework
--
-- Architecture:
--   - Utilities perform operations and return status via OUT parameters
--   - ILM execution engine calls utilities and logs to dwh_ilm_execution_log
--   - No direct logging from utilities (separation of concerns)
--
-- Dependencies:
--   - pck_dwh_partition_utils_helper (validation functions only)
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_partition_utilities AUTHID CURRENT_USER AS

    -- ==========================================================================
    -- SECTION 1: ILM-AWARE PARTITION PRE-CREATION
    -- ==========================================================================

    -- Pre-create HOT tier partitions for next period
    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Pre-create HOT tier partitions for all tables with active ILM policies
    PROCEDURE precreate_all_hot_partitions(
        p_tables_processed OUT NUMBER,
        p_total_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Preview what HOT tier partitions would be created (no execution)
    -- Note: This is informational only, no OUT parameters needed
    PROCEDURE preview_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 2: PARTITION CREATION AND MAINTENANCE
    -- ==========================================================================

    -- Create future partitions (generic, for non-ILM tables)
    PROCEDURE create_future_partitions(
        p_table_name VARCHAR2,
        p_months_ahead NUMBER DEFAULT 12,
        p_partition_interval VARCHAR2 DEFAULT 'MONTH',
        p_sql_executed OUT CLOB,
        p_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Split partition at specified date
    PROCEDURE split_partition(
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_split_date DATE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Merge adjacent partitions
    PROCEDURE merge_partitions(
        p_table_name VARCHAR2,
        p_partition1 VARCHAR2,
        p_partition2 VARCHAR2,
        p_merged_partition VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 3: PARTITION DATA LOADING
    -- ==========================================================================

    -- Exchange partition for fast bulk loading
    PROCEDURE exchange_partition_load(
        p_target_table VARCHAR2,
        p_staging_table VARCHAR2,
        p_partition_name VARCHAR2,
        p_validate BOOLEAN DEFAULT TRUE,
        p_update_indexes BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Parallel partition loading with direct path insert
    PROCEDURE parallel_partition_load(
        p_target_table VARCHAR2,
        p_source_query VARCHAR2,
        p_partition_name VARCHAR2,
        p_degree NUMBER DEFAULT 4,
        p_sql_executed OUT CLOB,
        p_rows_loaded OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 4: PARTITION MAINTENANCE OPERATIONS
    -- ==========================================================================

    -- Truncate partitions older than retention period
    PROCEDURE truncate_old_partitions(
        p_table_name VARCHAR2,
        p_retention_days NUMBER,
        p_sql_executed OUT CLOB,
        p_partitions_truncated OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Compress partitions older than specified age
    PROCEDURE compress_partitions(
        p_table_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_age_days NUMBER DEFAULT 90,
        p_sql_executed OUT CLOB,
        p_partitions_compressed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Move partitions to different tablespace
    PROCEDURE move_partitions_to_tablespace(
        p_table_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_age_days NUMBER DEFAULT NULL,
        p_compression VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_partitions_moved OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 4B: SINGLE PARTITION OPERATIONS (For ILM Queue Execution)
    -- ==========================================================================

    -- Compress a single partition
    PROCEDURE compress_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Move a single partition to tablespace
    PROCEDURE move_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Drop a single partition
    PROCEDURE drop_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Truncate a single partition
    PROCEDURE truncate_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Make a single partition read-only
    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 5: PARTITION STATISTICS MANAGEMENT
    -- ==========================================================================

    -- Gather statistics on partitions (with incremental option)
    PROCEDURE gather_partition_statistics(
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2 DEFAULT NULL,
        p_degree NUMBER DEFAULT 4,
        p_incremental BOOLEAN DEFAULT TRUE,
        p_partitions_analyzed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Identify and gather statistics on stale partitions
    PROCEDURE gather_stale_partition_stats(
        p_table_name VARCHAR2,
        p_stale_percent NUMBER DEFAULT 10,
        p_degree NUMBER DEFAULT 4,
        p_partitions_analyzed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 6: PARTITION HEALTH CHECK AND VALIDATION
    -- ==========================================================================

    -- Check partition health and identify issues
    -- Note: This is informational/reporting, no OUT parameters needed
    PROCEDURE check_partition_health(
        p_table_name VARCHAR2
    );

    -- Validate partition constraints
    -- Note: This is validation/reporting, no OUT parameters needed
    PROCEDURE validate_partition_constraints(
        p_table_name VARCHAR2
    );

    -- ==========================================================================
    -- SECTION 7: PARTITION REPORTING
    -- ==========================================================================

    -- Generate partition size and configuration report
    -- Note: This is reporting only, no OUT parameters needed
    PROCEDURE partition_size_report(
        p_table_name VARCHAR2
    );

END pck_dwh_partition_utilities;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_partition_utilities AS

    -- ==========================================================================
    -- SECTION 1: ILM-AWARE PARTITION PRE-CREATION (REFACTORED)
    -- ==========================================================================

    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_interval VARCHAR2(20);
        v_tablespace VARCHAR2(30);
        v_compression VARCHAR2(50);
        v_config_source VARCHAR2(50);
        v_pctfree NUMBER := 10;

        v_naming_compliance VARCHAR2(20);
        v_current_date DATE := SYSDATE;
        v_start_date DATE;
        v_end_date DATE;
        v_partition_date DATE;
        v_next_date DATE;
        v_partition_name VARCHAR2(30);
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_count NUMBER;
        v_created_count NUMBER := 0;
        v_skipped_count NUMBER := 0;
        v_warning_msg VARCHAR2(4000);

    BEGIN
        -- Initialize OUT parameters
        p_partitions_created := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Pre-creating HOT tier partitions');
        DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_owner || '.' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- STEP 1: Validate partition naming compliance
        v_naming_compliance := pck_dwh_partition_utils_helper.validate_partition_naming(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_sample_size => 10
        );

        DBMS_OUTPUT.PUT_LINE('Partition naming compliance: ' || v_naming_compliance);

        IF v_naming_compliance = 'NON_COMPLIANT' THEN
            v_warning_msg := 'Table partitions do NOT follow framework naming patterns. ' ||
                           'Pre-creation skipped. ' ||
                           'Framework patterns: P_YYYY, P_YYYY_MM, P_YYYY_MM_DD, P_IYYY_IW';

            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('⚠️  ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('Operation SKIPPED due to non-compliant partition naming.');
            DBMS_OUTPUT.PUT_LINE('=========================================');

            p_status := 'SKIPPED';
            p_error_message := v_warning_msg;
            p_partitions_created := 0;
            RETURN;

        ELSIF v_naming_compliance = 'MIXED' THEN
            v_warning_msg := 'WARNING: Table has MIXED partition naming (some framework, some custom). ' ||
                           'Pre-creation will continue but may create inconsistent names.';

            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('⚠️  ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('');
        END IF;

        -- STEP 2: Detect/get partition configuration
        pck_dwh_partition_utils_helper.detect_partition_config(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_interval => v_interval,
            p_tablespace => v_tablespace,
            p_compression => v_compression,
            p_config_source => v_config_source
        );

        DBMS_OUTPUT.PUT_LINE('Configuration source: ' || v_config_source);
        DBMS_OUTPUT.PUT_LINE('Detected interval: ' || NVL(v_interval, 'UNKNOWN'));
        DBMS_OUTPUT.PUT_LINE('Tablespace: ' || NVL(v_tablespace, 'N/A'));
        DBMS_OUTPUT.PUT_LINE('Compression: ' || NVL(v_compression, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('');

        -- STEP 3: Validate we can proceed
        IF v_interval IS NULL OR v_interval = 'UNKNOWN' THEN
            v_warning_msg := 'Cannot detect partition interval type.';
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('=========================================');

            p_status := 'ERROR';
            p_error_message := v_warning_msg;
            p_partitions_created := 0;
            RETURN;
        END IF;

        IF v_tablespace IS NULL THEN
            v_warning_msg := 'No tablespace detected for new partitions.';
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('=========================================');

            p_status := 'ERROR';
            p_error_message := v_warning_msg;
            p_partitions_created := 0;
            RETURN;
        END IF;

        -- STEP 4: Determine date range for pre-creation
        IF UPPER(v_interval) = 'MONTHLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY');
            DBMS_OUTPUT.PUT_LINE('Creating MONTHLY partitions from ' ||
                TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' to ' ||
                TO_CHAR(v_end_date, 'YYYY-MM-DD'));

        ELSIF UPPER(v_interval) = 'DAILY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := LAST_DAY(v_start_date);
            DBMS_OUTPUT.PUT_LINE('Creating DAILY partitions from ' ||
                TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' to ' ||
                TO_CHAR(v_end_date, 'YYYY-MM-DD'));

        ELSIF UPPER(v_interval) = 'WEEKLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'IW');
            v_end_date := LAST_DAY(ADD_MONTHS(v_current_date, 1));
            DBMS_OUTPUT.PUT_LINE('Creating WEEKLY partitions from ' ||
                TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' to ' ||
                TO_CHAR(v_end_date, 'YYYY-MM-DD'));

        ELSIF UPPER(v_interval) = 'YEARLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY');
            v_end_date := ADD_MONTHS(v_start_date, 12);
            DBMS_OUTPUT.PUT_LINE('Creating YEARLY partition for ' ||
                TO_CHAR(v_start_date, 'YYYY'));

        ELSE
            v_warning_msg := 'Unsupported interval type: ' || v_interval;
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('=========================================');

            p_status := 'ERROR';
            p_error_message := v_warning_msg;
            p_partitions_created := 0;
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('');

        -- STEP 5: Generate partitions
        v_partition_date := v_start_date;

        WHILE v_partition_date <= v_end_date LOOP
            -- Calculate next partition boundary
            IF UPPER(v_interval) = 'MONTHLY' THEN
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

            ELSIF UPPER(v_interval) = 'DAILY' THEN
                v_next_date := v_partition_date + 1;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM_DD');

            ELSIF UPPER(v_interval) = 'WEEKLY' THEN
                v_next_date := v_partition_date + 7;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'IYYY_IW');

            ELSIF UPPER(v_interval) = 'YEARLY' THEN
                v_next_date := ADD_MONTHS(v_partition_date, 12);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');
            END IF;

            -- Check if partition already exists
            SELECT COUNT(*)
            INTO v_count
            FROM all_tab_partitions
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            AND partition_name = v_partition_name;

            IF v_count = 0 THEN
                -- Build ADD PARTITION DDL
                v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                        ' ADD PARTITION ' || v_partition_name ||
                        ' VALUES LESS THAN (TO_DATE(''' ||
                        TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                        ' TABLESPACE ' || v_tablespace ||
                        ' PCTFREE ' || v_pctfree;

                -- Add compression if specified
                IF v_compression IS NOT NULL AND v_compression != 'NONE' THEN
                    v_sql := v_sql || ' COMPRESS FOR ' || v_compression;
                END IF;

                DBMS_OUTPUT.PUT_LINE('Creating: ' || v_partition_name);

                BEGIN
                    EXECUTE IMMEDIATE v_sql;
                    v_created_count := v_created_count + 1;

                    -- Log SQL executed
                    DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
                        v_warning_msg := v_warning_msg || CHR(10) ||
                                       'Failed to create ' || v_partition_name || ': ' || SQLERRM;
                        -- Log failed SQL too
                        DBMS_LOB.APPEND(v_sql_log, '-- FAILED: ' || v_sql || ';' || CHR(10));
                END;

            ELSE
                DBMS_OUTPUT.PUT_LINE('Skipping (exists): ' || v_partition_name);
                v_skipped_count := v_skipped_count + 1;
            END IF;

            v_partition_date := v_next_date;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Pre-creation complete');
        DBMS_OUTPUT.PUT_LINE('Created: ' || v_created_count || ' partition(s)');
        DBMS_OUTPUT.PUT_LINE('Skipped: ' || v_skipped_count || ' partition(s)');
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- Set OUT parameters for ILM engine logging
        p_partitions_created := v_created_count;
        p_sql_executed := v_sql_log;
        p_status := CASE WHEN v_warning_msg IS NOT NULL THEN 'WARNING' ELSE 'SUCCESS' END;
        p_error_message := v_warning_msg;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('=========================================');

            -- Set OUT parameters for error
            p_partitions_created := v_created_count;
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;

            RAISE;
    END precreate_hot_partitions;

    PROCEDURE precreate_all_hot_partitions(
        p_tables_processed OUT NUMBER,
        p_total_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_count NUMBER := 0;
        v_success_count NUMBER := 0;
        v_error_count NUMBER := 0;
        v_total_partitions NUMBER := 0;
        v_sql_executed CLOB;
        v_partitions_created NUMBER;
        v_proc_status VARCHAR2(50);
        v_proc_error VARCHAR2(4000);
        v_all_errors CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_tables_processed := 0;
        p_total_partitions_created := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(v_all_errors, TRUE);

        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Batch: Pre-creating HOT tier partitions for all ILM-managed tables');
        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('');

        -- Process all tables with active ILM policies
        FOR rec IN (
            SELECT DISTINCT
                p.table_owner,
                p.table_name
            FROM cmr.dwh_ilm_policies p
            WHERE p.enabled = 'Y'
            ORDER BY p.table_owner, p.table_name
        ) LOOP
            BEGIN
                v_count := v_count + 1;
                DBMS_OUTPUT.PUT_LINE('Processing: ' || rec.table_owner || '.' || rec.table_name);
                DBMS_OUTPUT.PUT_LINE('');

                precreate_hot_partitions(
                    p_table_owner => rec.table_owner,
                    p_table_name => rec.table_name,
                    p_sql_executed => v_sql_executed,
                    p_partitions_created => v_partitions_created,
                    p_status => v_proc_status,
                    p_error_message => v_proc_error
                );

                IF v_proc_status IN ('SUCCESS', 'WARNING') THEN
                    v_success_count := v_success_count + 1;
                    v_total_partitions := v_total_partitions + v_partitions_created;
                ELSE
                    v_error_count := v_error_count + 1;
                    DBMS_LOB.APPEND(v_all_errors,
                        TO_CLOB(rec.table_owner || '.' || rec.table_name || ': ' ||
                        NVL(v_proc_error, 'Unknown error') || CHR(10)));
                END IF;

                DBMS_OUTPUT.PUT_LINE('');

            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('ERROR processing ' || rec.table_owner || '.' ||
                        rec.table_name || ': ' || SQLERRM);
                    DBMS_LOB.APPEND(v_all_errors,
                        rec.table_owner || '.' || rec.table_name || ': ' || SUBSTR(SQLERRM, 1, 4000) || CHR(10));
                    DBMS_OUTPUT.PUT_LINE('');
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Batch pre-creation complete');
        DBMS_OUTPUT.PUT_LINE('Tables processed: ' || v_count);
        DBMS_OUTPUT.PUT_LINE('Success: ' || v_success_count);
        DBMS_OUTPUT.PUT_LINE('Errors: ' || v_error_count);
        DBMS_OUTPUT.PUT_LINE('Total partitions created: ' || v_total_partitions);
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- Set OUT parameters
        p_tables_processed := v_count;
        p_total_partitions_created := v_total_partitions;
        p_status := CASE WHEN v_error_count > 0 THEN 'WARNING' ELSE 'SUCCESS' END;
        p_error_message := CASE WHEN DBMS_LOB.GETLENGTH(v_all_errors) > 0
                               THEN DBMS_LOB.SUBSTR(v_all_errors, 4000, 1)
                               ELSE NULL END;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('FATAL ERROR in batch operation: ' || SQLERRM);

            p_tables_processed := v_count;
            p_total_partitions_created := v_total_partitions;
            p_status := 'ERROR';
            p_error_message := SQLERRM;

            RAISE;
    END precreate_all_hot_partitions;

    PROCEDURE preview_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) AS
        v_interval VARCHAR2(20);
        v_tablespace VARCHAR2(30);
        v_compression VARCHAR2(50);
        v_config_source VARCHAR2(50);
        v_naming_compliance VARCHAR2(20);

        v_current_date DATE := SYSDATE;
        v_start_date DATE;
        v_end_date DATE;
        v_partition_date DATE;
        v_next_date DATE;
        v_partition_name VARCHAR2(30);
        v_count NUMBER;
        v_preview_count NUMBER := 0;

    BEGIN
        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('PREVIEW: HOT tier partition pre-creation');
        DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_owner || '.' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- Check naming compliance
        v_naming_compliance := pck_dwh_partition_utils_helper.validate_partition_naming(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_sample_size => 10
        );

        DBMS_OUTPUT.PUT_LINE('Partition naming: ' || v_naming_compliance);

        IF v_naming_compliance = 'NON_COMPLIANT' THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('⚠️  WARNING: Partitions do NOT follow framework patterns.');
            DBMS_OUTPUT.PUT_LINE('Pre-creation would be SKIPPED in actual execution.');
            DBMS_OUTPUT.PUT_LINE('=========================================');
            RETURN;
        END IF;

        -- Detect configuration
        pck_dwh_partition_utils_helper.detect_partition_config(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_interval => v_interval,
            p_tablespace => v_tablespace,
            p_compression => v_compression,
            p_config_source => v_config_source
        );

        DBMS_OUTPUT.PUT_LINE('Configuration source: ' || v_config_source);
        DBMS_OUTPUT.PUT_LINE('Interval: ' || NVL(v_interval, 'UNKNOWN'));
        DBMS_OUTPUT.PUT_LINE('Tablespace: ' || NVL(v_tablespace, 'N/A'));
        DBMS_OUTPUT.PUT_LINE('Compression: ' || NVL(v_compression, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('');

        IF v_interval IS NULL OR v_interval = 'UNKNOWN' THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: Cannot detect partition interval.');
            DBMS_OUTPUT.PUT_LINE('=========================================');
            RETURN;
        END IF;

        -- Determine date range
        IF UPPER(v_interval) = 'MONTHLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY');
        ELSIF UPPER(v_interval) = 'DAILY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := LAST_DAY(v_start_date);
        ELSIF UPPER(v_interval) = 'WEEKLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'IW');
            v_end_date := LAST_DAY(ADD_MONTHS(v_current_date, 1));
        ELSIF UPPER(v_interval) = 'YEARLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY');
            v_end_date := ADD_MONTHS(v_start_date, 12);
        END IF;

        DBMS_OUTPUT.PUT_LINE('Partitions that would be created:');
        DBMS_OUTPUT.PUT_LINE('');

        -- Preview partitions
        v_partition_date := v_start_date;

        WHILE v_partition_date <= v_end_date LOOP
            IF UPPER(v_interval) = 'MONTHLY' THEN
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');
            ELSIF UPPER(v_interval) = 'DAILY' THEN
                v_next_date := v_partition_date + 1;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM_DD');
            ELSIF UPPER(v_interval) = 'WEEKLY' THEN
                v_next_date := v_partition_date + 7;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'IYYY_IW');
            ELSIF UPPER(v_interval) = 'YEARLY' THEN
                v_next_date := ADD_MONTHS(v_partition_date, 12);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY');
            END IF;

            SELECT COUNT(*)
            INTO v_count
            FROM all_tab_partitions
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            AND partition_name = v_partition_name;

            IF v_count = 0 THEN
                DBMS_OUTPUT.PUT_LINE('  [NEW] ' || v_partition_name ||
                    ' (< ' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ')');
                v_preview_count := v_preview_count + 1;
            ELSE
                DBMS_OUTPUT.PUT_LINE('  [EXISTS] ' || v_partition_name);
            END IF;

            v_partition_date := v_next_date;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Preview complete');
        DBMS_OUTPUT.PUT_LINE('Would create: ' || v_preview_count || ' new partition(s)');
        DBMS_OUTPUT.PUT_LINE('=========================================');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            RAISE;
    END preview_hot_partitions;


    PROCEDURE create_future_partitions(
        p_table_name VARCHAR2,
        p_months_ahead NUMBER DEFAULT 12,
        p_partition_interval VARCHAR2 DEFAULT 'MONTH',
        p_sql_executed OUT CLOB,
        p_partitions_created OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_max_partition_date DATE;
        v_next_partition_date DATE;
        v_partition_name VARCHAR2(30);
        v_high_value_str VARCHAR2(100);
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_count NUMBER := 0;
    BEGIN
        -- Initialize OUT parameters
        p_partitions_created := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);
        -- Get the highest partition date
        SELECT MAX(TO_DATE(
            SUBSTR(high_value, INSTR(high_value, '''') + 1,
                   INSTR(high_value, '''', 1, 2) - INSTR(high_value, '''') - 1),
            'YYYY-MM-DD'))
        INTO v_max_partition_date
        FROM user_tab_partitions
        WHERE table_name = UPPER(p_table_name)
        AND partition_position > 1;

        IF v_max_partition_date IS NULL THEN
            v_max_partition_date := TRUNC(SYSDATE, 'MM');
        END IF;

        -- Create partitions for future months
        FOR i IN 1..p_months_ahead LOOP
            IF p_partition_interval = 'MONTH' THEN
                v_next_partition_date := ADD_MONTHS(v_max_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_next_partition_date, 'YYYY_MM');
            ELSIF p_partition_interval = 'QUARTER' THEN
                v_next_partition_date := ADD_MONTHS(v_max_partition_date, 3);
                v_partition_name := 'P_' || TO_CHAR(v_next_partition_date, 'YYYY') || '_Q' ||
                                  TO_CHAR(TO_NUMBER(TO_CHAR(v_next_partition_date, 'Q')));
            ELSIF p_partition_interval = 'YEAR' THEN
                v_next_partition_date := ADD_MONTHS(v_max_partition_date, 12);
                v_partition_name := 'P_' || TO_CHAR(v_next_partition_date, 'YYYY');
            END IF;

            v_high_value_str := 'DATE ''' || TO_CHAR(v_next_partition_date, 'YYYY-MM-DD') || '''';

            -- Check if partition already exists
            SELECT COUNT(*) INTO v_count
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            AND partition_name = v_partition_name;

            IF v_count = 0 THEN
                v_sql := 'ALTER TABLE ' || p_table_name ||
                        ' ADD PARTITION ' || v_partition_name ||
                        ' VALUES LESS THAN (' || v_high_value_str || ')';

                DBMS_OUTPUT.PUT_LINE('Creating partition: ' || v_partition_name);
                BEGIN
                    EXECUTE IMMEDIATE v_sql;
                    p_partitions_created := p_partitions_created + 1;
                    DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_LOB.APPEND(v_sql_log, '-- FAILED: ' || v_sql || ';' || CHR(10));
                        IF p_error_message IS NULL THEN
                            p_error_message := SQLERRM;
                        ELSE
                            p_error_message := p_error_message || '; ' || SQLERRM;
                        END IF;
                END;
            END IF;

            v_max_partition_date := v_next_partition_date;
        END LOOP;

        p_sql_executed := v_sql_log;
        p_status := CASE WHEN p_error_message IS NOT NULL THEN 'WARNING' ELSE 'SUCCESS' END;
        DBMS_OUTPUT.PUT_LINE('Created ' || p_partitions_created || ' future partitions for ' || p_table_name);

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE split_partition(
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_split_date DATE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_new_partition1 VARCHAR2(30);
        v_new_partition2 VARCHAR2(30);
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        v_new_partition1 := p_partition_name || '_1';
        v_new_partition2 := p_partition_name || '_2';

        v_sql := 'ALTER TABLE ' || p_table_name ||
                ' SPLIT PARTITION ' || p_partition_name ||
                ' AT (DATE ''' || TO_CHAR(p_split_date, 'YYYY-MM-DD') || ''')' ||
                ' INTO (PARTITION ' || v_new_partition1 ||
                ', PARTITION ' || v_new_partition2 || ')';

        DBMS_OUTPUT.PUT_LINE('Splitting partition: ' || p_partition_name);
        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        -- Rebuild local indexes
        FOR idx IN (
            SELECT index_name, partition_name
            FROM user_ind_partitions
            WHERE index_name IN (
                SELECT index_name FROM user_part_indexes
                WHERE table_name = UPPER(p_table_name)
                AND locality = 'LOCAL'
            )
            AND partition_name IN (v_new_partition1, v_new_partition2)
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx.index_name ||
                ' REBUILD PARTITION ' || idx.partition_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Partition split completed');

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE merge_partitions(
        p_table_name VARCHAR2,
        p_partition1 VARCHAR2,
        p_partition2 VARCHAR2,
        p_merged_partition VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_merged_name VARCHAR2(30);
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        v_merged_name := NVL(p_merged_partition, p_partition1);

        v_sql := 'ALTER TABLE ' || p_table_name ||
                ' MERGE PARTITIONS ' || p_partition1 || ', ' || p_partition2 ||
                ' INTO PARTITION ' || v_merged_name;

        DBMS_OUTPUT.PUT_LINE('Merging partitions: ' || p_partition1 || ' and ' || p_partition2);
        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        -- Rebuild local indexes on merged partition
        FOR idx IN (
            SELECT index_name, partition_name
            FROM user_ind_partitions
            WHERE index_name IN (
                SELECT index_name FROM user_part_indexes
                WHERE table_name = UPPER(p_table_name)
                AND locality = 'LOCAL'
            )
            AND partition_name = v_merged_name
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx.index_name ||
                ' REBUILD PARTITION ' || idx.partition_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Partition merge completed');

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE exchange_partition_load(
        p_target_table VARCHAR2,
        p_staging_table VARCHAR2,
        p_partition_name VARCHAR2,
        p_validate BOOLEAN DEFAULT TRUE,
        p_update_indexes BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_validation VARCHAR2(20) := 'WITH VALIDATION';
        v_update_idx VARCHAR2(30) := 'UPDATE INDEXES';
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        IF NOT p_validate THEN
            v_validation := 'WITHOUT VALIDATION';
        END IF;

        IF NOT p_update_indexes THEN
            v_update_idx := '';
        END IF;

        -- Disable constraints on staging table (optional but recommended)
        FOR cons IN (
            SELECT constraint_name
            FROM user_constraints
            WHERE table_name = UPPER(p_staging_table)
            AND constraint_type IN ('P', 'U', 'R')
        ) LOOP
            v_sql := 'ALTER TABLE ' || p_staging_table ||
                            ' DISABLE CONSTRAINT ' || cons.constraint_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        -- Exchange partition
        v_sql := 'ALTER TABLE ' || p_target_table ||
                ' EXCHANGE PARTITION ' || p_partition_name ||
                ' WITH TABLE ' || p_staging_table ||
                ' ' || v_validation ||
                ' ' || v_update_idx;

        DBMS_OUTPUT.PUT_LINE('Exchanging partition: ' || p_partition_name);
        DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        -- Re-enable constraints
        FOR cons IN (
            SELECT constraint_name
            FROM user_constraints
            WHERE table_name = UPPER(p_staging_table)
            AND constraint_type IN ('P', 'U', 'R')
            AND status = 'DISABLED'
        ) LOOP
            v_sql := 'ALTER TABLE ' || p_staging_table ||
                            ' ENABLE CONSTRAINT ' || cons.constraint_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        -- Gather statistics on exchanged partition
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => p_target_table,
            partname => p_partition_name,
            cascade => TRUE,
            degree => 4
        );
        DBMS_LOB.APPEND(v_sql_log, '-- DBMS_STATS.GATHER_TABLE_STATS for partition ' ||
                       p_partition_name || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Exchange partition completed successfully');

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE parallel_partition_load(
        p_target_table VARCHAR2,
        p_source_query VARCHAR2,
        p_partition_name VARCHAR2,
        p_degree NUMBER DEFAULT 4,
        p_sql_executed OUT CLOB,
        p_rows_loaded OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_rows_loaded := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Enable parallel DML
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
        DBMS_LOB.APPEND(v_sql_log, 'ALTER SESSION ENABLE PARALLEL DML;' || CHR(10));

        -- Direct path insert into specific partition
        v_sql := 'INSERT /*+ APPEND PARALLEL(' || p_degree || ') */ INTO ' ||
                p_target_table || ' PARTITION (' || p_partition_name || ') ' ||
                p_source_query;

        DBMS_OUTPUT.PUT_LINE('Loading partition: ' || p_partition_name);
        EXECUTE IMMEDIATE v_sql;
        p_rows_loaded := SQL%ROWCOUNT;
        COMMIT;

        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        DBMS_LOB.APPEND(v_sql_log, 'COMMIT;' || CHR(10));
        DBMS_LOB.APPEND(v_sql_log, '-- Rows loaded: ' || p_rows_loaded || CHR(10));

        -- Gather statistics
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => USER,
            tabname => p_target_table,
            partname => p_partition_name,
            cascade => TRUE,
            degree => p_degree
        );
        DBMS_LOB.APPEND(v_sql_log, '-- DBMS_STATS.GATHER_TABLE_STATS for partition ' ||
                       p_partition_name || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Parallel load completed: ' || p_rows_loaded || ' rows');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE truncate_old_partitions(
        p_table_name VARCHAR2,
        p_retention_days NUMBER,
        p_sql_executed OUT CLOB,
        p_partitions_truncated OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_cutoff_date DATE := TRUNC(SYSDATE) - p_retention_days;
        v_partition_date DATE;
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_count NUMBER := 0;
        v_errors CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_partitions_truncated := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_errors, TRUE);

        FOR rec IN (
            SELECT partition_name, high_value, partition_position
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            ORDER BY partition_position
        ) LOOP
            BEGIN
                -- Extract partition date
                EXECUTE IMMEDIATE 'SELECT ' || rec.high_value || ' FROM DUAL'
                    INTO v_partition_date;

                IF v_partition_date < v_cutoff_date THEN
                    DBMS_OUTPUT.PUT_LINE('Truncating partition: ' || rec.partition_name ||
                                       ' (Date: ' || TO_CHAR(v_partition_date, 'YYYY-MM-DD') || ')');

                    v_sql := 'ALTER TABLE ' || p_table_name ||
                        ' TRUNCATE PARTITION ' || rec.partition_name ||
                        ' UPDATE INDEXES';

                    EXECUTE IMMEDIATE v_sql;
                    DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

                    v_count := v_count + 1;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error truncating partition ' || rec.partition_name || ': ' || SQLERRM);
                    DBMS_LOB.APPEND(v_errors, 'Partition ' || rec.partition_name || ': ' || SUBSTR(SQLERRM, 1, 4000) || CHR(10));
                    DBMS_LOB.APPEND(v_sql_log, '-- FAILED: ' || v_sql || ';' || CHR(10));
            END;
        END LOOP;

        p_sql_executed := v_sql_log;
        p_partitions_truncated := v_count;
        p_status := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0 THEN 'WARNING' ELSE 'SUCCESS' END;
        p_error_message := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0
                                THEN DBMS_LOB.SUBSTR(v_errors, 4000, 1)
                                ELSE NULL END;

        DBMS_OUTPUT.PUT_LINE('Truncated ' || v_count || ' partition(s)');

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_partitions_truncated := v_count;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE compress_partitions(
        p_table_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_age_days NUMBER DEFAULT 90,
        p_sql_executed OUT CLOB,
        p_partitions_compressed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_cutoff_date DATE := TRUNC(SYSDATE) - p_age_days;
        v_partition_date DATE;
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_count NUMBER := 0;
        v_errors CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_partitions_compressed := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_errors, TRUE);

        FOR rec IN (
            SELECT tp.partition_name, tp.high_value, tp.compression
            FROM user_tab_partitions tp
            WHERE tp.table_name = UPPER(p_table_name)
            AND tp.compression = 'DISABLED'
            ORDER BY tp.partition_position
        ) LOOP
            BEGIN
                -- Extract partition date
                EXECUTE IMMEDIATE 'SELECT ' || rec.high_value || ' FROM DUAL'
                    INTO v_partition_date;

                IF v_partition_date < v_cutoff_date THEN
                    DBMS_OUTPUT.PUT_LINE('Compressing partition: ' || rec.partition_name ||
                                       ' (Type: ' || p_compression_type || ')');

                    v_sql := 'ALTER TABLE ' || p_table_name ||
                        ' MOVE PARTITION ' || rec.partition_name ||
                        ' COMPRESS FOR ' || p_compression_type;

                    EXECUTE IMMEDIATE v_sql;
                    DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

                    -- Rebuild local indexes
                    FOR idx IN (
                        SELECT index_name, partition_name
                        FROM user_ind_partitions
                        WHERE index_name IN (
                            SELECT index_name FROM user_part_indexes
                            WHERE table_name = UPPER(p_table_name)
                            AND locality = 'LOCAL'
                        )
                        AND partition_name = rec.partition_name
                    ) LOOP
                        v_sql := 'ALTER INDEX ' || idx.index_name ||
                            ' REBUILD PARTITION ' || idx.partition_name;
                        EXECUTE IMMEDIATE v_sql;
                        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
                    END LOOP;

                    v_count := v_count + 1;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error compressing partition ' || rec.partition_name || ': ' || SQLERRM);
                    DBMS_LOB.APPEND(v_errors, 'Partition ' || rec.partition_name || ': ' || SUBSTR(SQLERRM, 1, 4000) || CHR(10));
                    DBMS_LOB.APPEND(v_sql_log, '-- FAILED: ' || v_sql || ';' || CHR(10));
            END;
        END LOOP;

        p_sql_executed := v_sql_log;
        p_partitions_compressed := v_count;
        p_status := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0 THEN 'WARNING' ELSE 'SUCCESS' END;
        p_error_message := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0
                                THEN DBMS_LOB.SUBSTR(v_errors, 4000, 1)
                                ELSE NULL END;

        DBMS_OUTPUT.PUT_LINE('Compressed ' || v_count || ' partition(s)');

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_partitions_compressed := v_count;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE move_partitions_to_tablespace(
        p_table_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_age_days NUMBER DEFAULT NULL,
        p_compression VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_partitions_moved OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_cutoff_date DATE := TRUNC(SYSDATE) - NVL(p_age_days, 0);
        v_partition_date DATE;
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
        v_compress_clause VARCHAR2(100) := '';
        v_count NUMBER := 0;
        v_errors CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_partitions_moved := 0;
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_errors, TRUE);

        IF p_compression IS NOT NULL THEN
            v_compress_clause := ' COMPRESS FOR ' || p_compression;
        END IF;

        FOR rec IN (
            SELECT tp.partition_name, tp.high_value, tp.tablespace_name
            FROM user_tab_partitions tp
            WHERE tp.table_name = UPPER(p_table_name)
            AND tp.tablespace_name != UPPER(p_target_tablespace)
            ORDER BY tp.partition_position
        ) LOOP
            BEGIN
                -- Extract partition date
                IF p_age_days IS NOT NULL THEN
                    EXECUTE IMMEDIATE 'SELECT ' || rec.high_value || ' FROM DUAL'
                        INTO v_partition_date;

                    IF v_partition_date >= v_cutoff_date THEN
                        CONTINUE;
                    END IF;
                END IF;

                DBMS_OUTPUT.PUT_LINE('Moving partition: ' || rec.partition_name ||
                                   ' from ' || rec.tablespace_name || ' to ' || p_target_tablespace);

                v_sql := 'ALTER TABLE ' || p_table_name ||
                    ' MOVE PARTITION ' || rec.partition_name ||
                    ' TABLESPACE ' || p_target_tablespace ||
                    v_compress_clause;

                EXECUTE IMMEDIATE v_sql;
                DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

                -- Rebuild local indexes in target tablespace
                FOR idx IN (
                    SELECT index_name, partition_name
                    FROM user_ind_partitions
                    WHERE index_name IN (
                        SELECT index_name FROM user_part_indexes
                        WHERE table_name = UPPER(p_table_name)
                        AND locality = 'LOCAL'
                    )
                    AND partition_name = rec.partition_name
                ) LOOP
                    v_sql := 'ALTER INDEX ' || idx.index_name ||
                        ' REBUILD PARTITION ' || idx.partition_name ||
                        ' TABLESPACE ' || p_target_tablespace;
                    EXECUTE IMMEDIATE v_sql;
                    DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
                END LOOP;

                v_count := v_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error moving partition ' || rec.partition_name || ': ' || SQLERRM);
                    DBMS_LOB.APPEND(v_errors, 'Partition ' || rec.partition_name || ': ' || SUBSTR(SQLERRM, 1, 4000) || CHR(10));
                    DBMS_LOB.APPEND(v_sql_log, '-- FAILED: ' || v_sql || ';' || CHR(10));
            END;
        END LOOP;

        p_sql_executed := v_sql_log;
        p_partitions_moved := v_count;
        p_status := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0 THEN 'WARNING' ELSE 'SUCCESS' END;
        p_error_message := CASE WHEN DBMS_LOB.GETLENGTH(v_errors) > 0
                                THEN DBMS_LOB.SUBSTR(v_errors, 4000, 1)
                                ELSE NULL END;

        DBMS_OUTPUT.PUT_LINE('Moved ' || v_count || ' partition(s) to ' || p_target_tablespace);

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_partitions_moved := v_count;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;

    -- ==========================================================================
    -- SECTION 4B: SINGLE PARTITION OPERATIONS (For ILM Queue Execution)
    -- ==========================================================================

    PROCEDURE compress_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Compress partition
        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                 ' MOVE PARTITION ' || p_partition_name ||
                 ' COMPRESS FOR ' || p_compression_type;

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        -- Rebuild unusable local indexes
        FOR idx IN (
            SELECT ip.index_owner, ip.index_name
            FROM all_ind_partitions ip
            JOIN all_indexes i ON i.owner = ip.index_owner AND i.index_name = ip.index_name
            WHERE i.table_owner = p_table_owner
            AND i.table_name = p_table_name
            AND ip.partition_name = p_partition_name
            AND ip.status = 'UNUSABLE'
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx.index_owner || '.' || idx.index_name ||
                     ' REBUILD PARTITION ' || p_partition_name;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        -- Gather partition statistics
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => p_table_owner,
            tabname => p_table_name,
            partname => p_partition_name,
            granularity => 'PARTITION'
        );
        DBMS_LOB.APPEND(v_sql_log, '-- Gathered statistics on partition ' || p_partition_name || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END compress_single_partition;

    PROCEDURE move_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Move partition
        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                 ' MOVE PARTITION ' || p_partition_name ||
                 ' TABLESPACE ' || p_target_tablespace;

        IF p_compression_type IS NOT NULL THEN
            v_sql := v_sql || ' COMPRESS FOR ' || p_compression_type;
        END IF;

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        -- Rebuild all local indexes in target tablespace
        FOR idx IN (
            SELECT ip.index_owner, ip.index_name
            FROM all_ind_partitions ip
            JOIN all_indexes i ON i.owner = ip.index_owner AND i.index_name = ip.index_name
            WHERE i.table_owner = p_table_owner
            AND i.table_name = p_table_name
            AND ip.partition_name = p_partition_name
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx.index_owner || '.' || idx.index_name ||
                     ' REBUILD PARTITION ' || p_partition_name ||
                     ' TABLESPACE ' || p_target_tablespace;
            EXECUTE IMMEDIATE v_sql;
            DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));
        END LOOP;

        -- Gather partition statistics
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => p_table_owner,
            tabname => p_table_name,
            partname => p_partition_name,
            granularity => 'PARTITION'
        );
        DBMS_LOB.APPEND(v_sql_log, '-- Gathered statistics on partition ' || p_partition_name || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END move_single_partition;

    PROCEDURE drop_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Drop partition
        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                 ' DROP PARTITION ' || p_partition_name;

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END drop_single_partition;

    PROCEDURE truncate_single_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Truncate partition
        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                 ' TRUNCATE PARTITION ' || p_partition_name;

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END truncate_single_partition;

    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_sql_log CLOB;
    BEGIN
        -- Initialize OUT parameters
        p_status := 'PENDING';
        p_error_message := NULL;
        DBMS_LOB.CREATETEMPORARY(p_sql_executed, TRUE);
        DBMS_LOB.CREATETEMPORARY(v_sql_log, TRUE);

        -- Make partition read-only
        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                 ' MODIFY PARTITION ' || p_partition_name || ' READ ONLY';

        EXECUTE IMMEDIATE v_sql;
        DBMS_LOB.APPEND(v_sql_log, v_sql || ';' || CHR(10));

        p_sql_executed := v_sql_log;
        p_status := 'SUCCESS';

    EXCEPTION
        WHEN OTHERS THEN
            p_sql_executed := v_sql_log;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END make_partition_readonly;


    PROCEDURE gather_partition_statistics(
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2 DEFAULT NULL,
        p_degree NUMBER DEFAULT 4,
        p_incremental BOOLEAN DEFAULT TRUE,
        p_partitions_analyzed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_granularity VARCHAR2(30) := 'AUTO';
    BEGIN
        -- Initialize OUT parameters
        p_partitions_analyzed := 0;
        p_status := 'PENDING';
        p_error_message := NULL;

        IF p_incremental THEN
            -- Enable incremental statistics (note: assumes table already partitioned)
            DBMS_STATS.SET_TABLE_PREFS(
                ownname => USER,
                tabname => p_table_name,
                pname => 'INCREMENTAL',
                pvalue => 'TRUE'
            );

            DBMS_STATS.SET_TABLE_PREFS(
                ownname => USER,
                tabname => p_table_name,
                pname => 'INCREMENTAL_STALENESS',
                pvalue => 'USE_STALE_PERCENT'
            );
        END IF;

        IF p_partition_name IS NOT NULL THEN
            -- Gather stats for specific partition
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => USER,
                tabname => p_table_name,
                partname => p_partition_name,
                granularity => 'PARTITION',
                cascade => TRUE,
                degree => p_degree,
                estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
            );
            p_partitions_analyzed := 1;
        ELSE
            -- Gather stats for all partitions
            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => USER,
                tabname => p_table_name,
                granularity => v_granularity,
                cascade => TRUE,
                degree => p_degree,
                estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
            );
            -- Count analyzed partitions
            SELECT COUNT(*) INTO p_partitions_analyzed
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name);
        END IF;

        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Statistics gathered successfully');

    EXCEPTION
        WHEN OTHERS THEN
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE gather_stale_partition_stats(
        p_table_name VARCHAR2,
        p_stale_percent NUMBER DEFAULT 10,
        p_degree NUMBER DEFAULT 4,
        p_partitions_analyzed OUT NUMBER,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
        v_count NUMBER := 0;
    BEGIN
        -- Initialize OUT parameters
        p_partitions_analyzed := 0;
        p_status := 'PENDING';
        p_error_message := NULL;

        -- Set stale percentage threshold
        DBMS_STATS.SET_TABLE_PREFS(
            ownname => USER,
            tabname => p_table_name,
            pname => 'STALE_PERCENT',
            pvalue => TO_CHAR(p_stale_percent)
        );

        -- Gather stats on stale partitions
        FOR rec IN (
            SELECT partition_name, stale_stats
            FROM user_tab_statistics
            WHERE table_name = UPPER(p_table_name)
            AND object_type = 'PARTITION'
            AND stale_stats = 'YES'
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Gathering stats on stale partition: ' || rec.partition_name);

            DBMS_STATS.GATHER_TABLE_STATS(
                ownname => USER,
                tabname => p_table_name,
                partname => rec.partition_name,
                granularity => 'PARTITION',
                cascade => TRUE,
                degree => p_degree
            );

            v_count := v_count + 1;
        END LOOP;

        p_partitions_analyzed := v_count;
        p_status := 'SUCCESS';
        DBMS_OUTPUT.PUT_LINE('Gathered statistics on ' || v_count || ' stale partition(s)');

    EXCEPTION
        WHEN OTHERS THEN
            p_partitions_analyzed := v_count;
            p_status := 'ERROR';
            p_error_message := SQLERRM;
            RAISE;
    END;


    PROCEDURE check_partition_health(
        p_table_name VARCHAR2
    ) AS
        v_issue_count NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('===========================================');
        DBMS_OUTPUT.PUT_LINE('Partition Health Check: ' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('===========================================');

        -- Check for unusable indexes
        FOR rec IN (
            SELECT ip.index_name, ip.partition_name, ip.status
            FROM user_ind_partitions ip
            JOIN user_indexes i ON i.index_name = ip.index_name
            WHERE i.table_name = UPPER(p_table_name)
            AND ip.status = 'UNUSABLE'
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('ISSUE: Unusable index partition - ' ||
                               rec.index_name || '.' || rec.partition_name);
            v_issue_count := v_issue_count + 1;
        END LOOP;

        -- Check for missing statistics
        FOR rec IN (
            SELECT partition_name, num_rows, last_analyzed
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            AND (last_analyzed IS NULL OR last_analyzed < SYSDATE - 30)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('ISSUE: Missing or old statistics - Partition: ' ||
                               rec.partition_name || ', Last Analyzed: ' ||
                               NVL(TO_CHAR(rec.last_analyzed, 'YYYY-MM-DD'), 'NEVER'));
            v_issue_count := v_issue_count + 1;
        END LOOP;

        -- Check for empty partitions (may need cleanup)
        FOR rec IN (
            SELECT partition_name, num_rows
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            AND (num_rows = 0 OR num_rows IS NULL)
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('INFO: Empty partition - ' || rec.partition_name);
        END LOOP;

        -- Check for uncompressed old partitions
        FOR rec IN (
            SELECT tp.partition_name, tp.high_value, tp.compression
            FROM user_tab_partitions tp
            WHERE tp.table_name = UPPER(p_table_name)
            AND tp.compression = 'DISABLED'
            AND tp.partition_position < (
                SELECT MAX(partition_position) - 3
                FROM user_tab_partitions
                WHERE table_name = UPPER(p_table_name)
            )
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('ISSUE: Old partition not compressed - ' || rec.partition_name);
            v_issue_count := v_issue_count + 1;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('===========================================');
        IF v_issue_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Health Check PASSED: No issues found');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Health Check FAILED: ' || v_issue_count || ' issue(s) found');
        END IF;
        DBMS_OUTPUT.PUT_LINE('===========================================');
    END;


    PROCEDURE validate_partition_constraints(
        p_table_name VARCHAR2
    ) AS
        v_error_count NUMBER := 0;
        v_sql VARCHAR2(4000);
        v_count NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Validating partition constraints for: ' || p_table_name);

        FOR rec IN (
            SELECT partition_name
            FROM user_tab_partitions
            WHERE table_name = UPPER(p_table_name)
            ORDER BY partition_position
        ) LOOP
            BEGIN
                -- Check for constraint violations
                FOR cons IN (
                    SELECT constraint_name, search_condition
                    FROM user_constraints
                    WHERE table_name = UPPER(p_table_name)
                    AND constraint_type = 'C'
                    AND status = 'ENABLED'
                ) LOOP
                    v_sql := 'SELECT COUNT(*) FROM ' || p_table_name ||
                            ' PARTITION (' || rec.partition_name || ')' ||
                            ' WHERE NOT (' || cons.search_condition || ')';

                    EXECUTE IMMEDIATE v_sql INTO v_count;

                    IF v_count > 0 THEN
                        DBMS_OUTPUT.PUT_LINE('ERROR: Constraint violation in partition ' ||
                                           rec.partition_name || ' - ' || cons.constraint_name ||
                                           ' (' || v_count || ' rows)');
                        v_error_count := v_error_count + 1;
                    END IF;
                END LOOP;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error validating partition ' || rec.partition_name || ': ' || SQLERRM);
            END;
        END LOOP;

        IF v_error_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('Validation PASSED: No constraint violations found');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Validation FAILED: ' || v_error_count || ' violation(s) found');
        END IF;
    END;


    PROCEDURE partition_size_report(
        p_table_name VARCHAR2
    ) AS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Partition Size Report: ' || p_table_name);
        DBMS_OUTPUT.PUT_LINE(RPAD('=', 120, '='));
        DBMS_OUTPUT.PUT_LINE(RPAD('Partition', 30) || RPAD('Rows', 15) ||
                            RPAD('Size (MB)', 15) || RPAD('Compression', 20) ||
                            RPAD('Tablespace', 25) || 'Read Only');
        DBMS_OUTPUT.PUT_LINE(RPAD('=', 120, '='));

        FOR rec IN (
            SELECT
                tp.partition_name,
                tp.num_rows,
                ROUND(s.bytes/1024/1024, 2) AS size_mb,
                tp.compression || ' ' || NVL(tp.compress_for, '') AS compression_info,
                s.tablespace_name,
                tp.read_only
            FROM user_tab_partitions tp
            LEFT JOIN user_segments s
                ON s.segment_name = tp.table_name
                AND s.partition_name = tp.partition_name
            WHERE tp.table_name = UPPER(p_table_name)
            ORDER BY tp.partition_position DESC
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                RPAD(NVL(rec.partition_name, 'N/A'), 30) ||
                RPAD(NVL(TO_CHAR(rec.num_rows, '999,999,999'), 'N/A'), 15) ||
                RPAD(NVL(TO_CHAR(rec.size_mb, '999,999.99'), 'N/A'), 15) ||
                RPAD(NVL(rec.compression_info, 'NONE'), 20) ||
                RPAD(NVL(rec.tablespace_name, 'N/A'), 25) ||
                NVL(rec.read_only, 'NO')
            );
        END LOOP;

        DBMS_OUTPUT.PUT_LINE(RPAD('=', 120, '='));
    END;

END pck_dwh_partition_utilities;
/
