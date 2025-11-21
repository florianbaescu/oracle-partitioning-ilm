-- =============================================================================
-- Package: pck_dwh_partition_utilities (Version 2 with Validation & Logging)
-- Description: Enhanced partition utilities with validation and logging
-- Dependencies:
--   - pck_dwh_partition_utils_helper
--   - cmr.dwh_partition_utilities_log
-- =============================================================================

-- NOTE: This file shows the refactored precreate_hot_partitions procedure
-- with validation and logging. The full package will need similar updates
-- for all procedures.

CREATE OR REPLACE PACKAGE pck_dwh_partition_utilities AUTHID CURRENT_USER AS

    -- ==========================================================================
    -- SECTION 1: ILM-AWARE PARTITION PRE-CREATION
    -- ==========================================================================

    -- Pre-create HOT tier partitions for next period
    -- Now with validation, auto-detection, and logging
    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

    -- Pre-create HOT tier partitions for all tables with active ILM policies
    PROCEDURE precreate_all_hot_partitions;

    -- Preview what HOT tier partitions would be created (no execution)
    PROCEDURE preview_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

    -- [... rest of procedures from previous version ...]

END pck_dwh_partition_utilities;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_partition_utilities AS

    -- ==========================================================================
    -- SECTION 1: ILM-AWARE PARTITION PRE-CREATION (REFACTORED)
    -- ==========================================================================

    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) AS
        v_log_id NUMBER;
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
        v_count NUMBER;
        v_created_count NUMBER := 0;
        v_skipped_count NUMBER := 0;
        v_warning_msg VARCHAR2(4000);

    BEGIN
        -- Start logging
        v_log_id := pck_dwh_partition_utils_helper.log_operation_start(
            p_operation_name => 'precreate_hot_partitions',
            p_operation_type => 'PRECREATION',
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_message => 'Starting HOT tier partition pre-creation'
        );

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
            v_warning_msg := 'WARNING: Table partitions do NOT follow framework naming patterns. ' ||
                           'Pre-creation may fail or create inconsistent partition names. ' ||
                           'Framework patterns: P_YYYY, P_YYYY_MM, P_YYYY_MM_DD, P_IYYY_IW';

            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('⚠️  ' || v_warning_msg);
            DBMS_OUTPUT.PUT_LINE('');

            -- Log warning and skip
            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'SKIPPED',
                p_partitions_skipped => 1,
                p_message => v_warning_msg
            );

            DBMS_OUTPUT.PUT_LINE('Operation SKIPPED due to non-compliant partition naming.');
            DBMS_OUTPUT.PUT_LINE('=========================================');
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
            v_warning_msg := 'Cannot detect partition interval type. Please add configuration to ' ||
                           'cmr.dwh_partition_precreation_config table.';

            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_warning_msg);

            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'ERROR',
                p_config_source => v_config_source,
                p_error_message => v_warning_msg
            );

            DBMS_OUTPUT.PUT_LINE('=========================================');
            RETURN;
        END IF;

        IF v_tablespace IS NULL THEN
            v_warning_msg := 'No tablespace detected. Please specify in cmr.dwh_partition_precreation_config.';

            DBMS_OUTPUT.PUT_LINE('ERROR: ' || v_warning_msg);

            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'ERROR',
                p_config_source => v_config_source,
                p_interval_type => v_interval,
                p_error_message => v_warning_msg
            );

            DBMS_OUTPUT.PUT_LINE('=========================================');
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

            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'ERROR',
                p_config_source => v_config_source,
                p_interval_type => v_interval,
                p_error_message => v_warning_msg
            );

            DBMS_OUTPUT.PUT_LINE('=========================================');
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
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
                        v_warning_msg := v_warning_msg || CHR(10) ||
                                       'Failed to create ' || v_partition_name || ': ' || SQLERRM;
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

        -- Log success
        pck_dwh_partition_utils_helper.log_operation_end(
            p_log_id => v_log_id,
            p_status => CASE WHEN v_warning_msg IS NOT NULL THEN 'WARNING' ELSE 'SUCCESS' END,
            p_partitions_created => v_created_count,
            p_partitions_skipped => v_skipped_count,
            p_config_source => v_config_source,
            p_interval_type => v_interval,
            p_message => 'Created ' || v_created_count || ' partitions, skipped ' || v_skipped_count,
            p_error_message => v_warning_msg
        );

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('=========================================');

            -- Log error
            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'ERROR',
                p_partitions_created => v_created_count,
                p_partitions_skipped => v_skipped_count,
                p_config_source => v_config_source,
                p_interval_type => v_interval,
                p_error_message => SQLERRM
            );

            RAISE;
    END precreate_hot_partitions;

    PROCEDURE precreate_all_hot_partitions AS
        v_log_id NUMBER;
        v_count NUMBER := 0;
        v_success_count NUMBER := 0;
        v_error_count NUMBER := 0;
    BEGIN
        -- Start batch logging
        v_log_id := pck_dwh_partition_utils_helper.log_operation_start(
            p_operation_name => 'precreate_all_hot_partitions',
            p_operation_type => 'BATCH_PRECREATION',
            p_message => 'Starting batch pre-creation for all ILM-managed tables'
        );

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
            WHERE p.status = 'ACTIVE'
            ORDER BY p.table_owner, p.table_name
        ) LOOP
            BEGIN
                v_count := v_count + 1;
                DBMS_OUTPUT.PUT_LINE('Processing: ' || rec.table_owner || '.' || rec.table_name);
                DBMS_OUTPUT.PUT_LINE('');

                precreate_hot_partitions(rec.table_owner, rec.table_name);
                v_success_count := v_success_count + 1;
                DBMS_OUTPUT.PUT_LINE('');

            EXCEPTION
                WHEN OTHERS THEN
                    v_error_count := v_error_count + 1;
                    DBMS_OUTPUT.PUT_LINE('ERROR processing ' || rec.table_owner || '.' ||
                        rec.table_name || ': ' || SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('');
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('=========================================');
        DBMS_OUTPUT.PUT_LINE('Batch pre-creation complete');
        DBMS_OUTPUT.PUT_LINE('Tables processed: ' || v_count);
        DBMS_OUTPUT.PUT_LINE('Success: ' || v_success_count);
        DBMS_OUTPUT.PUT_LINE('Errors: ' || v_error_count);
        DBMS_OUTPUT.PUT_LINE('=========================================');

        -- Log batch completion
        pck_dwh_partition_utils_helper.log_operation_end(
            p_log_id => v_log_id,
            p_status => CASE WHEN v_error_count > 0 THEN 'WARNING' ELSE 'SUCCESS' END,
            p_message => 'Processed ' || v_count || ' tables: ' || v_success_count ||
                        ' success, ' || v_error_count || ' errors'
        );

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('FATAL ERROR in batch operation: ' || SQLERRM);

            pck_dwh_partition_utils_helper.log_operation_end(
                p_log_id => v_log_id,
                p_status => 'ERROR',
                p_error_message => SQLERRM
            );

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

    -- [... rest of the procedures from previous version would go here ...]
    -- (truncate_old_partitions, compress_partitions, etc.)
    -- Each should be enhanced with similar logging using the helper package

END pck_dwh_partition_utilities;
/
