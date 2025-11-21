-- =============================================================================
-- ILM-Aware Partition Pre-Creation Utility
-- Creates future HOT tier partitions based on ILM template configuration
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_partition_precreation AUTHID CURRENT_USER AS

    -- Pre-create HOT tier partitions for next period
    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

    -- Pre-create for all tables with ILM templates
    PROCEDURE precreate_all_hot_partitions;

    -- Preview what partitions would be created (no actual creation)
    PROCEDURE preview_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

END pck_dwh_partition_precreation;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_partition_precreation AS

    -- ==========================================================================
    -- Private Helper Functions
    -- ==========================================================================

    FUNCTION get_ilm_template_name(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN VARCHAR2 AS
        v_template_name VARCHAR2(100);
    BEGIN
        -- Try to find template from migration tasks
        SELECT ilm_policy_template
        INTO v_template_name
        FROM cmr.dwh_migration_tasks
        WHERE source_owner = p_table_owner
        AND source_table = p_table_name
        AND ilm_policy_template IS NOT NULL
        AND ROWNUM = 1;

        RETURN v_template_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END get_ilm_template_name;


    FUNCTION get_hot_tier_config(
        p_template_name VARCHAR2,
        p_config_item VARCHAR2
    ) RETURN VARCHAR2 AS
        v_result VARCHAR2(100);
    BEGIN
        SELECT JSON_VALUE(policies_json, '$.tier_config.hot.' || p_config_item)
        INTO v_result
        FROM cmr.dwh_migration_ilm_templates
        WHERE template_name = p_template_name;

        RETURN v_result;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END get_hot_tier_config;


    FUNCTION partition_exists(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) RETURN BOOLEAN AS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
        INTO v_count
        FROM dba_tab_partitions
        WHERE table_owner = p_table_owner
        AND table_name = p_table_name
        AND partition_name = p_partition_name;

        RETURN v_count > 0;
    END partition_exists;


    FUNCTION get_compression_clause(p_compression VARCHAR2) RETURN VARCHAR2 AS
    BEGIN
        IF p_compression IS NULL OR UPPER(p_compression) = 'NONE' THEN
            RETURN '';
        ELSIF UPPER(p_compression) = 'BASIC' THEN
            RETURN ' COMPRESS BASIC';
        ELSIF UPPER(p_compression) = 'OLTP' THEN
            RETURN ' COMPRESS FOR OLTP';
        ELSIF UPPER(p_compression) IN ('QUERY', 'QUERY LOW', 'QUERY HIGH') THEN
            RETURN ' COMPRESS FOR ' || UPPER(p_compression);
        ELSIF UPPER(p_compression) IN ('ARCHIVE', 'ARCHIVE LOW', 'ARCHIVE HIGH') THEN
            RETURN ' COMPRESS FOR ' || UPPER(p_compression);
        ELSE
            RETURN '';
        END IF;
    END get_compression_clause;


    -- ==========================================================================
    -- Main Procedures
    -- ==========================================================================

    PROCEDURE precreate_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) AS
        v_template_name VARCHAR2(100);
        v_interval VARCHAR2(20);
        v_tablespace VARCHAR2(128);
        v_compression VARCHAR2(50);
        v_pctfree NUMBER;

        v_current_date DATE := SYSDATE;
        v_start_date DATE;
        v_end_date DATE;
        v_partition_date DATE;
        v_next_date DATE;
        v_partition_name VARCHAR2(128);
        v_ddl VARCHAR2(4000);
        v_created_count NUMBER := 0;
        v_skipped_count NUMBER := 0;

    BEGIN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Pre-creating HOT tier partitions');
        DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_owner || '.' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('========================================');

        -- Get ILM template
        v_template_name := get_ilm_template_name(p_table_owner, p_table_name);

        IF v_template_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: No ILM template found for this table');
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('ILM Template: ' || v_template_name);

        -- Get HOT tier configuration
        v_interval := get_hot_tier_config(v_template_name, 'interval');
        v_tablespace := get_hot_tier_config(v_template_name, 'tablespace');
        v_compression := get_hot_tier_config(v_template_name, 'compression');
        v_pctfree := TO_NUMBER(NVL(get_hot_tier_config(v_template_name, 'pctfree'), '10'));

        IF v_interval IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: HOT tier interval not found in template');
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('HOT tier interval: ' || v_interval);
        DBMS_OUTPUT.PUT_LINE('HOT tier tablespace: ' || v_tablespace);
        DBMS_OUTPUT.PUT_LINE('HOT tier compression: ' || NVL(v_compression, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('');

        -- Determine date range based on interval
        IF UPPER(v_interval) = 'MONTHLY' THEN
            -- Pre-create from next month through end of next year
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := ADD_MONTHS(TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY'), 12);

            DBMS_OUTPUT.PUT_LINE('Creating monthly partitions:');
            DBMS_OUTPUT.PUT_LINE('  From: ' || TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' (next month)');
            DBMS_OUTPUT.PUT_LINE('  To: ' || TO_CHAR(v_end_date, 'YYYY-MM-DD') || ' (end of next year)');
            DBMS_OUTPUT.PUT_LINE('');

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');

                IF NOT partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    v_ddl := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                            ' ADD PARTITION ' || v_partition_name ||
                            ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                            ' TABLESPACE ' || v_tablespace ||
                            get_compression_clause(v_compression) ||
                            ' PCTFREE ' || v_pctfree;

                    DBMS_OUTPUT.PUT_LINE('Creating: ' || v_partition_name);
                    EXECUTE IMMEDIATE v_ddl;
                    v_created_count := v_created_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Skipped: ' || v_partition_name || ' (already exists)');
                    v_skipped_count := v_skipped_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;

        ELSIF UPPER(v_interval) = 'DAILY' THEN
            -- Pre-create for next month
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := LAST_DAY(v_start_date) + 1;

            DBMS_OUTPUT.PUT_LINE('Creating daily partitions:');
            DBMS_OUTPUT.PUT_LINE('  From: ' || TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' (next month)');
            DBMS_OUTPUT.PUT_LINE('  To: ' || TO_CHAR(v_end_date - 1, 'YYYY-MM-DD') || ' (end of next month)');
            DBMS_OUTPUT.PUT_LINE('');

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := v_partition_date + 1;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM_DD');

                IF NOT partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    v_ddl := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                            ' ADD PARTITION ' || v_partition_name ||
                            ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                            ' TABLESPACE ' || v_tablespace ||
                            get_compression_clause(v_compression) ||
                            ' PCTFREE ' || v_pctfree;

                    DBMS_OUTPUT.PUT_LINE('Creating: ' || v_partition_name);
                    EXECUTE IMMEDIATE v_ddl;
                    v_created_count := v_created_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Skipped: ' || v_partition_name || ' (already exists)');
                    v_skipped_count := v_skipped_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;

        ELSIF UPPER(v_interval) = 'WEEKLY' THEN
            -- Pre-create for next month
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'IW');
            v_end_date := LAST_DAY(ADD_MONTHS(v_current_date, 1)) + 1;

            DBMS_OUTPUT.PUT_LINE('Creating weekly partitions:');
            DBMS_OUTPUT.PUT_LINE('  From: ' || TO_CHAR(v_start_date, 'YYYY-MM-DD') || ' (next month)');
            DBMS_OUTPUT.PUT_LINE('  To: ' || TO_CHAR(v_end_date - 1, 'YYYY-MM-DD') || ' (end of next month)');
            DBMS_OUTPUT.PUT_LINE('');

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := v_partition_date + 7;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'IYYY_IW');

                IF NOT partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    v_ddl := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                            ' ADD PARTITION ' || v_partition_name ||
                            ' VALUES LESS THAN (TO_DATE(''' || TO_CHAR(v_next_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD''))' ||
                            ' TABLESPACE ' || v_tablespace ||
                            get_compression_clause(v_compression) ||
                            ' PCTFREE ' || v_pctfree;

                    DBMS_OUTPUT.PUT_LINE('Creating: ' || v_partition_name);
                    EXECUTE IMMEDIATE v_ddl;
                    v_created_count := v_created_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Skipped: ' || v_partition_name || ' (already exists)');
                    v_skipped_count := v_skipped_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;

        ELSE
            DBMS_OUTPUT.PUT_LINE('ERROR: Unsupported interval: ' || v_interval);
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Summary:');
        DBMS_OUTPUT.PUT_LINE('  Created: ' || v_created_count || ' partition(s)');
        DBMS_OUTPUT.PUT_LINE('  Skipped: ' || v_skipped_count || ' partition(s)');
        DBMS_OUTPUT.PUT_LINE('========================================');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('Error Stack: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
            RAISE;
    END precreate_hot_partitions;


    PROCEDURE precreate_all_hot_partitions AS
        v_count NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Pre-creating HOT partitions for all tables with ILM templates');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('');

        FOR rec IN (
            SELECT DISTINCT source_owner, source_table
            FROM cmr.dwh_migration_tasks
            WHERE ilm_policy_template IS NOT NULL
            AND status = 'COMPLETED'
            ORDER BY source_owner, source_table
        ) LOOP
            BEGIN
                v_count := v_count + 1;
                precreate_hot_partitions(rec.source_owner, rec.source_table);
                DBMS_OUTPUT.PUT_LINE('');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR processing ' || rec.source_owner || '.' || rec.source_table || ': ' || SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('');
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Processed ' || v_count || ' table(s)');
        DBMS_OUTPUT.PUT_LINE('========================================');
    END precreate_all_hot_partitions;


    PROCEDURE preview_hot_partitions(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) AS
        v_template_name VARCHAR2(100);
        v_interval VARCHAR2(20);
        v_tablespace VARCHAR2(128);
        v_compression VARCHAR2(50);

        v_current_date DATE := SYSDATE;
        v_start_date DATE;
        v_end_date DATE;
        v_partition_date DATE;
        v_next_date DATE;
        v_partition_name VARCHAR2(128);
        v_total_count NUMBER := 0;
        v_exists_count NUMBER := 0;
        v_new_count NUMBER := 0;

    BEGIN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('PREVIEW: HOT tier partitions to be created');
        DBMS_OUTPUT.PUT_LINE('Table: ' || p_table_owner || '.' || p_table_name);
        DBMS_OUTPUT.PUT_LINE('========================================');

        -- Get ILM template
        v_template_name := get_ilm_template_name(p_table_owner, p_table_name);

        IF v_template_name IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: No ILM template found for this table');
            RETURN;
        END IF;

        DBMS_OUTPUT.PUT_LINE('ILM Template: ' || v_template_name);

        -- Get HOT tier configuration
        v_interval := get_hot_tier_config(v_template_name, 'interval');
        v_tablespace := get_hot_tier_config(v_template_name, 'tablespace');
        v_compression := get_hot_tier_config(v_template_name, 'compression');

        DBMS_OUTPUT.PUT_LINE('HOT tier interval: ' || v_interval);
        DBMS_OUTPUT.PUT_LINE('HOT tier tablespace: ' || v_tablespace);
        DBMS_OUTPUT.PUT_LINE('HOT tier compression: ' || NVL(v_compression, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('');

        -- Determine date range based on interval
        IF UPPER(v_interval) = 'MONTHLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := ADD_MONTHS(TRUNC(ADD_MONTHS(v_current_date, 12), 'YYYY'), 12);

            DBMS_OUTPUT.PUT_LINE('Monthly partitions to create:');
            DBMS_OUTPUT.PUT_LINE(RPAD('-', 60, '-'));

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := ADD_MONTHS(v_partition_date, 1);
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM');
                v_total_count := v_total_count + 1;

                IF partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [EXISTS]');
                    v_exists_count := v_exists_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [NEW]');
                    v_new_count := v_new_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;

        ELSIF UPPER(v_interval) = 'DAILY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'MM');
            v_end_date := LAST_DAY(v_start_date) + 1;

            DBMS_OUTPUT.PUT_LINE('Daily partitions to create (next month):');
            DBMS_OUTPUT.PUT_LINE(RPAD('-', 60, '-'));

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := v_partition_date + 1;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'YYYY_MM_DD');
                v_total_count := v_total_count + 1;

                IF partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [EXISTS]');
                    v_exists_count := v_exists_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [NEW]');
                    v_new_count := v_new_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;

        ELSIF UPPER(v_interval) = 'WEEKLY' THEN
            v_start_date := TRUNC(ADD_MONTHS(v_current_date, 1), 'IW');
            v_end_date := LAST_DAY(ADD_MONTHS(v_current_date, 1)) + 1;

            DBMS_OUTPUT.PUT_LINE('Weekly partitions to create (next month):');
            DBMS_OUTPUT.PUT_LINE(RPAD('-', 60, '-'));

            v_partition_date := v_start_date;
            WHILE v_partition_date < v_end_date LOOP
                v_next_date := v_partition_date + 7;
                v_partition_name := 'P_' || TO_CHAR(v_partition_date, 'IYYY_IW');
                v_total_count := v_total_count + 1;

                IF partition_exists(p_table_owner, p_table_name, v_partition_name) THEN
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [EXISTS]');
                    v_exists_count := v_exists_count + 1;
                ELSE
                    DBMS_OUTPUT.PUT_LINE(RPAD(v_partition_name, 20) || ' [NEW]');
                    v_new_count := v_new_count + 1;
                END IF;

                v_partition_date := v_next_date;
            END LOOP;
        END IF;

        DBMS_OUTPUT.PUT_LINE(RPAD('-', 60, '-'));
        DBMS_OUTPUT.PUT_LINE('Total: ' || v_total_count || ' partition(s)');
        DBMS_OUTPUT.PUT_LINE('  - Already exist: ' || v_exists_count);
        DBMS_OUTPUT.PUT_LINE('  - Will be created: ' || v_new_count);
        DBMS_OUTPUT.PUT_LINE('========================================');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
    END preview_hot_partitions;

END pck_dwh_partition_precreation;
/
