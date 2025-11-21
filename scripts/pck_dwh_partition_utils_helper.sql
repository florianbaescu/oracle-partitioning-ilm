-- =============================================================================
-- Package: pck_dwh_partition_utils_helper
-- Description: Helper functions for partition utilities (validation, logging)
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_partition_utils_helper AUTHID CURRENT_USER AS

    -- Validate if partition names follow framework patterns
    FUNCTION validate_partition_naming(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_sample_size NUMBER DEFAULT 10
    ) RETURN VARCHAR2;  -- Returns: 'COMPLIANT', 'NON_COMPLIANT', 'MIXED', 'UNKNOWN'

    -- Detect partition interval from existing partitions
    PROCEDURE detect_partition_config(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_interval OUT VARCHAR2,           -- DAILY, WEEKLY, MONTHLY, YEARLY
        p_tablespace OUT VARCHAR2,
        p_compression OUT VARCHAR2,
        p_config_source OUT VARCHAR2       -- AUTO_DETECTED, ILM_POLICY, CONFIG_OVERRIDE
    );

    -- Log operation start
    FUNCTION log_operation_start(
        p_operation_name VARCHAR2,
        p_operation_type VARCHAR2,
        p_table_owner VARCHAR2 DEFAULT NULL,
        p_table_name VARCHAR2 DEFAULT NULL,
        p_partition_name VARCHAR2 DEFAULT NULL,
        p_message VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;  -- Returns log_id

    -- Log operation end
    PROCEDURE log_operation_end(
        p_log_id NUMBER,
        p_status VARCHAR2,                 -- SUCCESS, WARNING, ERROR, SKIPPED
        p_partitions_created NUMBER DEFAULT 0,
        p_partitions_modified NUMBER DEFAULT 0,
        p_partitions_skipped NUMBER DEFAULT 0,
        p_rows_affected NUMBER DEFAULT NULL,
        p_config_source VARCHAR2 DEFAULT NULL,
        p_interval_type VARCHAR2 DEFAULT NULL,
        p_message VARCHAR2 DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL,
        p_sql_statement CLOB DEFAULT NULL
    );

    -- Check if partition name follows framework pattern
    FUNCTION is_framework_partition_name(
        p_partition_name VARCHAR2
    ) RETURN BOOLEAN;

    -- Parse partition name to detect interval type
    FUNCTION detect_interval_from_name(
        p_partition_name VARCHAR2
    ) RETURN VARCHAR2;  -- Returns: DAILY, WEEKLY, MONTHLY, YEARLY, UNKNOWN

END pck_dwh_partition_utils_helper;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_partition_utils_helper AS

    -- ==========================================================================
    -- PARTITION NAME VALIDATION
    -- ==========================================================================

    FUNCTION is_framework_partition_name(
        p_partition_name VARCHAR2
    ) RETURN BOOLEAN IS
    BEGIN
        -- Framework patterns:
        -- P_YYYY (YEARLY)
        -- P_YYYY_MM (MONTHLY)
        -- P_YYYY_MM_DD (DAILY)
        -- P_IYYY_IW (WEEKLY - ISO year and week)

        IF p_partition_name IS NULL THEN
            RETURN FALSE;
        END IF;

        -- Check YEARLY: P_2024
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}$') THEN
            RETURN TRUE;
        END IF;

        -- Check MONTHLY: P_2024_11
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}_[0-9]{2}$') THEN
            RETURN TRUE;
        END IF;

        -- Check DAILY: P_2024_11_21
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}_[0-9]{2}_[0-9]{2}$') THEN
            RETURN TRUE;
        END IF;

        -- Check WEEKLY: P_2024_47 (ISO year and week number)
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}_[0-9]{2}$') THEN
            -- Could be MONTHLY or WEEKLY - need context to distinguish
            RETURN TRUE;
        END IF;

        RETURN FALSE;
    END is_framework_partition_name;

    FUNCTION detect_interval_from_name(
        p_partition_name VARCHAR2
    ) RETURN VARCHAR2 IS
    BEGIN
        IF p_partition_name IS NULL THEN
            RETURN 'UNKNOWN';
        END IF;

        -- P_YYYY (YEARLY)
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}$') THEN
            RETURN 'YEARLY';
        END IF;

        -- P_YYYY_MM_DD (DAILY)
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}_[0-9]{2}_[0-9]{2}$') THEN
            RETURN 'DAILY';
        END IF;

        -- P_YYYY_MM (MONTHLY) - ambiguous with WEEKLY
        -- Assume MONTHLY by default for this pattern
        IF REGEXP_LIKE(p_partition_name, '^P_[0-9]{4}_[0-9]{2}$') THEN
            -- Check if value could be a month (01-12) or week (01-53)
            DECLARE
                v_second_part NUMBER;
            BEGIN
                v_second_part := TO_NUMBER(SUBSTR(p_partition_name, 8, 2));
                IF v_second_part <= 12 THEN
                    RETURN 'MONTHLY';  -- Could be both, default to MONTHLY
                ELSE
                    RETURN 'WEEKLY';   -- Definitely weekly (> 12)
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN 'UNKNOWN';
            END;
        END IF;

        RETURN 'UNKNOWN';
    END detect_interval_from_name;

    FUNCTION validate_partition_naming(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_sample_size NUMBER DEFAULT 10
    ) RETURN VARCHAR2 IS
        v_total NUMBER := 0;
        v_compliant NUMBER := 0;
        v_non_compliant NUMBER := 0;
    BEGIN
        -- Check recent partitions for naming compliance
        FOR rec IN (
            SELECT partition_name
            FROM all_tab_partitions
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            ORDER BY partition_position DESC
            FETCH FIRST p_sample_size ROWS ONLY
        ) LOOP
            v_total := v_total + 1;

            IF is_framework_partition_name(rec.partition_name) THEN
                v_compliant := v_compliant + 1;
            ELSE
                v_non_compliant := v_non_compliant + 1;
            END IF;
        END LOOP;

        IF v_total = 0 THEN
            RETURN 'UNKNOWN';
        ELSIF v_compliant = v_total THEN
            RETURN 'COMPLIANT';
        ELSIF v_non_compliant = v_total THEN
            RETURN 'NON_COMPLIANT';
        ELSE
            RETURN 'MIXED';
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'UNKNOWN';
    END validate_partition_naming;

    -- ==========================================================================
    -- PARTITION CONFIGURATION DETECTION
    -- ==========================================================================

    PROCEDURE detect_partition_config(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_interval OUT VARCHAR2,
        p_tablespace OUT VARCHAR2,
        p_compression OUT VARCHAR2,
        p_config_source OUT VARCHAR2
    ) IS
        v_count NUMBER;
        v_interval_detected VARCHAR2(20);
    BEGIN
        -- Priority 1: Check manual configuration override
        BEGIN
            SELECT partition_interval, tablespace, compression, 'CONFIG_OVERRIDE'
            INTO p_interval, p_tablespace, p_compression, p_config_source
            FROM cmr.dwh_partition_precreation_config
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            AND enabled = 'Y';

            RETURN;  -- Found override, use it
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;  -- Continue to next priority
        END;

        -- Priority 2: Check if table has active ILM policy
        BEGIN
            SELECT COUNT(*)
            INTO v_count
            FROM cmr.dwh_ilm_policies
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            AND status = 'ACTIVE';

            IF v_count > 0 THEN
                -- Has ILM policy - get config from most recent partition (assumed HOT)
                SELECT
                    detect_interval_from_name(partition_name),
                    tablespace_name,
                    CASE
                        WHEN compression = 'DISABLED' THEN NULL
                        ELSE compression || CASE WHEN compress_for IS NOT NULL
                                                 THEN ' FOR ' || compress_for
                                                 ELSE '' END
                    END,
                    'ILM_POLICY'
                INTO p_interval, p_tablespace, p_compression, p_config_source
                FROM all_tab_partitions
                WHERE table_owner = UPPER(p_table_owner)
                AND table_name = UPPER(p_table_name)
                ORDER BY partition_position DESC
                FETCH FIRST 1 ROW ONLY;

                RETURN;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                NULL;  -- Continue to auto-detect
        END;

        -- Priority 3: Auto-detect from table structure
        BEGIN
            -- Get settings from most recent partition
            SELECT
                detect_interval_from_name(partition_name),
                tablespace_name,
                CASE
                    WHEN compression = 'DISABLED' THEN NULL
                    ELSE compression || CASE WHEN compress_for IS NOT NULL
                                             THEN ' ' || compress_for
                                             ELSE '' END
                END,
                'AUTO_DETECTED'
            INTO p_interval, p_tablespace, p_compression, p_config_source
            FROM all_tab_partitions
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            ORDER BY partition_position DESC
            FETCH FIRST 1 ROW ONLY;

            -- If interval is still unknown, try to detect from HIGH_VALUE intervals
            IF p_interval = 'UNKNOWN' OR p_interval IS NULL THEN
                -- Calculate average days between partitions
                DECLARE
                    v_avg_days NUMBER;
                BEGIN
                    SELECT AVG(days_between)
                    INTO v_avg_days
                    FROM (
                        SELECT
                            LEAD(partition_position) OVER (ORDER BY partition_position) - partition_position as days_between
                        FROM all_tab_partitions
                        WHERE table_owner = UPPER(p_table_owner)
                        AND table_name = UPPER(p_table_name)
                        ORDER BY partition_position DESC
                        FETCH FIRST 5 ROWS ONLY
                    )
                    WHERE days_between IS NOT NULL;

                    IF v_avg_days IS NOT NULL THEN
                        IF v_avg_days < 3 THEN
                            p_interval := 'DAILY';
                        ELSIF v_avg_days BETWEEN 5 AND 9 THEN
                            p_interval := 'WEEKLY';
                        ELSIF v_avg_days BETWEEN 25 AND 35 THEN
                            p_interval := 'MONTHLY';
                        ELSIF v_avg_days > 300 THEN
                            p_interval := 'YEARLY';
                        END IF;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL;  -- Keep interval as UNKNOWN
                END;
            END IF;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                p_interval := 'UNKNOWN';
                p_tablespace := NULL;
                p_compression := NULL;
                p_config_source := 'NOT_DETECTED';
        END;

    END detect_partition_config;

    -- ==========================================================================
    -- LOGGING FUNCTIONS
    -- ==========================================================================

    FUNCTION log_operation_start(
        p_operation_name VARCHAR2,
        p_operation_type VARCHAR2,
        p_table_owner VARCHAR2 DEFAULT NULL,
        p_table_name VARCHAR2 DEFAULT NULL,
        p_partition_name VARCHAR2 DEFAULT NULL,
        p_message VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_log_id NUMBER;
    BEGIN
        INSERT INTO cmr.dwh_partition_utilities_log (
            operation_name,
            operation_type,
            table_owner,
            table_name,
            partition_name,
            start_time,
            status,
            message
        ) VALUES (
            p_operation_name,
            p_operation_type,
            p_table_owner,
            p_table_name,
            p_partition_name,
            SYSTIMESTAMP,
            'RUNNING',
            p_message
        ) RETURNING log_id INTO v_log_id;

        COMMIT;
        RETURN v_log_id;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            -- If logging fails, don't fail the operation
            DBMS_OUTPUT.PUT_LINE('WARNING: Failed to log operation start: ' || SQLERRM);
            RETURN NULL;
    END log_operation_start;

    PROCEDURE log_operation_end(
        p_log_id NUMBER,
        p_status VARCHAR2,
        p_partitions_created NUMBER DEFAULT 0,
        p_partitions_modified NUMBER DEFAULT 0,
        p_partitions_skipped NUMBER DEFAULT 0,
        p_rows_affected NUMBER DEFAULT NULL,
        p_config_source VARCHAR2 DEFAULT NULL,
        p_interval_type VARCHAR2 DEFAULT NULL,
        p_message VARCHAR2 DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL,
        p_sql_statement CLOB DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_duration NUMBER;
    BEGIN
        IF p_log_id IS NULL THEN
            RETURN;  -- No log to update
        END IF;

        UPDATE cmr.dwh_partition_utilities_log
        SET end_time = SYSTIMESTAMP,
            duration_seconds = EXTRACT(SECOND FROM (SYSTIMESTAMP - start_time)) +
                             EXTRACT(MINUTE FROM (SYSTIMESTAMP - start_time)) * 60 +
                             EXTRACT(HOUR FROM (SYSTIMESTAMP - start_time)) * 3600,
            status = p_status,
            partitions_created = p_partitions_created,
            partitions_modified = p_partitions_modified,
            partitions_skipped = p_partitions_skipped,
            rows_affected = p_rows_affected,
            config_source = p_config_source,
            interval_type = p_interval_type,
            message = NVL(p_message, message),  -- Keep original if new is null
            error_message = p_error_message,
            sql_statement = p_sql_statement
        WHERE log_id = p_log_id;

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('WARNING: Failed to log operation end: ' || SQLERRM);
    END log_operation_end;

END pck_dwh_partition_utils_helper;
/
