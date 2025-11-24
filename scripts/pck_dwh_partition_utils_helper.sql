-- =============================================================================
-- Package: pck_dwh_partition_utils_helper
-- Description: Helper functions for partition utilities (validation only)
-- Note: Logging is handled by ILM execution engine, not by utilities
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
    BEGIN
        -- Priority 1: Check if table has active ILM policy
        BEGIN
            SELECT COUNT(*)
            INTO v_count
            FROM cmr.dwh_ilm_policies
            WHERE table_owner = UPPER(p_table_owner)
            AND table_name = UPPER(p_table_name)
            AND enabled = 'Y';

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

        -- Priority 2: Auto-detect from table structure
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

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                p_interval := 'UNKNOWN';
                p_tablespace := NULL;
                p_compression := NULL;
                p_config_source := 'NOT_DETECTED';
        END;

    END detect_partition_config;

END pck_dwh_partition_utils_helper;
/
