-- =============================================================================
-- Table Migration Analysis Package
-- Analyzes tables and recommends optimal partitioning strategies
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_table_migration_analyzer AUTHID CURRENT_USER AS
    -- Main analysis procedures
    PROCEDURE analyze_table(
        p_task_id NUMBER
    );

    PROCEDURE analyze_all_pending_tasks(
        p_project_id NUMBER DEFAULT NULL
    );

    -- Analysis helper functions
    FUNCTION recommend_partition_strategy(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_reason OUT VARCHAR2
    ) RETURN VARCHAR2;

    FUNCTION estimate_partition_count(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_partition_type VARCHAR2
    ) RETURN NUMBER;

    FUNCTION calculate_complexity_score(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN NUMBER;

    -- Data distribution analysis
    PROCEDURE analyze_column_distribution(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2,
        p_distinct_values OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_distribution_type OUT VARCHAR2
    );

    -- Dependency analysis
    FUNCTION get_dependent_objects(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN CLOB;

    FUNCTION identify_blocking_issues(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN CLOB;

    -- Utility functions
    FUNCTION get_table_size_mb(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN NUMBER;

END pck_dwh_table_migration_analyzer;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_table_migration_analyzer AUTHID CURRENT_USER AS

    -- ==========================================================================
    -- Private Helper Functions
    -- ==========================================================================

    FUNCTION get_date_columns(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN SYS.ODCIVARCHAR2LIST
    AS
        v_columns SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
    BEGIN
        SELECT column_name
        BULK COLLECT INTO v_columns
        FROM dba_tab_columns
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
        ORDER BY column_id;

        RETURN v_columns;
    END get_date_columns;


    FUNCTION get_column_usage_score(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_usage_count NUMBER := 0;
        v_view_where_count NUMBER := 0;
        v_view_join_count NUMBER := 0;
        v_source_where_count NUMBER := 0;
        v_source_join_count NUMBER := 0;
        v_index_count NUMBER := 0;
        v_col_pattern VARCHAR2(200);
    BEGIN
        v_col_pattern := UPPER(p_column_name);

        -- Check usage in views - WHERE clauses
        BEGIN
            SELECT COUNT(*)
            INTO v_view_where_count
            FROM dba_views
            WHERE owner = p_owner
            AND (
                UPPER(text) LIKE '%WHERE%' || v_col_pattern || '%'
                OR UPPER(text) LIKE '%WHERE%' || v_col_pattern || ' %'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || '=%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' >%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' <%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' BETWEEN%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' IN%'
            );
        EXCEPTION WHEN OTHERS THEN
            v_view_where_count := 0;
        END;

        -- Check usage in views - JOIN conditions
        BEGIN
            SELECT COUNT(*)
            INTO v_view_join_count
            FROM dba_views
            WHERE owner = p_owner
            AND (
                UPPER(text) LIKE '%JOIN%ON%' || v_col_pattern || '%'
                OR UPPER(text) LIKE '%JOIN%' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '%ON%' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '%ON%' || v_col_pattern || '=%'
            );
        EXCEPTION WHEN OTHERS THEN
            v_view_join_count := 0;
        END;

        -- Check usage in stored procedures, functions, packages - WHERE clauses
        BEGIN
            SELECT COUNT(*)
            INTO v_source_where_count
            FROM dba_source
            WHERE owner = p_owner
            AND type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION')
            AND (
                UPPER(text) LIKE '%WHERE%' || v_col_pattern || '%'
                OR UPPER(text) LIKE '%WHERE%' || v_col_pattern || ' %'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || '=%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' >%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' <%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' BETWEEN%'
                OR UPPER(text) LIKE '% ' || v_col_pattern || ' IN%'
            );
        EXCEPTION WHEN OTHERS THEN
            v_source_where_count := 0;
        END;

        -- Check usage in stored procedures, functions, packages - JOIN conditions
        BEGIN
            SELECT COUNT(*)
            INTO v_source_join_count
            FROM dba_source
            WHERE owner = p_owner
            AND type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION')
            AND (
                UPPER(text) LIKE '%JOIN%ON%' || v_col_pattern || '%'
                OR UPPER(text) LIKE '%JOIN%' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '%ON%' || v_col_pattern || ' =%'
                OR UPPER(text) LIKE '%ON%' || v_col_pattern || '=%'
            );
        EXCEPTION WHEN OTHERS THEN
            v_source_join_count := 0;
        END;

        -- Check if column is indexed (strongest indicator - actual query usage)
        BEGIN
            SELECT COUNT(*)
            INTO v_index_count
            FROM dba_ind_columns ic
            JOIN dba_indexes i ON i.owner = ic.index_owner AND i.index_name = ic.index_name
            WHERE ic.table_owner = p_owner
            AND ic.table_name = p_table_name
            AND ic.column_name = p_column_name;
        EXCEPTION WHEN OTHERS THEN
            v_index_count := 0;
        END;

        -- Calculate weighted score
        -- Indexes: weight 15 (strongest - proven query usage with statistics)
        -- WHERE clauses in code: weight 3 (filtering = partition pruning benefit)
        -- JOIN conditions: weight 2 (join predicates benefit from pruning)
        v_usage_count := (v_index_count * 15) +
                        (v_source_where_count * 3) +
                        (v_source_join_count * 2) +
                        (v_view_where_count * 3) +
                        (v_view_join_count * 2);

        RETURN v_usage_count;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_column_usage_score;


    FUNCTION analyze_date_column(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2,
        p_min_date OUT DATE,
        p_max_date OUT DATE,
        p_range_days OUT NUMBER,
        p_null_count OUT NUMBER,
        p_non_null_count OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_has_time_component OUT VARCHAR2,
        p_distinct_dates OUT NUMBER,
        p_usage_score OUT NUMBER
    ) RETURN BOOLEAN
    AS
        v_sql VARCHAR2(4000);
        v_error_code NUMBER;
        v_error_msg VARCHAR2(4000);
        v_total_count NUMBER;
        v_min_time VARCHAR2(8);
        v_max_time VARCHAR2(8);
        v_time_sample NUMBER;
    BEGIN
        -- Get date range and NULL statistics in a single query
        v_sql := 'SELECT ' ||
                '  MIN(' || p_column_name || '), ' ||
                '  MAX(' || p_column_name || '), ' ||
                '  COUNT(*), ' ||
                '  COUNT(' || p_column_name || '), ' ||
                '  COUNT(*) - COUNT(' || p_column_name || '), ' ||
                '  TO_CHAR(MIN(' || p_column_name || '), ''HH24:MI:SS''), ' ||
                '  TO_CHAR(MAX(' || p_column_name || '), ''HH24:MI:SS'') ' ||
                ' FROM ' || p_owner || '.' || p_table_name;

        EXECUTE IMMEDIATE v_sql INTO p_min_date, p_max_date, v_total_count, p_non_null_count, p_null_count, v_min_time, v_max_time;

        -- Calculate NULL percentage
        IF v_total_count > 0 THEN
            p_null_percentage := ROUND((p_null_count / v_total_count) * 100, 2);
        ELSE
            p_null_percentage := 0;
        END IF;

        -- If MIN/MAX are NULL, column has no non-NULL data
        IF p_min_date IS NOT NULL AND p_max_date IS NOT NULL THEN
            p_range_days := p_max_date - p_min_date;

            -- Check if column has time component (not all midnight)
            -- Sample: check if any non-midnight times exist
            v_sql := 'SELECT COUNT(*) FROM (' ||
                    '  SELECT ' || p_column_name ||
                    '  FROM ' || p_owner || '.' || p_table_name ||
                    '  WHERE ' || p_column_name || ' IS NOT NULL' ||
                    '    AND ' || p_column_name || ' != TRUNC(' || p_column_name || ')' ||
                    '  AND ROWNUM <= 1' ||
                    ')';

            EXECUTE IMMEDIATE v_sql INTO v_time_sample;

            IF v_time_sample > 0 OR v_min_time != '00:00:00' OR v_max_time != '00:00:00' THEN
                p_has_time_component := 'Y';

                -- Get distinct date count (without time)
                v_sql := 'SELECT COUNT(DISTINCT TRUNC(' || p_column_name || ')) ' ||
                        'FROM ' || p_owner || '.' || p_table_name ||
                        ' WHERE ' || p_column_name || ' IS NOT NULL';
                EXECUTE IMMEDIATE v_sql INTO p_distinct_dates;
            ELSE
                p_has_time_component := 'N';
                p_distinct_dates := NULL;  -- Not needed if no time component
            END IF;

            -- Get column usage score (how often used in queries/code)
            p_usage_score := get_column_usage_score(p_owner, p_table_name, p_column_name);

            RETURN TRUE;
        END IF;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            v_error_code := SQLCODE;
            v_error_msg := SQLERRM;

            -- Log specific error types
            IF v_error_code = -942 THEN
                DBMS_OUTPUT.PUT_LINE('  ERROR analyzing ' || p_column_name || ': Table or view does not exist');
            ELSIF v_error_code = -1031 THEN
                DBMS_OUTPUT.PUT_LINE('  ERROR analyzing ' || p_column_name || ': Insufficient privileges to read table');
            ELSIF v_error_code = -904 THEN
                DBMS_OUTPUT.PUT_LINE('  ERROR analyzing ' || p_column_name || ': Invalid column name');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  ERROR analyzing ' || p_column_name || ': ' || v_error_msg);
            END IF;

            RETURN FALSE;
    END analyze_date_column;


    FUNCTION detect_scd2_pattern(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_scd2_type OUT VARCHAR2,
        p_date_column OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_has_effective_date NUMBER := 0;
        v_has_valid_from NUMBER := 0;
        v_has_valid_to NUMBER := 0;
        v_has_current_flag NUMBER := 0;
    BEGIN
        -- Check for effective_date pattern
        SELECT COUNT(*) INTO v_has_effective_date
        FROM (
            SELECT 1 FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('EFFECTIVE_DATE', 'EFF_DATE', 'START_DATE')
            AND data_type IN ('DATE', 'TIMESTAMP')
            FETCH FIRST 1 ROW ONLY
        );

        SELECT COUNT(*) INTO v_has_current_flag
        FROM (
            SELECT 1 FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('CURRENT_FLAG', 'IS_CURRENT', 'CURRENT_IND')
            FETCH FIRST 1 ROW ONLY
        );

        -- Check for valid_from/valid_to pattern
        SELECT COUNT(*) INTO v_has_valid_from
        FROM (
            SELECT 1 FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('VALID_FROM_DTTM', 'VALID_FROM', 'START_DTTM', 'BEGIN_DTTM', 'VALID_ON', 'DATA_MODIF')
            AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
            FETCH FIRST 1 ROW ONLY
        );

        SELECT COUNT(*) INTO v_has_valid_to
        FROM (
            SELECT 1 FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('VALID_TO_DTTM', 'VALID_TO', 'END_DTTM', 'EXPIRY_DTTM')
            AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
            FETCH FIRST 1 ROW ONLY
        );

        -- Determine SCD2 type
        IF v_has_effective_date > 0 AND v_has_current_flag > 0 THEN
            p_scd2_type := 'EFFECTIVE_DATE';
            -- Get actual column name
            SELECT column_name INTO p_date_column
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('EFFECTIVE_DATE', 'EFF_DATE', 'START_DATE')
            AND data_type IN ('DATE', 'TIMESTAMP')
            FETCH FIRST 1 ROW ONLY;
            RETURN TRUE;

        ELSIF v_has_valid_from > 0 AND v_has_valid_to > 0 THEN
            p_scd2_type := 'VALID_FROM_TO';
            -- Get actual column name
            SELECT column_name INTO p_date_column
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND UPPER(column_name) IN ('VALID_FROM_DTTM', 'VALID_FROM', 'START_DTTM', 'BEGIN_DTTM', 'VALID_ON', 'DATA_MODIF')
            AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
            FETCH FIRST 1 ROW ONLY;
            RETURN TRUE;
        END IF;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_scd2_pattern;


    FUNCTION detect_events_table(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_event_column OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_count NUMBER;
    BEGIN
        -- Check for event-related column patterns
        SELECT COUNT(*) INTO v_count
        FROM dba_tab_columns
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND (
            UPPER(column_name) IN ('EVENT_DTTM', 'EVENT_TIMESTAMP', 'EVENT_DATE', 'EVENT_TIME', 'AUDIT_DTTM', 'CAPTURE_DTTM', 'INGESTION_DTTM',
                                    'TRN_DT', 'TXN_DATE', 'TRANSACTION_DATE', 'DATA_TRANZACTIEI', 'LOG_DATE')
            OR UPPER(table_name) LIKE '%EVENT%'
            OR UPPER(table_name) LIKE '%AUDIT%'
            OR UPPER(table_name) LIKE '%LOG%'
            OR UPPER(table_name) LIKE '%TRN%'
            OR UPPER(table_name) LIKE '%TRANSACTION%'
        );

        IF v_count > 0 THEN
            -- Try to find the event timestamp column
            BEGIN
                SELECT column_name INTO p_event_column
                FROM dba_tab_columns
                WHERE owner = p_owner
                AND table_name = p_table_name
                AND UPPER(column_name) IN ('EVENT_DTTM', 'EVENT_TIMESTAMP', 'EVENT_DATE', 'AUDIT_DTTM', 'CAPTURE_DTTM', 'INGESTION_DTTM',
                                            'CREATED_DTTM', 'INSERT_DTTM', 'TRN_DT', 'TXN_DATE', 'TRANSACTION_DATE', 'DATA_TRANZACTIEI', 'LOG_DATE')
                AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
                ORDER BY
                    CASE UPPER(column_name)
                        WHEN 'EVENT_DTTM' THEN 1
                        WHEN 'EVENT_TIMESTAMP' THEN 2
                        WHEN 'TRANSACTION_DATE' THEN 3
                        WHEN 'TXN_DATE' THEN 4
                        WHEN 'TRN_DT' THEN 5
                        WHEN 'DATA_TRANZACTIEI' THEN 6
                        WHEN 'LOG_DATE' THEN 7
                        WHEN 'AUDIT_DTTM' THEN 8
                        WHEN 'CAPTURE_DTTM' THEN 9
                        ELSE 10
                    END
                FETCH FIRST 1 ROW ONLY;

                RETURN TRUE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RETURN FALSE;
            END;
        END IF;

        RETURN FALSE;
    END detect_events_table;


    -- Detect date-like columns stored as NUMBER (YYYYMMDD, Unix timestamp, etc.)
    FUNCTION detect_numeric_date_column(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_count NUMBER;
        v_sample_value NUMBER;
        v_sql VARCHAR2(4000);
        v_min_val NUMBER;
        v_max_val NUMBER;
    BEGIN
        -- Look for columns with date-like names that are NUMBER type
        FOR rec IN (
            SELECT column_name, data_type, data_length, data_precision
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type = 'NUMBER'
            AND (
                UPPER(column_name) LIKE '%DATE%'
                OR UPPER(column_name) LIKE '%TIME%'
                OR UPPER(column_name) LIKE '%DTTM%'
                OR UPPER(column_name) LIKE '%TIMESTAMP%'
                OR UPPER(column_name) LIKE '%DT'
                OR UPPER(column_name) IN ('EFFECTIVE_DT', 'VALID_FROM', 'VALID_TO', 'START_DT', 'END_DT')
            )
        ) LOOP
            -- Sample the column to detect format
            BEGIN
                v_sql := 'SELECT MIN(' || rec.column_name || '), MAX(' || rec.column_name || '), ' ||
                         'COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                         ' WHERE ' || rec.column_name || ' IS NOT NULL AND ROWNUM <= 1000';

                EXECUTE IMMEDIATE v_sql INTO v_min_val, v_max_val, v_count;

                IF v_count > 0 THEN
                    -- Check for YYYYMMDD format (8 digits, values between 19000101 and 21000101)
                    IF v_min_val >= 19000101 AND v_max_val <= 21001231
                       AND LENGTH(TRUNC(v_min_val)) = 8 THEN
                        p_date_column := rec.column_name;
                        p_date_format := 'YYYYMMDD';
                        RETURN TRUE;

                    -- Check for Unix timestamp (10 digits, reasonable range)
                    ELSIF v_min_val >= 946684800 AND v_max_val <= 2147483647  -- 2000-01-01 to 2038-01-19
                          AND LENGTH(TRUNC(v_min_val)) >= 10 THEN
                        p_date_column := rec.column_name;
                        p_date_format := 'UNIX_TIMESTAMP';
                        RETURN TRUE;

                    -- Check for YYMMDD format (6 digits)
                    ELSIF v_min_val >= 000101 AND v_max_val <= 991231
                          AND LENGTH(TRUNC(v_min_val)) = 6 THEN
                        p_date_column := rec.column_name;
                        p_date_format := 'YYMMDD';
                        RETURN TRUE;
                    END IF;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL; -- Continue to next column
            END;
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_numeric_date_column;


    -- Detect date-like columns stored as VARCHAR/CHAR
    FUNCTION detect_varchar_date_column(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_count NUMBER;
        v_sample_value VARCHAR2(100);
        v_sql VARCHAR2(4000);
    BEGIN
        -- Look for columns with date-like names that are VARCHAR/CHAR type
        FOR rec IN (
            SELECT column_name, data_type, data_length
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type IN ('VARCHAR2', 'CHAR')
            AND (
                UPPER(column_name) LIKE '%DATE%'
                OR UPPER(column_name) LIKE '%TIME%'
                OR UPPER(column_name) LIKE '%DTTM%'
                OR UPPER(column_name) LIKE '%TIMESTAMP%'
                OR UPPER(column_name) LIKE '%DT'
                OR UPPER(column_name) IN ('EFFECTIVE_DT', 'VALID_FROM', 'VALID_TO', 'START_DT', 'END_DT')
            )
        ) LOOP
            -- Sample the column to detect format
            BEGIN
                v_sql := 'SELECT ' || rec.column_name || ' FROM ' || p_owner || '.' || p_table_name ||
                         ' WHERE ' || rec.column_name || ' IS NOT NULL AND ROWNUM = 1';

                EXECUTE IMMEDIATE v_sql INTO v_sample_value;

                IF v_sample_value IS NOT NULL THEN
                    -- Try various date format conversions
                    -- YYYY-MM-DD
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                                        ' WHERE TO_DATE(' || rec.column_name || ', ''YYYY-MM-DD'') IS NOT NULL AND ROWNUM <= 100'
                                        INTO v_count;
                        IF v_count > 0 THEN
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM-DD';
                            RETURN TRUE;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;

                    -- DD/MM/YYYY
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                                        ' WHERE TO_DATE(' || rec.column_name || ', ''DD/MM/YYYY'') IS NOT NULL AND ROWNUM <= 100'
                                        INTO v_count;
                        IF v_count > 0 THEN
                            p_date_column := rec.column_name;
                            p_date_format := 'DD/MM/YYYY';
                            RETURN TRUE;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;

                    -- MM/DD/YYYY
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                                        ' WHERE TO_DATE(' || rec.column_name || ', ''MM/DD/YYYY'') IS NOT NULL AND ROWNUM <= 100'
                                        INTO v_count;
                        IF v_count > 0 THEN
                            p_date_column := rec.column_name;
                            p_date_format := 'MM/DD/YYYY';
                            RETURN TRUE;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;

                    -- YYYYMMDD
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                                        ' WHERE TO_DATE(' || rec.column_name || ', ''YYYYMMDD'') IS NOT NULL AND ROWNUM <= 100'
                                        INTO v_count;
                        IF v_count > 0 THEN
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            RETURN TRUE;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;

                    -- YYYY-MM-DD HH24:MI:SS
                    BEGIN
                        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name ||
                                        ' WHERE TO_DATE(' || rec.column_name || ', ''YYYY-MM-DD HH24:MI:SS'') IS NOT NULL AND ROWNUM <= 100'
                                        INTO v_count;
                        IF v_count > 0 THEN
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM-DD HH24:MI:SS';
                            RETURN TRUE;
                        END IF;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    NULL; -- Continue to next column
            END;
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_varchar_date_column;


    -- Generate conversion expression for non-standard date formats
    FUNCTION get_date_conversion_expr(
        p_column_name VARCHAR2,
        p_data_type VARCHAR2,
        p_date_format VARCHAR2
    ) RETURN VARCHAR2
    AS
    BEGIN
        -- Handle NUMBER-based dates
        IF p_data_type = 'NUMBER' THEN
            IF p_date_format = 'YYYYMMDD' THEN
                RETURN 'TO_DATE(TO_CHAR(' || p_column_name || '), ''YYYYMMDD'')';
            ELSIF p_date_format = 'YYMMDD' THEN
                RETURN 'TO_DATE(TO_CHAR(' || p_column_name || '), ''YYMMDD'')';
            ELSIF p_date_format = 'UNIX_TIMESTAMP' THEN
                RETURN 'TO_DATE(''1970-01-01'', ''YYYY-MM-DD'') + (' || p_column_name || ' / 86400)';
            END IF;

        -- Handle VARCHAR/CHAR-based dates
        ELSIF p_data_type IN ('VARCHAR2', 'CHAR') THEN
            RETURN 'TO_DATE(' || p_column_name || ', ''' || p_date_format || ''')';
        END IF;

        -- Default: return column as-is
        RETURN p_column_name;
    END get_date_conversion_expr;


    FUNCTION detect_staging_table(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_load_column OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_count NUMBER;
    BEGIN
        -- Check for staging table patterns
        IF UPPER(p_table_name) LIKE 'STG_%'
            OR UPPER(p_table_name) LIKE '%_STG'
            OR UPPER(p_table_name) LIKE '%_STAGING'
            OR UPPER(p_table_name) LIKE 'STAGING_%'
            OR UPPER(p_table_name) LIKE '%_TEMP'
            OR UPPER(p_table_name) LIKE 'TEMP_%'
        THEN
            -- Try to find load timestamp column
            BEGIN
                SELECT column_name INTO p_load_column
                FROM dba_tab_columns
                WHERE owner = p_owner
                AND table_name = p_table_name
                AND UPPER(column_name) IN ('LOAD_DTTM', 'LOAD_DATE', 'LOAD_TIMESTAMP', 'INSERT_DTTM', 'CREATED_DTTM', 'INGESTION_DTTM', 'CAPTURE_DTTM',
                                            'EXTRACTDATE', 'EXTRCT_DATE', 'RUN_DATE', 'PURGE_DATE', 'VAL_DT')
                AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
                ORDER BY
                    CASE UPPER(column_name)
                        WHEN 'LOAD_DTTM' THEN 1
                        WHEN 'LOAD_DATE' THEN 2
                        WHEN 'LOAD_TIMESTAMP' THEN 3
                        WHEN 'EXTRACTDATE' THEN 4
                        WHEN 'EXTRCT_DATE' THEN 5
                        WHEN 'RUN_DATE' THEN 6
                        WHEN 'VAL_DT' THEN 7
                        WHEN 'PURGE_DATE' THEN 8
                        ELSE 9
                    END
                FETCH FIRST 1 ROW ONLY;

                RETURN TRUE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RETURN FALSE;
            END;
        END IF;

        RETURN FALSE;
    END detect_staging_table;


    -- Detect historical (HIST) tables
    FUNCTION detect_hist_table(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_hist_column OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_count NUMBER;
    BEGIN
        -- Check for HIST table patterns
        IF UPPER(p_table_name) LIKE 'HIST_%'
            OR UPPER(p_table_name) LIKE '%_HIST'
            OR UPPER(p_table_name) LIKE '%_HISTORY'
            OR UPPER(p_table_name) LIKE 'HISTORY_%'
        THEN
            -- Try to find historical date column
            BEGIN
                SELECT column_name INTO p_hist_column
                FROM dba_tab_columns
                WHERE owner = p_owner
                AND table_name = p_table_name
                AND (
                    UPPER(column_name) IN ('HIST_DATE', 'HIST_DTTM', 'HIST_TIMESTAMP', 'HISTORY_DATE', 'HISTORY_DTTM',
                                           'SNAPSHOT_DATE', 'SNAPSHOT_DTTM', 'ARCHIVE_DATE', 'ARCHIVE_DTTM',
                                           'CREATED_DATE', 'CREATED_DTTM', 'INSERT_DATE', 'INSERT_DTTM',
                                           'HIST_DATA', 'HIST_MONTH')
                    OR UPPER(column_name) LIKE 'HIST_%DATE%'
                    OR UPPER(column_name) LIKE 'HISTORY_%DATE%'
                    OR UPPER(column_name) LIKE 'SNAPSHOT_%'
                )
                AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
                ORDER BY
                    CASE UPPER(column_name)
                        WHEN 'HIST_DATE' THEN 1
                        WHEN 'HIST_DTTM' THEN 2
                        WHEN 'HIST_MONTH' THEN 3
                        WHEN 'HIST_DATA' THEN 4
                        WHEN 'HISTORY_DATE' THEN 5
                        WHEN 'SNAPSHOT_DATE' THEN 6
                        WHEN 'ARCHIVE_DATE' THEN 7
                        WHEN 'CREATED_DATE' THEN 8
                        ELSE 9
                    END
                FETCH FIRST 1 ROW ONLY;

                RETURN TRUE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- Table name matches HIST pattern but no clear date column found
                    -- Try generic date columns
                    BEGIN
                        SELECT column_name INTO p_hist_column
                        FROM dba_tab_columns
                        WHERE owner = p_owner
                        AND table_name = p_table_name
                        AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
                        ORDER BY column_id
                        FETCH FIRST 1 ROW ONLY;

                        RETURN TRUE;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            RETURN FALSE;
                    END;
            END;
        END IF;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_hist_table;


    -- ==========================================================================
    -- Public Functions
    -- ==========================================================================

    PROCEDURE analyze_column_distribution(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2,
        p_distinct_values OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_distribution_type OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_total_rows NUMBER;
    BEGIN
        -- Get total rows
        v_sql := 'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name;
        EXECUTE IMMEDIATE v_sql INTO v_total_rows;

        -- Get distinct values
        v_sql := 'SELECT COUNT(DISTINCT ' || p_column_name || ') FROM ' ||
                p_owner || '.' || p_table_name;
        EXECUTE IMMEDIATE v_sql INTO p_distinct_values;

        -- Get null percentage
        v_sql := 'SELECT ROUND(COUNT(*) * 100.0 / ' || v_total_rows || ', 2) FROM ' ||
                p_owner || '.' || p_table_name ||
                ' WHERE ' || p_column_name || ' IS NULL';
        EXECUTE IMMEDIATE v_sql INTO p_null_percentage;

        -- Classify distribution
        IF p_distinct_values = v_total_rows THEN
            p_distribution_type := 'UNIQUE';
        ELSIF p_distinct_values > v_total_rows * 0.9 THEN
            p_distribution_type := 'HIGH_CARDINALITY';
        ELSIF p_distinct_values > 100 THEN
            p_distribution_type := 'MEDIUM_CARDINALITY';
        ELSIF p_distinct_values BETWEEN 10 AND 100 THEN
            p_distribution_type := 'LOW_CARDINALITY';
        ELSE
            p_distribution_type := 'VERY_LOW_CARDINALITY';
        END IF;
    END analyze_column_distribution;


    FUNCTION recommend_partition_strategy(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_reason OUT VARCHAR2
    ) RETURN VARCHAR2
    AS
        v_date_columns SYS.ODCIVARCHAR2LIST;
        v_min_date DATE;
        v_max_date DATE;
        v_range_days NUMBER;
        v_num_rows NUMBER;
        v_best_column VARCHAR2(128);
        v_max_range NUMBER := 0;
        v_strategy VARCHAR2(100);
        v_scd2_type VARCHAR2(30);
        v_scd2_column VARCHAR2(128);
        v_event_column VARCHAR2(128);
        v_staging_column VARCHAR2(128);
    BEGIN
        -- Get row count
        SELECT num_rows INTO v_num_rows
        FROM dba_tables
        WHERE owner = p_owner
        AND table_name = p_table_name;

        -- Check for SCD2 pattern first
        IF detect_scd2_pattern(p_owner, p_table_name, v_scd2_type, v_scd2_column) THEN
            IF v_scd2_type = 'EFFECTIVE_DATE' THEN
                v_strategy := 'RANGE(' || v_scd2_column || ') INTERVAL YEARLY';
                p_reason := 'SCD2 table with effective_date pattern detected - yearly partitioning for historical tracking';
            ELSIF v_scd2_type = 'VALID_FROM_TO' THEN
                v_strategy := 'RANGE(' || v_scd2_column || ') INTERVAL YEARLY';
                p_reason := 'SCD2 table with valid_from_dttm/valid_to_dttm pattern detected - yearly partitioning for historical tracking';
            END IF;
            RETURN v_strategy;
        END IF;

        -- Check for EVENTS table pattern
        IF detect_events_table(p_owner, p_table_name, v_event_column) THEN
            IF UPPER(p_table_name) LIKE '%AUDIT%' OR UPPER(p_table_name) LIKE '%COMPLIANCE%' THEN
                v_strategy := 'RANGE(' || v_event_column || ') INTERVAL MONTHLY';
                p_reason := 'Audit/compliance events table detected - monthly partitioning for long-term retention';
            ELSE
                v_strategy := 'RANGE(' || v_event_column || ') INTERVAL DAILY';
                p_reason := 'Events table detected - daily partitioning for high-volume event data';
            END IF;
            RETURN v_strategy;
        END IF;

        -- Check for STAGING table pattern
        IF detect_staging_table(p_owner, p_table_name, v_staging_column) THEN
            v_strategy := 'RANGE(' || v_staging_column || ') INTERVAL DAILY';
            p_reason := 'Staging table detected - daily partitioning for easy partition exchange and purging';
            RETURN v_strategy;
        END IF;

        -- Check for HIST table pattern
        DECLARE
            v_hist_column VARCHAR2(128);
        BEGIN
            IF detect_hist_table(p_owner, p_table_name, v_hist_column) THEN
                -- HIST tables typically have monthly or yearly snapshots
                v_strategy := 'RANGE(' || v_hist_column || ') INTERVAL MONTHLY';
                p_reason := 'Historical table detected - monthly partitioning for snapshot data retention';
                RETURN v_strategy;
            END IF;
        END;

        -- Note: Detection of non-standard date formats (NUMBER, VARCHAR) is handled
        -- in the analyze_table procedure and stored in dwh_migration_analysis table

        -- Get date columns for general analysis
        v_date_columns := get_date_columns(p_owner, p_table_name);

        -- Analyze each date column
        IF v_date_columns IS NOT NULL AND v_date_columns.COUNT > 0 THEN
            FOR i IN 1..v_date_columns.COUNT LOOP
                IF analyze_date_column(
                    p_owner, p_table_name, v_date_columns(i),
                    v_min_date, v_max_date, v_range_days
                ) THEN
                    IF v_range_days > v_max_range THEN
                        v_max_range := v_range_days;
                        v_best_column := v_date_columns(i);
                    END IF;
                END IF;
            END LOOP;

            -- Recommend strategy based on date range
            IF v_best_column IS NOT NULL THEN
                IF v_max_range > 365 * 3 THEN
                    v_strategy := 'RANGE(' || v_best_column || ') INTERVAL MONTHLY';
                    p_reason := 'Date range spans ' || ROUND(v_max_range/365, 1) ||
                               ' years - monthly interval partitioning recommended';
                ELSIF v_max_range > 365 THEN
                    v_strategy := 'RANGE(' || v_best_column || ') INTERVAL MONTHLY';
                    p_reason := 'Date range spans ' || ROUND(v_max_range/30, 0) ||
                               ' months - monthly partitioning recommended';
                ELSIF v_max_range > 90 THEN
                    v_strategy := 'RANGE(' || v_best_column || ')';
                    p_reason := 'Date range spans ' || v_max_range ||
                               ' days - range partitioning recommended';
                ELSE
                    v_strategy := NULL;
                    p_reason := 'Date range too small for effective partitioning';
                END IF;

                RETURN v_strategy;
            END IF;
        END IF;

        -- No suitable date column, recommend hash partitioning for large tables
        IF v_num_rows > 10000000 THEN
            -- Find primary key or first indexed column
            BEGIN
                SELECT cc.column_name INTO v_best_column
                FROM dba_constraints c
                JOIN all_cons_columns cc
                    ON cc.owner = c.owner
                    AND cc.constraint_name = c.constraint_name
                WHERE c.owner = p_owner
                AND c.table_name = p_table_name
                AND c.constraint_type = 'P'
                AND ROWNUM = 1;

                v_strategy := 'HASH(' || v_best_column || ') PARTITIONS 16';
                p_reason := 'Large table (' || v_num_rows || ' rows) without date column - hash partitioning for even distribution';
                RETURN v_strategy;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    NULL;
            END;
        END IF;

        -- No partitioning recommended
        v_strategy := NULL;
        p_reason := 'Table not suitable for partitioning - no date columns and not large enough for hash';
        RETURN v_strategy;

    EXCEPTION
        WHEN OTHERS THEN
            p_reason := 'Error during analysis: ' || SQLERRM;
            RETURN NULL;
    END recommend_partition_strategy;


    FUNCTION estimate_partition_count(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_partition_type VARCHAR2
    ) RETURN NUMBER
    AS
        v_sql VARCHAR2(4000);
        v_distinct_values NUMBER;
        v_min_date DATE;
        v_max_date DATE;
        v_months NUMBER;
    BEGIN
        IF p_partition_type LIKE 'RANGE%' THEN
            -- For range partitioning on dates
            v_sql := 'SELECT MIN(' || p_partition_key || '), MAX(' || p_partition_key || ')' ||
                    ' FROM ' || p_owner || '.' || p_table_name;

            EXECUTE IMMEDIATE v_sql INTO v_min_date, v_max_date;

            v_months := MONTHS_BETWEEN(v_max_date, v_min_date);
            RETURN CEIL(v_months);

        ELSIF p_partition_type LIKE 'HASH%' THEN
            -- For hash partitioning, return recommended partition count
            RETURN 16;

        ELSIF p_partition_type LIKE 'LIST%' THEN
            -- For list partitioning, return distinct values
            v_sql := 'SELECT COUNT(DISTINCT ' || p_partition_key || ')' ||
                    ' FROM ' || p_owner || '.' || p_table_name;

            EXECUTE IMMEDIATE v_sql INTO v_distinct_values;
            RETURN LEAST(v_distinct_values, 100);  -- Cap at 100 partitions

        ELSE
            RETURN 1;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END estimate_partition_count;


    FUNCTION calculate_complexity_score(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_score NUMBER := 0;
        v_count NUMBER;
    BEGIN
        -- Base complexity
        v_score := 1;

        -- Add points for indexes
        SELECT COUNT(*) INTO v_count
        FROM dba_indexes
        WHERE table_owner = p_owner
        AND table_name = p_table_name;
        v_score := v_score + LEAST(v_count * 0.5, 3);

        -- Add points for constraints
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND constraint_type IN ('R', 'C', 'U');
        v_score := v_score + LEAST(v_count * 0.3, 2);

        -- Add points for foreign keys
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND constraint_type = 'R';
        IF v_count > 0 THEN
            v_score := v_score + 2;
        END IF;

        -- Add points for triggers
        SELECT COUNT(*) INTO v_count
        FROM dba_triggers
        WHERE table_owner = p_owner
        AND table_name = p_table_name;
        v_score := v_score + v_count;

        -- Add points for LOBs
        SELECT COUNT(*) INTO v_count
        FROM dba_tab_columns
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND data_type IN ('CLOB', 'BLOB', 'NCLOB');
        IF v_count > 0 THEN
            v_score := v_score + 1;
        END IF;

        RETURN LEAST(v_score, 10);  -- Cap at 10
    END calculate_complexity_score;


    FUNCTION get_dependent_objects(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN CLOB
    AS
        v_result CLOB;
        v_count NUMBER;
    BEGIN
        DBMS_LOB.CREATETEMPORARY(v_result, TRUE);

        DBMS_LOB.APPEND(v_result, '{');

        -- Indexes
        SELECT COUNT(*) INTO v_count
        FROM dba_indexes
        WHERE table_owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '"indexes": ' || v_count || ',');

        -- Constraints
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '"constraints": ' || v_count || ',');

        -- Triggers
        SELECT COUNT(*) INTO v_count
        FROM dba_triggers
        WHERE table_owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '"triggers": ' || v_count || ',');

        -- Foreign keys referencing this table
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE r_owner = p_owner
        AND r_constraint_name IN (
            SELECT constraint_name FROM dba_constraints
            WHERE owner = p_owner AND table_name = p_table_name
            AND constraint_type = 'P'
        );
        DBMS_LOB.APPEND(v_result, '"referenced_by": ' || v_count);

        DBMS_LOB.APPEND(v_result, '}');

        RETURN v_result;
    END get_dependent_objects;


    FUNCTION identify_blocking_issues(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN CLOB
    AS
        v_issues CLOB;
        v_count NUMBER;
        v_first BOOLEAN := TRUE;
    BEGIN
        DBMS_LOB.CREATETEMPORARY(v_issues, TRUE);
        DBMS_LOB.APPEND(v_issues, '[');

        -- Check for materialized views
        SELECT COUNT(*) INTO v_count
        FROM all_mviews
        WHERE owner = p_owner
        AND mview_name = p_table_name;

        IF v_count > 0 THEN
            IF NOT v_first THEN DBMS_LOB.APPEND(v_issues, ','); END IF;
            DBMS_LOB.APPEND(v_issues,
                '{"type": "ERROR", "issue": "Table is a materialized view", ' ||
                '"action": "Cannot partition materialized views directly"}');
            v_first := FALSE;
        END IF;

        -- Check for IOTs
        SELECT COUNT(*) INTO v_count
        FROM dba_tables
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND iot_type IS NOT NULL;

        IF v_count > 0 THEN
            IF NOT v_first THEN DBMS_LOB.APPEND(v_issues, ','); END IF;
            DBMS_LOB.APPEND(v_issues,
                '{"type": "ERROR", "issue": "Table is an Index-Organized Table", ' ||
                '"action": "IOT partitioning requires special handling"}');
            v_first := FALSE;
        END IF;

        DBMS_LOB.APPEND(v_issues, ']');
        RETURN v_issues;
    END identify_blocking_issues;


    -- ==========================================================================
    -- Main Analysis Procedures
    -- ==========================================================================

    PROCEDURE analyze_table(
        p_task_id NUMBER
    ) AS
        v_task dwh_migration_tasks%ROWTYPE;
        v_analysis_id NUMBER;
        v_reason VARCHAR2(1000);
        v_recommended_strategy VARCHAR2(100);
        v_complexity NUMBER;
        v_partition_count NUMBER;
        v_table_size NUMBER;
        v_dependent_objects CLOB;
        v_blocking_issues CLOB;
        v_warnings CLOB;
        v_num_rows NUMBER;
        v_date_column VARCHAR2(128);
        v_date_format VARCHAR2(50);
        v_date_type VARCHAR2(30);
        v_conversion_expr VARCHAR2(500);
        v_requires_conversion CHAR(1) := 'N';
        v_all_date_analysis CLOB;
        v_date_found BOOLEAN := FALSE;
        v_error_count NUMBER := 0;
    BEGIN
        -- Get task details
        SELECT * INTO v_task
        FROM cmr.dwh_migration_tasks
        WHERE task_id = p_task_id
        FOR UPDATE;

        DBMS_OUTPUT.PUT_LINE('Analyzing table: ' || v_task.source_owner || '.' || v_task.source_table);

        -- Update task status and reset error message (supports rerun)
        UPDATE cmr.dwh_migration_tasks
        SET status = 'ANALYZING',
            analysis_date = SYSTIMESTAMP,
            error_message = NULL
        WHERE task_id = p_task_id;
        COMMIT;

        -- Initialize warnings tracking
        DBMS_LOB.CREATETEMPORARY(v_warnings, TRUE);
        DBMS_LOB.APPEND(v_warnings, '[');

        -- Get table statistics
        BEGIN
            SELECT num_rows INTO v_num_rows
            FROM dba_tables
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_LOB.APPEND(v_warnings, '{"type":"ERROR","issue":"Table not found in DBA_TABLES","action":"Verify table exists and gather statistics"}');
                v_num_rows := NULL;
            WHEN OTHERS THEN
                DBMS_LOB.APPEND(v_warnings, '{"type":"ERROR","issue":"Cannot access table metadata: ' || REPLACE(SQLERRM, '"', '\"') || '","action":"Check privileges"}');
                v_num_rows := NULL;
        END;

        BEGIN
            v_table_size := get_table_size_mb(v_task.source_owner, v_task.source_table);
        EXCEPTION
            WHEN OTHERS THEN
                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ','); END IF;
                DBMS_LOB.APPEND(v_warnings, '{"type":"WARNING","issue":"Cannot calculate table size: ' || REPLACE(SQLERRM, '"', '\"') || '"}');
                v_error_count := v_error_count + 1;
                v_table_size := 0;
        END;

        -- Comprehensive date column analysis
        DECLARE
            v_scd2_type VARCHAR2(30);
            v_event_column VARCHAR2(128);
            v_staging_column VARCHAR2(128);
            v_hist_column VARCHAR2(128);
            v_date_columns SYS.ODCIVARCHAR2LIST;
            v_min_date DATE;
            v_max_date DATE;
            v_range_days NUMBER;
            v_max_range NUMBER := 0;
            v_null_count NUMBER;
            v_non_null_count NUMBER;
            v_null_percentage NUMBER;
            v_selected_null_pct NUMBER := 0;
            v_has_time_component VARCHAR2(1);
            v_distinct_dates NUMBER;
            v_selected_has_time VARCHAR2(1) := 'N';
            v_usage_score NUMBER;
            v_max_usage_score NUMBER := 0;
            v_selected_usage_score NUMBER := 0;
            v_json_analysis VARCHAR2(32767);
            v_first_json BOOLEAN := TRUE;
        BEGIN
            -- Try SCD2 pattern first
            IF detect_scd2_pattern(v_task.source_owner, v_task.source_table, v_scd2_type, v_date_column) THEN
                v_date_type := 'DATE';
                v_date_format := v_scd2_type;
                v_conversion_expr := v_date_column;
                v_date_found := TRUE;
                DBMS_OUTPUT.PUT_LINE('Detected SCD2 date column: ' || v_date_column || ' (Type: ' || v_scd2_type || ')');
            -- Try Events pattern
            ELSIF detect_events_table(v_task.source_owner, v_task.source_table, v_event_column) THEN
                v_date_column := v_event_column;
                v_date_type := 'DATE';
                v_date_format := 'EVENTS';
                v_conversion_expr := v_date_column;
                v_date_found := TRUE;
                DBMS_OUTPUT.PUT_LINE('Detected Events date column: ' || v_date_column);
            -- Try Staging pattern
            ELSIF detect_staging_table(v_task.source_owner, v_task.source_table, v_staging_column) THEN
                v_date_column := v_staging_column;
                v_date_type := 'DATE';
                v_date_format := 'STAGING';
                v_conversion_expr := v_date_column;
                v_date_found := TRUE;
                DBMS_OUTPUT.PUT_LINE('Detected Staging date column: ' || v_date_column);
            -- Try HIST pattern
            ELSIF detect_hist_table(v_task.source_owner, v_task.source_table, v_hist_column) THEN
                v_date_column := v_hist_column;
                v_date_type := 'DATE';
                v_date_format := 'HIST';
                v_conversion_expr := v_date_column;
                v_date_found := TRUE;
                DBMS_OUTPUT.PUT_LINE('Detected HIST date column: ' || v_date_column);
            END IF;

            -- Analyze ALL date columns for comprehensive analysis
            DBMS_LOB.CREATETEMPORARY(v_all_date_analysis, TRUE);
            DBMS_LOB.APPEND(v_all_date_analysis, '[');

            v_date_columns := get_date_columns(v_task.source_owner, v_task.source_table);

            IF v_date_columns IS NOT NULL AND v_date_columns.COUNT > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Analyzing ' || v_date_columns.COUNT || ' date column(s)...');

                FOR i IN 1..v_date_columns.COUNT LOOP
                    IF analyze_date_column(
                        v_task.source_owner, v_task.source_table, v_date_columns(i),
                        v_min_date, v_max_date, v_range_days,
                        v_null_count, v_non_null_count, v_null_percentage,
                        v_has_time_component, v_distinct_dates, v_usage_score
                    ) THEN
                        -- Build JSON entry for this date column
                        IF NOT v_first_json THEN
                            DBMS_LOB.APPEND(v_all_date_analysis, ',');
                        END IF;
                        v_first_json := FALSE;

                        v_json_analysis := '{' ||
                            '"column_name":"' || v_date_columns(i) || '",' ||
                            '"data_type":"DATE",' ||
                            '"min_date":"' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || '",' ||
                            '"max_date":"' || TO_CHAR(v_max_date, 'YYYY-MM-DD') || '",' ||
                            '"range_days":' || v_range_days || ',' ||
                            '"range_years":' || ROUND(v_range_days/365.25, 2) || ',' ||
                            '"null_count":' || v_null_count || ',' ||
                            '"non_null_count":' || v_non_null_count || ',' ||
                            '"null_percentage":' || v_null_percentage || ',' ||
                            '"has_time_component":"' || v_has_time_component || '",' ||
                            '"distinct_dates":' || NVL(TO_CHAR(v_distinct_dates), 'null') || ',' ||
                            '"usage_score":' || v_usage_score || ',' ||
                            '"is_primary":' || CASE WHEN v_date_columns(i) = v_date_column THEN 'true' ELSE 'false' END ||
                        '}';

                        DBMS_LOB.APPEND(v_all_date_analysis, v_json_analysis);

                        DBMS_OUTPUT.PUT_LINE('  - ' || v_date_columns(i) || ': ' ||
                            TO_CHAR(v_min_date, 'YYYY-MM-DD') || ' to ' ||
                            TO_CHAR(v_max_date, 'YYYY-MM-DD') ||
                            ' (' || v_range_days || ' days, ' || v_null_percentage || '% NULLs' ||
                            CASE WHEN v_has_time_component = 'Y' THEN ', has time component' ELSE '' END ||
                            ', usage score: ' || v_usage_score || ')');

                        -- Track stats of selected column
                        IF v_date_columns(i) = v_date_column THEN
                            v_selected_null_pct := v_null_percentage;
                            v_selected_has_time := v_has_time_component;
                            v_selected_usage_score := v_usage_score;
                        END IF;

                        -- If no stereotype found, pick column with best combination of:
                        -- 1. Highest usage score (primary factor)
                        -- 2. Widest range (secondary factor if usage scores are similar)
                        IF NOT v_date_found THEN
                            -- Prefer column with higher usage score, or if similar usage, prefer wider range
                            IF v_usage_score > v_max_usage_score OR
                               (v_usage_score >= v_max_usage_score * 0.8 AND v_range_days > v_max_range) THEN
                                v_max_range := v_range_days;
                                v_max_usage_score := v_usage_score;
                                v_date_column := v_date_columns(i);
                                v_date_type := 'DATE';
                                v_date_format := 'STANDARD';
                                v_conversion_expr := v_date_column;
                                v_selected_null_pct := v_null_percentage;
                                v_selected_has_time := v_has_time_component;
                                v_selected_usage_score := v_usage_score;
                            END IF;
                        END IF;
                    END IF;
                END LOOP;

                -- If we found a date column via range/usage analysis
                IF NOT v_date_found AND v_date_column IS NOT NULL THEN
                    v_date_found := TRUE;
                    DBMS_OUTPUT.PUT_LINE('Selected date column: ' || v_date_column ||
                        ' (usage score: ' || v_selected_usage_score || ', range: ' || v_max_range || ' days)');
                END IF;

                -- Add warning if selected column has NULLs
                IF v_date_found AND v_selected_null_pct > 0 THEN
                    IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ','); END IF;
                    DBMS_LOB.APPEND(v_warnings,
                        '{"type":"WARNING",' ||
                        '"issue":"Selected date column ' || v_date_column || ' has ' || v_selected_null_pct || '% NULL values",' ||
                        '"action":"Consider: 1) Choose different date column, 2) Use DEFAULT partition for NULLs, 3) Populate NULL values before migration"}');
                    v_error_count := v_error_count + 1;

                    DBMS_OUTPUT.PUT_LINE('WARNING: Selected date column has ' || v_selected_null_pct || '% NULL values');

                    -- Escalate to blocking issue if >25% NULLs
                    IF v_selected_null_pct > 25 THEN
                        DBMS_OUTPUT.PUT_LINE('CRITICAL: >25% NULL values - strongly recommend addressing before migration');
                    END IF;
                END IF;

                -- Add warning if selected column has time component
                IF v_date_found AND v_selected_has_time = 'Y' THEN
                    IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ','); END IF;
                    DBMS_LOB.APPEND(v_warnings,
                        '{"type":"WARNING",' ||
                        '"issue":"Selected date column ' || v_date_column || ' contains time component (HH:MI:SS)",' ||
                        '"action":"Partition key must use TRUNC(' || v_date_column || ') for daily partitions or TO_CHAR(' || v_date_column || ', ''YYYY-MM'') for monthly partitions"}');
                    v_error_count := v_error_count + 1;

                    DBMS_OUTPUT.PUT_LINE('WARNING: Selected date column has time component - use TRUNC() in partition key');
                END IF;
            ELSE
                DBMS_OUTPUT.PUT_LINE('No DATE/TIMESTAMP columns found');
            END IF;

            DBMS_LOB.APPEND(v_all_date_analysis, ']');
        END;

        -- If no standard DATE column found, check for non-standard formats (NUMBER or VARCHAR-based dates)
        IF v_date_column IS NULL THEN
            IF detect_numeric_date_column(v_task.source_owner, v_task.source_table, v_date_column, v_date_format) THEN
                v_requires_conversion := 'Y';
                v_date_type := 'NUMBER';
                v_conversion_expr := get_date_conversion_expr(v_date_column, 'NUMBER', v_date_format);
                DBMS_OUTPUT.PUT_LINE('Detected NUMBER-based date column: ' || v_date_column || ' (Format: ' || v_date_format || ')');
            ELSIF detect_varchar_date_column(v_task.source_owner, v_task.source_table, v_date_column, v_date_format) THEN
                v_requires_conversion := 'Y';
                v_date_type := 'VARCHAR2';
                v_conversion_expr := get_date_conversion_expr(v_date_column, 'VARCHAR2', v_date_format);
                DBMS_OUTPUT.PUT_LINE('Detected VARCHAR-based date column: ' || v_date_column || ' (Format: ' || v_date_format || ')');
            END IF;
        END IF;

        -- Recommend partitioning strategy if not specified
        IF v_task.partition_type IS NULL THEN
            v_recommended_strategy := recommend_partition_strategy(
                v_task.source_owner,
                v_task.source_table,
                v_reason
            );

            -- If non-standard date format detected and no strategy found, suggest conversion
            IF v_requires_conversion = 'Y' AND v_recommended_strategy IS NULL THEN
                v_recommended_strategy := 'RANGE(' || v_date_column || '_CONVERTED) INTERVAL MONTHLY';
                v_reason := 'Date column ' || v_date_column || ' stored as ' || v_date_type ||
                           ' (format: ' || v_date_format || ') - conversion to DATE recommended for partitioning';
            END IF;
        ELSE
            v_recommended_strategy := v_task.partition_type;
            v_reason := 'Strategy specified by user';
        END IF;

        -- Calculate complexity
        v_complexity := calculate_complexity_score(v_task.source_owner, v_task.source_table);

        -- Estimate partition count
        IF v_task.partition_key IS NOT NULL AND v_recommended_strategy IS NOT NULL THEN
            v_partition_count := estimate_partition_count(
                v_task.source_owner,
                v_task.source_table,
                v_task.partition_key,
                v_recommended_strategy
            );
        END IF;

        -- Get dependencies
        v_dependent_objects := get_dependent_objects(v_task.source_owner, v_task.source_table);
        v_blocking_issues := identify_blocking_issues(v_task.source_owner, v_task.source_table);

        -- Close warnings JSON array
        DBMS_LOB.APPEND(v_warnings, ']');

        -- Store or update analysis results (supports rerun)
        MERGE INTO cmr.dwh_migration_analysis a
        USING (SELECT p_task_id AS task_id FROM DUAL) src
        ON (a.task_id = src.task_id)
        WHEN MATCHED THEN
            UPDATE SET
                table_rows = v_num_rows,
                table_size_mb = v_table_size,
                recommended_strategy = v_recommended_strategy,
                recommendation_reason = v_reason,
                date_column_name = v_date_column,
                date_column_type = v_date_type,
                date_format_detected = v_date_format,
                date_conversion_expr = v_conversion_expr,
                requires_conversion = v_requires_conversion,
                all_date_columns_analysis = v_all_date_analysis,
                estimated_partitions = v_partition_count,
                avg_partition_size_mb = CASE WHEN v_partition_count > 0 THEN v_table_size / v_partition_count ELSE 0 END,
                estimated_compression_ratio = CASE WHEN v_task.use_compression = 'Y' THEN 4 ELSE 1 END,
                estimated_space_savings_mb = CASE WHEN v_task.use_compression = 'Y' THEN v_table_size * 0.75 ELSE 0 END,
                complexity_score = v_complexity,
                dependent_objects = v_dependent_objects,
                blocking_issues = v_blocking_issues,
                warnings = v_warnings,
                analysis_date = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (
                task_id,
                table_rows,
                table_size_mb,
                recommended_strategy,
                recommendation_reason,
                date_column_name,
                date_column_type,
                date_format_detected,
                date_conversion_expr,
                requires_conversion,
                all_date_columns_analysis,
                estimated_partitions,
                avg_partition_size_mb,
                estimated_compression_ratio,
                estimated_space_savings_mb,
                complexity_score,
                dependent_objects,
                blocking_issues,
                warnings,
                analysis_date
            ) VALUES (
                p_task_id,
                v_num_rows,
                v_table_size,
                v_recommended_strategy,
                v_reason,
                v_date_column,
                v_date_type,
                v_date_format,
                v_conversion_expr,
                v_requires_conversion,
                v_all_date_analysis,
                v_partition_count,
                CASE WHEN v_partition_count > 0 THEN v_table_size / v_partition_count ELSE 0 END,
                CASE WHEN v_task.use_compression = 'Y' THEN 4 ELSE 1 END,
                CASE WHEN v_task.use_compression = 'Y' THEN v_table_size * 0.75 ELSE 0 END,
                v_complexity,
                v_dependent_objects,
                v_blocking_issues,
                v_warnings,
                SYSTIMESTAMP
            );

        -- Get analysis_id for newly inserted or updated record
        SELECT analysis_id INTO v_analysis_id
        FROM cmr.dwh_migration_analysis
        WHERE task_id = p_task_id;

        -- Update task
        UPDATE cmr.dwh_migration_tasks
        SET status = 'ANALYZED',
            source_rows = v_num_rows,
            source_size_mb = v_table_size,
            validation_status = CASE
                WHEN DBMS_LOB.INSTR(v_blocking_issues, 'ERROR') > 0 THEN 'BLOCKED'
                ELSE 'READY'
            END
        WHERE task_id = p_task_id;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Analysis complete:');
        DBMS_OUTPUT.PUT_LINE('  Recommended strategy: ' || NVL(v_recommended_strategy, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('  Complexity score: ' || v_complexity || '/10');
        DBMS_OUTPUT.PUT_LINE('  Estimated partitions: ' || NVL(v_partition_count, 0));

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_msg VARCHAR2(4000) := SQLERRM;
            BEGIN
                UPDATE cmr.dwh_migration_tasks
                SET status = 'FAILED',
                    error_message = 'Analysis failed: ' || v_error_msg
                WHERE task_id = p_task_id;
                COMMIT;
            END;
            RAISE;
    END analyze_table;


    PROCEDURE analyze_all_pending_tasks(
        p_project_id NUMBER DEFAULT NULL
    ) AS
        v_count NUMBER := 0;
    BEGIN
        FOR task IN (
            SELECT task_id, source_owner, source_table
            FROM cmr.dwh_migration_tasks
            WHERE (p_project_id IS NULL OR project_id = p_project_id)
            AND status IN ('PENDING', 'READY')
            ORDER BY task_id
        ) LOOP
            BEGIN
                analyze_table(task.task_id);
                v_count := v_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR analyzing ' || task.source_table || ': ' || SQLERRM);
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Analyzed ' || v_count || ' task(s)');
    END analyze_all_pending_tasks;


    -- ==========================================================================
    -- Utility Functions
    -- ==========================================================================

    FUNCTION get_table_size_mb(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_size_mb NUMBER;
    BEGIN
        SELECT ROUND(SUM(bytes)/1024/1024, 2)
        INTO v_size_mb
        FROM dba_segments
        WHERE owner = p_owner
        AND segment_name = p_table_name
        AND segment_type LIKE 'TABLE%';

        RETURN v_size_mb;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
        WHEN OTHERS THEN
            RETURN 0;
    END get_table_size_mb;

END pck_dwh_table_migration_analyzer;
/

SELECT 'Table Migration Analyzer Package Created Successfully!' AS status FROM DUAL;
