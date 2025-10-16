-- =============================================================================
-- Table Migration Analysis Package
-- Analyzes tables and recommends optimal partitioning strategies
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_table_migration_analyzer AUTHID CURRENT_USER AS
    -- ==========================================================================
    -- Configuration Constants
    -- ==========================================================================

    -- Sample size for Stage 1 detection (fast, no IS NOT NULL filter)
    -- Larger sample = better detection but slower on tables with many NULLs
    c_stage1_sample_size CONSTANT NUMBER := 100;

    -- Sample size for Stage 2 detection (with IS NOT NULL + parallel hint)
    -- Smaller sample since IS NOT NULL may trigger full table scan
    c_stage2_sample_size CONSTANT NUMBER := 20;

    -- Minimum non-null samples required for validation (must be <= stage1_sample_size)
    c_min_valid_samples CONSTANT NUMBER := 5;

    -- ==========================================================================
    -- Main analysis procedures
    -- ==========================================================================

    PROCEDURE analyze_table(
        p_task_id NUMBER
    );

    PROCEDURE analyze_all_pending_tasks(
        p_project_id NUMBER DEFAULT NULL
    );

    -- ==========================================================================
    -- Partitioning Detection Functions
    -- ==========================================================================

    -- Check if table is already partitioned by a date column
    -- Returns: 'SKIP' - skip analysis (already optimally partitioned by date)
    --          'WARN' - add warning (partitioned but not by date)
    --          'CONTINUE' - continue analysis (not partitioned)
    FUNCTION check_existing_partitioning(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_details OUT VARCHAR2,  -- Detailed partition info for logging
        p_warning_json OUT CLOB            -- JSON warning for non-date partitioning
    ) RETURN VARCHAR2;

    -- ==========================================================================
    -- Analysis helper functions
    -- ==========================================================================
    FUNCTION recommend_partition_strategy(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_reason OUT VARCHAR2
    ) RETURN VARCHAR2;

    FUNCTION estimate_partition_count(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_partition_type VARCHAR2,
        p_interval_clause VARCHAR2 DEFAULT NULL
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
        p_parallel_degree NUMBER,
        p_distinct_values OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_distribution_type OUT VARCHAR2
    );

    -- Dependency analysis
    FUNCTION get_dependent_objects(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_column VARCHAR2 DEFAULT NULL,
        p_analyzed_columns SYS.ODCIVARCHAR2LIST DEFAULT NULL
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

    FUNCTION get_compression_ratio(
        p_compression_type VARCHAR2
    ) RETURN NUMBER;

    FUNCTION calculate_optimal_initial_extent(
        p_avg_partition_mb NUMBER,
        p_total_table_mb NUMBER
    ) RETURN NUMBER;

    FUNCTION get_dwh_ilm_config(
        p_config_key VARCHAR2
    ) RETURN VARCHAR2;

    -- ==========================================================================
    -- AUTOMATIC LIST Partitioning helper functions
    -- ==========================================================================

    -- Get default values for P_XDEF partition based on data type
    -- Returns user-provided defaults or auto-generated type-aware defaults
    FUNCTION get_list_default_values(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_user_defaults VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2;

    -- Validate user-provided default values against column data types
    FUNCTION validate_list_defaults(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_user_defaults VARCHAR2
    ) RETURN BOOLEAN;

END pck_dwh_table_migration_analyzer;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_table_migration_analyzer AS

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


    -- Get ALL potential date columns (DATE, NUMBER, VARCHAR) with their data types
    FUNCTION get_all_potential_date_columns(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_names OUT SYS.ODCIVARCHAR2LIST,
        p_data_types OUT SYS.ODCIVARCHAR2LIST
    ) RETURN NUMBER
    AS
        TYPE t_column_rec IS RECORD (column_name VARCHAR2(128), data_type VARCHAR2(30));
        TYPE t_column_list IS TABLE OF t_column_rec;
        v_all_columns t_column_list;
        v_count NUMBER := 0;
    BEGIN
        p_column_names := SYS.ODCIVARCHAR2LIST();
        p_data_types := SYS.ODCIVARCHAR2LIST();

        -- Get DATE/TIMESTAMP columns AND NUMBER/VARCHAR columns with date-like names
        SELECT column_name, data_type
        BULK COLLECT INTO v_all_columns
        FROM dba_tab_columns
        WHERE owner = p_owner
        AND table_name = p_table_name
        AND (
            -- Standard date types
            data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
            OR
            -- NUMBER columns with date-like names
            (data_type = 'NUMBER' AND (
                UPPER(column_name) LIKE '%DATE%' OR UPPER(column_name) LIKE '%TIME%' OR
                UPPER(column_name) LIKE '%DTTM%' OR UPPER(column_name) LIKE '%DT' OR
                UPPER(column_name) LIKE '%MONTH%' OR UPPER(column_name) LIKE '%PERIOD%' OR
                UPPER(column_name) LIKE '%YM%' OR UPPER(column_name) LIKE '%YR%' OR
                UPPER(column_name) LIKE '%YEAR%' OR UPPER(column_name) LIKE '%MO%' OR
                UPPER(column_name) LIKE '%FISCAL%' OR UPPER(column_name) LIKE '%RPT%' OR
                UPPER(column_name) LIKE '%REPORT%'
            ))
            OR
            -- VARCHAR columns with date-like names
            (data_type IN ('VARCHAR2', 'CHAR') AND (
                UPPER(column_name) LIKE '%DATE%' OR UPPER(column_name) LIKE '%TIME%' OR
                UPPER(column_name) LIKE '%DTTM%' OR UPPER(column_name) LIKE '%DT' OR
                UPPER(column_name) LIKE '%MONTH%' OR UPPER(column_name) LIKE '%PERIOD%' OR
                UPPER(column_name) LIKE '%YM%' OR UPPER(column_name) LIKE '%YR%' OR
                UPPER(column_name) LIKE '%YEAR%' OR UPPER(column_name) LIKE '%MO%' OR
                UPPER(column_name) LIKE '%FISCAL%' OR UPPER(column_name) LIKE '%RPT%' OR
                UPPER(column_name) LIKE '%REPORT%'
            ))
        )
        ORDER BY
            CASE data_type
                WHEN 'DATE' THEN 1
                WHEN 'TIMESTAMP' THEN 2
                WHEN 'TIMESTAMP(6)' THEN 3
                WHEN 'NUMBER' THEN 4
                ELSE 5
            END,
            column_id;

        -- Convert to separate lists
        IF v_all_columns IS NOT NULL AND v_all_columns.COUNT > 0 THEN
            FOR i IN 1..v_all_columns.COUNT LOOP
                p_column_names.EXTEND;
                p_data_types.EXTEND;
                p_column_names(p_column_names.COUNT) := v_all_columns(i).column_name;
                p_data_types(p_data_types.COUNT) := v_all_columns(i).data_type;
                v_count := v_count + 1;
            END LOOP;
        END IF;

        RETURN v_count;
    END get_all_potential_date_columns;


    FUNCTION get_parallel_degree(
        p_owner VARCHAR2,
        p_table_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_num_rows NUMBER;
        v_bytes NUMBER;
        v_size_mb NUMBER;
        v_parallel_by_rows NUMBER := 1;
        v_parallel_by_size NUMBER := 1;
        v_parallel_degree NUMBER;
    BEGIN
        -- Get table row count and size from statistics
        BEGIN
            SELECT NVL(num_rows, 0), NVL(bytes, 0)
            INTO v_num_rows, v_bytes
            FROM dba_tables t
            LEFT JOIN dba_segments s ON s.owner = t.owner AND s.segment_name = t.table_name
            WHERE t.owner = p_owner
            AND t.table_name = p_table_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_num_rows := 0;
                v_bytes := 0;
        END;

        -- Calculate parallelism based on SIZE (MB)
        -- For DWH tables, size is often more important than row count (wide rows)
        IF v_bytes > 0 THEN
            v_size_mb := v_bytes / 1024 / 1024;

            -- Size-based parallelism
            -- <10 MB: degree 1
            -- 10-100 MB: degree 2
            -- 100-1000 MB (100MB-1GB): degree 4
            -- 1-10 GB: degree 8
            -- >10 GB: degree 16
            IF v_size_mb < 10 THEN
                v_parallel_by_size := 1;
            ELSIF v_size_mb < 100 THEN
                v_parallel_by_size := 2;
            ELSIF v_size_mb < 1000 THEN
                v_parallel_by_size := 4;
            ELSIF v_size_mb < 10000 THEN
                v_parallel_by_size := 8;
            ELSE
                v_parallel_by_size := 16;
            END IF;
        END IF;

        -- Calculate parallelism based on ROW COUNT
        IF v_num_rows > 0 THEN
            -- Row count based parallelism
            -- Small tables (<100K rows): degree 1
            -- Medium tables (100K-1M rows): degree 2
            -- Large tables (1M-10M rows): degree 4
            -- Very large tables (10M-100M rows): degree 8
            -- Huge tables (>100M rows): degree 16
            IF v_num_rows < 100000 THEN
                v_parallel_by_rows := 1;
            ELSIF v_num_rows < 1000000 THEN
                v_parallel_by_rows := 2;
            ELSIF v_num_rows < 10000000 THEN
                v_parallel_by_rows := 4;
            ELSIF v_num_rows < 100000000 THEN
                v_parallel_by_rows := 8;
            ELSE
                v_parallel_by_rows := 16;
            END IF;
        END IF;

        -- Take the GREATER of the two recommendations
        -- This ensures wide-row tables get appropriate parallelism
        -- even if they have relatively few rows
        v_parallel_degree := GREATEST(v_parallel_by_size, v_parallel_by_rows);

        RETURN v_parallel_degree;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 1; -- Default to no parallelism on error
    END get_parallel_degree;


    FUNCTION get_column_usage_score(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_index_count NUMBER := 0;
        v_view_count NUMBER := 0;
        v_source_count NUMBER := 0;
        v_usage_score NUMBER := 0;
    BEGIN
        -- Check if column is indexed (strongest indicator - proven query usage)
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

        -- Check usage in views (search TEXT_VC for table and column references together)
        BEGIN
            SELECT COUNT(DISTINCT view_name)
            INTO v_view_count
            FROM dba_views
            WHERE owner = p_owner
            AND UPPER(text_vc) LIKE '%' || UPPER(p_table_name) || '%' ||
                                          UPPER(p_column_name) || '%';
        EXCEPTION WHEN OTHERS THEN
            v_view_count := 0;
        END;

        -- Check usage in stored code (search DBA_SOURCE.TEXT for table and column references together)
        BEGIN
            SELECT COUNT(DISTINCT name || type)
            INTO v_source_count
            FROM dba_source
            WHERE owner = p_owner
            AND type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION')
            AND UPPER(text) LIKE '%' || UPPER(p_table_name) || '%' ||
                                        UPPER(p_column_name) || '%';
        EXCEPTION WHEN OTHERS THEN
            v_source_count := 0;
        END;

        -- Calculate weighted score
        -- Indexes: weight 10 (strongest - proven query usage)
        -- Views: weight 3 (likely filtering/joining)
        -- Stored code: weight 2 (programmatic access)
        v_usage_score := (v_index_count * 10) +
                        (v_view_count * 3) +
                        (v_source_count * 2);

        RETURN v_usage_score;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_column_usage_score;


    FUNCTION analyze_date_column(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2,
        p_parallel_degree NUMBER,
        p_min_date OUT DATE,
        p_max_date OUT DATE,
        p_range_days OUT NUMBER,
        p_null_count OUT NUMBER,
        p_non_null_count OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_has_time_component OUT VARCHAR2,
        p_distinct_dates OUT NUMBER,
        p_usage_score OUT NUMBER,
        p_data_quality_issue OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_sql VARCHAR2(4000);
        v_error_code NUMBER;
        v_error_msg VARCHAR2(4000);
        v_total_count NUMBER;
        v_min_time VARCHAR2(8);
        v_max_time VARCHAR2(8);
        v_time_sample NUMBER;
        v_min_year NUMBER;
        v_max_year NUMBER;
        v_filtered_min_date DATE;
        v_filtered_max_date DATE;
    BEGIN
        -- Initialize data quality flag
        p_data_quality_issue := 'N';

        -- Get date range and NULL statistics in a single query with parallel hint
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' ||
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
            p_null_percentage := ROUND((p_null_count / v_total_count) * 100, 4);
        ELSE
            p_null_percentage := 0;
        END IF;

        -- If MIN/MAX are NULL, column has no non-NULL data
        IF p_min_date IS NOT NULL AND p_max_date IS NOT NULL THEN
            -- Check for suspicious years and recalculate range excluding them
            v_min_year := EXTRACT(YEAR FROM p_min_date);
            v_max_year := EXTRACT(YEAR FROM p_max_date);

            IF v_min_year < 1900 OR v_min_year > 2100 OR v_max_year < 1900 OR v_max_year > 2100 THEN
                p_data_quality_issue := 'Y';

                -- Recalculate MIN/MAX filtering out suspicious dates
                v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' ||
                        '  MIN(' || p_column_name || '), ' ||
                        '  MAX(' || p_column_name || ') ' ||
                        ' FROM ' || p_owner || '.' || p_table_name ||
                        ' WHERE ' || p_column_name || ' IS NOT NULL' ||
                        '   AND EXTRACT(YEAR FROM ' || p_column_name || ') >= 1900' ||
                        '   AND EXTRACT(YEAR FROM ' || p_column_name || ') <= 2100';

                BEGIN
                    EXECUTE IMMEDIATE v_sql INTO v_filtered_min_date, v_filtered_max_date;

                    -- Use filtered dates for range calculation if valid
                    IF v_filtered_min_date IS NOT NULL AND v_filtered_max_date IS NOT NULL THEN
                        p_range_days := ROUND(v_filtered_max_date - v_filtered_min_date, 4);
                    ELSE
                        p_range_days := ROUND(p_max_date - p_min_date, 4);
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        -- If filtering fails, use original range
                        p_range_days := ROUND(p_max_date - p_min_date, 4);
                END;
            ELSE
                -- Dates are clean, no data quality issue
                p_data_quality_issue := 'N';
                p_range_days := ROUND(p_max_date - p_min_date, 4);
            END IF;

            -- Check if column has time component (not all midnight)
            -- Sample: check if any non-midnight times exist
            v_sql := 'SELECT COUNT(*) FROM (' ||
                    '  SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' || p_column_name ||
                    '  FROM ' || p_owner || '.' || p_table_name ||
                    '  WHERE ' || p_column_name || ' IS NOT NULL' ||
                    '    AND ' || p_column_name || ' != TRUNC(' || p_column_name || ')' ||
                    '  AND ROWNUM <= 1' ||
                    ')';

            EXECUTE IMMEDIATE v_sql INTO v_time_sample;

            IF v_time_sample > 0 OR v_min_time != '00:00:00' OR v_max_time != '00:00:00' THEN
                p_has_time_component := 'Y';

                -- Get distinct date count (without time) with parallel hint
                v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ COUNT(DISTINCT TRUNC(' || p_column_name || ')) ' ||
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


    -- Helper function to build conversion expression for NUMBER/VARCHAR date columns
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
            ELSIF p_date_format = 'YYYYMM' THEN
                -- Convert YYYYMM to date by appending '01' for first day of month
                RETURN 'TO_DATE(TO_CHAR(' || p_column_name || ') || ''01'', ''YYYYMMDD'')';
            ELSIF p_date_format = 'YYMMDD' THEN
                RETURN 'TO_DATE(TO_CHAR(' || p_column_name || '), ''YYMMDD'')';
            ELSIF p_date_format = 'UNIX_TIMESTAMP' THEN
                RETURN 'TO_DATE(''1970-01-01'', ''YYYY-MM-DD'') + (' || p_column_name || ' / 86400)';
            END IF;

        -- Handle VARCHAR/CHAR-based dates
        ELSIF p_data_type IN ('VARCHAR2', 'CHAR') THEN
            IF p_date_format = 'YYYY-MM' THEN
                -- Convert YYYY-MM to date by appending '-01' for first day of month
                RETURN 'TO_DATE(' || p_column_name || ' || ''-01'', ''YYYY-MM-DD'')';
            ELSIF p_date_format = 'YYYYMM' THEN
                -- Convert YYYYMM (VARCHAR) to date by appending '01'
                RETURN 'TO_DATE(' || p_column_name || ' || ''01'', ''YYYYMMDD'')';
            ELSE
                RETURN 'TO_DATE(' || p_column_name || ', ''' || p_date_format || ''')';
            END IF;
        END IF;

        -- Default: return column as-is
        RETURN p_column_name;
    END get_date_conversion_expr;


    -- Unified analysis function that works for DATE, NUMBER, and VARCHAR columns
    -- For NUMBER/VARCHAR, it converts to DATE using the detected format
    FUNCTION analyze_any_date_column(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_column_name VARCHAR2,
        p_data_type VARCHAR2,          -- 'DATE', 'NUMBER', 'VARCHAR2'
        p_date_format VARCHAR2,        -- Format for NUMBER/VARCHAR conversion
        p_parallel_degree NUMBER,
        p_min_date OUT DATE,
        p_max_date OUT DATE,
        p_range_days OUT NUMBER,
        p_null_count OUT NUMBER,
        p_non_null_count OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_has_time_component OUT VARCHAR2,
        p_distinct_dates OUT NUMBER,
        p_usage_score OUT NUMBER,
        p_data_quality_issue OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_sql VARCHAR2(4000);
        v_column_expr VARCHAR2(500);
        v_total_count NUMBER;
        v_min_year NUMBER;
        v_max_year NUMBER;
    BEGIN
        -- Initialize
        p_data_quality_issue := 'N';
        p_has_time_component := 'N';

        -- Build column expression based on data type
        IF p_data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') THEN
            v_column_expr := p_column_name;
        ELSE
            -- For NUMBER/VARCHAR, use conversion expression
            v_column_expr := get_date_conversion_expr(p_column_name, p_data_type, p_date_format);
        END IF;

        -- Get date range and NULL statistics
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' ||
                '  MIN(' || v_column_expr || '), ' ||
                '  MAX(' || v_column_expr || '), ' ||
                '  COUNT(*), ' ||
                '  COUNT(' || p_column_name || '), ' ||  -- Count original column for NULLs
                '  COUNT(*) - COUNT(' || p_column_name || ') ' ||
                ' FROM ' || p_owner || '.' || p_table_name;

        BEGIN
            EXECUTE IMMEDIATE v_sql INTO p_min_date, p_max_date, v_total_count, p_non_null_count, p_null_count;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  ERROR analyzing ' || p_column_name || ': ' || SQLERRM);
                RETURN FALSE;
        END;

        -- Calculate NULL percentage
        IF v_total_count > 0 THEN
            p_null_percentage := ROUND((p_null_count / v_total_count) * 100, 4);
        ELSE
            p_null_percentage := 0;
        END IF;

        -- If MIN/MAX are NULL, column has no non-NULL data
        IF p_min_date IS NULL OR p_max_date IS NULL THEN
            RETURN FALSE;
        END IF;

        -- Check for data quality issues (years outside 1900-2100)
        v_min_year := EXTRACT(YEAR FROM p_min_date);
        v_max_year := EXTRACT(YEAR FROM p_max_date);

        IF v_min_year < 1900 OR v_min_year > 2100 OR v_max_year < 1900 OR v_max_year > 2100 THEN
            p_data_quality_issue := 'Y';
        END IF;

        -- Calculate range
        p_range_days := ROUND(p_max_date - p_min_date, 4);

        -- Check for time component (only for DATE columns)
        IF p_data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') THEN
            v_sql := 'SELECT COUNT(*) FROM (' ||
                    '  SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' || p_column_name ||
                    '  FROM ' || p_owner || '.' || p_table_name ||
                    '  WHERE ' || p_column_name || ' IS NOT NULL' ||
                    '  AND TO_CHAR(' || p_column_name || ', ''HH24:MI:SS'') != ''00:00:00''' ||
                    '  AND ROWNUM <= 100)';
            BEGIN
                EXECUTE IMMEDIATE v_sql INTO v_total_count;
                IF v_total_count > 0 THEN
                    p_has_time_component := 'Y';
                END IF;
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
        ELSE
            -- NUMBER/VARCHAR date columns don't have time component
            p_has_time_component := 'N';
        END IF;

        -- Get distinct date count
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ COUNT(DISTINCT ' || v_column_expr || ') ' ||
                ' FROM ' || p_owner || '.' || p_table_name ||
                ' WHERE ' || p_column_name || ' IS NOT NULL';
        BEGIN
            EXECUTE IMMEDIATE v_sql INTO p_distinct_dates;
        EXCEPTION
            WHEN OTHERS THEN
                p_distinct_dates := 0;
        END;

        -- Get usage score (indexes, FKs, views, etc.)
        p_usage_score := get_column_usage_score(p_owner, p_table_name, p_column_name);

        RETURN TRUE;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  ERROR in analyze_any_date_column for ' || p_column_name || ': ' || SQLERRM);
            RETURN FALSE;
    END analyze_any_date_column;


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
        p_parallel_degree NUMBER,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        TYPE t_number_list IS TABLE OF NUMBER;
        v_samples t_number_list;
        v_sql VARCHAR2(4000);
        v_valid_count NUMBER;
        v_sample_val NUMBER;
    BEGIN
        -- Look for columns with date-like names that are NUMBER type
        FOR rec IN (
            SELECT column_name
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
                OR UPPER(column_name) LIKE '%MONTH%'
                OR UPPER(column_name) LIKE '%PERIOD%'
                OR UPPER(column_name) LIKE '%YM%'
                OR UPPER(column_name) LIKE '%YR%'
                OR UPPER(column_name) LIKE '%YEAR%'
                OR UPPER(column_name) LIKE '%MO%'
                OR UPPER(column_name) LIKE '%FISCAL%'
                OR UPPER(column_name) LIKE '%RPT%'
                OR UPPER(column_name) LIKE '%REPORT%'
                OR UPPER(column_name) IN ('EFFECTIVE_DT', 'VALID_FROM', 'VALID_TO', 'START_DT', 'END_DT')
            )
            ORDER BY column_id
        ) LOOP
            -- Sample 10 rows to validate format (no NOT NULL filter - fast, no full scan)
            BEGIN
                v_sql := 'SELECT ' || rec.column_name ||
                         ' FROM ' || p_owner || '.' || p_table_name ||
                         ' WHERE ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage1_sample_size;

                EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_samples;

                DBMS_OUTPUT.PUT_LINE('  Sampling NUMBER column: ' || rec.column_name || ' (' || v_samples.COUNT || ' samples)');

                IF v_samples IS NOT NULL AND v_samples.COUNT > 0 THEN
                    -- Filter out NULLs and check if we have enough valid samples
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_valid_count := v_valid_count + 1;
                        END IF;
                    END LOOP;

                    -- Need at least 5 non-null samples to validate
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        v_valid_count := 0;

                        -- Check YYYYMMDD format (8 digits, 19000101-21001231)
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF v_samples(i) IS NOT NULL THEN
                                v_sample_val := v_samples(i);
                                IF v_sample_val >= 19000101 AND v_sample_val <= 21001231
                                   AND LENGTH(TRUNC(v_sample_val)) = 8 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMMDD format (' || v_valid_count || ' valid samples)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMM format (6 digits, 190001-210012, month 01-12)
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF v_samples(i) IS NOT NULL THEN
                                v_sample_val := v_samples(i);
                                IF v_sample_val >= 190001 AND v_sample_val <= 210012
                                   AND LENGTH(TRUNC(v_sample_val)) = 6
                                   AND MOD(v_sample_val, 100) BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMM format (' || v_valid_count || ' valid samples)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMM';
                            RETURN TRUE;
                        END IF;

                        -- Check Unix timestamp (10+ digits, 946684800-2147483647)
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF v_samples(i) IS NOT NULL THEN
                                v_sample_val := v_samples(i);
                                IF v_sample_val >= 946684800 AND v_sample_val <= 2147483647
                                   AND LENGTH(TRUNC(v_sample_val)) >= 10 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected UNIX_TIMESTAMP format (' || v_valid_count || ' valid samples)');
                            p_date_column := rec.column_name;
                            p_date_format := 'UNIX_TIMESTAMP';
                            RETURN TRUE;
                        END IF;

                        DBMS_OUTPUT.PUT_LINE('    -> No valid date format detected');
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('    -> Not enough non-null samples (' || v_valid_count || '/' || pck_dwh_table_migration_analyzer.c_stage1_sample_size || ')');
                    END IF;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('    -> Error sampling: ' || SQLERRM);
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
        p_parallel_degree NUMBER,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        TYPE t_varchar_list IS TABLE OF VARCHAR2(4000);
        v_samples t_varchar_list;
        v_sql VARCHAR2(4000);
        v_valid_count NUMBER;
        v_sample_val VARCHAR2(4000);
        v_month_part NUMBER;
    BEGIN
        -- Look for columns with date-like names that are VARCHAR/CHAR type
        FOR rec IN (
            SELECT column_name
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
                OR UPPER(column_name) LIKE '%MONTH%'
                OR UPPER(column_name) LIKE '%PERIOD%'
                OR UPPER(column_name) LIKE '%YM%'
                OR UPPER(column_name) LIKE '%YR%'
                OR UPPER(column_name) LIKE '%YEAR%'
                OR UPPER(column_name) LIKE '%MO%'
                OR UPPER(column_name) LIKE '%FISCAL%'
                OR UPPER(column_name) LIKE '%RPT%'
                OR UPPER(column_name) LIKE '%REPORT%'
                OR UPPER(column_name) IN ('EFFECTIVE_DT', 'VALID_FROM', 'VALID_TO', 'START_DT', 'END_DT')
            )
            ORDER BY column_id
        ) LOOP
            -- Sample values to validate format (Stage 1: no IS NOT NULL filter)
            BEGIN
                v_sql := 'SELECT ' || rec.column_name ||
                         ' FROM ' || p_owner || '.' || p_table_name ||
                         ' WHERE ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage1_sample_size;

                EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_samples;

                DBMS_OUTPUT.PUT_LINE('  Sampling VARCHAR column: ' || rec.column_name || ' (' || v_samples.COUNT || ' samples)');

                IF v_samples IS NOT NULL AND v_samples.COUNT > 0 THEN
                    -- Filter out NULLs and check if we have enough valid samples
                    DECLARE
                        v_non_null_count NUMBER := 0;
                    BEGIN
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF v_samples(i) IS NOT NULL THEN
                                v_non_null_count := v_non_null_count + 1;
                            END IF;
                        END LOOP;

                        -- Need at least 5 non-null samples to validate
                        IF v_non_null_count < pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Not enough non-null samples (' || v_non_null_count || '/' || pck_dwh_table_migration_analyzer.c_stage1_sample_size || ')');
                            GOTO next_varchar_column;
                        END IF;
                    END;

                    v_valid_count := 0;

                    -- Check YYYY-MM-DD format
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{4}-\d{2}-\d{2}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected YYYY-MM-DD format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'YYYY-MM-DD';
                        RETURN TRUE;
                    END IF;

                    -- Check DD/MM/YYYY format
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{2}/\d{2}/\d{4}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected DD/MM/YYYY format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'DD/MM/YYYY';
                        RETURN TRUE;
                    END IF;

                    -- Check MM/DD/YYYY format
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{2}/\d{2}/\d{4}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected MM/DD/YYYY format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'MM/DD/YYYY';
                        RETURN TRUE;
                    END IF;

                    -- Check YYYY-MM format (year-month with hyphen)
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{4}-\d{2}$') THEN
                                v_month_part := TO_NUMBER(SUBSTR(v_sample_val, 6, 2));
                                IF v_month_part BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected YYYY-MM format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'YYYY-MM';
                        RETURN TRUE;
                    END IF;

                    -- Check YYYYMM format (6 digits, no separator)
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{6}$') THEN
                                v_month_part := TO_NUMBER(SUBSTR(v_sample_val, 5, 2));
                                IF v_month_part BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMM format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'YYYYMM';
                        RETURN TRUE;
                    END IF;

                    -- Check YYYYMMDD format (8 digits)
                    v_valid_count := 0;
                    FOR i IN 1..v_samples.COUNT LOOP
                        IF v_samples(i) IS NOT NULL THEN
                            v_sample_val := v_samples(i);
                            IF REGEXP_LIKE(v_sample_val, '^\d{8}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END IF;
                    END LOOP;
                    IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMMDD format (all ' || v_valid_count || ' samples valid)');
                        p_date_column := rec.column_name;
                        p_date_format := 'YYYYMMDD';
                        RETURN TRUE;
                    END IF;

                    DBMS_OUTPUT.PUT_LINE('    -> No valid date format detected');
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('    -> Error sampling: ' || SQLERRM);
                    NULL; -- Continue to next column
            END;
            <<next_varchar_column>>
            NULL; -- Label for GOTO
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_varchar_date_column;


    -- FALLBACK: Detect date columns by sampling content (when name patterns don't match)
    -- This checks ALL columns that were NOT already checked by name pattern functions
    FUNCTION detect_date_column_by_content(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_parallel_degree NUMBER,
        p_exclude_columns SYS.ODCIVARCHAR2LIST,  -- Columns already checked by name patterns
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2,
        p_data_type OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_sql VARCHAR2(4000);
        TYPE t_number_list IS TABLE OF NUMBER;
        TYPE t_varchar_list IS TABLE OF VARCHAR2(4000);
        v_number_samples t_number_list;
        v_varchar_samples t_varchar_list;
        v_sample_num NUMBER;
        v_valid_count NUMBER;
        v_month_part NUMBER;
        v_skip BOOLEAN;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('FALLBACK: Sampling columns without date-related names...');

        -- Get ALL NUMBER columns (excluding those already checked)
        FOR rec IN (
            SELECT column_name
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type = 'NUMBER'
            ORDER BY column_id
        ) LOOP
            -- Skip if already checked by name pattern
            v_skip := FALSE;
            IF p_exclude_columns IS NOT NULL THEN
                FOR i IN 1..p_exclude_columns.COUNT LOOP
                    IF p_exclude_columns(i) = rec.column_name THEN
                        v_skip := TRUE;
                        EXIT;
                    END IF;
                END LOOP;
            END IF;

            IF NOT v_skip THEN
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('  Sampling NUMBER column (no name pattern): ' || rec.column_name);

                    -- Sample values (Stage 1: no IS NOT NULL filter)
                    v_sql := 'SELECT ' || rec.column_name ||
                             ' FROM ' || p_owner || '.' || p_table_name ||
                             ' WHERE ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage1_sample_size;

                    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_number_samples;

                    IF v_number_samples IS NOT NULL AND v_number_samples.COUNT > 0 THEN
                        -- Filter out NULLs and check if we have enough valid samples
                        DECLARE
                            v_non_null_count NUMBER := 0;
                        BEGIN
                            FOR i IN 1..v_number_samples.COUNT LOOP
                                IF v_number_samples(i) IS NOT NULL THEN
                                    v_non_null_count := v_non_null_count + 1;
                                END IF;
                            END LOOP;

                            -- Need at least 5 non-null samples to validate
                            IF v_non_null_count < pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                                DBMS_OUTPUT.PUT_LINE('    -> Not enough non-null samples (' || v_non_null_count || '/' || pck_dwh_table_migration_analyzer.c_stage1_sample_size || ')');
                                GOTO next_number_column;
                            END IF;
                        END;

                        v_valid_count := 0;

                        -- Check YYYYMMDD format (need at least 5 valid)
                        FOR i IN 1..v_number_samples.COUNT LOOP
                            IF v_number_samples(i) IS NOT NULL THEN
                                v_sample_num := v_number_samples(i);
                                IF v_sample_num >= 19000101 AND v_sample_num <= 21001231
                                   AND LENGTH(TRUNC(v_sample_num)) = 8 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMMDD format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            p_data_type := 'NUMBER';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMM format (need at least 5 valid)
                        v_valid_count := 0;
                        FOR i IN 1..v_number_samples.COUNT LOOP
                            IF v_number_samples(i) IS NOT NULL THEN
                                v_sample_num := v_number_samples(i);
                                IF v_sample_num >= 190001 AND v_sample_num <= 210012
                                   AND LENGTH(TRUNC(v_sample_num)) = 6
                                   AND MOD(v_sample_num, 100) BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMM format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMM';
                            p_data_type := 'NUMBER';
                            RETURN TRUE;
                        END IF;

                        DBMS_OUTPUT.PUT_LINE('    -> No valid date format');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Error: ' || SQLERRM);
                        NULL; -- Skip columns with errors
                END;
                <<next_number_column>>
                NULL; -- Label for GOTO
            END IF;
        END LOOP;

        -- Get ALL VARCHAR columns (excluding those already checked)
        FOR rec IN (
            SELECT column_name
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type IN ('VARCHAR2', 'CHAR')
            ORDER BY column_id
        ) LOOP
            -- Skip if already checked by name pattern
            v_skip := FALSE;
            IF p_exclude_columns IS NOT NULL THEN
                FOR i IN 1..p_exclude_columns.COUNT LOOP
                    IF p_exclude_columns(i) = rec.column_name THEN
                        v_skip := TRUE;
                        EXIT;
                    END IF;
                END LOOP;
            END IF;

            IF NOT v_skip THEN
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('  Sampling VARCHAR column (no name pattern): ' || rec.column_name);

                    -- Sample values (Stage 1: no IS NOT NULL filter)
                    v_sql := 'SELECT ' || rec.column_name ||
                             ' FROM ' || p_owner || '.' || p_table_name ||
                             ' WHERE ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage1_sample_size;

                    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_varchar_samples;

                    IF v_varchar_samples IS NOT NULL AND v_varchar_samples.COUNT > 0 THEN
                        -- Filter out NULLs and check if we have enough valid samples
                        DECLARE
                            v_non_null_count NUMBER := 0;
                        BEGIN
                            FOR i IN 1..v_varchar_samples.COUNT LOOP
                                IF v_varchar_samples(i) IS NOT NULL THEN
                                    v_non_null_count := v_non_null_count + 1;
                                END IF;
                            END LOOP;

                            -- Need at least 5 non-null samples to validate
                            IF v_non_null_count < pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                                DBMS_OUTPUT.PUT_LINE('    -> Not enough non-null samples (' || v_non_null_count || '/' || pck_dwh_table_migration_analyzer.c_stage1_sample_size || ')');
                                GOTO next_varchar_fallback_column;
                            END IF;
                        END;

                        v_valid_count := 0;

                        -- Check YYYY-MM-DD format (need at least 5 valid)
                        FOR i IN 1..v_varchar_samples.COUNT LOOP
                            IF v_varchar_samples(i) IS NOT NULL THEN
                                IF REGEXP_LIKE(v_varchar_samples(i), '^\d{4}-\d{2}-\d{2}$') THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYY-MM-DD format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM-DD';
                            p_data_type := 'VARCHAR2';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYY-MM format (need at least 5 valid with month validation)
                        v_valid_count := 0;
                        FOR i IN 1..v_varchar_samples.COUNT LOOP
                            IF v_varchar_samples(i) IS NOT NULL THEN
                                IF REGEXP_LIKE(v_varchar_samples(i), '^\d{4}-\d{2}$') THEN
                                    v_month_part := TO_NUMBER(SUBSTR(v_varchar_samples(i), 6, 2));
                                    IF v_month_part BETWEEN 1 AND 12 THEN
                                        v_valid_count := v_valid_count + 1;
                                    END IF;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYY-MM format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM';
                            p_data_type := 'VARCHAR2';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMM format (need at least 5 valid with month validation)
                        v_valid_count := 0;
                        FOR i IN 1..v_varchar_samples.COUNT LOOP
                            IF v_varchar_samples(i) IS NOT NULL THEN
                                IF REGEXP_LIKE(v_varchar_samples(i), '^\d{6}$') THEN
                                    v_month_part := TO_NUMBER(SUBSTR(v_varchar_samples(i), 5, 2));
                                    IF v_month_part BETWEEN 1 AND 12 THEN
                                        v_valid_count := v_valid_count + 1;
                                    END IF;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMM format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMM';
                            p_data_type := 'VARCHAR2';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMMDD format (need at least 5 valid)
                        v_valid_count := 0;
                        FOR i IN 1..v_varchar_samples.COUNT LOOP
                            IF v_varchar_samples(i) IS NOT NULL THEN
                                IF REGEXP_LIKE(v_varchar_samples(i), '^\d{8}$') THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Detected YYYYMMDD format (all ' || v_valid_count || ' samples valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            p_data_type := 'VARCHAR2';
                            RETURN TRUE;
                        END IF;

                        DBMS_OUTPUT.PUT_LINE('    -> No valid date format');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Error: ' || SQLERRM);
                        NULL; -- Skip columns with errors
                END;
                <<next_varchar_fallback_column>>
                NULL; -- Label for GOTO
            END IF;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('  No date columns found by content sampling');
        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  Error in content-based detection: ' || SQLERRM);
            RETURN FALSE;
    END detect_date_column_by_content;


    -- STAGE 2: Detect NUMBER date columns with IS NOT NULL + parallel hint
    -- Only runs if Stage 1 found nothing (more expensive due to potential full scan)
    FUNCTION detect_numeric_date_column_stage2(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_parallel_degree NUMBER,
        p_exclude_columns SYS.ODCIVARCHAR2LIST,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        TYPE t_number_list IS TABLE OF NUMBER;
        v_samples t_number_list;
        v_sql VARCHAR2(4000);
        v_valid_count NUMBER;
        v_sample_val NUMBER;
        v_skip BOOLEAN;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('STAGE 2: Checking NUMBER columns with IS NOT NULL + parallel hint...');

        FOR rec IN (
            SELECT column_name
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type = 'NUMBER'
            ORDER BY column_id
        ) LOOP
            -- Skip if already checked
            v_skip := FALSE;
            IF p_exclude_columns IS NOT NULL THEN
                FOR i IN 1..p_exclude_columns.COUNT LOOP
                    IF p_exclude_columns(i) = rec.column_name THEN
                        v_skip := TRUE;
                        EXIT;
                    END IF;
                END LOOP;
            END IF;

            IF NOT v_skip THEN
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('  Stage 2 sampling NUMBER: ' || rec.column_name);

                    -- Sample with IS NOT NULL + parallel hint (may trigger full scan)
                    v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' || rec.column_name ||
                             ' FROM ' || p_owner || '.' || p_table_name ||
                             ' WHERE ' || rec.column_name || ' IS NOT NULL AND ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage2_sample_size;

                    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_samples;

                    IF v_samples IS NOT NULL AND v_samples.COUNT >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        -- Check YYYYMMDD format
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            v_sample_val := v_samples(i);
                            IF v_sample_val >= 19000101 AND v_sample_val <= 21001231
                               AND LENGTH(TRUNC(v_sample_val)) = 8 THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYYMMDD (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMM format
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            v_sample_val := v_samples(i);
                            IF v_sample_val >= 190001 AND v_sample_val <= 210012
                               AND LENGTH(TRUNC(v_sample_val)) = 6
                               AND MOD(v_sample_val, 100) BETWEEN 1 AND 12 THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYYMM (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMM';
                            RETURN TRUE;
                        END IF;

                        -- Check UNIX_TIMESTAMP format
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            v_sample_val := v_samples(i);
                            IF v_sample_val >= 946684800 AND v_sample_val <= 4102444800 THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected UNIX_TIMESTAMP (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'UNIX_TIMESTAMP';
                            RETURN TRUE;
                        END IF;
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('    -> Insufficient samples (' || NVL(v_samples.COUNT, 0) || ')');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Error: ' || SQLERRM);
                        NULL;
                END;
            END IF;
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_numeric_date_column_stage2;


    -- STAGE 2: Detect VARCHAR date columns with IS NOT NULL + parallel hint
    FUNCTION detect_varchar_date_column_stage2(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_parallel_degree NUMBER,
        p_exclude_columns SYS.ODCIVARCHAR2LIST,
        p_date_column OUT VARCHAR2,
        p_date_format OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        TYPE t_varchar_list IS TABLE OF VARCHAR2(4000);
        v_samples t_varchar_list;
        v_sql VARCHAR2(4000);
        v_valid_count NUMBER;
        v_month_part NUMBER;
        v_skip BOOLEAN;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('STAGE 2: Checking VARCHAR columns with IS NOT NULL + parallel hint...');

        FOR rec IN (
            SELECT column_name
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type IN ('VARCHAR2', 'CHAR')
            ORDER BY column_id
        ) LOOP
            -- Skip if already checked
            v_skip := FALSE;
            IF p_exclude_columns IS NOT NULL THEN
                FOR i IN 1..p_exclude_columns.COUNT LOOP
                    IF p_exclude_columns(i) = rec.column_name THEN
                        v_skip := TRUE;
                        EXIT;
                    END IF;
                END LOOP;
            END IF;

            IF NOT v_skip THEN
                BEGIN
                    DBMS_OUTPUT.PUT_LINE('  Stage 2 sampling VARCHAR: ' || rec.column_name);

                    -- Sample with IS NOT NULL + parallel hint
                    v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' || rec.column_name ||
                             ' FROM ' || p_owner || '.' || p_table_name ||
                             ' WHERE ' || rec.column_name || ' IS NOT NULL AND ROWNUM <= ' || pck_dwh_table_migration_analyzer.c_stage2_sample_size;

                    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_samples;

                    IF v_samples IS NOT NULL AND v_samples.COUNT >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                        -- Check YYYY-MM-DD
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF REGEXP_LIKE(v_samples(i), '^\d{4}-\d{2}-\d{2}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYY-MM-DD (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM-DD';
                            RETURN TRUE;
                        END IF;

                        -- Check DD/MM/YYYY
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF REGEXP_LIKE(v_samples(i), '^\d{2}/\d{2}/\d{4}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected DD/MM/YYYY (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'DD/MM/YYYY';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYY-MM
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF REGEXP_LIKE(v_samples(i), '^\d{4}-\d{2}$') THEN
                                v_month_part := TO_NUMBER(SUBSTR(v_samples(i), 6, 2));
                                IF v_month_part BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYY-MM (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYY-MM';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMM
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF REGEXP_LIKE(v_samples(i), '^\d{6}$') THEN
                                v_month_part := TO_NUMBER(SUBSTR(v_samples(i), 5, 2));
                                IF v_month_part BETWEEN 1 AND 12 THEN
                                    v_valid_count := v_valid_count + 1;
                                END IF;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYYMM (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMM';
                            RETURN TRUE;
                        END IF;

                        -- Check YYYYMMDD
                        v_valid_count := 0;
                        FOR i IN 1..v_samples.COUNT LOOP
                            IF REGEXP_LIKE(v_samples(i), '^\d{8}$') THEN
                                v_valid_count := v_valid_count + 1;
                            END IF;
                        END LOOP;
                        IF v_valid_count >= pck_dwh_table_migration_analyzer.c_min_valid_samples THEN
                            DBMS_OUTPUT.PUT_LINE('    -> Stage 2 detected YYYYMMDD (' || v_valid_count || ' valid)');
                            p_date_column := rec.column_name;
                            p_date_format := 'YYYYMMDD';
                            RETURN TRUE;
                        END IF;
                    ELSE
                        DBMS_OUTPUT.PUT_LINE('    -> Insufficient samples (' || NVL(v_samples.COUNT, 0) || ')');
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('    -> Error: ' || SQLERRM);
                        NULL;
                END;
            END IF;
        END LOOP;

        RETURN FALSE;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN FALSE;
    END detect_varchar_date_column_stage2;


    -- Collect ALL potential date columns with their types and formats
    -- Returns a collection that can be used for unified analysis
    PROCEDURE collect_all_date_candidates(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_parallel_degree NUMBER,
        p_column_names OUT SYS.ODCIVARCHAR2LIST,
        p_data_types OUT SYS.ODCIVARCHAR2LIST,
        p_date_formats OUT SYS.ODCIVARCHAR2LIST
    ) AS
        v_count NUMBER := 0;
        v_format VARCHAR2(50);
        v_detected BOOLEAN;
        v_temp_col VARCHAR2(128);
        v_temp_type VARCHAR2(30);
    BEGIN
        p_column_names := SYS.ODCIVARCHAR2LIST();
        p_data_types := SYS.ODCIVARCHAR2LIST();
        p_date_formats := SYS.ODCIVARCHAR2LIST();

        DBMS_OUTPUT.PUT_LINE('Collecting ALL potential date column candidates...');

        -- 1. Get all DATE/TIMESTAMP columns
        FOR rec IN (
            SELECT column_name, data_type
            FROM dba_tab_columns
            WHERE owner = p_owner
            AND table_name = p_table_name
            AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
            ORDER BY column_id
        ) LOOP
            p_column_names.EXTEND;
            p_data_types.EXTEND;
            p_date_formats.EXTEND;
            p_column_names(p_column_names.COUNT) := rec.column_name;
            p_data_types(p_data_types.COUNT) := rec.data_type;
            p_date_formats(p_date_formats.COUNT) := 'STANDARD';
            v_count := v_count + 1;
            DBMS_OUTPUT.PUT_LINE('  Added DATE candidate: ' || rec.column_name || ' (' || rec.data_type || ')');
        END LOOP;

        -- 2. Try NUMBER columns with name patterns
        DBMS_OUTPUT.PUT_LINE('  Checking NUMBER columns by name pattern...');
        IF detect_numeric_date_column(p_owner, p_table_name, p_parallel_degree, v_temp_col, v_format) THEN
            p_column_names.EXTEND;
            p_data_types.EXTEND;
            p_date_formats.EXTEND;
            p_column_names(p_column_names.COUNT) := v_temp_col;
            p_data_types(p_data_types.COUNT) := 'NUMBER';
            p_date_formats(p_date_formats.COUNT) := v_format;
            v_count := v_count + 1;
            DBMS_OUTPUT.PUT_LINE('  Added NUMBER candidate: ' || v_temp_col || ' (format: ' || v_format || ')');
        END IF;

        -- 3. Try VARCHAR columns with name patterns
        DBMS_OUTPUT.PUT_LINE('  Checking VARCHAR columns by name pattern...');
        IF detect_varchar_date_column(p_owner, p_table_name, p_parallel_degree, v_temp_col, v_format) THEN
            p_column_names.EXTEND;
            p_data_types.EXTEND;
            p_date_formats.EXTEND;
            p_column_names(p_column_names.COUNT) := v_temp_col;
            p_data_types(p_data_types.COUNT) := 'VARCHAR2';
            p_date_formats(p_date_formats.COUNT) := v_format;
            v_count := v_count + 1;
            DBMS_OUTPUT.PUT_LINE('  Added VARCHAR candidate: ' || v_temp_col || ' (format: ' || v_format || ')');
        END IF;

        -- 4. FALLBACK: Content-based detection for columns WITHOUT name patterns
        -- Build exclusion list of columns already checked
        DBMS_OUTPUT.PUT_LINE('  Checking remaining columns by content sampling...');
        DECLARE
            v_exclude_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
        BEGIN
            -- Build exclusion list from all columns already found by name patterns
            FOR i IN 1..p_column_names.COUNT LOOP
                v_exclude_list.EXTEND;
                v_exclude_list(v_exclude_list.COUNT) := p_column_names(i);
            END LOOP;

            -- Run fallback with exclusion list
            IF detect_date_column_by_content(p_owner, p_table_name, p_parallel_degree, v_exclude_list, v_temp_col, v_format, v_temp_type) THEN
                p_column_names.EXTEND;
                p_data_types.EXTEND;
                p_date_formats.EXTEND;
                p_column_names(p_column_names.COUNT) := v_temp_col;
                p_data_types(p_data_types.COUNT) := v_temp_type;
                p_date_formats(p_date_formats.COUNT) := v_format;
                v_count := v_count + 1;
                DBMS_OUTPUT.PUT_LINE('  Added by content: ' || v_temp_col || ' (' || v_temp_type || ' ' || v_format || ')');
            END IF;
        END;

        -- 5. STAGE 2: If no candidates found and no DATE columns, try IS NOT NULL + parallel
        -- This is more expensive (may trigger full scans) so only run as last resort
        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('No candidates found in Stage 1. Trying Stage 2 (IS NOT NULL + parallel)...');

            DECLARE
                v_exclude_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
                v_date_count NUMBER := 0;
            BEGIN
                -- Count how many DATE columns we have
                SELECT COUNT(*)
                INTO v_date_count
                FROM dba_tab_columns
                WHERE owner = p_owner
                AND table_name = p_table_name
                AND data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)');

                -- Only run Stage 2 if NO DATE columns found
                IF v_date_count = 0 THEN
                    -- Build exclusion list (should be empty at this point, but include for safety)
                    FOR i IN 1..p_column_names.COUNT LOOP
                        v_exclude_list.EXTEND;
                        v_exclude_list(v_exclude_list.COUNT) := p_column_names(i);
                    END LOOP;

                    -- Try Stage 2 NUMBER detection
                    IF detect_numeric_date_column_stage2(p_owner, p_table_name, p_parallel_degree, v_exclude_list, v_temp_col, v_format) THEN
                        p_column_names.EXTEND;
                        p_data_types.EXTEND;
                        p_date_formats.EXTEND;
                        p_column_names(p_column_names.COUNT) := v_temp_col;
                        p_data_types(p_data_types.COUNT) := 'NUMBER';
                        p_date_formats(p_date_formats.COUNT) := v_format;
                        v_count := v_count + 1;
                        DBMS_OUTPUT.PUT_LINE('  Added by Stage 2 NUMBER: ' || v_temp_col || ' (format: ' || v_format || ')');
                    ELSE
                        -- If Stage 2 NUMBER didn't find anything, try Stage 2 VARCHAR
                        -- Update exclusion list
                        IF p_column_names.COUNT > 0 THEN
                            v_exclude_list.EXTEND;
                            v_exclude_list(v_exclude_list.COUNT) := p_column_names(p_column_names.COUNT);
                        END IF;

                        IF detect_varchar_date_column_stage2(p_owner, p_table_name, p_parallel_degree, v_exclude_list, v_temp_col, v_format) THEN
                            p_column_names.EXTEND;
                            p_data_types.EXTEND;
                            p_date_formats.EXTEND;
                            p_column_names(p_column_names.COUNT) := v_temp_col;
                            p_data_types(p_data_types.COUNT) := 'VARCHAR2';
                            p_date_formats(p_date_formats.COUNT) := v_format;
                            v_count := v_count + 1;
                            DBMS_OUTPUT.PUT_LINE('  Added by Stage 2 VARCHAR: ' || v_temp_col || ' (format: ' || v_format || ')');
                        END IF;
                    END IF;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Stage 2 skipped: Found ' || v_date_count || ' DATE/TIMESTAMP columns');
                END IF;
            END;
        END IF;

        DBMS_OUTPUT.PUT_LINE('Total candidates collected: ' || v_count);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR in collect_all_date_candidates: ' || SQLERRM);
            -- Return empty lists on error
            p_column_names := SYS.ODCIVARCHAR2LIST();
            p_data_types := SYS.ODCIVARCHAR2LIST();
            p_date_formats := SYS.ODCIVARCHAR2LIST();
    END collect_all_date_candidates;


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
        p_parallel_degree NUMBER,
        p_distinct_values OUT NUMBER,
        p_null_percentage OUT NUMBER,
        p_distribution_type OUT VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_total_rows NUMBER;
    BEGIN
        -- Get total rows with parallel hint
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ COUNT(*) FROM ' ||
                p_owner || '.' || p_table_name;
        EXECUTE IMMEDIATE v_sql INTO v_total_rows;

        -- Get distinct values with parallel hint
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ COUNT(DISTINCT ' || p_column_name || ') FROM ' ||
                p_owner || '.' || p_table_name;
        EXECUTE IMMEDIATE v_sql INTO p_distinct_values;

        -- Get null percentage with parallel hint
        v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ROUND(COUNT(*) * 100.0 / ' || v_total_rows || ', 4) FROM ' ||
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
        v_null_count NUMBER;
        v_non_null_count NUMBER;
        v_null_percentage NUMBER;
        v_has_time_component VARCHAR2(1);
        v_distinct_dates NUMBER;
        v_usage_score NUMBER;
        v_num_rows NUMBER;
        v_best_column VARCHAR2(128);
        v_max_range NUMBER := 0;
        v_strategy VARCHAR2(100);
        v_scd2_type VARCHAR2(30);
        v_scd2_column VARCHAR2(128);
        v_event_column VARCHAR2(128);
        v_staging_column VARCHAR2(128);
        v_parallel_degree NUMBER;
        v_data_quality_issue VARCHAR2(1);
    BEGIN
        -- Get parallel degree once for all operations
        v_parallel_degree := get_parallel_degree(p_owner, p_table_name);

        -- Get row count
        SELECT num_rows INTO v_num_rows
        FROM dba_tables
        WHERE owner = p_owner
        AND table_name = p_table_name;

        -- Check for SCD2 pattern first
        IF detect_scd2_pattern(p_owner, p_table_name, v_scd2_type, v_scd2_column) THEN
            -- Validate data quality of detected column
            DECLARE
                v_temp_min DATE; v_temp_max DATE; v_temp_range NUMBER;
                v_temp_null_cnt NUMBER; v_temp_non_null NUMBER; v_temp_null_pct NUMBER;
                v_temp_time VARCHAR2(1); v_temp_distinct NUMBER; v_temp_score NUMBER;
                v_temp_quality VARCHAR2(1);
            BEGIN
                IF analyze_date_column(p_owner, p_table_name, v_scd2_column, v_parallel_degree,
                    v_temp_min, v_temp_max, v_temp_range, v_temp_null_cnt, v_temp_non_null,
                    v_temp_null_pct, v_temp_time, v_temp_distinct, v_temp_score, v_temp_quality)
                AND v_temp_quality = 'N' THEN
                    -- Data quality OK, use stereotype recommendation
                    IF v_scd2_type = 'EFFECTIVE_DATE' THEN
                        v_strategy := 'RANGE(' || v_scd2_column || ') INTERVAL YEARLY';
                        p_reason := 'SCD2 table with effective_date pattern detected - yearly partitioning for historical tracking';
                    ELSIF v_scd2_type = 'VALID_FROM_TO' THEN
                        v_strategy := 'RANGE(' || v_scd2_column || ') INTERVAL YEARLY';
                        p_reason := 'SCD2 table with valid_from_dttm/valid_to_dttm pattern detected - yearly partitioning for historical tracking';
                    END IF;
                    RETURN v_strategy;
                END IF;
                -- If quality issue, fall through to general analysis
            END;
        END IF;

        -- Check for EVENTS table pattern
        IF detect_events_table(p_owner, p_table_name, v_event_column) THEN
            -- Validate data quality of detected column
            DECLARE
                v_temp_min DATE; v_temp_max DATE; v_temp_range NUMBER;
                v_temp_null_cnt NUMBER; v_temp_non_null NUMBER; v_temp_null_pct NUMBER;
                v_temp_time VARCHAR2(1); v_temp_distinct NUMBER; v_temp_score NUMBER;
                v_temp_quality VARCHAR2(1);
            BEGIN
                IF analyze_date_column(p_owner, p_table_name, v_event_column, v_parallel_degree,
                    v_temp_min, v_temp_max, v_temp_range, v_temp_null_cnt, v_temp_non_null,
                    v_temp_null_pct, v_temp_time, v_temp_distinct, v_temp_score, v_temp_quality)
                AND v_temp_quality = 'N' THEN
                    -- Data quality OK, use stereotype recommendation
                    IF UPPER(p_table_name) LIKE '%AUDIT%' OR UPPER(p_table_name) LIKE '%COMPLIANCE%' THEN
                        v_strategy := 'RANGE(' || v_event_column || ') INTERVAL MONTHLY';
                        p_reason := 'Audit/compliance events table detected - monthly partitioning for long-term retention';
                    ELSE
                        v_strategy := 'RANGE(' || v_event_column || ') INTERVAL DAILY';
                        p_reason := 'Events table detected - daily partitioning for high-volume event data';
                    END IF;
                    RETURN v_strategy;
                END IF;
                -- If quality issue, fall through to general analysis
            END;
        END IF;

        -- Check for STAGING table pattern
        IF detect_staging_table(p_owner, p_table_name, v_staging_column) THEN
            -- Validate data quality of detected column
            DECLARE
                v_temp_min DATE; v_temp_max DATE; v_temp_range NUMBER;
                v_temp_null_cnt NUMBER; v_temp_non_null NUMBER; v_temp_null_pct NUMBER;
                v_temp_time VARCHAR2(1); v_temp_distinct NUMBER; v_temp_score NUMBER;
                v_temp_quality VARCHAR2(1);
            BEGIN
                IF analyze_date_column(p_owner, p_table_name, v_staging_column, v_parallel_degree,
                    v_temp_min, v_temp_max, v_temp_range, v_temp_null_cnt, v_temp_non_null,
                    v_temp_null_pct, v_temp_time, v_temp_distinct, v_temp_score, v_temp_quality)
                AND v_temp_quality = 'N' THEN
                    -- Data quality OK, use stereotype recommendation
                    v_strategy := 'RANGE(' || v_staging_column || ') INTERVAL DAILY';
                    p_reason := 'Staging table detected - daily partitioning for easy partition exchange and purging';
                    RETURN v_strategy;
                END IF;
                -- If quality issue, fall through to general analysis
            END;
        END IF;

        -- Check for HIST table pattern
        DECLARE
            v_hist_column VARCHAR2(128);
            v_temp_min DATE; v_temp_max DATE; v_temp_range NUMBER;
            v_temp_null_cnt NUMBER; v_temp_non_null NUMBER; v_temp_null_pct NUMBER;
            v_temp_time VARCHAR2(1); v_temp_distinct NUMBER; v_temp_score NUMBER;
            v_temp_quality VARCHAR2(1);
        BEGIN
            IF detect_hist_table(p_owner, p_table_name, v_hist_column) THEN
                -- Validate data quality of detected column
                IF analyze_date_column(p_owner, p_table_name, v_hist_column, v_parallel_degree,
                    v_temp_min, v_temp_max, v_temp_range, v_temp_null_cnt, v_temp_non_null,
                    v_temp_null_pct, v_temp_time, v_temp_distinct, v_temp_score, v_temp_quality)
                AND v_temp_quality = 'N' THEN
                    -- Data quality OK, use stereotype recommendation
                    v_strategy := 'RANGE(' || v_hist_column || ') INTERVAL MONTHLY';
                    p_reason := 'Historical table detected - monthly partitioning for snapshot data retention';
                    RETURN v_strategy;
                END IF;
                -- If quality issue, fall through to general analysis
            END IF;
        END;

        -- Note: Detection of non-standard date formats (NUMBER, VARCHAR) is handled
        -- in the analyze_table procedure and stored in dwh_migration_analysis table

        -- Get date columns for general analysis
        v_date_columns := get_date_columns(p_owner, p_table_name);

        -- Analyze each date column with quality-first selection
        DECLARE
            v_best_quality VARCHAR2(1) := 'Y';  -- Start pessimistic, prefer 'N' (clean)
            v_max_usage_score NUMBER := 0;
        BEGIN
            IF v_date_columns IS NOT NULL AND v_date_columns.COUNT > 0 THEN
                FOR i IN 1..v_date_columns.COUNT LOOP
                    IF analyze_date_column(
                        p_owner, p_table_name, v_date_columns(i), v_parallel_degree,
                        v_min_date, v_max_date, v_range_days,
                        v_null_count, v_non_null_count, v_null_percentage,
                        v_has_time_component, v_distinct_dates, v_usage_score,
                        v_data_quality_issue
                    ) THEN
                        -- Quality-first selection: clean columns always beat dirty columns
                        IF v_best_column IS NULL THEN
                            -- First column - select as baseline
                            v_best_column := v_date_columns(i);
                            v_best_quality := v_data_quality_issue;
                            v_max_range := v_range_days;
                            v_max_usage_score := v_usage_score;
                        ELSIF (
                            -- Case 1: Current clean, best dirty -> always replace
                            (v_data_quality_issue = 'N' AND v_best_quality = 'Y') OR
                            -- Case 2: Same quality -> use score then range
                            (v_data_quality_issue = v_best_quality AND (
                                v_usage_score > v_max_usage_score OR
                                (v_usage_score >= v_max_usage_score * 0.8 AND v_range_days > v_max_range)
                            ))
                        ) THEN
                            v_best_column := v_date_columns(i);
                            v_best_quality := v_data_quality_issue;
                            v_max_range := v_range_days;
                            v_max_usage_score := v_usage_score;
                        END IF;
                    END IF;
                END LOOP;
            END IF;
        END;

        -- Recommend strategy based on date range
        IF v_best_column IS NOT NULL THEN
            IF v_max_range > 365 * 3 THEN
                v_strategy := 'RANGE(' || v_best_column || ') INTERVAL MONTHLY';
                p_reason := 'Date range spans ' || ROUND(v_max_range/365, 4) ||
                           ' years - monthly interval partitioning recommended';
            ELSIF v_max_range > 365 THEN
                v_strategy := 'RANGE(' || v_best_column || ') INTERVAL MONTHLY';
                p_reason := 'Date range spans ' || ROUND(v_max_range/30, 4) ||
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
        p_partition_type VARCHAR2,
        p_interval_clause VARCHAR2 DEFAULT NULL  -- NEW: Determines granularity
    ) RETURN NUMBER
    AS
        v_sql VARCHAR2(4000);
        v_distinct_count NUMBER;
        v_interval_unit VARCHAR2(20);
        v_interval_number NUMBER;
    BEGIN
        IF p_partition_type LIKE 'RANGE%' THEN
            -- For range partitioning on dates: Count DISTINCT periods with actual data

            -- Parse interval_clause to determine granularity
            IF p_interval_clause IS NOT NULL THEN
                -- Extract unit from NUMTOYMINTERVAL(N,'MONTH') or NUMTODSINTERVAL(N,'DAY')
                v_interval_number := TO_NUMBER(REGEXP_SUBSTR(p_interval_clause, '\d+'));
                v_interval_unit := REGEXP_SUBSTR(p_interval_clause, '''(\w+)''', 1, 1, NULL, 1);

                -- Count DISTINCT periods based on granularity
                IF UPPER(v_interval_unit) = 'MONTH' THEN
                    IF v_interval_number = 1 THEN
                        -- Monthly: Count distinct YYYYMM
                        v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYYMM'')) ' ||
                                'FROM ' || p_owner || '.' || p_table_name ||
                                ' WHERE ' || p_partition_key || ' IS NOT NULL';
                    ELSIF v_interval_number = 3 THEN
                        -- Quarterly: Count distinct YYYYQ
                        v_sql := 'SELECT COUNT(DISTINCT (TO_CHAR(' || p_partition_key || ', ''YYYY'') || ' ||
                                '''Q'' || TO_CHAR(CEIL(TO_NUMBER(TO_CHAR(' || p_partition_key || ', ''MM'')) / 3)))) ' ||
                                'FROM ' || p_owner || '.' || p_table_name ||
                                ' WHERE ' || p_partition_key || ' IS NOT NULL';
                    ELSIF v_interval_number = 12 THEN
                        -- Yearly: Count distinct YYYY
                        v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYY'')) ' ||
                                'FROM ' || p_owner || '.' || p_table_name ||
                                ' WHERE ' || p_partition_key || ' IS NOT NULL';
                    ELSE
                        -- Other month intervals: Count distinct YYYYMM (start month of each period)
                        v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYYMM'')) ' ||
                                'FROM ' || p_owner || '.' || p_table_name ||
                                ' WHERE ' || p_partition_key || ' IS NOT NULL';
                    END IF;
                ELSIF UPPER(v_interval_unit) = 'YEAR' THEN
                    -- Yearly: Count distinct YYYY
                    v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYY'')) ' ||
                            'FROM ' || p_owner || '.' || p_table_name ||
                            ' WHERE ' || p_partition_key || ' IS NOT NULL';
                ELSIF UPPER(v_interval_unit) = 'DAY' THEN
                    -- Daily: Count distinct YYYYMMDD
                    v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYYMMDD'')) ' ||
                            'FROM ' || p_owner || '.' || p_table_name ||
                            ' WHERE ' || p_partition_key || ' IS NOT NULL';
                ELSE
                    -- Unknown interval: Fallback to monthly
                    v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYYMM'')) ' ||
                            'FROM ' || p_owner || '.' || p_table_name ||
                            ' WHERE ' || p_partition_key || ' IS NOT NULL';
                END IF;
            ELSE
                -- No interval_clause: Assume monthly partitioning
                v_sql := 'SELECT COUNT(DISTINCT TO_CHAR(' || p_partition_key || ', ''YYYYMM'')) ' ||
                        'FROM ' || p_owner || '.' || p_table_name ||
                        ' WHERE ' || p_partition_key || ' IS NOT NULL';
            END IF;

            EXECUTE IMMEDIATE v_sql INTO v_distinct_count;
            RETURN v_distinct_count;

        ELSIF p_partition_type LIKE 'HASH%' THEN
            -- For hash partitioning, return recommended partition count
            RETURN 16;

        ELSIF p_partition_type LIKE 'LIST%' OR p_partition_type LIKE '%AUTOMATIC%' THEN
            -- For list partitioning (including AUTOMATIC LIST): Count distinct values
            v_sql := 'SELECT COUNT(DISTINCT ' || p_partition_key || ') ' ||
                    'FROM ' || p_owner || '.' || p_table_name ||
                    ' WHERE ' || p_partition_key || ' IS NOT NULL';

            EXECUTE IMMEDIATE v_sql INTO v_distinct_count;

            -- Add 1 for DEFAULT partition (p_xdef in AUTOMATIC LIST, or manually defined)
            RETURN LEAST(v_distinct_count + 1, 101);  -- Cap at 101 (100 value partitions + 1 default)

        ELSE
            RETURN 1;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            -- Log error but return 0 instead of failing
            DBMS_OUTPUT.PUT_LINE('WARNING: Failed to estimate partition count: ' || SQLERRM);
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
        p_table_name VARCHAR2,
        p_partition_column VARCHAR2 DEFAULT NULL,
        p_analyzed_columns SYS.ODCIVARCHAR2LIST DEFAULT NULL
    ) RETURN CLOB
    AS
        v_result CLOB;
        v_count NUMBER;
        v_idx_count NUMBER;
        v_cons_count NUMBER;
        v_fk_count NUMBER;
        v_view_count NUMBER;
        v_code_count NUMBER;
        v_temp_clob CLOB;
        v_first_item BOOLEAN;

        TYPE t_object_rec IS RECORD (
            obj_name VARCHAR2(128),
            obj_owner VARCHAR2(128),
            obj_type VARCHAR2(30)
        );
        TYPE t_object_list IS TABLE OF t_object_rec;
        v_objects t_object_list;
    BEGIN
        DBMS_LOB.CREATETEMPORARY(v_result, TRUE, DBMS_LOB.SESSION);

        DBMS_LOB.APPEND(v_result, '{' || CHR(10));

        -- Partition column being used
        IF p_partition_column IS NOT NULL THEN
            DBMS_LOB.APPEND(v_result, '  "partition_column": "' || p_partition_column || '",' || CHR(10));
        END IF;

        -- Total Indexes
        SELECT COUNT(*) INTO v_count
        FROM dba_indexes
        WHERE table_owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '  "total_indexes": ' || v_count || ',' || CHR(10));

        -- Total Constraints
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '  "total_constraints": ' || v_count || ',' || CHR(10));

        -- Total Triggers
        SELECT COUNT(*) INTO v_count
        FROM dba_triggers
        WHERE table_owner = p_owner AND table_name = p_table_name;
        DBMS_LOB.APPEND(v_result, '  "total_triggers": ' || v_count || ',' || CHR(10));

        -- Total Foreign keys referencing this table
        SELECT COUNT(*) INTO v_count
        FROM dba_constraints
        WHERE r_owner = p_owner
        AND r_constraint_name IN (
            SELECT constraint_name FROM dba_constraints
            WHERE owner = p_owner AND table_name = p_table_name
            AND constraint_type = 'P'
        );
        DBMS_LOB.APPEND(v_result, '  "total_referenced_by": ' || v_count || ',' || CHR(10));

        -- Analyzed columns usage
        DBMS_LOB.APPEND(v_result, '  "analyzed_columns": {' || CHR(10));

        IF p_analyzed_columns IS NOT NULL AND p_analyzed_columns.COUNT > 0 THEN
            FOR i IN 1..p_analyzed_columns.COUNT LOOP
                IF i > 1 THEN DBMS_LOB.APPEND(v_result, ',' || CHR(10)); END IF;
                DBMS_LOB.APPEND(v_result, '    "' || p_analyzed_columns(i) || '": {' || CHR(10));

                -- Get indexes using this column
                DBMS_LOB.APPEND(v_result, '      "indexes": [');
                v_first_item := TRUE;
                BEGIN
                    SELECT i.index_name, i.owner, i.index_type BULK COLLECT INTO v_objects
                    FROM (SELECT DISTINCT i.index_name, i.owner, i.index_type
                          FROM dba_ind_columns ic
                          JOIN dba_indexes i ON i.owner = ic.index_owner AND i.index_name = ic.index_name
                          WHERE ic.table_owner = p_owner
                          AND ic.table_name = p_table_name
                          AND ic.column_name = p_analyzed_columns(i)) i;

                    FOR j IN 1..v_objects.COUNT LOOP
                        IF NOT v_first_item THEN DBMS_LOB.APPEND(v_result, ', '); END IF;
                        DBMS_LOB.APPEND(v_result, '{"name": "' || v_objects(j).obj_name ||
                                                  '", "schema": "' || v_objects(j).obj_owner ||
                                                  '", "type": "' || v_objects(j).obj_type || '"}');
                        v_first_item := FALSE;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN NULL; END;
                DBMS_LOB.APPEND(v_result, '],' || CHR(10));

                -- Get constraints using this column
                DBMS_LOB.APPEND(v_result, '      "constraints": [');
                v_first_item := TRUE;
                BEGIN
                    SELECT c.constraint_name, c.owner, c.constraint_type BULK COLLECT INTO v_objects
                    FROM (SELECT DISTINCT c.constraint_name, c.owner, c.constraint_type
                          FROM dba_cons_columns cc
                          JOIN dba_constraints c ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
                          WHERE cc.owner = p_owner
                          AND cc.table_name = p_table_name
                          AND cc.column_name = p_analyzed_columns(i)
                          AND c.constraint_type IN ('P', 'U', 'C')) c;

                    FOR j IN 1..v_objects.COUNT LOOP
                        IF NOT v_first_item THEN DBMS_LOB.APPEND(v_result, ', '); END IF;
                        DBMS_LOB.APPEND(v_result, '{"name": "' || v_objects(j).obj_name ||
                                                  '", "schema": "' || v_objects(j).obj_owner ||
                                                  '", "type": "' || v_objects(j).obj_type || '"}');
                        v_first_item := FALSE;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN NULL; END;
                DBMS_LOB.APPEND(v_result, '],' || CHR(10));

                -- Get foreign keys using this column
                DBMS_LOB.APPEND(v_result, '      "foreign_keys": [');
                v_first_item := TRUE;
                BEGIN
                    SELECT c.constraint_name, c.owner, 'R' BULK COLLECT INTO v_objects
                    FROM (SELECT DISTINCT c.constraint_name, c.owner
                          FROM dba_cons_columns cc
                          JOIN dba_constraints c ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name
                          WHERE cc.owner = p_owner
                          AND cc.table_name = p_table_name
                          AND cc.column_name = p_analyzed_columns(i)
                          AND c.constraint_type = 'R') c;

                    FOR j IN 1..v_objects.COUNT LOOP
                        IF NOT v_first_item THEN DBMS_LOB.APPEND(v_result, ', '); END IF;
                        DBMS_LOB.APPEND(v_result, '{"name": "' || v_objects(j).obj_name ||
                                                  '", "schema": "' || v_objects(j).obj_owner || '"}');
                        v_first_item := FALSE;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN NULL; END;
                DBMS_LOB.APPEND(v_result, '],' || CHR(10));

                -- Get views using this column
                DBMS_LOB.APPEND(v_result, '      "views": [');
                v_first_item := TRUE;
                BEGIN
                    SELECT view_name, owner, 'VIEW' BULK COLLECT INTO v_objects
                    FROM dba_views
                    WHERE owner = p_owner
                    AND UPPER(text_vc) LIKE '%' || UPPER(p_table_name) || '%' || UPPER(p_analyzed_columns(i)) || '%';

                    FOR j IN 1..v_objects.COUNT LOOP
                        IF NOT v_first_item THEN DBMS_LOB.APPEND(v_result, ', '); END IF;
                        DBMS_LOB.APPEND(v_result, '{"name": "' || v_objects(j).obj_name ||
                                                  '", "schema": "' || v_objects(j).obj_owner || '"}');
                        v_first_item := FALSE;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN NULL; END;
                DBMS_LOB.APPEND(v_result, '],' || CHR(10));

                -- Get stored code using this column
                DBMS_LOB.APPEND(v_result, '      "stored_code": [');
                v_first_item := TRUE;
                BEGIN
                    SELECT DISTINCT name, owner, type BULK COLLECT INTO v_objects
                    FROM dba_source
                    WHERE owner = p_owner
                    AND type IN ('PACKAGE', 'PACKAGE BODY', 'PROCEDURE', 'FUNCTION')
                    AND UPPER(text) LIKE '%' || UPPER(p_table_name) || '%' || UPPER(p_analyzed_columns(i)) || '%';

                    FOR j IN 1..v_objects.COUNT LOOP
                        IF NOT v_first_item THEN DBMS_LOB.APPEND(v_result, ', '); END IF;
                        DBMS_LOB.APPEND(v_result, '{"name": "' || v_objects(j).obj_name ||
                                                  '", "schema": "' || v_objects(j).obj_owner ||
                                                  '", "type": "' || v_objects(j).obj_type || '"}');
                        v_first_item := FALSE;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN NULL; END;
                DBMS_LOB.APPEND(v_result, ']' || CHR(10));

                DBMS_LOB.APPEND(v_result, '    }');
            END LOOP;
        END IF;

        DBMS_LOB.APPEND(v_result, CHR(10) || '  }' || CHR(10));
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
        DBMS_LOB.CREATETEMPORARY(v_issues, TRUE, DBMS_LOB.SESSION);
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
    -- Partitioning Detection Functions
    -- ==========================================================================

    -- Check if table is already partitioned by a date column
    -- Returns: 'SKIP' - skip analysis (already optimally partitioned)
    --          'WARN' - add warning (partitioned but not by date)
    --          'CONTINUE' - continue analysis (not partitioned or no date found)
    FUNCTION check_existing_partitioning(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_details OUT VARCHAR2,  -- Detailed partition info for logging
        p_warning_json OUT CLOB            -- JSON warning for non-date partitioning
    ) RETURN VARCHAR2
    AS
        v_is_partitioned VARCHAR2(3);
        v_partitioning_type VARCHAR2(30);
        v_subpartitioning_type VARCHAR2(30);
        v_partition_key_cols VARCHAR2(4000);
        v_subpartition_key_cols VARCHAR2(4000);
        v_partition_count NUMBER;
        v_partition_key_is_date CHAR(1) := 'N';
        v_subpartition_key_is_date CHAR(1) := 'N';
        v_date_partition_cols VARCHAR2(4000);
        v_non_date_partition_cols VARCHAR2(4000);
    BEGIN
        -- Check if table is partitioned
        BEGIN
            SELECT partitioned
            INTO v_is_partitioned
            FROM dba_tables
            WHERE owner = p_owner
            AND table_name = p_table_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RETURN 'CONTINUE';  -- Table not found (will be handled elsewhere)
        END;

        -- If not partitioned, continue with analysis
        IF v_is_partitioned = 'NO' THEN
            RETURN 'CONTINUE';
        END IF;

        -- Get partitioning details
        SELECT partitioning_type, subpartitioning_type
        INTO v_partitioning_type, v_subpartitioning_type
        FROM dba_part_tables
        WHERE owner = p_owner
        AND table_name = p_table_name;

        -- Get partition key columns (main partitioning)
        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position)
        INTO v_partition_key_cols
        FROM dba_part_key_columns
        WHERE owner = p_owner
        AND name = p_table_name
        AND object_type = 'TABLE';

        -- Get subpartition key columns if exists
        BEGIN
            SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY column_position)
            INTO v_subpartition_key_cols
            FROM dba_subpart_key_columns
            WHERE owner = p_owner
            AND name = p_table_name
            AND object_type = 'TABLE';
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_subpartition_key_cols := NULL;
        END;

        -- Identify DATE partition key columns
        BEGIN
            SELECT LISTAGG(pkc.column_name, ', ') WITHIN GROUP (ORDER BY pkc.column_position)
            INTO v_date_partition_cols
            FROM dba_part_key_columns pkc
            JOIN dba_tab_columns tc
              ON tc.owner = pkc.owner
              AND tc.table_name = pkc.name
              AND tc.column_name = pkc.column_name
            WHERE pkc.owner = p_owner
            AND pkc.name = p_table_name
            AND pkc.object_type = 'TABLE'
            AND tc.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_date_partition_cols := NULL;
        END;

        -- Identify non-DATE partition key columns
        BEGIN
            SELECT LISTAGG(pkc.column_name, ', ') WITHIN GROUP (ORDER BY pkc.column_position)
            INTO v_non_date_partition_cols
            FROM dba_part_key_columns pkc
            JOIN dba_tab_columns tc
              ON tc.owner = pkc.owner
              AND tc.table_name = pkc.name
              AND tc.column_name = pkc.column_name
            WHERE pkc.owner = p_owner
            AND pkc.name = p_table_name
            AND pkc.object_type = 'TABLE'
            AND tc.data_type NOT IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)');
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_non_date_partition_cols := NULL;
        END;

        -- Check if any partition key is a date
        IF v_date_partition_cols IS NOT NULL THEN
            v_partition_key_is_date := 'Y';
        END IF;

        -- Check subpartition keys
        IF v_subpartition_key_cols IS NOT NULL THEN
            BEGIN
                SELECT CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END
                INTO v_subpartition_key_is_date
                FROM dba_subpart_key_columns spkc
                JOIN dba_tab_columns tc
                  ON tc.owner = spkc.owner
                  AND tc.table_name = spkc.name
                  AND tc.column_name = spkc.column_name
                WHERE spkc.owner = p_owner
                AND spkc.name = p_table_name
                AND spkc.object_type = 'TABLE'
                AND tc.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_subpartition_key_is_date := 'N';
            END;
        END IF;

        -- Get partition count
        SELECT COUNT(*)
        INTO v_partition_count
        FROM dba_tab_partitions
        WHERE table_owner = p_owner
        AND table_name = p_table_name;

        -- Build detailed message for output
        p_partition_details := 'Partition keys: ' || v_partition_key_cols;
        IF v_date_partition_cols IS NOT NULL THEN
            p_partition_details := p_partition_details || CHR(10) || '  - DATE columns: ' || v_date_partition_cols;
        END IF;
        IF v_non_date_partition_cols IS NOT NULL THEN
            p_partition_details := p_partition_details || CHR(10) || '  - Non-DATE columns: ' || v_non_date_partition_cols;
        END IF;
        IF v_subpartition_key_cols IS NOT NULL THEN
            p_partition_details := p_partition_details || CHR(10) || 'Subpartition keys: ' || v_subpartition_key_cols;
        END IF;
        p_partition_details := p_partition_details || CHR(10) || 'Partitioning: ' || v_partitioning_type;
        IF v_subpartitioning_type IS NOT NULL THEN
            p_partition_details := p_partition_details || '-' || v_subpartitioning_type;
        END IF;
        p_partition_details := p_partition_details || CHR(10) || 'Partitions: ' || v_partition_count;

        -- Decision: SKIP if partitioned by date, WARN if not
        IF v_partition_key_is_date = 'Y' OR v_subpartition_key_is_date = 'Y' THEN
            -- Already optimally partitioned by date - SKIP analysis
            RETURN 'SKIP';
        ELSE
            -- Partitioned but not by date - WARN and continue
            DECLARE
                v_warning_detail VARCHAR2(4000);
            BEGIN
                v_warning_detail := 'Partition key: ' || v_partition_key_cols || ' (' || v_partitioning_type;
                IF v_subpartitioning_type IS NOT NULL THEN
                    v_warning_detail := v_warning_detail || '-' || v_subpartitioning_type;
                END IF;
                IF v_subpartition_key_cols IS NOT NULL THEN
                    v_warning_detail := v_warning_detail || ', Subpartition: ' || v_subpartition_key_cols;
                END IF;
                v_warning_detail := v_warning_detail || ', ' || v_partition_count || ' partitions)';

                -- Build JSON warning
                DBMS_LOB.CREATETEMPORARY(p_warning_json, TRUE, DBMS_LOB.SESSION);
                DBMS_LOB.APPEND(p_warning_json, '  {' || CHR(10) ||
                    '    "type": "WARNING",' || CHR(10) ||
                    '    "issue": "Table already partitioned by non-date columns: ' || v_warning_detail || '",' || CHR(10) ||
                    '    "action": "Re-partitioning by date requires: 1) Create new partitioned table, 2) Copy data with INSERT SELECT, 3) Drop old table, 4) Rename new table, 5) Recreate indexes/constraints. Consider if date-based partitioning provides sufficient benefit."' || CHR(10) ||
                    '  }');
            END;
            RETURN 'WARN';
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Warning: Could not check partitioning status: ' || SQLERRM);
            RETURN 'CONTINUE';  -- Continue with analysis on error
    END check_existing_partitioning;


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
        v_parallel_degree NUMBER;
        v_compression_ratio NUMBER;
        v_space_savings_mb NUMBER;
        v_num_indexes NUMBER;
        v_num_constraints NUMBER;
        v_num_triggers NUMBER;
        v_has_lobs CHAR(1);
        v_has_foreign_keys CHAR(1);
        v_candidate_columns CLOB;
        v_complexity_factors VARCHAR2(2000);
        v_estimated_downtime NUMBER;
        v_date_columns SYS.ODCIVARCHAR2LIST;
        v_start_time TIMESTAMP;
        v_end_time TIMESTAMP;
        v_duration_seconds NUMBER;
        -- Online redefinition capability
        v_supports_online_redef CHAR(1) := 'N';
        v_online_redef_method VARCHAR2(30) := NULL;
        v_recommended_method VARCHAR2(30) := 'CTAS';  -- Default to CTAS
        -- Tablespace configuration variables
        v_target_tablespace VARCHAR2(128);
        v_ts_extent_mgmt VARCHAR2(30);
        v_ts_allocation VARCHAR2(30);
        v_ts_ssm VARCHAR2(30);
        v_ts_uniform_size NUMBER;
        v_recommended_initial NUMBER;
        v_recommended_next NUMBER;
        v_storage_clause VARCHAR2(500);
        v_storage_reason VARCHAR2(1000);

        -- NULL handling variables
        v_null_count NUMBER;
        v_null_percentage NUMBER;
        v_null_strategy VARCHAR2(30);
        v_null_default_value VARCHAR2(100);
        v_null_reason VARCHAR2(1000);

        -- Partition boundary variables
        v_boundary_min_date DATE;
        v_boundary_max_date DATE;
        v_boundary_range_years NUMBER;
        v_boundary_recommendation VARCHAR2(1000);
    BEGIN
        v_start_time := SYSTIMESTAMP;
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
        DBMS_LOB.CREATETEMPORARY(v_warnings, TRUE, DBMS_LOB.SESSION);
        DBMS_LOB.APPEND(v_warnings, '[' || CHR(10));

        -- Verify table access early
        BEGIN
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_task.source_owner || '.' || v_task.source_table || ' WHERE ROWNUM = 1' INTO v_error_count;
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                    '    "type": "ERROR",' || CHR(10) ||
                    '    "issue": "Cannot access table ' || v_task.source_owner || '.' || v_task.source_table || ': ' || SQLERRM || '",' || CHR(10) ||
                    '    "action": "Grant SELECT privilege on the table or verify table exists"' || CHR(10) ||
                    '  }');
                DBMS_OUTPUT.PUT_LINE('ERROR: Cannot access table ' || v_task.source_owner || '.' || v_task.source_table);
                DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);
                RAISE;  -- Re-raise to stop analysis
        END;

        -- Check if table is already partitioned by a date column
        DECLARE
            v_partition_action VARCHAR2(20);
            v_partition_details VARCHAR2(4000);
            v_partition_warning CLOB;
        BEGIN
            v_partition_action := check_existing_partitioning(
                p_owner => v_task.source_owner,
                p_table_name => v_task.source_table,
                p_partition_details => v_partition_details,
                p_warning_json => v_partition_warning
            );

            -- Handle partition check result
            IF v_partition_action = 'SKIP' THEN
                -- Already optimally partitioned by date - skip analysis
                DBMS_OUTPUT.PUT_LINE('Table is already optimally partitioned by date:');
                DBMS_OUTPUT.PUT_LINE(v_partition_details);
                DBMS_OUTPUT.PUT_LINE('Skipping analysis.');

                -- Calculate duration
                v_end_time := SYSTIMESTAMP;
                v_duration_seconds := EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400 +
                                      EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600 +
                                      EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                                      EXTRACT(SECOND FROM (v_end_time - v_start_time));

                -- Create analysis record with skip reason
                DECLARE
                    v_skip_reason CLOB;
                BEGIN
                    DBMS_LOB.CREATETEMPORARY(v_skip_reason, TRUE, DBMS_LOB.SESSION);
                    DBMS_LOB.APPEND(v_skip_reason, '[' || CHR(10) ||
                        '  {' || CHR(10) ||
                        '    "type": "INFO",' || CHR(10) ||
                        '    "issue": "Table already partitioned with date column(s)",' || CHR(10) ||
                        '    "details": "' || REPLACE(REPLACE(v_partition_details, CHR(10), '; '), '"', '\"') || '",' || CHR(10) ||
                        '    "action": "No migration needed - table is already optimally partitioned"' || CHR(10) ||
                        '  }' || CHR(10) ||
                        ']');

                    -- Insert/Update analysis record
                    MERGE INTO cmr.dwh_migration_analysis a
                    USING (SELECT p_task_id AS task_id FROM DUAL) src
                    ON (a.task_id = src.task_id)
                    WHEN MATCHED THEN
                        UPDATE SET
                            warnings = v_skip_reason,
                            analysis_date = SYSTIMESTAMP,
                            analysis_duration_seconds = v_duration_seconds
                    WHEN NOT MATCHED THEN
                        INSERT (
                            task_id,
                            warnings,
                            analysis_date,
                            analysis_duration_seconds
                        ) VALUES (
                            p_task_id,
                            v_skip_reason,
                            SYSTIMESTAMP,
                            v_duration_seconds
                        );

                    -- Clean up
                    IF DBMS_LOB.ISTEMPORARY(v_skip_reason) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_skip_reason);
                    END IF;
                END;

                -- Update task with detailed info
                UPDATE cmr.dwh_migration_tasks
                SET status = 'ANALYZED',
                    analysis_date = SYSTIMESTAMP,
                    error_message = 'Already partitioned with date column(s). ' || REPLACE(v_partition_details, CHR(10), '; ')
                WHERE task_id = p_task_id;
                COMMIT;

                DBMS_OUTPUT.PUT_LINE('Analysis skipped in ' || ROUND(v_duration_seconds, 2) || ' seconds');

                -- Clean up LOB
                IF DBMS_LOB.ISTEMPORARY(v_warnings) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_warnings);
                END IF;

                RETURN;  -- Exit procedure

            ELSIF v_partition_action = 'WARN' THEN
                -- Partitioned but not by date - add warning and continue
                DBMS_OUTPUT.PUT_LINE('WARNING: Table is already partitioned but NOT by a date column');
                DBMS_OUTPUT.PUT_LINE(v_partition_details);
                DBMS_OUTPUT.PUT_LINE('Analysis will continue to identify potential date-based partitioning opportunities.');

                -- Add warning to warnings CLOB
                IF v_error_count > 0 THEN
                    DBMS_LOB.APPEND(v_warnings, ',' || CHR(10));
                END IF;
                DBMS_LOB.APPEND(v_warnings, v_partition_warning);
                v_error_count := v_error_count + 1;

                -- Clean up temporary LOB
                IF DBMS_LOB.ISTEMPORARY(v_partition_warning) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(v_partition_warning);
                END IF;
            END IF;
            -- If 'CONTINUE' - proceed with normal analysis
        END;

        -- Get table statistics
        BEGIN
            SELECT num_rows INTO v_num_rows
            FROM dba_tables
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                    '    "type": "ERROR",' || CHR(10) ||
                    '    "issue": "Table not found in DBA_TABLES",' || CHR(10) ||
                    '    "action": "Verify table exists and gather statistics"' || CHR(10) ||
                    '  }');
                v_error_count := v_error_count + 1;
                v_num_rows := NULL;
            WHEN OTHERS THEN
                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                    '    "type": "ERROR",' || CHR(10) ||
                    '    "issue": "Cannot access table metadata: ' || REPLACE(SQLERRM, '"', '\"') || '",' || CHR(10) ||
                    '    "action": "Check privileges"' || CHR(10) ||
                    '  }');
                v_error_count := v_error_count + 1;
                v_num_rows := NULL;
        END;

        BEGIN
            v_table_size := get_table_size_mb(v_task.source_owner, v_task.source_table);
        EXCEPTION
            WHEN OTHERS THEN
                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                    '    "type": "WARNING",' || CHR(10) ||
                    '    "issue": "Cannot calculate table size: ' || REPLACE(SQLERRM, '"', '\"') || '"' || CHR(10) ||
                    '  }');
                v_error_count := v_error_count + 1;
                v_table_size := 0;
        END;

        -- Get parallel degree once for all date column analysis
        v_parallel_degree := get_parallel_degree(v_task.source_owner, v_task.source_table);

        -- Collect table metadata statistics
        BEGIN
            -- Count indexes
            SELECT COUNT(*)
            INTO v_num_indexes
            FROM dba_indexes
            WHERE table_owner = v_task.source_owner
            AND table_name = v_task.source_table;

            -- Count constraints
            SELECT COUNT(*)
            INTO v_num_constraints
            FROM dba_constraints
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table
            AND constraint_type IN ('P', 'U', 'C', 'R');  -- Primary, Unique, Check, Foreign Key

            -- Count triggers
            SELECT COUNT(*)
            INTO v_num_triggers
            FROM dba_triggers
            WHERE table_owner = v_task.source_owner
            AND table_name = v_task.source_table;

            -- Check for LOB columns
            SELECT CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END
            INTO v_has_lobs
            FROM dba_tab_columns
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table
            AND data_type IN ('CLOB', 'BLOB', 'NCLOB', 'BFILE');

            -- Check for foreign keys
            SELECT CASE WHEN COUNT(*) > 0 THEN 'Y' ELSE 'N' END
            INTO v_has_foreign_keys
            FROM dba_constraints
            WHERE owner = v_task.source_owner
            AND table_name = v_task.source_table
            AND constraint_type = 'R';  -- Referential (Foreign Key)

        EXCEPTION
            WHEN OTHERS THEN
                -- If any metadata query fails, set defaults
                v_num_indexes := 0;
                v_num_constraints := 0;
                v_num_triggers := 0;
                v_has_lobs := 'N';
                v_has_foreign_keys := 'N';
        END;

        -- Initialize all LOBs in main scope (must be created before DECLARE blocks)
        -- Use DBMS_LOB.SESSION duration to survive across DECLARE blocks and until procedure ends
        DBMS_LOB.CREATETEMPORARY(v_all_date_analysis, TRUE, DBMS_LOB.SESSION);
        DBMS_LOB.APPEND(v_all_date_analysis, '[');

        DBMS_LOB.CREATETEMPORARY(v_candidate_columns, TRUE, DBMS_LOB.SESSION);

        -- Unified date column analysis (DATE, NUMBER, VARCHAR)
        DECLARE
            -- Candidate collection arrays
            v_all_column_names SYS.ODCIVARCHAR2LIST;
            v_all_data_types SYS.ODCIVARCHAR2LIST;
            v_all_date_formats SYS.ODCIVARCHAR2LIST;

            -- Analysis results for current column
            v_min_date DATE;
            v_max_date DATE;
            v_range_days NUMBER;
            v_null_count NUMBER;
            v_non_null_count NUMBER;
            v_null_percentage NUMBER;
            v_has_time_component VARCHAR2(1);
            v_distinct_dates NUMBER;
            v_usage_score NUMBER;
            v_data_quality_issue VARCHAR2(1);

            -- Selection tracking
            v_max_range NUMBER := 0;
            v_selected_null_pct NUMBER := 0;
            v_selected_has_time VARCHAR2(1) := 'N';
            v_max_usage_score NUMBER := 0;
            v_best_has_quality_issue VARCHAR2(1) := 'N';

            -- Stereotype tracking
            v_scd2_type VARCHAR2(30);
            v_event_column VARCHAR2(128);
            v_staging_column VARCHAR2(128);
            v_hist_column VARCHAR2(128);
            v_stereotype_column VARCHAR2(128);  -- Track which column was detected via stereotype
            v_stereotype_type VARCHAR2(30);     -- Track the stereotype type
            v_stereotype_quality VARCHAR2(1);   -- Track quality of stereotype column

            -- JSON building
            v_json_analysis VARCHAR2(32767);
            v_first_json BOOLEAN := TRUE;

        BEGIN
            -- Phase 1: Stereotype Detection (for priority override later)
            IF detect_scd2_pattern(v_task.source_owner, v_task.source_table, v_scd2_type, v_stereotype_column) THEN
                v_stereotype_type := 'SCD2';
                DBMS_OUTPUT.PUT_LINE('Detected SCD2 stereotype column: ' || v_stereotype_column || ' (Type: ' || v_scd2_type || ')');
            ELSIF detect_events_table(v_task.source_owner, v_task.source_table, v_event_column) THEN
                v_stereotype_column := v_event_column;
                v_stereotype_type := 'EVENTS';
                DBMS_OUTPUT.PUT_LINE('Detected Events stereotype column: ' || v_stereotype_column);
            ELSIF detect_staging_table(v_task.source_owner, v_task.source_table, v_staging_column) THEN
                v_stereotype_column := v_staging_column;
                v_stereotype_type := 'STAGING';
                DBMS_OUTPUT.PUT_LINE('Detected Staging stereotype column: ' || v_stereotype_column);
            ELSIF detect_hist_table(v_task.source_owner, v_task.source_table, v_hist_column) THEN
                v_stereotype_column := v_hist_column;
                v_stereotype_type := 'HIST';
                DBMS_OUTPUT.PUT_LINE('Detected HIST stereotype column: ' || v_stereotype_column);
            END IF;

            -- Phase 2: Collect ALL date column candidates (DATE + NUMBER + VARCHAR)
            collect_all_date_candidates(
                v_task.source_owner,
                v_task.source_table,
                v_parallel_degree,
                v_all_column_names,
                v_all_data_types,
                v_all_date_formats
            );

            -- Phase 3: Unified analysis loop for ALL candidates
            IF v_all_column_names IS NOT NULL AND v_all_column_names.COUNT > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Analyzing ' || v_all_column_names.COUNT || ' total date candidate(s) (all types)...');

                FOR i IN 1..v_all_column_names.COUNT LOOP
                    -- Analyze this candidate using unified function
                    IF analyze_any_date_column(
                        v_task.source_owner, v_task.source_table,
                        v_all_column_names(i), v_all_data_types(i), v_all_date_formats(i),
                        v_parallel_degree,
                        v_min_date, v_max_date, v_range_days,
                        v_null_count, v_non_null_count, v_null_percentage,
                        v_has_time_component, v_distinct_dates, v_usage_score,
                        v_data_quality_issue
                    ) THEN
                        -- Penalize usage score for data quality issues (heavy penalty)
                        IF v_data_quality_issue = 'Y' THEN
                            v_usage_score := GREATEST(0, v_usage_score - 50);
                            DBMS_OUTPUT.PUT_LINE('  *** PENALIZED: Score reduced by 50 points due to data quality issue');
                        END IF;

                        -- Validate date ranges and generate warnings
                        DECLARE
                            v_min_year NUMBER;
                            v_max_year NUMBER;
                        BEGIN
                            v_min_year := EXTRACT(YEAR FROM v_min_date);
                            v_max_year := EXTRACT(YEAR FROM v_max_date);

                            -- Check for suspicious years (likely data quality issues)
                            IF v_min_year < 1900 THEN
                                DBMS_OUTPUT.PUT_LINE('  *** WARNING: MIN date has year ' || v_min_year || ' (< 1900)');
                                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                                    '    "type": "DATA_QUALITY",' || CHR(10) ||
                                    '    "column": "' || v_all_column_names(i) || '",' || CHR(10) ||
                                    '    "issue": "MIN date year ' || v_min_year || ' is before 1900",' || CHR(10) ||
                                    '    "min_date": "' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                                    '    "action": "Review and clean data before migration"' || CHR(10) ||
                                    '  }');
                                v_error_count := v_error_count + 1;
                            ELSIF v_min_year > 2100 THEN
                                DBMS_OUTPUT.PUT_LINE('  *** WARNING: MIN date has year ' || v_min_year || ' (> 2100)');
                                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                                    '    "type": "DATA_QUALITY",' || CHR(10) ||
                                    '    "column": "' || v_all_column_names(i) || '",' || CHR(10) ||
                                    '    "issue": "MIN date year ' || v_min_year || ' is after 2100",' || CHR(10) ||
                                    '    "min_date": "' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                                    '    "action": "Review and clean data before migration"' || CHR(10) ||
                                    '  }');
                                v_error_count := v_error_count + 1;
                            END IF;

                            IF v_max_year < 1900 THEN
                                DBMS_OUTPUT.PUT_LINE('  *** MAX date year ' || v_max_year || ' < 1900');
                                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                                    '    "type": "DATA_QUALITY",' || CHR(10) ||
                                    '    "column": "' || v_all_column_names(i) || '",' || CHR(10) ||
                                    '    "issue": "MAX date year ' || v_max_year || ' is before 1900",' || CHR(10) ||
                                    '    "max_date": "' || TO_CHAR(v_max_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                                    '    "action": "Review and clean data before migration"' || CHR(10) ||
                                    '  }');
                                v_error_count := v_error_count + 1;
                            ELSIF v_max_year > 2100 THEN
                                DBMS_OUTPUT.PUT_LINE('  *** MAX date year ' || v_max_year || ' > 2100');
                                IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ',' || CHR(10)); END IF;
                                DBMS_LOB.APPEND(v_warnings, '  {' || CHR(10) ||
                                    '    "type": "DATA_QUALITY",' || CHR(10) ||
                                    '    "column": "' || v_all_column_names(i) || '",' || CHR(10) ||
                                    '    "issue": "MAX date year ' || v_max_year || ' is after 2100",' || CHR(10) ||
                                    '    "max_date": "' || TO_CHAR(v_max_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                                    '    "action": "Review and clean data before migration"' || CHR(10) ||
                                    '  }');
                                v_error_count := v_error_count + 1;
                            END IF;
                        END;

                        -- Build JSON entry for this candidate
                        IF NOT v_first_json THEN
                            DBMS_LOB.APPEND(v_all_date_analysis, ',');
                        END IF;
                        v_first_json := FALSE;

                        v_json_analysis := '{' || CHR(10) ||
                            '    "column_name": "' || v_all_column_names(i) || '",' || CHR(10) ||
                            '    "data_type": "' || v_all_data_types(i) || '",' || CHR(10) ||
                            '    "date_format": "' || NVL(v_all_date_formats(i), 'STANDARD') || '",' || CHR(10) ||
                            '    "min_date": "' || TO_CHAR(v_min_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                            '    "max_date": "' || TO_CHAR(v_max_date, 'YYYY-MM-DD') || '",' || CHR(10) ||
                            '    "min_year": ' || EXTRACT(YEAR FROM v_min_date) || ',' || CHR(10) ||
                            '    "max_year": ' || EXTRACT(YEAR FROM v_max_date) || ',' || CHR(10) ||
                            '    "range_days": ' || v_range_days || ',' || CHR(10) ||
                            '    "range_years": ' || ROUND(v_range_days/365.25, 4) || ',' || CHR(10) ||
                            '    "null_count": ' || v_null_count || ',' || CHR(10) ||
                            '    "non_null_count": ' || v_non_null_count || ',' || CHR(10) ||
                            '    "null_percentage": ' || ROUND(v_null_percentage, 4) || ',' || CHR(10) ||
                            '    "has_time_component": "' || v_has_time_component || '",' || CHR(10) ||
                            '    "distinct_dates": ' || NVL(TO_CHAR(v_distinct_dates), 'null') || ',' || CHR(10) ||
                            '    "usage_score": ' || v_usage_score || ',' || CHR(10) ||
                            '    "data_quality_issue": "' || v_data_quality_issue || '",' || CHR(10) ||
                            '    "stereotype_detected": "' || CASE WHEN v_all_column_names(i) = v_stereotype_column THEN 'Y' ELSE 'N' END || '",' || CHR(10) ||
                            '    "stereotype_type": "' || CASE WHEN v_all_column_names(i) = v_stereotype_column THEN NVL(v_stereotype_type, 'NONE') ELSE 'NONE' END || '",' || CHR(10) ||
                            '    "is_primary": ' || CASE WHEN v_all_column_names(i) = v_date_column THEN 'true' ELSE 'false' END || CHR(10) ||
                        '  }';

                        DBMS_LOB.APPEND(v_all_date_analysis, v_json_analysis);

                        DBMS_OUTPUT.PUT_LINE('  - ' || v_all_column_names(i) || ' (' || v_all_data_types(i) ||
                            CASE WHEN v_all_date_formats(i) IS NOT NULL THEN ' ' || v_all_date_formats(i) ELSE '' END || '): ' ||
                            TO_CHAR(v_min_date, 'YYYY-MM-DD') || ' (year ' || EXTRACT(YEAR FROM v_min_date) || ') to ' ||
                            TO_CHAR(v_max_date, 'YYYY-MM-DD') || ' (year ' || EXTRACT(YEAR FROM v_max_date) || ')' ||
                            ' (' || v_range_days || ' days, ' || v_null_percentage || '% NULLs' ||
                            CASE WHEN v_has_time_component = 'Y' THEN ', has time component' ELSE '' END ||
                            ', usage score: ' || v_usage_score || ')');

                        -- Track stereotype column quality
                        IF v_all_column_names(i) = v_stereotype_column THEN
                            v_stereotype_quality := v_data_quality_issue;
                        END IF;

                        -- Unified selection logic (applied to ALL column types equally)
                        -- Selection priority:
                        -- 1. Data quality (years 1900-2100)
                        -- 2. NULL percentage (lower is better)
                        -- 3. Time component (prefer DATE without time)
                        -- 4. Usage score (higher is better)
                        -- 5. Date range (wider is better - tiebreaker)
                        IF v_date_column IS NULL THEN
                            -- No column selected yet - select first column as baseline
                            v_date_column := v_all_column_names(i);
                            v_date_type := v_all_data_types(i);
                            v_date_format := NVL(v_all_date_formats(i), 'STANDARD');
                            v_max_range := v_range_days;
                            v_max_usage_score := v_usage_score;
                            v_selected_null_pct := v_null_percentage;
                            v_selected_has_time := v_has_time_component;
                            v_best_has_quality_issue := v_data_quality_issue;
                            v_requires_conversion := CASE WHEN v_all_data_types(i) IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') THEN 'N' ELSE 'Y' END;
                            v_conversion_expr := get_date_conversion_expr(v_all_column_names(i), v_all_data_types(i), v_all_date_formats(i));
                        ELSIF (
                            -- Case 1: Current has no data quality issue, best has issue -> always select current
                            (v_data_quality_issue = 'N' AND v_best_has_quality_issue = 'Y') OR

                            -- Case 2: Both have same data quality status -> apply additional criteria
                            (v_data_quality_issue = v_best_has_quality_issue AND (
                                -- Prefer column with significantly fewer NULLs (>10% difference)
                                (v_null_percentage < v_selected_null_pct - 10) OR

                                -- If NULL percentages similar, prefer column without time component
                                (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
                                 v_has_time_component = 'N' AND v_selected_has_time = 'Y') OR

                                -- If NULL% and time component same, use usage score
                                (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
                                 v_has_time_component = v_selected_has_time AND
                                 v_usage_score > v_max_usage_score) OR

                                -- If all similar, use wider range as tiebreaker
                                (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
                                 v_has_time_component = v_selected_has_time AND
                                 v_usage_score >= v_max_usage_score * 0.8 AND
                                 v_range_days > v_max_range)
                            ))
                            -- Case 3: Current has issue, best has no issue -> don't select (implicit)
                        ) THEN
                            v_date_column := v_all_column_names(i);
                            v_date_type := v_all_data_types(i);
                            v_date_format := NVL(v_all_date_formats(i), 'STANDARD');
                            v_max_range := v_range_days;
                            v_max_usage_score := v_usage_score;
                            v_selected_null_pct := v_null_percentage;
                            v_selected_has_time := v_has_time_component;
                            v_best_has_quality_issue := v_data_quality_issue;
                            v_requires_conversion := CASE WHEN v_all_data_types(i) IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') THEN 'N' ELSE 'Y' END;
                            v_conversion_expr := get_date_conversion_expr(v_all_column_names(i), v_all_data_types(i), v_all_date_formats(i));
                        END IF;
                    END IF;
                END LOOP;

                -- Phase 4: Stereotype Priority Override
                -- If stereotype column found and has acceptable quality, prefer it
                IF v_stereotype_column IS NOT NULL AND (v_stereotype_quality = 'N' OR v_date_column IS NULL) THEN
                    DBMS_OUTPUT.PUT_LINE('Applying stereotype priority override: ' || v_stereotype_column || ' (' || v_stereotype_type || ')');
                    -- Find stereotype column in candidates to get its properties
                    FOR i IN 1..v_all_column_names.COUNT LOOP
                        IF v_all_column_names(i) = v_stereotype_column THEN
                            v_date_column := v_stereotype_column;
                            v_date_type := v_all_data_types(i);
                            v_date_format := NVL(v_all_date_formats(i), v_stereotype_type);
                            v_requires_conversion := CASE WHEN v_all_data_types(i) IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') THEN 'N' ELSE 'Y' END;
                            v_conversion_expr := get_date_conversion_expr(v_all_column_names(i), v_all_data_types(i), v_all_date_formats(i));
                            EXIT;
                        END IF;
                    END LOOP;
                ELSIF v_stereotype_column IS NOT NULL AND v_stereotype_quality = 'Y' THEN
                    DBMS_OUTPUT.PUT_LINE('WARNING: Stereotype column ' || v_stereotype_column || ' has data quality issues - using quality-based selection instead');
                END IF;

                -- Final selection reporting
                IF v_date_column IS NOT NULL THEN
                    v_date_found := TRUE;
                    DBMS_OUTPUT.PUT_LINE('Selected date column: ' || v_date_column || ' (' || v_date_type ||
                        CASE WHEN v_date_format NOT IN ('STANDARD', 'SCD2', 'EVENTS', 'STAGING', 'HIST') THEN ' format: ' || v_date_format ELSE '' END ||
                        ', usage score: ' || v_max_usage_score || ', range: ' || v_max_range || ' days)');

                    -- Add warning if selected column has NULLs
                    IF v_selected_null_pct > 0 THEN
                        IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ','); END IF;
                        DBMS_LOB.APPEND(v_warnings,
                            '{"type":"WARNING",' ||
                            '"issue":"Selected date column ' || v_date_column || ' has ' || v_selected_null_pct || '% NULL values",' ||
                            '"action":"Consider: 1) Choose different date column, 2) Use DEFAULT partition for NULLs, 3) Populate NULL values before migration"}');
                        v_error_count := v_error_count + 1;
                        DBMS_OUTPUT.PUT_LINE('WARNING: Selected date column has ' || v_selected_null_pct || '% NULL values');

                        IF v_selected_null_pct > 25 THEN
                            DBMS_OUTPUT.PUT_LINE('CRITICAL: >25% NULL values - strongly recommend addressing before migration');
                        END IF;
                    END IF;

                    -- Add warning if selected column has time component
                    IF v_selected_has_time = 'Y' THEN
                        IF v_error_count > 0 THEN DBMS_LOB.APPEND(v_warnings, ','); END IF;
                        DBMS_LOB.APPEND(v_warnings,
                            '{"type":"WARNING",' ||
                            '"issue":"Selected date column ' || v_date_column || ' contains time component (HH:MI:SS)",' ||
                            '"action":"Partition key must use TRUNC(' || v_date_column || ') for daily partitions or TO_CHAR(' || v_date_column || ', ''YYYY-MM'') for monthly partitions"}');
                        v_error_count := v_error_count + 1;
                        DBMS_OUTPUT.PUT_LINE('WARNING: Selected date column has time component - use TRUNC() in partition key');
                    END IF;
                ELSE
                    DBMS_OUTPUT.PUT_LINE('WARNING: No suitable date column found');
                END IF;
            ELSE
                DBMS_OUTPUT.PUT_LINE('WARNING: No date column candidates found');
            END IF;

            DBMS_LOB.APPEND(v_all_date_analysis, ']');

            -- Build candidate_columns from actual collected candidates
            IF v_all_column_names IS NOT NULL AND v_all_column_names.COUNT > 0 THEN
                DECLARE
                    v_temp_columns VARCHAR2(4000) := '';
                    v_first BOOLEAN := TRUE;
                BEGIN
                    FOR i IN 1..v_all_column_names.COUNT LOOP
                        IF NOT v_first THEN
                            v_temp_columns := v_temp_columns || ', ';
                        END IF;
                        -- Format: COLUMN_NAME (TYPE)
                        v_temp_columns := v_temp_columns || v_all_column_names(i) ||
                                         ' (' || v_all_data_types(i) || ')';
                        v_first := FALSE;
                    END LOOP;
                    DBMS_LOB.APPEND(v_candidate_columns, v_temp_columns);
                END;
            ELSE
                DBMS_LOB.APPEND(v_candidate_columns, 'No date column candidates found');
            END IF;
        END;

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

        -- Build complexity factors description
        DECLARE
            v_factors SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
        BEGIN
            IF v_num_indexes > 5 THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Many indexes (' || v_num_indexes || ')'; END IF;
            IF v_num_constraints > 5 THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Many constraints (' || v_num_constraints || ')'; END IF;
            IF v_num_triggers > 0 THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Has triggers (' || v_num_triggers || ')'; END IF;
            IF v_has_lobs = 'Y' THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Contains LOB columns'; END IF;
            IF v_has_foreign_keys = 'Y' THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Has foreign keys'; END IF;
            IF v_table_size > 100 THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Large table (' || ROUND(v_table_size, 2) || ' MB)'; END IF;
            IF v_requires_conversion = 'Y' THEN v_factors.EXTEND; v_factors(v_factors.COUNT) := 'Requires date conversion'; END IF;

            -- Convert array to comma-separated string
            IF v_factors.COUNT > 0 THEN
                v_complexity_factors := '';
                FOR i IN 1..v_factors.COUNT LOOP
                    v_complexity_factors := v_complexity_factors || v_factors(i);
                    IF i < v_factors.COUNT THEN
                        v_complexity_factors := v_complexity_factors || ', ';
                    END IF;
                END LOOP;
            ELSE
                v_complexity_factors := 'Simple table, low complexity';
            END IF;
        END;

        -- Estimate downtime in minutes based on table size, method, and complexity
        DECLARE
            v_base_time NUMBER;
            v_method_multiplier NUMBER;
        BEGIN
            -- Base time: ~1 minute per GB for ONLINE, ~30 seconds per GB for CTAS
            CASE v_task.migration_method
                WHEN 'ONLINE' THEN v_method_multiplier := 1.0;   -- Slower but near-zero downtime
                WHEN 'CTAS' THEN v_method_multiplier := 0.5;     -- Faster but requires downtime
                WHEN 'EXCHANGE' THEN v_method_multiplier := 0.1; -- Very fast (instant metadata swap)
                ELSE v_method_multiplier := 0.5;  -- Default to CTAS timing
            END CASE;

            -- Base calculation: size_in_GB * minutes_per_GB * method_multiplier * complexity_factor
            v_base_time := (v_table_size / 1024) * 60 * v_method_multiplier * (v_complexity / 5);

            -- Add time for indexes rebuild (significant for many indexes)
            IF v_num_indexes > 0 THEN
                v_base_time := v_base_time + (v_num_indexes * (v_table_size / 1024) * 10);
            END IF;

            -- Add time for LOBs (LOBs are slower to migrate)
            IF v_has_lobs = 'Y' THEN
                v_base_time := v_base_time * 1.5;
            END IF;

            v_estimated_downtime := ROUND(v_base_time, 2);
        END;

        -- Estimate partition count using the analyzed date column
        -- No REGEXP parsing needed - v_date_column is already available from analysis
        IF v_date_column IS NOT NULL AND v_recommended_strategy IS NOT NULL THEN
            v_partition_count := estimate_partition_count(
                p_owner => v_task.source_owner,
                p_table_name => v_task.source_table,
                p_partition_key => v_date_column,
                p_partition_type => v_recommended_strategy,
                p_interval_clause => v_task.interval_clause  -- Pass interval for accurate granularity
            );

            IF v_partition_count > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Estimated partitions: ' || v_partition_count ||
                                    ' (column: ' || v_date_column ||
                                    ', granularity: ' || NVL(v_task.interval_clause, 'monthly (default)') || ')');
            ELSE
                DBMS_OUTPUT.PUT_LINE('WARNING: Could not estimate partition count for column ' || v_date_column);
            END IF;
        ELSIF v_task.partition_key IS NOT NULL AND v_recommended_strategy IS NOT NULL THEN
            -- Fallback: Use explicit partition_key if no date column was analyzed
            v_partition_count := estimate_partition_count(
                p_owner => v_task.source_owner,
                p_table_name => v_task.source_table,
                p_partition_key => v_task.partition_key,
                p_partition_type => v_recommended_strategy,
                p_interval_clause => v_task.interval_clause
            );

            DBMS_OUTPUT.PUT_LINE('Estimated partitions: ' || NVL(TO_CHAR(v_partition_count), 'N/A') ||
                                ' (using explicit partition_key: ' || v_task.partition_key || ')');
        ELSE
            v_partition_count := NULL;
            DBMS_OUTPUT.PUT_LINE('Cannot estimate partition count - no partition column available');
        END IF;

        -- Analyze NULLs in partition key column and recommend handling strategy
        DECLARE
            v_partition_column VARCHAR2(128);
            v_total_rows NUMBER;
            v_config_strategy VARCHAR2(30);
            v_sql VARCHAR2(4000);
        BEGIN
            -- Determine which column to analyze (prefer analyzed date column, fallback to explicit partition_key)
            v_partition_column := COALESCE(v_date_column, v_task.partition_key);

            IF v_partition_column IS NOT NULL THEN
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('Analyzing NULL values in partition key column: ' || v_partition_column);

                -- Get NULL count and total rows
                v_sql := 'SELECT COUNT(*) - COUNT(' || v_partition_column || '), COUNT(*) ' ||
                        'FROM ' || v_task.source_owner || '.' || v_task.source_table;

                EXECUTE IMMEDIATE v_sql INTO v_null_count, v_total_rows;

                IF v_total_rows > 0 THEN
                    v_null_percentage := ROUND((v_null_count / v_total_rows) * 100, 2);
                ELSE
                    v_null_percentage := 0;
                END IF;

                DBMS_OUTPUT.PUT_LINE('  NULL count: ' || v_null_count || ' (' || v_null_percentage || '%)');

                -- Get configured NULL handling strategy
                v_config_strategy := get_dwh_ilm_config('NULL_HANDLING_STRATEGY');

                -- Recommend strategy based on NULL percentage
                IF UPPER(v_config_strategy) = 'AUTO' THEN
                    IF v_null_percentage = 0 THEN
                        v_null_strategy := 'ALLOW_NULLS';
                        v_null_reason := 'No NULL values detected - no action needed';
                        v_null_default_value := NULL;
                    ELSIF v_null_percentage <= 5 THEN
                        v_null_strategy := 'UPDATE';
                        v_null_reason := 'Low NULL percentage (<= 5%) - recommend updating to default value for data quality';
                        -- Get appropriate default value based on column type
                        IF v_date_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') OR v_date_column IS NOT NULL THEN
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_DATE');
                        ELSIF v_date_type LIKE 'VARCHAR%' OR v_date_type LIKE 'CHAR%' THEN
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_VARCHAR');
                        ELSE
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_NUMBER');
                        END IF;
                    ELSIF v_null_percentage <= 25 THEN
                        v_null_strategy := 'ALLOW_NULLS';
                        v_null_reason := 'Moderate NULL percentage (5-25%) - allow NULLs in first/default partition. Consider UPDATE strategy if data quality is critical.';
                        v_null_default_value := NULL;
                    ELSE
                        v_null_strategy := 'ALLOW_NULLS';
                        v_null_reason := 'High NULL percentage (> 25%) - allow NULLs in first/default partition. Updating this many rows may be expensive.';
                        v_null_default_value := NULL;
                    END IF;
                ELSE
                    -- Use configured strategy
                    v_null_strategy := UPPER(v_config_strategy);
                    IF v_null_strategy = 'UPDATE' THEN
                        IF v_date_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') OR v_date_column IS NOT NULL THEN
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_DATE');
                        ELSIF v_date_type LIKE 'VARCHAR%' OR v_date_type LIKE 'CHAR%' THEN
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_VARCHAR');
                        ELSE
                            v_null_default_value := get_dwh_ilm_config('NULL_DEFAULT_NUMBER');
                        END IF;
                        v_null_reason := 'Configured strategy: UPDATE to default value ' || v_null_default_value;
                    ELSE
                        v_null_default_value := NULL;
                        v_null_reason := 'Configured strategy: ALLOW_NULLS in first/default partition';
                    END IF;
                END IF;

                DBMS_OUTPUT.PUT_LINE('  Strategy: ' || v_null_strategy);
                DBMS_OUTPUT.PUT_LINE('  Reason: ' || v_null_reason);
                IF v_null_default_value IS NOT NULL THEN
                    DBMS_OUTPUT.PUT_LINE('  Default value: ' || v_null_default_value);
                END IF;

                -- Query actual MIN/MAX dates (excluding NULLs) for partition boundary calculation
                -- This prevents ORA-14300 when initial partition is set incorrectly
                IF (v_date_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') OR v_date_column IS NOT NULL) THEN
                    DECLARE
                        v_conversion_column VARCHAR2(500);
                    BEGIN
                        -- Use conversion expression if needed, otherwise raw column
                        IF v_requires_conversion = 'Y' AND v_conversion_expr IS NOT NULL THEN
                            v_conversion_column := v_conversion_expr;
                        ELSE
                            v_conversion_column := v_partition_column;
                        END IF;

                        v_sql := 'SELECT MIN(' || v_conversion_column || '), MAX(' || v_conversion_column || ') ' ||
                                'FROM ' || v_task.source_owner || '.' || v_task.source_table ||
                                ' WHERE ' || v_partition_column || ' IS NOT NULL';

                        EXECUTE IMMEDIATE v_sql INTO v_boundary_min_date, v_boundary_max_date;

                        IF v_boundary_min_date IS NOT NULL AND v_boundary_max_date IS NOT NULL THEN
                            v_boundary_range_years := ROUND(MONTHS_BETWEEN(v_boundary_max_date, v_boundary_min_date) / 12, 1);

                            DBMS_OUTPUT.PUT_LINE('');
                            DBMS_OUTPUT.PUT_LINE('Partition boundary analysis (excludes NULLs):');
                            DBMS_OUTPUT.PUT_LINE('  MIN date: ' || TO_CHAR(v_boundary_min_date, 'YYYY-MM-DD'));
                            DBMS_OUTPUT.PUT_LINE('  MAX date: ' || TO_CHAR(v_boundary_max_date, 'YYYY-MM-DD'));
                            DBMS_OUTPUT.PUT_LINE('  Range: ' || v_boundary_range_years || ' years');

                            -- Provide recommendation for initial partition
                            IF v_recommended_strategy LIKE 'RANGE%' THEN
                                -- For RANGE partitioning with interval, set initial partition BEFORE min date
                                v_boundary_recommendation := 'For interval partitioning, set initial partition VALUES LESS THAN (TO_DATE(''' ||
                                    TO_CHAR(v_boundary_min_date, 'YYYY-MM-DD') || ''', ''YYYY-MM-DD'')) to avoid ORA-14300. ' ||
                                    'Oracle will create interval partitions FORWARD from this boundary.';
                                DBMS_OUTPUT.PUT_LINE('  Recommendation: Set initial partition < ' || TO_CHAR(v_boundary_min_date, 'YYYY-MM-DD'));
                            ELSE
                                v_boundary_recommendation := 'Partition boundaries should cover the range from ' ||
                                    TO_CHAR(v_boundary_min_date, 'YYYY-MM-DD') || ' to ' ||
                                    TO_CHAR(v_boundary_max_date, 'YYYY-MM-DD');
                            END IF;

                            -- Warn if date range is suspicious (might indicate bad defaults like 5999-12-31 in data)
                            IF v_boundary_range_years > 100 THEN
                                DBMS_OUTPUT.PUT_LINE('  WARNING: Date range > 100 years - check for placeholder dates like 5999-12-31 in data');
                            END IF;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('  WARNING: Could not calculate partition boundaries - ' || SQLERRM);
                            v_boundary_min_date := NULL;
                            v_boundary_max_date := NULL;
                            v_boundary_range_years := NULL;
                            v_boundary_recommendation := 'Could not calculate - check data type compatibility';
                    END;
                END IF;
            ELSE
                -- No partition column available
                v_null_count := NULL;
                v_null_percentage := NULL;
                v_null_strategy := NULL;
                v_null_default_value := NULL;
                v_null_reason := 'No partition column available for NULL analysis';
                v_boundary_min_date := NULL;
                v_boundary_max_date := NULL;
                v_boundary_range_years := NULL;
                v_boundary_recommendation := NULL;
            END IF;
        END;

        -- Get dependencies (include partition column and all analyzed columns)
        v_dependent_objects := get_dependent_objects(v_task.source_owner, v_task.source_table, v_date_column, v_date_columns);
        v_blocking_issues := identify_blocking_issues(v_task.source_owner, v_task.source_table);

        -- Close warnings JSON array
        DBMS_LOB.APPEND(v_warnings, CHR(10) || ']');

        -- Calculate compression ratio and space savings based on compression type
        IF v_task.use_compression = 'Y' THEN
            v_compression_ratio := get_compression_ratio(v_task.compression_type);
            -- Space savings = original_size * (1 - 1/ratio)
            -- Example: ratio 4 -> savings = size * (1 - 0.25) = size * 0.75
            v_space_savings_mb := ROUND(v_table_size * (1 - 1/v_compression_ratio), 4);
        ELSE
            v_compression_ratio := 1;
            v_space_savings_mb := 0;
        END IF;

        -- Analyze target tablespace and calculate optimal storage parameters
        DECLARE
            v_ts_initial NUMBER;
            v_ts_next NUMBER;
            v_avg_part_mb NUMBER;
        BEGIN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('Analyzing tablespace configuration...');

            -- Step 1: Determine target tablespace
            IF v_task.target_tablespace IS NOT NULL THEN
                v_target_tablespace := v_task.target_tablespace;
                DBMS_OUTPUT.PUT_LINE('  Using explicitly set tablespace: ' || v_target_tablespace);
            ELSE
                -- Use source table's current tablespace
                BEGIN
                    SELECT tablespace_name INTO v_target_tablespace
                    FROM dba_tables
                    WHERE owner = v_task.source_owner
                    AND table_name = v_task.source_table;

                    DBMS_OUTPUT.PUT_LINE('  Using source table tablespace: ' || v_target_tablespace);
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_target_tablespace := NULL;
                        DBMS_OUTPUT.PUT_LINE('  WARNING: Could not determine tablespace');
                END;
            END IF;

            -- Step 2: Query tablespace configuration
            IF v_target_tablespace IS NOT NULL THEN
                BEGIN
                    SELECT
                        extent_management,
                        allocation_type,
                        segment_space_management,
                        initial_extent,
                        next_extent
                    INTO
                        v_ts_extent_mgmt,
                        v_ts_allocation,
                        v_ts_ssm,
                        v_ts_initial,
                        v_ts_next
                    FROM dba_tablespaces
                    WHERE tablespace_name = v_target_tablespace;

                    DBMS_OUTPUT.PUT_LINE('  Tablespace type: ' || v_ts_extent_mgmt || ' / ' ||
                                        v_ts_allocation || ' / ' || v_ts_ssm);

                    -- Step 3: Calculate optimal storage based on tablespace type
                    v_avg_part_mb := CASE WHEN v_partition_count > 0
                                         THEN v_table_size / v_partition_count
                                         ELSE NULL END;

                    IF v_ts_ssm = 'AUTO' THEN
                        -- ASSM: STORAGE clause mostly ignored, Oracle manages automatically
                        v_recommended_initial := NULL;
                        v_recommended_next := NULL;
                        v_storage_clause := NULL;
                        v_storage_reason := 'ASSM tablespace - Oracle automatically manages extent allocation (starts 64KB, grows to 64MB+)';
                        DBMS_OUTPUT.PUT_LINE('  Recommendation: Omit STORAGE clause (ASSM handles it)');

                    ELSIF v_ts_allocation = 'UNIFORM' THEN
                        -- UNIFORM: All extents same size, configured at tablespace level
                        v_ts_uniform_size := v_ts_initial;  -- UNIFORM size stored in initial_extent
                        v_recommended_initial := v_ts_uniform_size;
                        v_recommended_next := v_ts_uniform_size;
                        v_storage_clause := NULL;  -- Not needed, tablespace enforces uniform size
                        v_storage_reason := 'UNIFORM tablespace (' ||
                                           ROUND(v_ts_uniform_size / 1024 / 1024, 2) ||
                                           'MB uniform extent) - extent size fixed at tablespace level';
                        DBMS_OUTPUT.PUT_LINE('  Recommendation: Omit STORAGE clause (UNIFORM enforces ' ||
                                            ROUND(v_ts_uniform_size / 1024 / 1024, 2) || 'MB extents)');

                    ELSE
                        -- Manual/SYSTEM AUTOALLOCATE: Calculate optimal extent size
                        v_recommended_initial := calculate_optimal_initial_extent(v_avg_part_mb, v_table_size);
                        v_recommended_next := v_recommended_initial;  -- Keep consistent
                        v_storage_clause := 'STORAGE (INITIAL ' || v_recommended_initial ||
                                           ' NEXT ' || v_recommended_next || ')';
                        v_storage_reason := 'Manual space management - optimized for ' ||
                                           CASE WHEN v_avg_part_mb IS NOT NULL
                                                THEN ROUND(v_avg_part_mb, 2) || 'MB avg partition'
                                                ELSE ROUND(v_table_size, 2) || 'MB table' END ||
                                           ' (INITIAL=' || ROUND(v_recommended_initial / 1024 / 1024, 2) || 'MB)';
                        DBMS_OUTPUT.PUT_LINE('  Recommendation: INITIAL ' ||
                                            ROUND(v_recommended_initial / 1024 / 1024, 2) || 'MB, NEXT ' ||
                                            ROUND(v_recommended_next / 1024 / 1024, 2) || 'MB');
                    END IF;

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_ts_extent_mgmt := NULL;
                        v_ts_allocation := NULL;
                        v_ts_ssm := NULL;
                        v_storage_reason := 'Tablespace not found in data dictionary';
                        DBMS_OUTPUT.PUT_LINE('  WARNING: Tablespace not found');
                END;
            ELSE
                v_storage_reason := 'No target tablespace specified';
            END IF;

            -- Variables are already stored in outer scope for MERGE statement

        END;

        -- Check if table supports online redefinition (Enterprise Edition only)
        -- Using EXECUTE IMMEDIATE to defer privilege check to runtime
        -- This allows package to compile even without EXECUTE grant on DBMS_REDEFINITION
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Checking online redefinition capability...');

        BEGIN
            -- Try CONS_USE_PK first (Primary Key-based)
            -- Using EXECUTE IMMEDIATE to avoid compile-time grant dependency
            EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.CAN_REDEF_TABLE(' ||
                'uname => :1, tname => :2, options_flag => DBMS_REDEFINITION.CONS_USE_PK); END;'
                USING v_task.source_owner, v_task.source_table;

            v_supports_online_redef := 'Y';
            v_online_redef_method := 'CONS_USE_PK';
            DBMS_OUTPUT.PUT_LINE('  ✓ Table supports ONLINE redefinition using PRIMARY KEY');
        EXCEPTION
            WHEN OTHERS THEN
                -- No PK or PK-based failed, try ROWID-based
                BEGIN
                    -- Try CONS_USE_ROWID (for tables without primary key)
                    EXECUTE IMMEDIATE 'BEGIN DBMS_REDEFINITION.CAN_REDEF_TABLE(' ||
                        'uname => :1, tname => :2, options_flag => DBMS_REDEFINITION.CONS_USE_ROWID); END;'
                        USING v_task.source_owner, v_task.source_table;

                    v_supports_online_redef := 'Y';
                    v_online_redef_method := 'CONS_USE_ROWID';
                    DBMS_OUTPUT.PUT_LINE('  ✓ Table supports ONLINE redefinition using ROWID (no PK)');
                EXCEPTION
                    WHEN OTHERS THEN
                        v_supports_online_redef := 'N';
                        v_online_redef_method := NULL;
                        IF SQLCODE = -1031 THEN
                            DBMS_OUTPUT.PUT_LINE('  ✗ Insufficient privileges to check online redefinition');
                            DBMS_OUTPUT.PUT_LINE('    Grant needed: GRANT EXECUTE ON DBMS_REDEFINITION TO CMR;');
                        ELSE
                            DBMS_OUTPUT.PUT_LINE('  ✗ Table does NOT support online redefinition');
                            DBMS_OUTPUT.PUT_LINE('    Reason: ' || SQLERRM);
                        END IF;
                END;
        END;

        -- Determine recommended migration method
        IF v_supports_online_redef = 'Y' AND v_table_size >= 1024 THEN
            -- Large tables (1GB+) with online redef support → recommend ONLINE
            v_recommended_method := 'ONLINE';
            DBMS_OUTPUT.PUT_LINE('  Recommended method: ONLINE (large table + supports online redef)');
        ELSIF v_requires_conversion = 'N' AND v_supports_online_redef = 'Y' THEN
            -- Medium tables with online redef support but no conversion needed
            v_recommended_method := 'ONLINE';
            DBMS_OUTPUT.PUT_LINE('  Recommended method: ONLINE (supports online redef, no conversion needed)');
        ELSE
            -- Default to CTAS for smaller tables or those requiring conversion
            v_recommended_method := 'CTAS';
            DBMS_OUTPUT.PUT_LINE('  Recommended method: CTAS (standard migration)');
        END IF;

        -- Calculate duration
        v_end_time := SYSTIMESTAMP;
        v_duration_seconds := EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400 +
                              EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600 +
                              EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                              EXTRACT(SECOND FROM (v_end_time - v_start_time));

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Analysis completed in ' || ROUND(v_duration_seconds, 2) || ' seconds');

        -- Store or update analysis results (supports rerun)
        MERGE INTO cmr.dwh_migration_analysis a
        USING (SELECT p_task_id AS task_id FROM DUAL) src
        ON (a.task_id = src.task_id)
        WHEN MATCHED THEN
            UPDATE SET
                table_rows = v_num_rows,
                table_size_mb = v_table_size,
                num_indexes = v_num_indexes,
                num_constraints = v_num_constraints,
                num_triggers = v_num_triggers,
                has_lobs = v_has_lobs,
                has_foreign_keys = v_has_foreign_keys,
                candidate_columns = v_candidate_columns,
                recommended_strategy = v_recommended_strategy,
                recommendation_reason = v_reason,
                date_column_name = v_date_column,
                date_column_type = v_date_type,
                date_format_detected = v_date_format,
                date_conversion_expr = v_conversion_expr,
                requires_conversion = v_requires_conversion,
                all_date_columns_analysis = v_all_date_analysis,
                estimated_partitions = v_partition_count,
                avg_partition_size_mb = CASE WHEN v_partition_count > 0 THEN ROUND(v_table_size / v_partition_count, 4) ELSE 0 END,
                estimated_compression_ratio = v_compression_ratio,
                estimated_space_savings_mb = v_space_savings_mb,
                complexity_score = v_complexity,
                complexity_factors = v_complexity_factors,
                estimated_downtime_minutes = v_estimated_downtime,
                supports_online_redef = v_supports_online_redef,
                online_redef_method = v_online_redef_method,
                recommended_method = v_recommended_method,
                dependent_objects = v_dependent_objects,
                blocking_issues = v_blocking_issues,
                warnings = v_warnings,
                target_tablespace = v_target_tablespace,
                tablespace_extent_mgmt = v_ts_extent_mgmt,
                tablespace_allocation = v_ts_allocation,
                tablespace_ssm = v_ts_ssm,
                tablespace_uniform_size = v_ts_uniform_size,
                recommended_initial_extent = v_recommended_initial,
                recommended_next_extent = v_recommended_next,
                recommended_storage_clause = v_storage_clause,
                storage_recommendation_reason = v_storage_reason,
                partition_key_null_count = v_null_count,
                partition_key_null_percentage = v_null_percentage,
                null_handling_strategy = v_null_strategy,
                null_default_value = v_null_default_value,
                null_handling_reason = v_null_reason,
                partition_boundary_min_date = v_boundary_min_date,
                partition_boundary_max_date = v_boundary_max_date,
                partition_range_years = v_boundary_range_years,
                partition_boundary_recommendation = v_boundary_recommendation,
                analysis_date = SYSTIMESTAMP,
                analysis_duration_seconds = v_duration_seconds
        WHEN NOT MATCHED THEN
            INSERT (
                task_id,
                table_rows,
                table_size_mb,
                num_indexes,
                num_constraints,
                num_triggers,
                has_lobs,
                has_foreign_keys,
                candidate_columns,
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
                complexity_factors,
                estimated_downtime_minutes,
                supports_online_redef,
                online_redef_method,
                recommended_method,
                dependent_objects,
                blocking_issues,
                warnings,
                target_tablespace,
                tablespace_extent_mgmt,
                tablespace_allocation,
                tablespace_ssm,
                tablespace_uniform_size,
                recommended_initial_extent,
                recommended_next_extent,
                recommended_storage_clause,
                storage_recommendation_reason,
                partition_key_null_count,
                partition_key_null_percentage,
                null_handling_strategy,
                null_default_value,
                null_handling_reason,
                partition_boundary_min_date,
                partition_boundary_max_date,
                partition_range_years,
                partition_boundary_recommendation,
                analysis_date,
                analysis_duration_seconds
            ) VALUES (
                p_task_id,
                v_num_rows,
                v_table_size,
                v_num_indexes,
                v_num_constraints,
                v_num_triggers,
                v_has_lobs,
                v_has_foreign_keys,
                v_candidate_columns,
                v_recommended_strategy,
                v_reason,
                v_date_column,
                v_date_type,
                v_date_format,
                v_conversion_expr,
                v_requires_conversion,
                v_all_date_analysis,
                v_partition_count,
                CASE WHEN v_partition_count > 0 THEN ROUND(v_table_size / v_partition_count, 4) ELSE 0 END,
                v_compression_ratio,
                v_space_savings_mb,
                v_complexity,
                v_complexity_factors,
                v_estimated_downtime,
                v_supports_online_redef,
                v_online_redef_method,
                v_recommended_method,
                v_dependent_objects,
                v_blocking_issues,
                v_warnings,
                v_target_tablespace,
                v_ts_extent_mgmt,
                v_ts_allocation,
                v_ts_ssm,
                v_ts_uniform_size,
                v_recommended_initial,
                v_recommended_next,
                v_storage_clause,
                v_storage_reason,
                v_null_count,
                v_null_percentage,
                v_null_strategy,
                v_null_default_value,
                v_null_reason,
                v_boundary_min_date,
                v_boundary_max_date,
                v_boundary_range_years,
                v_boundary_recommendation,
                SYSTIMESTAMP,
                v_duration_seconds
            );

        -- Get analysis_id for newly inserted or updated record
        SELECT analysis_id INTO v_analysis_id
        FROM cmr.dwh_migration_analysis
        WHERE task_id = p_task_id;

        -- Update task with recommended method
        UPDATE cmr.dwh_migration_tasks
        SET status = 'ANALYZED',
            migration_method = v_recommended_method,
            source_rows = v_num_rows,
            source_size_mb = v_table_size,
            validation_status = CASE
                WHEN DBMS_LOB.INSTR(v_blocking_issues, 'ERROR') > 0 THEN 'BLOCKED'
                ELSE 'READY'
            END
        WHERE task_id = p_task_id;

        COMMIT;

        -- Clean up temporary LOBs
        IF DBMS_LOB.ISTEMPORARY(v_all_date_analysis) = 1 THEN
            DBMS_LOB.FREETEMPORARY(v_all_date_analysis);
        END IF;
        IF DBMS_LOB.ISTEMPORARY(v_candidate_columns) = 1 THEN
            DBMS_LOB.FREETEMPORARY(v_candidate_columns);
        END IF;

        DBMS_OUTPUT.PUT_LINE('Analysis complete:');
        DBMS_OUTPUT.PUT_LINE('  Recommended strategy: ' || NVL(v_recommended_strategy, 'NONE'));
        DBMS_OUTPUT.PUT_LINE('  Complexity score: ' || v_complexity || '/10');
        DBMS_OUTPUT.PUT_LINE('  Estimated partitions: ' || NVL(v_partition_count, 0));

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_msg VARCHAR2(4000) := SQLERRM;
                v_error_code NUMBER := SQLCODE;
                v_error_json CLOB;
            BEGIN
                -- Clean up temporary LOBs before handling error
                BEGIN
                    IF DBMS_LOB.ISTEMPORARY(v_all_date_analysis) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_all_date_analysis);
                    END IF;
                    IF DBMS_LOB.ISTEMPORARY(v_candidate_columns) = 1 THEN
                        DBMS_LOB.FREETEMPORARY(v_candidate_columns);
                    END IF;
                EXCEPTION WHEN OTHERS THEN NULL; END;

                -- Calculate duration even on error
                v_end_time := SYSTIMESTAMP;
                v_duration_seconds := EXTRACT(DAY FROM (v_end_time - v_start_time)) * 86400 +
                                      EXTRACT(HOUR FROM (v_end_time - v_start_time)) * 3600 +
                                      EXTRACT(MINUTE FROM (v_end_time - v_start_time)) * 60 +
                                      EXTRACT(SECOND FROM (v_end_time - v_start_time));

                -- Build error details as JSON
                DBMS_LOB.CREATETEMPORARY(v_error_json, TRUE, DBMS_LOB.SESSION);
                DBMS_LOB.APPEND(v_error_json, '[{"type":"ERROR","issue":"Analysis failed with ' ||
                    REPLACE(v_error_msg, '"', '\"') || '","code":' || v_error_code ||
                    ',"action":"Review error details and table structure"}]');

                -- Log to analysis table (MERGE to handle both new and rerun scenarios)
                MERGE INTO cmr.dwh_migration_analysis a
                USING (SELECT p_task_id AS task_id FROM dual) t
                ON (a.task_id = t.task_id)
                WHEN MATCHED THEN
                    UPDATE SET
                        warnings = v_error_json,
                        analysis_date = SYSTIMESTAMP,
                        analysis_duration_seconds = v_duration_seconds
                WHEN NOT MATCHED THEN
                    INSERT (
                        task_id,
                        warnings,
                        analysis_date,
                        analysis_duration_seconds
                    ) VALUES (
                        p_task_id,
                        v_error_json,
                        SYSTIMESTAMP,
                        v_duration_seconds
                    );

                -- Update task status
                UPDATE cmr.dwh_migration_tasks
                SET status = 'FAILED',
                    error_message = 'Analysis failed: ' || v_error_msg
                WHERE task_id = p_task_id;

                COMMIT;

                -- Log error but don't raise exception
                DBMS_OUTPUT.PUT_LINE('ERROR: Analysis failed for task ' || p_task_id || ': ' || v_error_msg);
            END;
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
        SELECT ROUND(SUM(bytes)/1024/1024, 4)
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


    FUNCTION get_compression_ratio(
        p_compression_type VARCHAR2
    ) RETURN NUMBER
    AS
        v_ratio NUMBER;
    BEGIN
        -- Return compression ratio based on Oracle compression type
        -- Ratios based on typical Oracle compression performance
        CASE UPPER(p_compression_type)
            WHEN 'BASIC' THEN
                v_ratio := 2;
            WHEN 'OLTP' THEN
                v_ratio := 2.5;
            WHEN 'QUERY LOW' THEN
                v_ratio := 3;
            WHEN 'QUERY HIGH' THEN
                v_ratio := 4;
            WHEN 'ARCHIVE LOW' THEN
                v_ratio := 5;
            WHEN 'ARCHIVE HIGH' THEN
                v_ratio := 10;
            ELSE
                -- Default to QUERY HIGH if unknown type
                v_ratio := 4;
        END CASE;

        RETURN v_ratio;
    END get_compression_ratio;

    FUNCTION get_dwh_ilm_config(
        p_config_key VARCHAR2
    ) RETURN VARCHAR2
    AS
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

    FUNCTION calculate_optimal_initial_extent(
        p_avg_partition_mb NUMBER,
        p_total_table_mb NUMBER
    ) RETURN NUMBER
    AS
        v_initial_bytes NUMBER;
    BEGIN
        -- Size based on average partition size for optimal performance
        -- Larger initial extent = fewer extents = better performance
        -- But avoid over-allocation on small tables

        IF p_avg_partition_mb IS NULL THEN
            -- No partition info, use table size
            IF p_total_table_mb > 10000 THEN       -- > 10GB
                v_initial_bytes := 128 * 1024 * 1024;  -- 128MB
            ELSIF p_total_table_mb > 1000 THEN     -- 1GB-10GB
                v_initial_bytes := 64 * 1024 * 1024;   -- 64MB
            ELSIF p_total_table_mb > 100 THEN      -- 100MB-1GB
                v_initial_bytes := 16 * 1024 * 1024;   -- 16MB
            ELSIF p_total_table_mb > 10 THEN       -- 10MB-100MB
                v_initial_bytes := 4 * 1024 * 1024;    -- 4MB
            ELSE                                    -- < 10MB
                v_initial_bytes := 1 * 1024 * 1024;    -- 1MB
            END IF;
        ELSE
            -- Use average partition size (more accurate)
            IF p_avg_partition_mb > 1000 THEN           -- > 1GB partitions
                v_initial_bytes := 128 * 1024 * 1024;   -- 128MB
            ELSIF p_avg_partition_mb > 500 THEN         -- 500MB-1GB
                v_initial_bytes := 64 * 1024 * 1024;    -- 64MB
            ELSIF p_avg_partition_mb > 100 THEN         -- 100MB-500MB
                v_initial_bytes := 32 * 1024 * 1024;    -- 32MB
            ELSIF p_avg_partition_mb > 50 THEN          -- 50MB-100MB
                v_initial_bytes := 16 * 1024 * 1024;    -- 16MB
            ELSIF p_avg_partition_mb > 10 THEN          -- 10MB-50MB
                v_initial_bytes := 8 * 1024 * 1024;     -- 8MB
            ELSIF p_avg_partition_mb > 1 THEN           -- 1MB-10MB
                v_initial_bytes := 2 * 1024 * 1024;     -- 2MB
            ELSE                                        -- < 1MB
                v_initial_bytes := 1 * 1024 * 1024;     -- 1MB
            END IF;
        END IF;

        RETURN v_initial_bytes;
    END calculate_optimal_initial_extent;

    -- ==========================================================================
    -- AUTOMATIC LIST Partitioning Helper Functions
    -- ==========================================================================

    FUNCTION get_list_default_values(
        p_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_key VARCHAR2,
        p_user_defaults VARCHAR2 DEFAULT NULL
    ) RETURN VARCHAR2
    AS
        -- Constants for default partition values (length-aware for strings)
        C_DEFAULT_NUMBER    CONSTANT VARCHAR2(100) := '-1';
        C_DEFAULT_DATE      CONSTANT VARCHAR2(100) := 'DATE ''5999-12-31''';
        C_DEFAULT_TIMESTAMP CONSTANT VARCHAR2(100) := 'TO_TIMESTAMP(''5999-12-31 23:59:59'',''YYYY-MM-DD HH24:MI:SS'')';

        v_columns SYS.ODCIVARCHAR2LIST;
        v_column_name VARCHAR2(128);
        v_data_type VARCHAR2(128);
        v_data_length NUMBER;
        v_default_values VARCHAR2(4000);
        v_values_list VARCHAR2(4000) := '';
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

            -- Get data type and length
            BEGIN
                SELECT data_type, data_length INTO v_data_type, v_data_length
                FROM dba_tab_columns
                WHERE owner = p_owner
                AND table_name = p_table_name
                AND column_name = v_column_name;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE_APPLICATION_ERROR(-20610,
                        'Column ' || v_column_name || ' not found in table ' || p_owner || '.' || p_table_name);
            END;

            -- Determine default based on data type and length
            IF v_data_type IN ('VARCHAR2', 'VARCHAR', 'CHAR', 'NVARCHAR2', 'NCHAR') THEN
                -- Length-aware default values for string types
                IF v_data_length >= 3 THEN
                    v_default_values := '''NAV''';  -- Not Available
                ELSIF v_data_length = 2 THEN
                    v_default_values := '''NA''';   -- Not Available (abbreviated)
                ELSIF v_data_length = 1 THEN
                    v_default_values := '''X''';    -- Unknown/Default marker
                ELSE
                    RAISE_APPLICATION_ERROR(-20617,
                        'Column ' || v_column_name || ' has invalid length: ' || v_data_length);
                END IF;
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
                            'String values must be quoted: ' || val.default_value || '. Example: ''NAV''');
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
                    IF SQLCODE BETWEEN -20620 AND -20610 THEN
                        -- Re-raise our custom errors
                        RAISE;
                    ELSE
                        RAISE_APPLICATION_ERROR(-20615,
                            'Invalid default value (' || val.default_value || ') for data type ' || v_data_type || ': ' || SQLERRM);
                    END IF;
            END;
        END LOOP;

        RETURN TRUE;
    END validate_list_defaults;

END pck_dwh_table_migration_analyzer;
/

SELECT 'Table Migration Analyzer Package Created Successfully!' AS status FROM DUAL;
