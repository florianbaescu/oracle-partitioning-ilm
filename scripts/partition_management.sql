-- =============================================================================
-- Partition Management Scripts
-- Common operations for Oracle partitioned tables
-- =============================================================================

-- =============================================================================
-- SECTION 1: PARTITION CREATION AND MAINTENANCE
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create future partitions (for non-interval partitioned tables)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE create_future_partitions(
    p_table_name VARCHAR2,
    p_months_ahead NUMBER DEFAULT 12,
    p_partition_interval VARCHAR2 DEFAULT 'MONTH'
) AS
    v_max_partition_date DATE;
    v_next_partition_date DATE;
    v_partition_name VARCHAR2(30);
    v_high_value_str VARCHAR2(100);
    v_sql VARCHAR2(4000);
    v_count NUMBER := 0;
BEGIN
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
            EXECUTE IMMEDIATE v_sql;
        END IF;

        v_max_partition_date := v_next_partition_date;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Created ' || p_months_ahead || ' future partitions for ' || p_table_name);
END;
/


-- -----------------------------------------------------------------------------
-- Split partition for reorganization or to fix uneven distribution
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE split_partition(
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2,
    p_split_date DATE
) AS
    v_new_partition1 VARCHAR2(30);
    v_new_partition2 VARCHAR2(30);
    v_sql VARCHAR2(4000);
BEGIN
    v_new_partition1 := p_partition_name || '_1';
    v_new_partition2 := p_partition_name || '_2';

    v_sql := 'ALTER TABLE ' || p_table_name ||
            ' SPLIT PARTITION ' || p_partition_name ||
            ' AT (DATE ''' || TO_CHAR(p_split_date, 'YYYY-MM-DD') || ''')' ||
            ' INTO (PARTITION ' || v_new_partition1 ||
            ', PARTITION ' || v_new_partition2 || ')';

    DBMS_OUTPUT.PUT_LINE('Splitting partition: ' || p_partition_name);
    EXECUTE IMMEDIATE v_sql;

    -- Rebuild local indexes
    FOR idx IN (
        SELECT index_name, partition_name
        FROM user_ind_partitions
        WHERE index_name IN (
            SELECT index_name FROM user_indexes
            WHERE table_name = UPPER(p_table_name)
            AND locality = 'LOCAL'
        )
        AND partition_name IN (v_new_partition1, v_new_partition2)
    ) LOOP
        EXECUTE IMMEDIATE
            'ALTER INDEX ' || idx.index_name ||
            ' REBUILD PARTITION ' || idx.partition_name;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Partition split completed');
END;
/


-- -----------------------------------------------------------------------------
-- Merge adjacent partitions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE merge_partitions(
    p_table_name VARCHAR2,
    p_partition1 VARCHAR2,
    p_partition2 VARCHAR2,
    p_merged_partition VARCHAR2 DEFAULT NULL
) AS
    v_merged_name VARCHAR2(30);
    v_sql VARCHAR2(4000);
BEGIN
    v_merged_name := NVL(p_merged_partition, p_partition1);

    v_sql := 'ALTER TABLE ' || p_table_name ||
            ' MERGE PARTITIONS ' || p_partition1 || ', ' || p_partition2 ||
            ' INTO PARTITION ' || v_merged_name;

    DBMS_OUTPUT.PUT_LINE('Merging partitions: ' || p_partition1 || ' and ' || p_partition2);
    EXECUTE IMMEDIATE v_sql;

    -- Rebuild local indexes on merged partition
    FOR idx IN (
        SELECT index_name, partition_name
        FROM user_ind_partitions
        WHERE index_name IN (
            SELECT index_name FROM user_indexes
            WHERE table_name = UPPER(p_table_name)
            AND locality = 'LOCAL'
        )
        AND partition_name = v_merged_name
    ) LOOP
        EXECUTE IMMEDIATE
            'ALTER INDEX ' || idx.index_name ||
            ' REBUILD PARTITION ' || idx.partition_name;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Partition merge completed');
END;
/


-- =============================================================================
-- SECTION 2: PARTITION DATA LOADING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Exchange partition for fast bulk loading
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE exchange_partition_load(
    p_target_table VARCHAR2,
    p_staging_table VARCHAR2,
    p_partition_name VARCHAR2,
    p_validate BOOLEAN DEFAULT TRUE,
    p_update_indexes BOOLEAN DEFAULT TRUE
) AS
    v_sql VARCHAR2(4000);
    v_validation VARCHAR2(20) := 'WITH VALIDATION';
    v_update_idx VARCHAR2(30) := 'UPDATE INDEXES';
BEGIN
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
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_staging_table ||
                        ' DISABLE CONSTRAINT ' || cons.constraint_name;
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

    -- Re-enable constraints
    FOR cons IN (
        SELECT constraint_name
        FROM user_constraints
        WHERE table_name = UPPER(p_staging_table)
        AND constraint_type IN ('P', 'U', 'R')
        AND status = 'DISABLED'
    ) LOOP
        EXECUTE IMMEDIATE 'ALTER TABLE ' || p_staging_table ||
                        ' ENABLE CONSTRAINT ' || cons.constraint_name;
    END LOOP;

    -- Gather statistics on exchanged partition
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => p_target_table,
        partname => p_partition_name,
        cascade => TRUE,
        degree => 4
    );

    DBMS_OUTPUT.PUT_LINE('Exchange partition completed successfully');
END;
/


-- -----------------------------------------------------------------------------
-- Parallel partition loading with direct path insert
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE parallel_partition_load(
    p_target_table VARCHAR2,
    p_source_query VARCHAR2,
    p_partition_name VARCHAR2,
    p_degree NUMBER DEFAULT 4
) AS
    v_sql VARCHAR2(4000);
BEGIN
    -- Enable parallel DML
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- Direct path insert into specific partition
    v_sql := 'INSERT /*+ APPEND PARALLEL(' || p_degree || ') */ INTO ' ||
            p_target_table || ' PARTITION (' || p_partition_name || ') ' ||
            p_source_query;

    DBMS_OUTPUT.PUT_LINE('Loading partition: ' || p_partition_name);
    EXECUTE IMMEDIATE v_sql;
    COMMIT;

    -- Gather statistics
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => p_target_table,
        partname => p_partition_name,
        cascade => TRUE,
        degree => p_degree
    );

    DBMS_OUTPUT.PUT_LINE('Parallel load completed');
END;
/


-- =============================================================================
-- SECTION 3: PARTITION MAINTENANCE OPERATIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Truncate old partitions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE truncate_old_partitions(
    p_table_name VARCHAR2,
    p_retention_days NUMBER
) AS
    v_cutoff_date DATE := TRUNC(SYSDATE) - p_retention_days;
    v_partition_date DATE;
    v_sql VARCHAR2(4000);
    v_count NUMBER := 0;
BEGIN
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

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_table_name ||
                    ' TRUNCATE PARTITION ' || rec.partition_name ||
                    ' UPDATE INDEXES';

                v_count := v_count + 1;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error truncating partition ' || rec.partition_name || ': ' || SQLERRM);
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Truncated ' || v_count || ' partition(s)');
END;
/


-- -----------------------------------------------------------------------------
-- Compress partitions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE compress_partitions(
    p_table_name VARCHAR2,
    p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
    p_age_days NUMBER DEFAULT 90
) AS
    v_cutoff_date DATE := TRUNC(SYSDATE) - p_age_days;
    v_partition_date DATE;
    v_sql VARCHAR2(4000);
    v_count NUMBER := 0;
BEGIN
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

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || p_table_name ||
                    ' MOVE PARTITION ' || rec.partition_name ||
                    ' COMPRESS FOR ' || p_compression_type;

                -- Rebuild local indexes
                FOR idx IN (
                    SELECT index_name, partition_name
                    FROM user_ind_partitions
                    WHERE index_name IN (
                        SELECT index_name FROM user_indexes
                        WHERE table_name = UPPER(p_table_name)
                        AND locality = 'LOCAL'
                    )
                    AND partition_name = rec.partition_name
                ) LOOP
                    EXECUTE IMMEDIATE
                        'ALTER INDEX ' || idx.index_name ||
                        ' REBUILD PARTITION ' || idx.partition_name;
                END LOOP;

                v_count := v_count + 1;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error compressing partition ' || rec.partition_name || ': ' || SQLERRM);
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Compressed ' || v_count || ' partition(s)');
END;
/


-- -----------------------------------------------------------------------------
-- Move partitions to different tablespace
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE move_partitions_to_tablespace(
    p_table_name VARCHAR2,
    p_target_tablespace VARCHAR2,
    p_age_days NUMBER DEFAULT NULL,
    p_compression VARCHAR2 DEFAULT NULL
) AS
    v_cutoff_date DATE := TRUNC(SYSDATE) - NVL(p_age_days, 0);
    v_partition_date DATE;
    v_sql VARCHAR2(4000);
    v_compress_clause VARCHAR2(100) := '';
    v_count NUMBER := 0;
BEGIN
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

            EXECUTE IMMEDIATE
                'ALTER TABLE ' || p_table_name ||
                ' MOVE PARTITION ' || rec.partition_name ||
                ' TABLESPACE ' || p_target_tablespace ||
                v_compress_clause;

            -- Rebuild local indexes in target tablespace
            FOR idx IN (
                SELECT index_name, partition_name
                FROM user_ind_partitions
                WHERE index_name IN (
                    SELECT index_name FROM user_indexes
                    WHERE table_name = UPPER(p_table_name)
                    AND locality = 'LOCAL'
                )
                AND partition_name = rec.partition_name
            ) LOOP
                EXECUTE IMMEDIATE
                    'ALTER INDEX ' || idx.index_name ||
                    ' REBUILD PARTITION ' || idx.partition_name ||
                    ' TABLESPACE ' || p_target_tablespace;
            END LOOP;

            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error moving partition ' || rec.partition_name || ': ' || SQLERRM);
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Moved ' || v_count || ' partition(s) to ' || p_target_tablespace);
END;
/


-- =============================================================================
-- SECTION 4: PARTITION STATISTICS MANAGEMENT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Gather statistics on partitions incrementally
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE gather_partition_statistics(
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2 DEFAULT NULL,
    p_degree NUMBER DEFAULT 4,
    p_incremental BOOLEAN DEFAULT TRUE
) AS
    v_granularity VARCHAR2(30) := 'AUTO';
BEGIN
    IF p_incremental THEN
        -- Enable incremental statistics
        EXECUTE IMMEDIATE
            'ALTER TABLE ' || p_table_name || ' MODIFY PARTITION BY RANGE (...) (' ||
            'PARTITION INTERVAL (NUMTOYMINTERVAL(1,''MONTH''))' ||
            ')';

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
    END IF;

    DBMS_OUTPUT.PUT_LINE('Statistics gathered successfully');
END;
/


-- -----------------------------------------------------------------------------
-- Identify and gather stats on stale partitions
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE gather_stale_partition_stats(
    p_table_name VARCHAR2,
    p_stale_percent NUMBER DEFAULT 10,
    p_degree NUMBER DEFAULT 4
) AS
    v_count NUMBER := 0;
BEGIN
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

    DBMS_OUTPUT.PUT_LINE('Gathered statistics on ' || v_count || ' stale partition(s)');
END;
/


-- =============================================================================
-- SECTION 5: PARTITION HEALTH CHECK AND VALIDATION
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check partition health and identify issues
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE check_partition_health(
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
/


-- -----------------------------------------------------------------------------
-- Validate partition constraints
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE validate_partition_constraints(
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
/


-- =============================================================================
-- SECTION 6: PARTITION REPORTING
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Generate partition size report
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE partition_size_report(
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
/


-- =============================================================================
-- USAGE EXAMPLES
-- =============================================================================

-- Create future partitions for next 12 months
-- EXEC create_future_partitions('SALES_FACT', 12, 'MONTH');

-- Exchange partition load
-- EXEC exchange_partition_load('SALES_FACT', 'SALES_STAGING', 'P_2024_10');

-- Compress old partitions
-- EXEC compress_partitions('SALES_FACT', 'ARCHIVE HIGH', 365);

-- Move old partitions to cold storage
-- EXEC move_partitions_to_tablespace('SALES_FACT', 'TBS_COLD', 730, 'ARCHIVE HIGH');

-- Truncate partitions older than 7 years
-- EXEC truncate_old_partitions('SALES_FACT', 2555);

-- Gather statistics on stale partitions
-- EXEC gather_stale_partition_stats('SALES_FACT', 10, 8);

-- Check partition health
-- EXEC check_partition_health('SALES_FACT');

-- Generate size report
-- EXEC partition_size_report('SALES_FACT');
