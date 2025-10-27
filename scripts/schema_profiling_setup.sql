-- =====================================================================
-- Schema Profiling Framework - Complete Setup
-- =====================================================================
-- Creates permanent tables, views, and package for schema-level profiling
--
-- Purpose: Metadata-only profiling for ILM candidate identification at
--          the schema level with tablespace consolidation analysis
--
-- Components:
--   1. Tables: dwh_tables_quick_profile, dwh_schema_tablespaces, dwh_schema_profile
--   2. View: v_dwh_ilm_schema_ranking
--   3. Package: pck_dwh_schema_profiler
--
-- Usage:
--   -- Run setup once:
--   @schema_profiling_setup.sql
--
--   -- Execute profiling:
--   BEGIN
--       cmr.pck_dwh_schema_profiler.run_profiling(
--           p_min_table_size_gb => 1,
--           p_calculate_scores => TRUE
--       );
--   END;
--   /
--
--   -- View results:
--   SELECT * FROM cmr.v_dwh_ilm_schema_ranking;
-- =====================================================================

PROMPT
PROMPT =====================================================================
PROMPT Starting Schema Profiling Framework Setup
PROMPT =====================================================================

-- SECTION 0: Cleanup (for rerunning)
-- =====================================================================

PROMPT
PROMPT Dropping existing objects (if any)...

BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE cmr.pck_dwh_schema_profiler';
    DBMS_OUTPUT.PUT_LINE('Dropped package cmr.pck_dwh_schema_profiler');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -4043 THEN
            DBMS_OUTPUT.PUT_LINE('Package cmr.pck_dwh_schema_profiler does not exist (OK)');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW cmr.v_dwh_ilm_schema_ranking';
    DBMS_OUTPUT.PUT_LINE('Dropped view cmr.v_dwh_ilm_schema_ranking');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('View cmr.v_dwh_ilm_schema_ranking does not exist (OK)');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_schema_profile CASCADE CONSTRAINTS';
    DBMS_OUTPUT.PUT_LINE('Dropped table cmr.dwh_schema_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('Table cmr.dwh_schema_profile does not exist (OK)');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_schema_tablespaces CASCADE CONSTRAINTS';
    DBMS_OUTPUT.PUT_LINE('Dropped table cmr.dwh_schema_tablespaces');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('Table cmr.dwh_schema_tablespaces does not exist (OK)');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_tables_quick_profile CASCADE CONSTRAINTS';
    DBMS_OUTPUT.PUT_LINE('Dropped table cmr.dwh_tables_quick_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -942 THEN
            DBMS_OUTPUT.PUT_LINE('Table cmr.dwh_tables_quick_profile does not exist (OK)');
        ELSE
            RAISE;
        END IF;
END;
/

-- SECTION 1: Create Tables
-- =====================================================================

PROMPT
PROMPT Creating tables...

-- Table 1: Tables Quick Profile (Table-Level Metrics)
-- =====================================================================
CREATE TABLE cmr.dwh_tables_quick_profile (
    owner VARCHAR2(128) NOT NULL,
    table_name VARCHAR2(128) NOT NULL,
    size_gb NUMBER,
    num_rows NUMBER,
    partitioned VARCHAR2(3),
    compression VARCHAR2(30),
    tablespace_name VARCHAR2(128),
    num_indexes NUMBER,
    num_fk_constraints NUMBER,
    has_pk VARCHAR2(3),
    has_date_columns VARCHAR2(3),
    date_column_count NUMBER,
    has_lobs VARCHAR2(3),
    lob_column_count NUMBER,
    lob_type VARCHAR2(30),  -- NONE, SECUREFILE, BASICFILE, MIXED
    basicfile_lob_count NUMBER,
    lob_total_size_gb NUMBER,
    last_analyzed DATE,
    stats_age_days NUMBER,
    profile_date DATE DEFAULT SYSDATE NOT NULL,
    CONSTRAINT dwh_tables_quick_profile_pk PRIMARY KEY (owner, table_name)
);

CREATE INDEX dwh_tables_quick_profile_i1 ON cmr.dwh_tables_quick_profile(profile_date);
CREATE INDEX dwh_tables_quick_profile_i2 ON cmr.dwh_tables_quick_profile(size_gb);

COMMENT ON TABLE cmr.dwh_tables_quick_profile IS 'Table-level profiling metrics for ILM candidate identification (metadata-only)';

PROMPT Table created: cmr.dwh_tables_quick_profile

-- Table 2: Schema Tablespaces (Tablespace Info per Schema)
-- =====================================================================
CREATE TABLE cmr.dwh_schema_tablespaces (
    owner VARCHAR2(128) NOT NULL,
    tablespace_name VARCHAR2(128) NOT NULL,
    tablespace_size_gb NUMBER,
    tablespace_used_gb NUMBER,
    tablespace_free_gb NUMBER,
    pct_used NUMBER,
    block_size NUMBER,
    extent_management VARCHAR2(30),
    datafile_count NUMBER,
    profile_date DATE DEFAULT SYSDATE NOT NULL,
    CONSTRAINT dwh_schema_tablespaces_pk PRIMARY KEY (owner, tablespace_name)
);

CREATE INDEX dwh_schema_tablespaces_i1 ON cmr.dwh_schema_tablespaces(profile_date);
CREATE INDEX dwh_schema_tablespaces_i2 ON cmr.dwh_schema_tablespaces(owner);

COMMENT ON TABLE cmr.dwh_schema_tablespaces IS 'Tablespace information per schema for consolidation planning';

PROMPT Table created: cmr.dwh_schema_tablespaces

-- Table 3: Schema Profile (Aggregated Schema-Level Metrics)
-- =====================================================================
CREATE TABLE cmr.dwh_schema_profile (
    owner VARCHAR2(128) PRIMARY KEY,

    -- Size metrics
    total_size_gb NUMBER,
    table_count NUMBER,
    large_table_count NUMBER, -- > 50 GB
    avg_table_size_gb NUMBER,

    -- Tablespace metrics
    tablespace_count NUMBER,
    total_tablespace_size_gb NUMBER,
    avg_tablespace_used_pct NUMBER,

    -- Partitioning metrics
    partitioned_table_count NUMBER,
    non_partitioned_table_count NUMBER,
    pct_partitioned NUMBER,
    tables_with_date_columns NUMBER,
    pct_partition_ready NUMBER,

    -- LOB metrics
    lob_table_count NUMBER,
    basicfile_lob_table_count NUMBER,
    securefile_lob_table_count NUMBER,
    total_basicfile_lob_columns NUMBER,
    pct_tables_with_basicfile NUMBER,

    -- Complexity metrics
    avg_indexes_per_table NUMBER,
    avg_fk_per_table NUMBER,
    tables_with_many_dependencies NUMBER, -- > 5 indexes or > 3 FKs

    -- Estimated savings
    estimated_compression_savings_gb NUMBER,
    estimated_savings_pct NUMBER,

    -- Scoring columns (populated by pck_dwh_schema_profiler.calculate_scores)
    storage_impact_score NUMBER,
    migration_readiness_score NUMBER,
    business_value_score NUMBER,
    schema_priority_score NUMBER,
    schema_category VARCHAR2(30),
    schema_type VARCHAR2(30),

    -- Metadata
    profile_date DATE DEFAULT SYSDATE NOT NULL
);

CREATE INDEX dwh_schema_profile_i1 ON cmr.dwh_schema_profile(profile_date);
CREATE INDEX dwh_schema_profile_i2 ON cmr.dwh_schema_profile(schema_priority_score);
CREATE INDEX dwh_schema_profile_i3 ON cmr.dwh_schema_profile(schema_category);

COMMENT ON TABLE cmr.dwh_schema_profile IS 'Schema-level aggregated metrics and scoring for ILM candidate ranking';

PROMPT Table created: cmr.dwh_schema_profile

-- SECTION 2: Create View
-- =====================================================================

PROMPT
PROMPT Creating view...

CREATE OR REPLACE VIEW cmr.v_dwh_ilm_schema_ranking AS
SELECT
    owner,
    total_size_gb,
    estimated_compression_savings_gb,
    estimated_savings_pct,
    table_count,
    tablespace_count,
    pct_partitioned,
    pct_partition_ready,
    basicfile_lob_table_count,
    total_basicfile_lob_columns,
    storage_impact_score,
    migration_readiness_score,
    business_value_score,
    schema_priority_score,
    schema_category,
    schema_type,
    profile_date
FROM cmr.dwh_schema_profile
ORDER BY schema_priority_score DESC;

COMMENT ON TABLE cmr.v_dwh_ilm_schema_ranking IS 'Ranked list of schemas for ILM migration (ordered by priority score)';

PROMPT View created: cmr.v_dwh_ilm_schema_ranking

-- SECTION 3: Create Package Specification
-- =====================================================================

PROMPT
PROMPT Creating package specification...

CREATE OR REPLACE PACKAGE cmr.pck_dwh_schema_profiler AS
    -- =====================================================================
    -- Package: pck_dwh_schema_profiler
    -- Purpose: Schema-level profiling for ILM candidate identification
    --
    -- Features:
    --   - Metadata-only profiling (no table data scanning)
    --   - Fast execution (15-30 minutes for large databases)
    --   - Schema-level ranking with tablespace analysis
    --   - Three-dimensional scoring (Storage Impact, Migration Readiness, Business Value)
    --
    -- Main Procedure:
    --   run_profiling - Execute complete profiling workflow
    --
    -- Usage Example:
    --   BEGIN
    --       cmr.pck_dwh_schema_profiler.run_profiling(
    --           p_min_table_size_gb => 1,
    --           p_calculate_scores => TRUE
    --       );
    --   END;
    --   /
    --
    --   SELECT * FROM cmr.v_dwh_ilm_schema_ranking;
    -- =====================================================================

    /**
     * Run complete schema profiling workflow
     *
     * @param p_min_table_size_gb Minimum table size in GB to include (default: 1)
     * @param p_calculate_scores Calculate scores after profiling (default: TRUE)
     * @param p_truncate_before Truncate tables before profiling (default: TRUE)
     */
    PROCEDURE run_profiling(
        p_min_table_size_gb IN NUMBER DEFAULT 1,
        p_calculate_scores IN BOOLEAN DEFAULT TRUE,
        p_truncate_before IN BOOLEAN DEFAULT TRUE
    );

    /**
     * Step 1: Profile table-level metrics
     * Gathers metadata for all tables > p_min_table_size_gb
     */
    PROCEDURE profile_tables(
        p_min_table_size_gb IN NUMBER DEFAULT 1
    );

    /**
     * Step 2: Profile tablespace information per schema
     */
    PROCEDURE profile_tablespaces;

    /**
     * Step 3: Aggregate table metrics to schema level
     */
    PROCEDURE aggregate_to_schema_level;

    /**
     * Step 4: Calculate scoring for schema ranking
     */
    PROCEDURE calculate_scores;

    /**
     * Truncate all profiling tables
     */
    PROCEDURE truncate_profiling_tables;

END pck_dwh_schema_profiler;
/

SHOW ERRORS PACKAGE cmr.pck_dwh_schema_profiler;

PROMPT Package specification created: cmr.pck_dwh_schema_profiler

-- SECTION 4: Create Package Body
-- =====================================================================

PROMPT
PROMPT Creating package body...

CREATE OR REPLACE PACKAGE BODY cmr.pck_dwh_schema_profiler AS

    -- =====================================================================
    -- Private Procedures
    -- =====================================================================

    PROCEDURE log_message(p_message IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' - ' || p_message);
    END log_message;

    -- =====================================================================
    -- Public Procedures
    -- =====================================================================

    PROCEDURE truncate_profiling_tables IS
    BEGIN
        log_message('Truncating profiling tables...');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE cmr.dwh_tables_quick_profile';
        log_message('  Truncated: cmr.dwh_tables_quick_profile');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE cmr.dwh_schema_tablespaces';
        log_message('  Truncated: cmr.dwh_schema_tablespaces');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE cmr.dwh_schema_profile';
        log_message('  Truncated: cmr.dwh_schema_profile');

        COMMIT;
    END truncate_profiling_tables;

    PROCEDURE profile_tables(p_min_table_size_gb IN NUMBER DEFAULT 1) IS
        v_count NUMBER := 0;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        log_message('Starting table profiling (min size: ' || p_min_table_size_gb || ' GB)...');

        -- Enable parallel DML for this session
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        INSERT /*+ APPEND */ INTO cmr.dwh_tables_quick_profile
        SELECT /*+ PARALLEL(t,4) PARALLEL(s,4) USE_HASH(t tracked_schemas s idx fk pk dc lob) */
            t.owner,
            t.table_name,
            ROUND(NVL(s.bytes, 0) / 1024 / 1024 / 1024, 2) AS size_gb,
            t.num_rows,
            t.partitioned,
            t.compression,
            t.tablespace_name,
            NVL(idx.index_count, 0) AS num_indexes,
            NVL(fk.fk_count, 0) AS num_fk_constraints,
            CASE WHEN pk.constraint_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS has_pk,
            CASE WHEN dc.date_column_count > 0 THEN 'YES' ELSE 'NO' END AS has_date_columns,
            NVL(dc.date_column_count, 0) AS date_column_count,
            CASE WHEN lob.lob_column_count > 0 THEN 'YES' ELSE 'NO' END AS has_lobs,
            NVL(lob.lob_column_count, 0) AS lob_column_count,
            NVL(lob.lob_type, 'NONE') AS lob_type,
            NVL(lob.basicfile_count, 0) AS basicfile_lob_count,
            ROUND(NVL(lob.lob_total_size, 0) / 1024 / 1024 / 1024, 2) AS lob_total_size_gb,
            t.last_analyzed,
            ROUND(SYSDATE - NVL(t.last_analyzed, SYSDATE - 365)) AS stats_age_days,
            SYSDATE AS profile_date
        FROM dba_tables t
        JOIN (
            SELECT /*+ NO_MERGE */ DISTINCT owner FROM LOGS.DWH_PROCESS WHERE owner IS NOT NULL
        ) tracked_schemas ON t.owner = tracked_schemas.owner
        LEFT JOIN dba_segments s
            ON t.owner = s.owner
            AND t.table_name = s.segment_name
            AND s.segment_type IN ('TABLE', 'TABLE PARTITION')
        -- Index count
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(dba_indexes,2) */
                owner, table_name, COUNT(*) AS index_count
            FROM dba_indexes
            GROUP BY owner, table_name
        ) idx ON t.owner = idx.owner AND t.table_name = idx.table_name
        -- FK constraints
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(dba_constraints,2) */
                owner, table_name, COUNT(*) AS fk_count
            FROM dba_constraints
            WHERE constraint_type = 'R'
            GROUP BY owner, table_name
        ) fk ON t.owner = fk.owner AND t.table_name = fk.table_name
        -- Primary key
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(dba_constraints,2) */
                owner, table_name, constraint_name
            FROM dba_constraints
            WHERE constraint_type = 'P'
        ) pk ON t.owner = pk.owner AND t.table_name = pk.table_name
        -- Date columns
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(dba_tab_columns,2) */
                owner, table_name, COUNT(*) AS date_column_count
            FROM dba_tab_columns
            WHERE data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)', 'TIMESTAMP(9)')
            GROUP BY owner, table_name
        ) dc ON t.owner = dc.owner AND t.table_name = dc.table_name
        -- LOB analysis
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(l,2) PARALLEL(ls,2) USE_HASH(l ls) */
                l.owner,
                l.table_name,
                COUNT(*) AS lob_column_count,
                SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) AS basicfile_count,
                SUM(CASE WHEN l.securefile = 'YES' THEN 1 ELSE 0 END) AS securefile_count,
                CASE
                    WHEN SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) > 0
                         AND SUM(CASE WHEN l.securefile = 'YES' THEN 1 ELSE 0 END) > 0
                    THEN 'MIXED'
                    WHEN SUM(CASE WHEN l.securefile = 'NO' THEN 1 ELSE 0 END) > 0
                    THEN 'BASICFILE'
                    ELSE 'SECUREFILE'
                END AS lob_type,
                SUM(NVL(ls.bytes, 0)) AS lob_total_size
            FROM dba_lobs l
            LEFT JOIN dba_segments ls
                ON l.owner = ls.owner
                AND l.segment_name = ls.segment_name
            GROUP BY l.owner, l.table_name
        ) lob ON t.owner = lob.owner AND t.table_name = lob.table_name
        WHERE t.temporary = 'N'
          AND t.nested = 'NO'
          AND NVL(s.bytes, 0) > (p_min_table_size_gb * 1024 * 1024 * 1024)
        ORDER BY s.bytes DESC;

        v_count := SQL%ROWCOUNT;
        COMMIT;

        log_message('Table profiling complete: ' || v_count || ' tables profiled in ' ||
                   ROUND(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), 1) || ' seconds');
    END profile_tables;

    PROCEDURE profile_tablespaces IS
        v_count NUMBER := 0;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        log_message('Starting tablespace profiling...');

        -- Enable parallel DML for this session
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        INSERT /*+ APPEND */ INTO cmr.dwh_schema_tablespaces
        SELECT /*+ PARALLEL(ts_owner,2) PARALLEL(ts,2) PARALLEL(df,2) USE_HASH(ts_owner ts df fs) */
            ts_owner.owner,
            ts.tablespace_name,
            ROUND(SUM(df.bytes) / 1024 / 1024 / 1024, 2) AS tablespace_size_gb,
            ROUND((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / 1024 / 1024 / 1024, 2) AS tablespace_used_gb,
            ROUND(NVL(SUM(fs.bytes), 0) / 1024 / 1024 / 1024, 2) AS tablespace_free_gb,
            ROUND(((SUM(df.bytes) - NVL(SUM(fs.bytes), 0)) / NULLIF(SUM(df.bytes), 0)) * 100, 1) AS pct_used,
            ts.block_size,
            ts.extent_management,
            COUNT(DISTINCT df.file_id) AS datafile_count,
            SYSDATE AS profile_date
        FROM (
            -- Get unique tablespaces per tracked schema
            SELECT /*+ NO_MERGE */ DISTINCT t.owner, t.tablespace_name
            FROM dba_tables t
            WHERE t.owner IN (SELECT owner FROM cmr.dwh_tables_quick_profile)
              AND t.tablespace_name IS NOT NULL
        ) ts_owner
        JOIN dba_tablespaces ts ON ts_owner.tablespace_name = ts.tablespace_name
        LEFT JOIN dba_data_files df ON ts.tablespace_name = df.tablespace_name
        LEFT JOIN (
            SELECT /*+ NO_MERGE PARALLEL(dba_free_space,2) */
                tablespace_name, SUM(bytes) AS bytes
            FROM dba_free_space
            GROUP BY tablespace_name
        ) fs ON ts.tablespace_name = fs.tablespace_name
        GROUP BY ts_owner.owner, ts.tablespace_name, ts.block_size, ts.extent_management
        ORDER BY ts_owner.owner, tablespace_size_gb DESC;

        v_count := SQL%ROWCOUNT;
        COMMIT;

        log_message('Tablespace profiling complete: ' || v_count || ' tablespace mappings in ' ||
                   ROUND(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), 1) || ' seconds');
    END profile_tablespaces;

    PROCEDURE aggregate_to_schema_level IS
        v_count NUMBER := 0;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        log_message('Starting schema-level aggregation...');

        -- Enable parallel DML for this session
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        INSERT /*+ APPEND */ INTO cmr.dwh_schema_profile
        SELECT /*+ PARALLEL(p,2) */
            p.owner,

            -- Size metrics
            ROUND(SUM(p.size_gb), 2) AS total_size_gb,
            COUNT(*) AS table_count,
            SUM(CASE WHEN p.size_gb > 50 THEN 1 ELSE 0 END) AS large_table_count,
            ROUND(AVG(p.size_gb), 2) AS avg_table_size_gb,

            -- Tablespace metrics
            (SELECT /*+ INDEX(dwh_schema_tablespaces dwh_schema_tablespaces_i2) */
                COUNT(DISTINCT tablespace_name)
             FROM cmr.dwh_schema_tablespaces
             WHERE owner = p.owner) AS tablespace_count,
            (SELECT /*+ INDEX(dwh_schema_tablespaces dwh_schema_tablespaces_i2) */
                ROUND(SUM(tablespace_size_gb), 2)
             FROM cmr.dwh_schema_tablespaces
             WHERE owner = p.owner) AS total_tablespace_size_gb,
            (SELECT /*+ INDEX(dwh_schema_tablespaces dwh_schema_tablespaces_i2) */
                ROUND(AVG(pct_used), 1)
             FROM cmr.dwh_schema_tablespaces
             WHERE owner = p.owner) AS avg_tablespace_used_pct,

            -- Partitioning metrics
            SUM(CASE WHEN p.partitioned = 'YES' THEN 1 ELSE 0 END) AS partitioned_table_count,
            SUM(CASE WHEN p.partitioned = 'NO' THEN 1 ELSE 0 END) AS non_partitioned_table_count,
            ROUND((SUM(CASE WHEN p.partitioned = 'YES' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_partitioned,
            SUM(CASE WHEN p.has_date_columns = 'YES' THEN 1 ELSE 0 END) AS tables_with_date_columns,
            ROUND((SUM(CASE WHEN p.has_date_columns = 'YES' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_partition_ready,

            -- LOB metrics
            SUM(CASE WHEN p.has_lobs = 'YES' THEN 1 ELSE 0 END) AS lob_table_count,
            SUM(CASE WHEN p.lob_type = 'BASICFILE' THEN 1 ELSE 0 END) AS basicfile_lob_table_count,
            SUM(CASE WHEN p.lob_type = 'SECUREFILE' THEN 1 ELSE 0 END) AS securefile_lob_table_count,
            SUM(p.basicfile_lob_count) AS total_basicfile_lob_columns,
            ROUND((SUM(CASE WHEN p.lob_type = 'BASICFILE' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 1) AS pct_tables_with_basicfile,

            -- Complexity metrics
            ROUND(AVG(p.num_indexes), 1) AS avg_indexes_per_table,
            ROUND(AVG(p.num_fk_constraints), 1) AS avg_fk_per_table,
            SUM(CASE WHEN p.num_indexes > 5 OR p.num_fk_constraints > 3 THEN 1 ELSE 0 END) AS tables_with_many_dependencies,

            -- Estimated savings (conservative: 40% compression on non-partitioned tables with LOBs)
            ROUND(SUM(
                CASE
                    WHEN p.partitioned = 'NO' AND p.has_lobs = 'YES' THEN p.size_gb * 0.40
                    WHEN p.partitioned = 'NO' THEN p.size_gb * 0.30
                    WHEN p.lob_type = 'BASICFILE' THEN p.lob_total_size_gb * 0.50
                    ELSE p.size_gb * 0.20
                END
            ), 2) AS estimated_compression_savings_gb,
            ROUND((SUM(
                CASE
                    WHEN p.partitioned = 'NO' AND p.has_lobs = 'YES' THEN p.size_gb * 0.40
                    WHEN p.partitioned = 'NO' THEN p.size_gb * 0.30
                    WHEN p.lob_type = 'BASICFILE' THEN p.lob_total_size_gb * 0.50
                    ELSE p.size_gb * 0.20
                END
            ) / NULLIF(SUM(p.size_gb), 0)) * 100, 1) AS estimated_savings_pct,

            -- Scoring columns (NULL initially, populated by calculate_scores)
            NULL, NULL, NULL, NULL, NULL, NULL,

            -- Profile date
            SYSDATE
        FROM cmr.dwh_tables_quick_profile p
        GROUP BY p.owner
        ORDER BY SUM(p.size_gb) DESC;

        v_count := SQL%ROWCOUNT;
        COMMIT;

        log_message('Schema aggregation complete: ' || v_count || ' schemas in ' ||
                   ROUND(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), 1) || ' seconds');
    END aggregate_to_schema_level;

    PROCEDURE calculate_scores IS
        v_count NUMBER := 0;
        v_start_time TIMESTAMP := SYSTIMESTAMP;
    BEGIN
        log_message('Starting score calculation...');

        -- Enable parallel DML for this session
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        -- Calculate Storage Impact Score (0-100)
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET storage_impact_score = ROUND(
            (total_size_gb / (SELECT MAX(total_size_gb) FROM cmr.dwh_schema_profile) * 60) +
            (estimated_compression_savings_gb / (SELECT MAX(estimated_compression_savings_gb) FROM cmr.dwh_schema_profile) * 40)
        , 1);

        -- Calculate Migration Readiness Score (0-100)
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET migration_readiness_score = ROUND(
            CASE
                WHEN tablespace_count <= 2 THEN 30
                WHEN tablespace_count <= 3 THEN 25
                WHEN tablespace_count <= 5 THEN 15
                ELSE 5
            END +
            CASE
                WHEN avg_indexes_per_table <= 3 AND avg_fk_per_table <= 1 THEN 30
                WHEN avg_indexes_per_table <= 5 AND avg_fk_per_table <= 2 THEN 20
                ELSE 10
            END +
            (pct_partition_ready * 0.40)
        , 1);

        -- Calculate Business Value Score (0-100)
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET business_value_score = ROUND(
            (estimated_savings_pct * 0.60) +
            CASE
                WHEN pct_tables_with_basicfile >= 50 THEN 40
                WHEN pct_tables_with_basicfile >= 30 THEN 30
                WHEN pct_tables_with_basicfile >= 10 THEN 20
                WHEN total_basicfile_lob_columns > 0 THEN 10
                ELSE 0
            END
        , 1);

        -- Calculate Schema Priority Score (weighted average)
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET schema_priority_score = ROUND(
            (storage_impact_score * 0.50) +
            (migration_readiness_score * 0.30) +
            (business_value_score * 0.20)
        , 1);

        -- Assign schema categories
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET schema_category = CASE
            WHEN schema_priority_score >= 80 THEN 'QUICK_WIN_SCHEMA'
            WHEN schema_priority_score >= 60 THEN 'HIGH_PRIORITY'
            WHEN schema_priority_score >= 40 THEN 'MEDIUM_PRIORITY'
            ELSE 'LOW_PRIORITY'
        END;

        -- Assign schema types
        UPDATE /*+ PARALLEL(cmr.dwh_schema_profile,2) */ cmr.dwh_schema_profile SET schema_type = CASE
            WHEN total_size_gb > 10000 AND tablespace_count <= 3 THEN 'TYPE_A_LARGE_SIMPLE'
            WHEN pct_tables_with_basicfile >= 50 THEN 'TYPE_B_BASICFILE_HEAVY'
            WHEN total_size_gb BETWEEN 1000 AND 10000 AND tablespace_count BETWEEN 3 AND 5 THEN 'TYPE_C_MEDIUM_MATURE'
            WHEN total_size_gb > 10000 AND tablespace_count > 5 THEN 'TYPE_D_COMPLEX_LARGE'
            ELSE 'TYPE_E_SMALL_LOW_VALUE'
        END;

        v_count := SQL%ROWCOUNT;
        COMMIT;

        log_message('Score calculation complete: ' || v_count || ' schemas scored in ' ||
                   ROUND(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), 1) || ' seconds');
    END calculate_scores;

    PROCEDURE run_profiling(
        p_min_table_size_gb IN NUMBER DEFAULT 1,
        p_calculate_scores IN BOOLEAN DEFAULT TRUE,
        p_truncate_before IN BOOLEAN DEFAULT TRUE
    ) IS
        v_start_time TIMESTAMP := SYSTIMESTAMP;
        v_duration NUMBER;
    BEGIN
        log_message('======================================================');
        log_message('Starting Schema Profiling Workflow');
        log_message('======================================================');
        log_message('Parameters:');
        log_message('  Min table size: ' || p_min_table_size_gb || ' GB');
        log_message('  Calculate scores: ' || CASE WHEN p_calculate_scores THEN 'YES' ELSE 'NO' END);
        log_message('  Truncate before: ' || CASE WHEN p_truncate_before THEN 'YES' ELSE 'NO' END);
        log_message('------------------------------------------------------');

        -- Step 0: Truncate if requested
        IF p_truncate_before THEN
            truncate_profiling_tables;
        END IF;

        -- Step 1: Profile tables
        profile_tables(p_min_table_size_gb);

        -- Step 2: Profile tablespaces
        profile_tablespaces;

        -- Step 3: Aggregate to schema level
        aggregate_to_schema_level;

        -- Step 4: Calculate scores if requested
        IF p_calculate_scores THEN
            calculate_scores;
        END IF;

        v_duration := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time));

        log_message('======================================================');
        log_message('Schema Profiling Workflow Complete');
        log_message('Total duration: ' || ROUND(v_duration / 60, 1) || ' minutes');
        log_message('======================================================');
        log_message('');
        log_message('View results:');
        log_message('  SELECT * FROM cmr.v_dwh_ilm_schema_ranking;');
        log_message('');

    EXCEPTION
        WHEN OTHERS THEN
            log_message('ERROR in run_profiling: ' || SQLERRM);
            log_message('SQLCODE: ' || SQLCODE);
            ROLLBACK;
            RAISE;
    END run_profiling;

END pck_dwh_schema_profiler;
/

SHOW ERRORS PACKAGE BODY cmr.pck_dwh_schema_profiler;

PROMPT Package body created: cmr.pck_dwh_schema_profiler

-- =====================================================================
-- Setup Complete
-- =====================================================================

PROMPT
PROMPT =====================================================================
PROMPT Schema Profiling Framework Setup Complete
PROMPT =====================================================================
PROMPT
PROMPT Objects Created:
PROMPT   Tables:
PROMPT     - cmr.dwh_tables_quick_profile
PROMPT     - cmr.dwh_schema_tablespaces
PROMPT     - cmr.dwh_schema_profile
PROMPT   View:
PROMPT     - cmr.v_dwh_ilm_schema_ranking
PROMPT   Package:
PROMPT     - cmr.pck_dwh_schema_profiler
PROMPT
PROMPT =====================================================================
PROMPT Quick Start
PROMPT =====================================================================
PROMPT
PROMPT -- Run profiling:
PROMPT BEGIN
PROMPT     cmr.pck_dwh_schema_profiler.run_profiling(
PROMPT         p_min_table_size_gb => 1,
PROMPT         p_calculate_scores => TRUE
PROMPT     );
PROMPT END;
PROMPT /
PROMPT
PROMPT -- View results:
PROMPT SELECT * FROM cmr.v_dwh_ilm_schema_ranking;
PROMPT
PROMPT =====================================================================
