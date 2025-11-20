-- =============================================================================
-- Table Migration Framework - Setup and Metadata
-- Convert non-partitioned tables to partitioned with ILM integration
-- =============================================================================

-- =============================================================================
-- SECTION 1: MIGRATION METADATA TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Migration Projects - Track migration initiatives
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_migration_projects (
            project_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            project_name        VARCHAR2(100) NOT NULL UNIQUE,
            description         VARCHAR2(500),
            status              VARCHAR2(30) DEFAULT ''PLANNING'',  -- PLANNING, ANALYSIS, READY, IN_PROGRESS, COMPLETED, FAILED, ROLLED_BACK
            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
            started_date        TIMESTAMP,
            completed_date      TIMESTAMP,

            CONSTRAINT chk_proj_status CHECK (status IN (''PLANNING'', ''ANALYSIS'', ''READY'', ''IN_PROGRESS'', ''COMPLETED'', ''FAILED'', ''ROLLED_BACK''))
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_migration_projects');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_migration_projects already exists - preserving existing data');
        ELSE RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_migration_projects IS 'Migration project tracking';


-- -----------------------------------------------------------------------------
-- Migration Tasks - Individual table migrations
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_migration_tasks (
            task_id             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            project_id          NUMBER,
            task_name           VARCHAR2(100) NOT NULL,

            -- Source table
            source_owner        VARCHAR2(30) DEFAULT USER,
            source_table        VARCHAR2(128) NOT NULL,

            -- Target partitioning strategy
            partition_type      VARCHAR2(30),
            partition_key       VARCHAR2(500),
            subpartition_type   VARCHAR2(30),
            subpartition_key    VARCHAR2(500),
            interval_clause     VARCHAR2(200),

            -- Migration strategy
            migration_method    VARCHAR2(30) DEFAULT ''CTAS'',
            use_compression     CHAR(1) DEFAULT ''Y'',
            compression_type    VARCHAR2(50) DEFAULT ''BASIC'',  -- BASIC for uniform partitioning, overridden by template HOT tier for tiered partitioning
            lob_compression     VARCHAR2(30) DEFAULT ''MEDIUM'',  -- LOB compression: LOW, MEDIUM, HIGH
            lob_deduplicate     CHAR(1) DEFAULT ''N'',            -- LOB deduplication: Y/N
            target_tablespace   VARCHAR2(30),
            parallel_degree     NUMBER DEFAULT 4,
            enable_row_movement CHAR(1) DEFAULT ''Y'',
            automatic_list      CHAR(1) DEFAULT ''N'',
            list_default_values VARCHAR2(4000),
            rename_original_table CHAR(1) DEFAULT ''Y'',          -- Y: rename original to _OLD, migrated becomes original name; N: keep original unchanged, migrated stays as _MIGR

            -- ILM integration
            apply_ilm_policies  CHAR(1) DEFAULT ''Y'',
            ilm_policy_template VARCHAR2(100),

            -- Status tracking
            status              VARCHAR2(30) DEFAULT ''PENDING'',
            validation_status   VARCHAR2(30),
            error_message       VARCHAR2(4000),

            -- Execution details
            analysis_date       TIMESTAMP,
            execution_start     TIMESTAMP,
            execution_end       TIMESTAMP,
            duration_seconds    NUMBER,

            -- Metrics
            source_rows         NUMBER,
            source_size_mb      NUMBER,
            target_size_mb      NUMBER,
            space_saved_mb      NUMBER,

            -- Rollback info
            backup_table_name   VARCHAR2(128),
            can_rollback        CHAR(1) DEFAULT ''N'',

            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,

            CONSTRAINT fk_mig_task_project FOREIGN KEY (project_id) REFERENCES cmr.dwh_migration_projects(project_id),
            CONSTRAINT chk_task_status CHECK (status IN (''PENDING'', ''ANALYZING'', ''ANALYZED'', ''READY'', ''RUNNING'', ''COMPLETED'', ''FAILED'', ''ROLLED_BACK'')),
            CONSTRAINT chk_mig_method CHECK (migration_method IN (''CTAS'', ''ONLINE'', ''EXCHANGE'')),
            CONSTRAINT chk_automatic_list CHECK (automatic_list IN (''Y'', ''N'')),
            CONSTRAINT chk_rename_original CHECK (rename_original_table IN (''Y'', ''N''))
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_migration_tasks');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_migration_tasks already exists - preserving existing data');
        ELSE RAISE;
        END IF;
END;
/

-- Create indexes
BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_task_project ON cmr.dwh_migration_tasks(project_id)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_task_project');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_task_project already exists');
        ELSE RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_task_source ON cmr.dwh_migration_tasks(source_owner, source_table)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_task_source');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_task_source already exists');
        ELSE RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_task_status ON cmr.dwh_migration_tasks(status)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_task_status');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_task_status already exists');
        ELSE RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_migration_tasks IS 'Individual table migration tasks';
COMMENT ON COLUMN cmr.dwh_migration_tasks.enable_row_movement IS 'Enable row movement for partitioned table - allows Oracle to move rows between partitions when partition key values change (recommended for partitioned tables)';
COMMENT ON COLUMN cmr.dwh_migration_tasks.automatic_list IS 'Enable AUTOMATIC LIST partitioning (Oracle 12.2+). Creates partitions automatically for new values.';
COMMENT ON COLUMN cmr.dwh_migration_tasks.list_default_values IS 'Default values for P_XDEF partition (e.g., ''NAV'' for VARCHAR, -1 for NUMBER, DATE ''5999-01-01'' for DATE). If NULL, framework determines based on data type. See pck_dwh_constants.c_maxvalue_date.';
COMMENT ON COLUMN cmr.dwh_migration_tasks.rename_original_table IS 'Y (default): rename original table to _OLD and migrated table becomes original name; N: keep original table name unchanged, migrated table stays as _MIGR suffix';


-- -----------------------------------------------------------------------------
-- Migration Analysis Results - Store analysis recommendations
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_migration_analysis (
            analysis_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            task_id             NUMBER NOT NULL,

            -- Table statistics
            table_rows          NUMBER,
            table_size_mb       NUMBER,
            num_indexes         NUMBER,
            num_constraints     NUMBER,
            num_triggers        NUMBER,
            has_lobs            CHAR(1),
            has_foreign_keys    CHAR(1),

            -- Existing compression detection
            existing_compression VARCHAR2(30),      -- ENABLED/DISABLED
            existing_compress_for VARCHAR2(30),     -- BASIC/OLTP/QUERY HIGH/QUERY LOW/ARCHIVE HIGH/ARCHIVE LOW
            storage_type VARCHAR2(10),              -- ROW/COLUMN/NONE (ROW=BASIC/OLTP, COLUMN=HCC, NONE=uncompressed)
            partition_compression_mixed CHAR(1),    -- Y if partitions have different compression
            compression_details VARCHAR2(4000),     -- Detailed compression information

            -- Data distribution analysis
            candidate_columns   CLOB,
            recommended_strategy VARCHAR2(100),
            recommendation_reason VARCHAR2(1000),

            -- Date column conversion (for non-standard date formats)
            date_column_name    VARCHAR2(128),
            date_column_type    VARCHAR2(30),
            date_format_detected VARCHAR2(50),
            date_conversion_expr VARCHAR2(500),
            requires_conversion CHAR(1) DEFAULT ''N'',

            -- Comprehensive date column analysis (JSON format)
            all_date_columns_analysis CLOB,

            -- Partition estimates
            estimated_partitions NUMBER,
            avg_partition_size_mb NUMBER,
            estimated_compression_ratio NUMBER,
            estimated_space_savings_mb NUMBER,

            -- Complexity assessment
            complexity_score    NUMBER,
            complexity_factors  VARCHAR2(2000),
            estimated_downtime_minutes NUMBER,

            -- Online redefinition capability
            supports_online_redef CHAR(1) DEFAULT ''N'',
            online_redef_method VARCHAR2(30),
            recommended_method  VARCHAR2(30),

            -- Dependencies
            dependent_objects   CLOB,
            blocking_issues     CLOB,
            warnings            CLOB,

            -- Tablespace configuration (detected during analysis)
            target_tablespace           VARCHAR2(128),
            tablespace_extent_mgmt      VARCHAR2(30),
            tablespace_allocation       VARCHAR2(30),
            tablespace_ssm              VARCHAR2(30),
            tablespace_uniform_size     NUMBER,

            -- Storage recommendations (calculated based on table size + tablespace type)
            recommended_initial_extent  NUMBER,
            recommended_next_extent     NUMBER,
            recommended_storage_clause  VARCHAR2(500),
            storage_recommendation_reason VARCHAR2(1000),

            -- NULL handling for partition key
            partition_key_null_count    NUMBER,
            partition_key_null_percentage NUMBER(5,2),
            null_handling_strategy      VARCHAR2(30),
            null_default_value          VARCHAR2(100),
            null_handling_reason        VARCHAR2(1000),

            -- Partition boundary dates (actual data range for initial partition)
            partition_boundary_min_date DATE,
            partition_boundary_max_date DATE,
            partition_range_years       NUMBER,
            partition_boundary_recommendation VARCHAR2(1000),

            analysis_date       TIMESTAMP DEFAULT SYSTIMESTAMP,
            analysis_duration_seconds NUMBER,

            CONSTRAINT fk_mig_analysis_task FOREIGN KEY (task_id) REFERENCES cmr.dwh_migration_tasks(task_id)
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_migration_analysis');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_migration_analysis already exists - preserving existing data');
        ELSE RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_analysis_task ON cmr.dwh_migration_analysis(task_id)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_analysis_task');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_analysis_task already exists');
        ELSE RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_migration_analysis IS 'Analysis results and recommendations for migrations';


-- -----------------------------------------------------------------------------
-- Migration Execution Log - Detailed step-by-step log
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_migration_execution_log (
            log_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            execution_id        NUMBER NOT NULL,
            task_id             NUMBER NOT NULL,

            step_number         NUMBER,
            step_name           VARCHAR2(200),
            step_type           VARCHAR2(50),
            sql_statement       CLOB,

            start_time          TIMESTAMP,
            end_time            TIMESTAMP,
            duration_seconds    NUMBER,
            status              VARCHAR2(20),

            rows_processed      NUMBER,
            error_code          NUMBER,
            error_message       VARCHAR2(4000),

            CONSTRAINT fk_mig_log_task FOREIGN KEY (task_id) REFERENCES cmr.dwh_migration_tasks(task_id)
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_migration_execution_log');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_migration_execution_log already exists - preserving existing data');
        ELSE RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_log_task ON cmr.dwh_migration_execution_log(task_id, step_number)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_log_task');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_log_task already exists');
        ELSE RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_mig_log_execution ON cmr.dwh_migration_execution_log(execution_id)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_mig_log_execution');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE IN (-955, -1408) THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_mig_log_execution already exists');
        ELSE RAISE;
        END IF;
END;
/

-- Create sequence for execution IDs
BEGIN
    EXECUTE IMMEDIATE 'CREATE SEQUENCE cmr.dwh_mig_execution_seq START WITH 1 INCREMENT BY 1 NOCACHE';
    DBMS_OUTPUT.PUT_LINE('Created sequence: dwh_mig_execution_seq');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Sequence dwh_mig_execution_seq already exists');
        ELSE RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_migration_execution_log IS 'Detailed execution log for migrations';
COMMENT ON COLUMN cmr.dwh_migration_execution_log.execution_id IS 'Groups all log entries from a single migration execution. All steps in one run share the same execution_id.';


-- -----------------------------------------------------------------------------
-- ILM Policy Templates for Migrations
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_migration_ilm_templates (
            template_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            template_name       VARCHAR2(100) NOT NULL UNIQUE,
            description         VARCHAR2(500),
            table_type          VARCHAR2(50),

            -- Default policies to create
            policies_json       CLOB,

            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_migration_ilm_templates');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_migration_ilm_templates already exists - preserving existing data');
        ELSE RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_migration_ilm_templates IS 'ILM policy templates for newly migrated tables';

-- Insert default templates (using MERGE for rerunnable script)
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'FACT_TABLE_STANDARD' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'FACT_TABLE_STANDARD',
        'Standard ILM policies for fact tables: compress at 90d, tier to WARM at 24m, tier to COLD at 60m',
        'FACT',
        '[
            {"policy_name": "{TABLE}_COMPRESS_90D", "age_days": 90, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_WARM_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "pctfree": 5, "priority": 200},
            {"policy_name": "{TABLE}_TIER_COLD_60M", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 300},
            {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 301}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'DIMENSION_LARGE' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'DIMENSION_LARGE',
        'ILM policies for large dimension tables',
        'DIMENSION',
        '[
            {"policy_name": "{TABLE}_COMPRESS_180D", "age_days": 180, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'STAGING_MINIMAL' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'STAGING_MINIMAL',
        'Minimal retention for staging tables',
        'STAGING',
        '[
            {"policy_name": "{TABLE}_PURGE_30D", "age_days": 30, "action": "DROP", "priority": 900}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'SCD2_EFFECTIVE_DATE' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'SCD2_EFFECTIVE_DATE',
        'ILM policies for SCD2 tables with effective_date - compress old versions at 24m, move to COLD at 60m, retain history',
        'SCD2',
        '[
            {"policy_name": "{TABLE}_COMPRESS_24M", "age_months": 24, "action": "COMPRESS", "compression": "BASIC", "pctfree": 5, "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_60M", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 200},
            {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 300}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'SCD2_VALID_FROM_TO' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'SCD2_VALID_FROM_TO',
        'ILM policies for SCD2 tables with valid_from_dttm/valid_to_dttm - compress old versions at 24m, move to COLD at 60m',
        'SCD2',
        '[
            {"policy_name": "{TABLE}_COMPRESS_24M", "age_months": 24, "action": "COMPRESS", "compression": "BASIC", "pctfree": 5, "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_60M", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 200},
            {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 300}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'EVENTS_SHORT_RETENTION' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'EVENTS_SHORT_RETENTION',
        'ILM policies for event tables with 90-day retention (clickstream, app events)',
        'EVENTS',
        '[
            {"policy_name": "{TABLE}_COMPRESS_7D", "age_days": 7, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_30D", "age_days": 30, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
            {"policy_name": "{TABLE}_PURGE_90D", "age_days": 90, "action": "DROP", "priority": 900}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'EVENTS_COMPLIANCE' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'EVENTS_COMPLIANCE',
        'ILM policies for audit/compliance event tables with 7-year retention (HOT=24m, WARM=24-60m, COLD=60-84m)',
        'EVENTS',
        '[
            {"policy_name": "{TABLE}_COMPRESS_90D", "age_days": 90, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_WARM_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "pctfree": 5, "priority": 200},
            {"policy_name": "{TABLE}_TIER_COLD_60M", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 300},
            {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 400},
            {"policy_name": "{TABLE}_PURGE_84M", "age_months": 84, "action": "DROP", "priority": 900}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'STAGING_7DAY' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'STAGING_7DAY',
        'Staging tables with 7-day retention',
        'STAGING',
        '[
            {"policy_name": "{TABLE}_PURGE_7D", "age_days": 7, "action": "DROP", "priority": 900}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'STAGING_CDC' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'STAGING_CDC',
        'CDC staging tables with 30-day retention and compression',
        'STAGING',
        '[
            {"policy_name": "{TABLE}_COMPRESS_3D", "age_days": 3, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_PURGE_30D", "age_days": 30, "action": "DROP", "priority": 900}
        ]'
    );

MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'STAGING_ERROR_QUARANTINE' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'STAGING_ERROR_QUARANTINE',
        'Error/quarantine tables with 1-year retention',
        'STAGING',
        '[
            {"policy_name": "{TABLE}_COMPRESS_30D", "age_days": 30, "action": "COMPRESS", "compression": "QUERY LOW", "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_6M", "age_months": 6, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
            {"policy_name": "{TABLE}_PURGE_12M", "age_months": 12, "action": "DROP", "priority": 900}
        ]'
    );

-- Historical/Snapshot Tables - Monthly retention
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'HIST_MONTHLY' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'HIST_MONTHLY',
        'Historical tables with monthly snapshots - 3 year retention (HOT=24m, COLD=24-36m)',
        'HIST',
        '[
            {"policy_name": "{TABLE}_COMPRESS_3M", "age_months": 3, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 200},
            {"policy_name": "{TABLE}_READONLY_24M", "age_months": 24, "action": "READ_ONLY", "priority": 300},
            {"policy_name": "{TABLE}_PURGE_36M", "age_months": 36, "action": "DROP", "priority": 900}
        ]'
    );

-- Historical/Snapshot Tables - Yearly snapshots
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'HIST_YEARLY' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'HIST_YEARLY',
        'Historical tables with yearly snapshots - 7 year retention (HOT=24m, WARM=24-60m, COLD=60-84m)',
        'HIST',
        '[
            {"policy_name": "{TABLE}_COMPRESS_12M", "age_months": 12, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_WARM_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "pctfree": 5, "priority": 200},
            {"policy_name": "{TABLE}_TIER_COLD_60M", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 300},
            {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 400},
            {"policy_name": "{TABLE}_PURGE_84M", "age_months": 84, "action": "DROP", "priority": 900}
        ]'
    );

-- Historical/Snapshot Tables - Compliance (permanent retention)
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'HIST_COMPLIANCE' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'HIST_COMPLIANCE',
        'Historical tables for compliance - permanent retention with compression (HOT=24m, COLD=>24m)',
        'HIST',
        '[
            {"policy_name": "{TABLE}_COMPRESS_6M", "age_months": 6, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
            {"policy_name": "{TABLE}_TIER_COLD_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 200},
            {"policy_name": "{TABLE}_READONLY_24M", "age_months": 24, "action": "READ_ONLY", "priority": 300}
        ]'
    );

-- =============================================================================
-- SECTION 1B: TIERED ILM TEMPLATES (ILM-Aware Initial Partitioning)
-- =============================================================================
-- These templates include tier_config for age-stratified partition creation
-- during migration. Creates HOT/WARM/COLD partitions with different intervals,
-- tablespaces, and compression - minimizing post-migration ILM work.
--
-- IMPORTANT: tier_config vs policies - TWO COMPONENTS WORKING TOGETHER:
--
--   tier_config: ONE-TIME partition creation at migration time
--     - Analyzes existing data age distribution
--     - Creates COLD/WARM/HOT partitions with appropriate intervals
--     - Places historical data in correct tier immediately
--     - Result: Zero post-migration moves for existing data
--
--   policies: ONGOING lifecycle management post-migration
--     - Applied by ILM scheduler to ALL partitions (old and new)
--     - Manages future partitions as they age
--     - Ensures uniform lifecycle treatment
--     - Result: Automated tier transitions for new partitions
--
--   Age thresholds MUST ALIGN between tier_config and policies!
--   Example: tier_config.warm.age_months=36 should match a MOVE policy at 36m
--            This ensures consistent treatment: historical data placed in WARM
--            at migration, future data moved to WARM at 36 months.
--
-- See docs/planning/ILM_AWARE_PARTITIONING_PLAN.md for detailed explanation.
-- =============================================================================

-- Tiered Fact Table - Standard Retention
-- NOTE: Age thresholds at 24m, 60m are aligned between tier_config and policies
--       tier_config places existing data, policies manage future partitions
--       HOT=2y (24 months), WARM=2-5y (24-60 months), COLD=>5y (>60 months)
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'FACT_TABLE_STANDARD_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'FACT_TABLE_STANDARD_TIERED',
        'ILM-aware partitioning: HOT=2y monthly/TBS_HOT/no compression/PCTFREE 10, WARM=2-5y yearly/TBS_WARM/BASIC/PCTFREE 5, COLD=>5y yearly/TBS_COLD/OLTP/PCTFREE 0',
        'FACT',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 24,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE",
                    "pctfree": 10
                },
                "warm": {
                    "age_months": 60,
                    "interval": "YEARLY",
                    "tablespace": "TBS_WARM",
                    "compression": "BASIC",
                    "pctfree": 5
                },
                "cold": {
                    "age_months": null,
                    "interval": "YEARLY",
                    "tablespace": "TBS_COLD",
                    "compression": "OLTP",
                    "pctfree": 0
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 24, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "BASIC", "pctfree": 5, "priority": 200, "comment": "Ongoing: move partitions to WARM at 24m (aligns with tier_config.hot.age_months)"},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "OLTP", "pctfree": 0, "priority": 300, "comment": "Ongoing: move partitions to COLD at 60m (aligns with tier_config.warm.age_months)"}
            ]
        }'
    );

-- Tiered Events Table - 90 Day Retention
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'EVENTS_SHORT_RETENTION_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN MATCHED THEN
    UPDATE SET
        description = 'ILM-aware partitioning for events: HOT=7d daily/TBS_HOT/PCTFREE 10, WARM=30d daily/TBS_WARM/QUERY HIGH/PCTFREE 5, COLD=90d monthly/TBS_COLD/ARCHIVE HIGH/PCTFREE 0',
        table_type = 'EVENTS',
        policies_json = '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_days": 7,
                    "interval": "DAILY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE",
                    "pctfree": 10
                },
                "warm": {
                    "age_days": 30,
                    "interval": "DAILY",
                    "tablespace": "TBS_WARM",
                    "compression": "QUERY HIGH",
                    "pctfree": 5
                },
                "cold": {
                    "age_days": 90,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_COLD",
                    "compression": "ARCHIVE HIGH",
                    "pctfree": 0
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_days": 7, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "pctfree": 5, "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_days": 30, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "pctfree": 0, "priority": 300},
                {"policy_name": "{TABLE}_PURGE", "age_days": 90, "action": "DROP", "priority": 900}
            ]
        }'
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'EVENTS_SHORT_RETENTION_TIERED',
        'ILM-aware partitioning for events: HOT=7d daily/TBS_HOT/PCTFREE 10, WARM=30d daily/TBS_WARM/QUERY HIGH/PCTFREE 5, COLD=90d monthly/TBS_COLD/ARCHIVE HIGH/PCTFREE 0',
        'EVENTS',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_days": 7,
                    "interval": "DAILY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE",
                    "pctfree": 10
                },
                "warm": {
                    "age_days": 30,
                    "interval": "DAILY",
                    "tablespace": "TBS_WARM",
                    "compression": "QUERY HIGH",
                    "pctfree": 5
                },
                "cold": {
                    "age_days": 90,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_COLD",
                    "compression": "ARCHIVE HIGH",
                    "pctfree": 0
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_days": 7, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "pctfree": 5, "priority": 200},
                {"policy_name": "{TABLE}_TIER_COLD", "age_days": 30, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "pctfree": 0, "priority": 300},
                {"policy_name": "{TABLE}_PURGE", "age_days": 90, "action": "DROP", "priority": 900}
            ]
        }'
    );

-- Tiered SCD2 Table - Permanent Retention
-- NOTE: Age thresholds at 24m, 60m are aligned between tier_config and policies
--       HOT=2y (24 months), WARM=2-5y (24-60 months), COLD=>5y (>60 months, permanent)
MERGE INTO cmr.dwh_migration_ilm_templates t
USING (SELECT 'SCD2_VALID_FROM_TO_TIERED' AS template_name FROM DUAL) s
ON (t.template_name = s.template_name)
WHEN NOT MATCHED THEN
    INSERT (template_name, description, table_type, policies_json)
    VALUES (
        'SCD2_VALID_FROM_TO_TIERED',
        'ILM-aware partitioning for SCD2: HOT=2y monthly/TBS_HOT/PCTFREE 10, WARM=2-5y yearly/TBS_WARM/QUERY HIGH/PCTFREE 5, COLD=>5y yearly/TBS_COLD/ARCHIVE HIGH/PCTFREE 0 (permanent)',
        'SCD2',
        '{
            "tier_config": {
                "enabled": true,
                "hot": {
                    "age_months": 24,
                    "interval": "MONTHLY",
                    "tablespace": "TBS_HOT",
                    "compression": "NONE",
                    "pctfree": 10
                },
                "warm": {
                    "age_months": 60,
                    "interval": "YEARLY",
                    "tablespace": "TBS_WARM",
                    "compression": "QUERY HIGH",
                    "pctfree": 5
                },
                "cold": {
                    "age_months": null,
                    "interval": "YEARLY",
                    "tablespace": "TBS_COLD",
                    "compression": "ARCHIVE HIGH",
                    "pctfree": 0
                }
            },
            "policies": [
                {"policy_name": "{TABLE}_TIER_WARM", "age_months": 24, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "pctfree": 5, "priority": 200, "comment": "Ongoing: move partitions to WARM at 24m"},
                {"policy_name": "{TABLE}_TIER_COLD", "age_months": 60, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "pctfree": 0, "priority": 300, "comment": "Ongoing: move partitions to COLD at 60m"},
                {"policy_name": "{TABLE}_READONLY", "age_months": 60, "action": "READ_ONLY", "priority": 400, "comment": "Ongoing: make partitions read-only at 60m (permanent retention)"}
            ]
        }'
    );

COMMIT;


-- =============================================================================
-- SECTION 2: HELPER VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Migration Dashboard
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_migration_dashboard AS
SELECT
    p.project_id,
    p.project_name,
    p.status AS project_status,
    COUNT(t.task_id) AS total_tasks,
    SUM(CASE WHEN t.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_tasks,
    SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_tasks,
    SUM(CASE WHEN t.status IN ('PENDING', 'ANALYZING', 'ANALYZED', 'READY') THEN 1 ELSE 0 END) AS pending_tasks,
    ROUND(SUM(t.source_size_mb), 2) AS total_source_size_mb,
    ROUND(SUM(t.target_size_mb), 2) AS total_target_size_mb,
    ROUND(SUM(t.space_saved_mb), 2) AS total_space_saved_mb,
    p.created_date,
    p.started_date,
    p.completed_date
FROM cmr.dwh_migration_projects p
LEFT JOIN cmr.dwh_migration_tasks t ON t.project_id = p.project_id
GROUP BY
    p.project_id, p.project_name, p.status,
    p.created_date, p.started_date, p.completed_date
ORDER BY p.created_date DESC;


-- -----------------------------------------------------------------------------
-- Task Status View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_migration_task_status AS
SELECT
    t.task_id,
    t.task_name,
    p.project_name,
    t.source_owner || '.' || t.source_table AS source_table,
    t.partition_type,
    t.migration_method,
    t.status,
    t.validation_status,
    a.recommended_strategy,
    a.complexity_score,
    a.estimated_downtime_minutes,
    a.existing_compression,
    a.existing_compress_for,
    a.storage_type,
    a.partition_compression_mixed,
    t.source_rows,
    ROUND(t.source_size_mb, 2) AS source_size_mb,
    ROUND(t.target_size_mb, 2) AS target_size_mb,
    ROUND(t.space_saved_mb, 2) AS space_saved_mb,
    CASE
        WHEN t.source_size_mb > 0 AND t.target_size_mb > 0 THEN
            ROUND((1 - t.target_size_mb / t.source_size_mb) * 100, 1)
        ELSE NULL
    END AS compression_pct,
    t.execution_start,
    t.execution_end,
    t.duration_seconds,
    t.can_rollback
FROM cmr.dwh_migration_tasks t
LEFT JOIN cmr.dwh_migration_projects p ON p.project_id = t.project_id
LEFT JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
ORDER BY t.created_date DESC;


-- -----------------------------------------------------------------------------
-- Candidate Tables for Migration
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_migration_candidates AS
SELECT
    t.owner,
    t.table_name,
    t.num_rows,
    ROUND(s.bytes/1024/1024, 2) AS size_mb,
    t.partitioned,
    t.compression,
    COUNT(DISTINCT i.index_name) AS num_indexes,
    COUNT(DISTINCT c.constraint_name) AS num_constraints,
    CASE
        WHEN t.num_rows > 10000000 THEN 'HIGH PRIORITY'
        WHEN t.num_rows > 1000000 THEN 'MEDIUM PRIORITY'
        WHEN t.num_rows > 100000 THEN 'LOW PRIORITY'
        ELSE 'NOT RECOMMENDED'
    END AS migration_priority,
    CASE
        WHEN t.num_rows > 10000000 THEN 'Large table - significant benefits expected'
        WHEN t.num_rows > 1000000 THEN 'Medium table - moderate benefits expected'
        WHEN t.num_rows > 100000 THEN 'Small table - minor benefits'
        ELSE 'Very small table - overhead may outweigh benefits'
    END AS recommendation_reason
FROM dba_tables t
LEFT JOIN (
    SELECT owner, segment_name, SUM(bytes) AS bytes
    FROM dba_segments
    GROUP BY owner, segment_name
) s ON s.owner = t.owner AND s.segment_name = t.table_name
LEFT JOIN dba_indexes i ON i.table_owner = t.owner AND i.table_name = t.table_name
LEFT JOIN dba_constraints c ON c.owner = t.owner AND c.table_name = t.table_name
WHERE t.owner IN (
    SELECT username
    FROM dba_users
    WHERE oracle_maintained = 'N'
    AND account_status = 'OPEN'
    AND default_tablespace NOT LIKE '%USERS%'
)
AND t.partitioned = 'NO'
AND t.temporary = 'N'
AND t.num_rows > 0
GROUP BY
    t.owner, t.table_name, t.num_rows, s.bytes,
    t.partitioned, t.compression
ORDER BY t.num_rows DESC;


-- -----------------------------------------------------------------------------
-- Date Column Analysis View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW cmr.v_dwh_date_column_analysis AS
SELECT
    t.task_id,
    t.task_name,
    t.source_owner,
    t.source_table,
    a.date_column_name AS primary_date_column,
    a.date_column_type,
    a.date_format_detected,
    a.requires_conversion,
    a.all_date_columns_analysis,
    -- Extract JSON array length to show count of date columns
    JSON_VALUE(a.all_date_columns_analysis, '$.size()') AS total_date_columns,
    a.recommended_strategy,
    a.recommendation_reason,
    a.analysis_date
FROM cmr.dwh_migration_tasks t
JOIN cmr.dwh_migration_analysis a ON a.task_id = t.task_id
WHERE a.all_date_columns_analysis IS NOT NULL
ORDER BY a.analysis_date DESC;

COMMENT ON TABLE cmr.v_dwh_date_column_analysis IS 'Comprehensive date column analysis for migration tasks';


-- =============================================================================
-- SECTION 3: CONFIGURATION AND UTILITIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create configuration table if it doesn't exist (for standalone migration installation)
-- -----------------------------------------------------------------------------

DECLARE
    v_table_exists NUMBER;
BEGIN
    -- Check if config table exists
    SELECT COUNT(*) INTO v_table_exists
    FROM dba_tables
    WHERE owner = 'CMR'
    AND table_name = 'DWH_ILM_CONFIG';

    -- Create table if it doesn't exist
    IF v_table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE cmr.dwh_ilm_config (
                config_key          VARCHAR2(100) PRIMARY KEY,
                config_value        VARCHAR2(4000),
                description         VARCHAR2(500),
                modified_by         VARCHAR2(50),
                modified_date       TIMESTAMP DEFAULT SYSTIMESTAMP
            )';

        DBMS_OUTPUT.PUT_LINE('Created cmr.dwh_ilm_config table for standalone migration framework');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Config table cmr.dwh_ilm_config already exists (ILM framework installed)');
    END IF;
END;
/

-- -----------------------------------------------------------------------------
-- Add migration configuration to ILM config (using MERGE for rerunnable script)
-- NOTE: These configurations are also defined in custom_ilm_setup.sql
--       This section ensures they exist even if custom_ilm_setup.sql is not run
-- -----------------------------------------------------------------------------

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_BACKUP_ENABLED' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('MIGRATION_BACKUP_ENABLED', 'Y', 'Create backup tables before migration');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_VALIDATE_ENABLED' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('MIGRATION_VALIDATE_ENABLED', 'Y', 'Validate data after migration');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_AUTO_ILM_ENABLED' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('MIGRATION_AUTO_ILM_ENABLED', 'Y', 'Automatically create ILM policies after migration');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_PARALLEL_DEGREE' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('MIGRATION_PARALLEL_DEGREE', '4', 'Default parallel degree for migration operations');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'STORAGE_INITIAL_EXTENT' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('STORAGE_INITIAL_EXTENT', '81920', 'Initial extent size in bytes for tables and partitions (80KB)');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'STORAGE_NEXT_EXTENT' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('STORAGE_NEXT_EXTENT', '1048576', 'Next extent size in bytes for tables and partitions (1MB)');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'STORAGE_EXTENT_MODE' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('STORAGE_EXTENT_MODE', 'AUTO', 'Storage extent mode: AUTO (use analysis recommendations), FORCED (always use fixed config), NONE (never use STORAGE clause)');

-- PCTFREE Configuration (block-level space management)
MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'PCTFREE_HOT_TIER' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('PCTFREE_HOT_TIER', '10', 'PCTFREE for HOT tier partitions (0-99). Reserve 10% for row updates in active data.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'PCTFREE_WARM_TIER' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('PCTFREE_WARM_TIER', '5', 'PCTFREE for WARM tier partitions (0-99). Reserve 5% for minimal updates in aging data.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'PCTFREE_COLD_TIER' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('PCTFREE_COLD_TIER', '0', 'PCTFREE for COLD tier partitions (0-99). 0% for read-only archive data (maximum storage efficiency).');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'PCTFREE_DEFAULT' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('PCTFREE_DEFAULT', '10', 'Default PCTFREE for non-tiered tables (0-99). Oracle default is 10.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'NULL_HANDLING_STRATEGY' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('NULL_HANDLING_STRATEGY', 'AUTO', 'NULL handling strategy: AUTO (analyze and recommend), UPDATE (update NULLs before migration), ALLOW_NULLS (allow in first/default partition)');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'NULL_DEFAULT_DATE' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('NULL_DEFAULT_DATE', '5999-01-01', 'Default date value for NULL date columns (YYYY-MM-DD format). Used with UPDATE strategy. Use pck_dwh_constants.c_maxvalue_date.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'NULL_DEFAULT_NUMBER' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('NULL_DEFAULT_NUMBER', '-1', 'Default number value for NULL number columns. Used with UPDATE strategy.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'NULL_DEFAULT_VARCHAR' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('NULL_DEFAULT_VARCHAR', 'nav', 'Default varchar value for NULL varchar columns. Used with UPDATE strategy.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'INITIAL_PARTITION_BUFFER_MONTHS' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('INITIAL_PARTITION_BUFFER_MONTHS', '1', 'Number of months before MIN date to set initial partition boundary. Prevents ORA-14300 when data exists before the boundary. Recommended: 1-3 months for safety buffer.');

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'FALLBACK_INITIAL_PARTITION_DATE' AS config_key FROM DUAL) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES ('FALLBACK_INITIAL_PARTITION_DATE', '1900-01-01', 'Fallback date for initial partition boundary when no data is found in table. Format: YYYY-MM-DD. This safe historical date ensures all realistic data will be AFTER the initial partition.');

COMMIT;


-- =============================================================================
-- SECTION 4: VERIFICATION
-- =============================================================================

SELECT 'Migration Framework Setup Complete!' AS status FROM DUAL;

SELECT 'Migration Tables Created: ' || COUNT(*) AS info
FROM user_tables
WHERE table_name LIKE 'MIGRATION_%';

SELECT 'ILM Templates Loaded: ' || COUNT(*) AS info
FROM cmr.dwh_migration_ilm_templates;

PROMPT
PROMPT ========================================
PROMPT Migration Framework Installed
PROMPT ========================================
PROMPT
PROMPT Next Steps:
PROMPT 1. Install analysis package: @scripts/table_dwh_migration_analysis.sql
PROMPT 2. Install execution package: @scripts/table_migration_execution.sql
PROMPT 3. View candidates: SELECT * FROM cmr.v_dwh_migration_candidates;
PROMPT
PROMPT ========================================
