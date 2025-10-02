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

CREATE TABLE dwh_migration_projects (
    project_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_name        VARCHAR2(100) NOT NULL UNIQUE,
    description         VARCHAR2(500),
    status              VARCHAR2(30) DEFAULT 'PLANNING',  -- PLANNING, ANALYSIS, READY, IN_PROGRESS, COMPLETED, FAILED, ROLLED_BACK
    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
    started_date        TIMESTAMP,
    completed_date      TIMESTAMP,

    CONSTRAINT chk_proj_status CHECK (status IN ('PLANNING', 'ANALYSIS', 'READY', 'IN_PROGRESS', 'COMPLETED', 'FAILED', 'ROLLED_BACK'))
);

COMMENT ON TABLE dwh_migration_projects IS 'Migration project tracking';


-- -----------------------------------------------------------------------------
-- Migration Tasks - Individual table migrations
-- -----------------------------------------------------------------------------

CREATE TABLE dwh_migration_tasks (
    task_id             NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    project_id          NUMBER,
    task_name           VARCHAR2(100) NOT NULL,

    -- Source table
    source_owner        VARCHAR2(30) DEFAULT USER,
    source_table        VARCHAR2(128) NOT NULL,

    -- Target partitioning strategy
    partition_type      VARCHAR2(30),          -- RANGE, LIST, HASH, RANGE-HASH, RANGE-LIST, etc.
    partition_key       VARCHAR2(500),         -- Column(s) for partitioning
    subpartition_type   VARCHAR2(30),
    subpartition_key    VARCHAR2(500),
    interval_clause     VARCHAR2(200),         -- For interval partitioning

    -- Migration strategy
    migration_method    VARCHAR2(30) DEFAULT 'ONLINE',  -- ONLINE, OFFLINE, CTAS, EXCHANGE
    use_compression     CHAR(1) DEFAULT 'Y',
    compression_type    VARCHAR2(50) DEFAULT 'QUERY HIGH',
    target_tablespace   VARCHAR2(30),
    parallel_degree     NUMBER DEFAULT 4,

    -- ILM integration
    apply_ilm_policies  CHAR(1) DEFAULT 'Y',
    ilm_policy_template VARCHAR2(100),         -- Reference to policy template

    -- Status tracking
    status              VARCHAR2(30) DEFAULT 'PENDING',
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
    can_rollback        CHAR(1) DEFAULT 'N',

    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT fk_mig_task_project FOREIGN KEY (project_id) REFERENCES dwh_migration_projects(project_id),
    CONSTRAINT chk_task_status CHECK (status IN ('PENDING', 'ANALYZING', 'ANALYZED', 'READY', 'RUNNING', 'COMPLETED', 'FAILED', 'ROLLED_BACK')),
    CONSTRAINT chk_mig_method CHECK (migration_method IN ('ONLINE', 'OFFLINE', 'CTAS', 'EXCHANGE'))
);

CREATE INDEX idx_mig_task_project ON dwh_migration_tasks(project_id);
CREATE INDEX idx_mig_task_source ON dwh_migration_tasks(source_owner, source_table);
CREATE INDEX idx_mig_task_status ON dwh_migration_tasks(status);

COMMENT ON TABLE dwh_migration_tasks IS 'Individual table migration tasks';


-- -----------------------------------------------------------------------------
-- Migration Analysis Results - Store analysis recommendations
-- -----------------------------------------------------------------------------

CREATE TABLE dwh_migration_analysis (
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

    -- Data distribution analysis
    candidate_columns   CLOB,                  -- JSON or list of candidate partition keys
    recommended_strategy VARCHAR2(100),
    recommendation_reason VARCHAR2(1000),

    -- Date column conversion (for non-standard date formats)
    date_column_name    VARCHAR2(128),         -- Original date column name
    date_column_type    VARCHAR2(30),          -- Original data type (NUMBER, VARCHAR2, etc.)
    date_format_detected VARCHAR2(50),         -- Detected format (YYYYMMDD, UNIX_TIMESTAMP, etc.)
    date_conversion_expr VARCHAR2(500),        -- Conversion expression for migration
    requires_conversion CHAR(1) DEFAULT 'N',   -- Y if date conversion needed

    -- Partition estimates
    estimated_partitions NUMBER,
    avg_partition_size_mb NUMBER,
    estimated_compression_ratio NUMBER,
    estimated_space_savings_mb NUMBER,

    -- Complexity assessment
    complexity_score    NUMBER,                -- 1-10, higher = more complex
    complexity_factors  VARCHAR2(2000),
    estimated_downtime_minutes NUMBER,

    -- Dependencies
    dependent_objects   CLOB,                  -- JSON list of dependent objects
    blocking_issues     CLOB,                  -- Issues that must be resolved
    warnings            CLOB,                  -- Non-blocking warnings

    analysis_date       TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT fk_mig_analysis_task FOREIGN KEY (task_id) REFERENCES dwh_migration_tasks(task_id)
);

CREATE INDEX idx_mig_analysis_task ON dwh_migration_analysis(task_id);

COMMENT ON TABLE dwh_migration_analysis IS 'Analysis results and recommendations for migrations';


-- -----------------------------------------------------------------------------
-- Migration Execution Log - Detailed step-by-step log
-- -----------------------------------------------------------------------------

CREATE TABLE dwh_migration_execution_log (
    log_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id             NUMBER NOT NULL,

    step_number         NUMBER,
    step_name           VARCHAR2(200),
    step_type           VARCHAR2(50),          -- ANALYZE, VALIDATE, CREATE, COPY, INDEX, CONSTRAINT, etc.
    sql_statement       CLOB,

    start_time          TIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    NUMBER,
    status              VARCHAR2(20),          -- RUNNING, SUCCESS, FAILED, SKIPPED

    rows_processed      NUMBER,
    error_code          NUMBER,
    error_message       VARCHAR2(4000),

    CONSTRAINT fk_mig_log_task FOREIGN KEY (task_id) REFERENCES dwh_migration_tasks(task_id)
);

CREATE INDEX idx_mig_log_task ON dwh_migration_execution_log(task_id, step_number);

COMMENT ON TABLE dwh_migration_execution_log IS 'Detailed execution log for migrations';


-- -----------------------------------------------------------------------------
-- ILM Policy Templates for Migrations
-- -----------------------------------------------------------------------------

CREATE TABLE dwh_migration_ilm_templates (
    template_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    template_name       VARCHAR2(100) NOT NULL UNIQUE,
    description         VARCHAR2(500),
    table_type          VARCHAR2(50),          -- FACT, DIMENSION, STAGING, etc.

    -- Default policies to create
    policies_json       CLOB,                  -- JSON array of policy definitions

    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP
);

COMMENT ON TABLE dwh_migration_ilm_templates IS 'ILM policy templates for newly migrated tables';

-- Insert default templates
INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'FACT_TABLE_STANDARD',
    'Standard ILM policies for fact tables: compress at 90d, tier at 12m, archive at 36m',
    'FACT',
    '[
        {"policy_name": "{TABLE}_COMPRESS_90D", "age_days": 90, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_WARM_12M", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "QUERY HIGH", "priority": 200},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 300},
        {"policy_name": "{TABLE}_READONLY_36M", "age_months": 36, "action": "READ_ONLY", "priority": 301}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'DIMENSION_LARGE',
    'ILM policies for large dimension tables',
    'DIMENSION',
    '[
        {"policy_name": "{TABLE}_COMPRESS_180D", "age_days": 180, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'STAGING_MINIMAL',
    'Minimal retention for staging tables',
    'STAGING',
    '[
        {"policy_name": "{TABLE}_PURGE_30D", "age_days": 30, "action": "DROP", "priority": 900}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'SCD2_EFFECTIVE_DATE',
    'ILM policies for SCD2 tables with effective_date - compress old versions, retain history',
    'SCD2',
    '[
        {"policy_name": "{TABLE}_COMPRESS_365D", "age_days": 365, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 300}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'SCD2_VALID_FROM_TO',
    'ILM policies for SCD2 tables with valid_from_dttm/valid_to_dttm - compress old versions',
    'SCD2',
    '[
        {"policy_name": "{TABLE}_COMPRESS_365D", "age_days": 365, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 300}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
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

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'EVENTS_COMPLIANCE',
    'ILM policies for audit/compliance event tables with 7-year retention',
    'EVENTS',
    '[
        {"policy_name": "{TABLE}_COMPRESS_90D", "age_days": 90, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_WARM_12M", "age_months": 12, "action": "MOVE", "tablespace": "TBS_WARM", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 300},
        {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 400},
        {"policy_name": "{TABLE}_PURGE_84M", "age_months": 84, "action": "DROP", "priority": 900}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'STAGING_7DAY',
    'Staging tables with 7-day retention',
    'STAGING',
    '[
        {"policy_name": "{TABLE}_PURGE_7D", "age_days": 7, "action": "DROP", "priority": 900}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
VALUES (
    'STAGING_CDC',
    'CDC staging tables with 30-day retention and compression',
    'STAGING',
    '[
        {"policy_name": "{TABLE}_COMPRESS_3D", "age_days": 3, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_PURGE_30D", "age_days": 30, "action": "DROP", "priority": 900}
    ]'
);

INSERT INTO dwh_migration_ilm_templates (template_name, description, table_type, policies_json)
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
INSERT INTO dwh_migration_ilm_templates VALUES (
    'HIST_MONTHLY',
    'Historical tables with monthly snapshots - 3 year retention',
    'HIST',
    '[
        {"policy_name": "{TABLE}_COMPRESS_3M", "age_months": 3, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_COLD_12M", "age_months": 12, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_READONLY_24M", "age_months": 24, "action": "READ_ONLY", "priority": 300},
        {"policy_name": "{TABLE}_PURGE_36M", "age_months": 36, "action": "DROP", "priority": 900}
    ]'
);

-- Historical/Snapshot Tables - Yearly snapshots
INSERT INTO dwh_migration_ilm_templates VALUES (
    'HIST_YEARLY',
    'Historical tables with yearly snapshots - 7 year retention',
    'HIST',
    '[
        {"policy_name": "{TABLE}_COMPRESS_12M", "age_months": 12, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_COLD_36M", "age_months": 36, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_READONLY_60M", "age_months": 60, "action": "READ_ONLY", "priority": 300},
        {"policy_name": "{TABLE}_PURGE_84M", "age_months": 84, "action": "DROP", "priority": 900}
    ]'
);

-- Historical/Snapshot Tables - Compliance (permanent retention)
INSERT INTO dwh_migration_ilm_templates VALUES (
    'HIST_COMPLIANCE',
    'Historical tables for compliance - permanent retention with compression',
    'HIST',
    '[
        {"policy_name": "{TABLE}_COMPRESS_6M", "age_months": 6, "action": "COMPRESS", "compression": "QUERY HIGH", "priority": 100},
        {"policy_name": "{TABLE}_TIER_COLD_24M", "age_months": 24, "action": "MOVE", "tablespace": "TBS_COLD", "compression": "ARCHIVE HIGH", "priority": 200},
        {"policy_name": "{TABLE}_READONLY_36M", "age_months": 36, "action": "READ_ONLY", "priority": 300}
    ]'
);

COMMIT;


-- =============================================================================
-- SECTION 2: HELPER VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Migration Dashboard
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_migration_dashboard AS
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
FROM dwh_migration_projects p
LEFT JOIN dwh_migration_tasks t ON t.project_id = p.project_id
GROUP BY
    p.project_id, p.project_name, p.status,
    p.created_date, p.started_date, p.completed_date
ORDER BY p.created_date DESC;


-- -----------------------------------------------------------------------------
-- Task Status View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_migration_task_status AS
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
FROM dwh_migration_tasks t
LEFT JOIN dwh_migration_projects p ON p.project_id = t.project_id
LEFT JOIN dwh_migration_analysis a ON a.task_id = t.task_id
ORDER BY t.created_date DESC;


-- -----------------------------------------------------------------------------
-- Candidate Tables for Migration
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_migration_candidates AS
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
LEFT JOIN all_indexes i ON i.table_owner = t.owner AND i.table_name = t.table_name
LEFT JOIN all_constraints c ON c.owner = t.owner AND c.table_name = t.table_name
WHERE t.owner = USER
AND t.partitioned = 'NO'
AND t.temporary = 'N'
AND t.num_rows > 0
GROUP BY
    t.owner, t.table_name, t.num_rows, s.bytes,
    t.partitioned, t.compression
ORDER BY t.num_rows DESC;


-- =============================================================================
-- SECTION 3: CONFIGURATION AND UTILITIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Add migration configuration to ILM config
-- -----------------------------------------------------------------------------

INSERT INTO ilm_config (config_key, config_value, description)
VALUES ('MIGRATION_BACKUP_ENABLED', 'Y', 'Create backup tables before migration');

INSERT INTO ilm_config (config_key, config_value, description)
VALUES ('MIGRATION_VALIDATE_ENABLED', 'Y', 'Validate data after migration');

INSERT INTO ilm_config (config_key, config_value, description)
VALUES ('MIGRATION_AUTO_ILM_ENABLED', 'Y', 'Automatically create ILM policies after migration');

INSERT INTO ilm_config (config_key, config_value, description)
VALUES ('MIGRATION_PARALLEL_DEGREE', '4', 'Default parallel degree for migration operations');

COMMIT;


-- =============================================================================
-- SECTION 4: VERIFICATION
-- =============================================================================

SELECT 'Migration Framework Setup Complete!' AS status FROM DUAL;

SELECT 'Migration Tables Created: ' || COUNT(*) AS info
FROM user_tables
WHERE table_name LIKE 'MIGRATION_%';

SELECT 'ILM Templates Loaded: ' || COUNT(*) AS info
FROM dwh_migration_ilm_templates;

PROMPT
PROMPT ========================================
PROMPT Migration Framework Installed
PROMPT ========================================
PROMPT
PROMPT Next Steps:
PROMPT 1. Install analysis package: @scripts/table_dwh_migration_analysis.sql
PROMPT 2. Install execution package: @scripts/table_migration_execution.sql
PROMPT 3. View candidates: SELECT * FROM dwh_v_migration_candidates;
PROMPT
PROMPT ========================================
