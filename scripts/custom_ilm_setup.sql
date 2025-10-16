-- =============================================================================
-- Custom ILM Framework - Setup and Metadata Tables
-- PL/SQL-based Information Lifecycle Management without ADO
-- =============================================================================

-- =============================================================================
-- SECTION 1: METADATA TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ILM Policy Definitions
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_policies (
    policy_id           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_name         VARCHAR2(100) NOT NULL UNIQUE,
    table_owner         VARCHAR2(30) DEFAULT USER,
    table_name          VARCHAR2(128) NOT NULL,
    policy_type         VARCHAR2(30) NOT NULL,  -- COMPRESSION, TIERING, ARCHIVAL, PURGE
    enabled             CHAR(1) DEFAULT 'Y' CHECK (enabled IN ('Y','N')),
    priority            NUMBER(3) DEFAULT 100,

    -- Condition criteria
    age_days            NUMBER,                 -- Age threshold in days
    age_months          NUMBER,                 -- Age threshold in months
    access_pattern      VARCHAR2(30),           -- HOT, WARM, COLD based on last access
    size_threshold_mb   NUMBER,                 -- Minimum partition size
    custom_condition    VARCHAR2(4000),         -- Custom SQL WHERE clause

    -- Action parameters
    action_type         VARCHAR2(30) NOT NULL,  -- COMPRESS, MOVE, READ_ONLY, DROP, CUSTOM
    target_tablespace   VARCHAR2(30),
    compression_type    VARCHAR2(50),           -- QUERY LOW/HIGH, ARCHIVE LOW/HIGH
    custom_action       VARCHAR2(4000),         -- Custom PL/SQL block

    -- Execution settings
    execution_mode      VARCHAR2(20) DEFAULT 'OFFLINE',  -- ONLINE, OFFLINE
    parallel_degree     NUMBER DEFAULT 4,
    rebuild_indexes     CHAR(1) DEFAULT 'Y',
    gather_stats        CHAR(1) DEFAULT 'Y',

    -- Audit fields
    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
    modified_by         VARCHAR2(50),
    modified_date       TIMESTAMP,

    CONSTRAINT chk_policy_type CHECK (policy_type IN ('COMPRESSION', 'TIERING', 'ARCHIVAL', 'PURGE', 'CUSTOM')),
    CONSTRAINT chk_action_type CHECK (action_type IN ('COMPRESS', 'MOVE', 'READ_ONLY', 'DROP', 'TRUNCATE', 'CUSTOM'))
);

CREATE INDEX idx_dwh_ilm_policies_table ON cmr.dwh_ilm_policies(table_owner, table_name);
CREATE INDEX idx_dwh_ilm_policies_enabled ON cmr.dwh_ilm_policies(enabled, priority);

COMMENT ON TABLE cmr.dwh_ilm_policies IS 'Custom ILM policy definitions';


-- -----------------------------------------------------------------------------
-- ILM Execution History
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_execution_log (
    execution_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_id           NUMBER NOT NULL,
    policy_name         VARCHAR2(100),
    table_owner         VARCHAR2(30),
    table_name          VARCHAR2(128),
    partition_name      VARCHAR2(128),

    -- Execution details
    execution_start     TIMESTAMP,
    execution_end       TIMESTAMP,
    duration_seconds    NUMBER,
    status              VARCHAR2(20),           -- PENDING, RUNNING, SUCCESS, FAILED, SKIPPED

    -- Action performed
    action_type         VARCHAR2(30),
    action_sql          CLOB,

    -- Results
    rows_affected       NUMBER,
    size_before_mb      NUMBER,
    size_after_mb       NUMBER,
    space_saved_mb      NUMBER,
    compression_ratio   NUMBER,

    -- Error handling
    error_code          NUMBER,
    error_message       VARCHAR2(4000),

    -- Audit
    executed_by         VARCHAR2(50) DEFAULT USER,
    execution_mode      VARCHAR2(20),

    CONSTRAINT fk_ilm_exec_policy FOREIGN KEY (policy_id) REFERENCES cmr.dwh_ilm_policies(policy_id)
);

CREATE INDEX idx_ilm_exec_policy ON cmr.dwh_ilm_execution_log(policy_id);
CREATE INDEX idx_ilm_exec_table ON cmr.dwh_ilm_execution_log(table_owner, table_name, partition_name);
CREATE INDEX idx_ilm_exec_date ON cmr.dwh_ilm_execution_log(execution_start);
CREATE INDEX idx_ilm_exec_status ON cmr.dwh_ilm_execution_log(status);

COMMENT ON TABLE cmr.dwh_ilm_execution_log IS 'History of ILM policy executions';


-- -----------------------------------------------------------------------------
-- Partition Access Tracking (Custom Heat Map)
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_partition_access (
    table_owner         VARCHAR2(30),
    table_name          VARCHAR2(128),
    partition_name      VARCHAR2(128),

    -- Access statistics
    last_read_time      TIMESTAMP,
    last_write_time     TIMESTAMP,
    read_count          NUMBER DEFAULT 0,
    write_count         NUMBER DEFAULT 0,

    -- Size statistics
    num_rows            NUMBER,
    size_mb             NUMBER,
    compression         VARCHAR2(30),
    tablespace_name     VARCHAR2(30),

    -- Calculated metrics
    days_since_read     NUMBER,
    days_since_write    NUMBER,
    temperature         VARCHAR2(10),           -- HOT, WARM, COLD

    -- Audit
    last_updated        TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_dwh_ilm_partition_access PRIMARY KEY (table_owner, table_name, partition_name)
);

CREATE INDEX idx_ilm_access_temp ON cmr.dwh_ilm_partition_access(temperature);
CREATE INDEX idx_ilm_access_write ON cmr.dwh_ilm_partition_access(days_since_write);

COMMENT ON TABLE cmr.dwh_ilm_partition_access IS 'Custom partition access tracking for ILM';


-- -----------------------------------------------------------------------------
-- ILM Policy Evaluation Queue
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_evaluation_queue (
    queue_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_id           NUMBER NOT NULL,
    table_owner         VARCHAR2(30),
    table_name          VARCHAR2(128),
    partition_name      VARCHAR2(128),

    -- Evaluation results
    evaluation_date     TIMESTAMP DEFAULT SYSTIMESTAMP,
    eligible            CHAR(1),                -- Y/N
    reason              VARCHAR2(500),
    scheduled_date      TIMESTAMP,

    -- Execution tracking
    execution_status    VARCHAR2(20) DEFAULT 'PENDING',  -- PENDING, SCHEDULED, EXECUTED, FAILED
    execution_id        NUMBER,

    CONSTRAINT fk_ilm_queue_policy FOREIGN KEY (policy_id) REFERENCES cmr.dwh_ilm_policies(policy_id),
    CONSTRAINT fk_ilm_queue_exec FOREIGN KEY (execution_id) REFERENCES cmr.dwh_ilm_execution_log(execution_id)
);

CREATE INDEX idx_ilm_queue_status ON cmr.dwh_ilm_evaluation_queue(execution_status);
CREATE INDEX idx_ilm_queue_eligible ON cmr.dwh_ilm_evaluation_queue(eligible, scheduled_date);

COMMENT ON TABLE cmr.dwh_ilm_evaluation_queue IS 'Queue of partitions eligible for ILM actions';


-- -----------------------------------------------------------------------------
-- ILM Configuration
-- -----------------------------------------------------------------------------

CREATE TABLE cmr.dwh_ilm_config (
    config_key          VARCHAR2(100) PRIMARY KEY,
    config_value        VARCHAR2(4000),
    description         VARCHAR2(500),
    modified_by         VARCHAR2(50),
    modified_date       TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Insert default configuration (using MERGE for rerunnable script)
MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ENABLE_AUTO_EXECUTION' AS config_key, 'Y' AS config_value,
              'Enable automatic policy execution via scheduler' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'EXECUTION_WINDOW_START' AS config_key, '22:00' AS config_value,
              'Start time for ILM operations (HH24:MI)' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'EXECUTION_WINDOW_END' AS config_key, '06:00' AS config_value,
              'End time for ILM operations (HH24:MI)' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MAX_CONCURRENT_OPERATIONS' AS config_key, '4' AS config_value,
              'Maximum number of concurrent partition operations' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ACCESS_TRACKING_ENABLED' AS config_key, 'Y' AS config_value,
              'Enable partition access tracking' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'HOT_THRESHOLD_DAYS' AS config_key, '90' AS config_value,
              'Days threshold for HOT classification' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'WARM_THRESHOLD_DAYS' AS config_key, '365' AS config_value,
              'Days threshold for WARM classification' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'COLD_THRESHOLD_DAYS' AS config_key, '1095' AS config_value,
              'Days threshold for COLD classification (3 years)' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'LOG_RETENTION_DAYS' AS config_key, '365' AS config_value,
              'Days to retain execution logs' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

-- Table migration framework configuration
MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_PARALLEL_DEGREE' AS config_key, '4' AS config_value,
              'Default parallel degree for table migration operations' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_BACKUP_ENABLED' AS config_key, 'Y' AS config_value,
              'Create backup table before migration' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'MIGRATION_VALIDATE_ENABLED' AS config_key, 'Y' AS config_value,
              'Validate row counts after migration' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

COMMIT;

COMMENT ON TABLE cmr.dwh_ilm_config IS 'Configuration parameters for custom ILM framework';


-- =============================================================================
-- SECTION 2: HELPER VIEWS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Active Policies View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_active_policies AS
SELECT
    p.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    p.policy_type,
    p.action_type,
    p.age_days,
    p.age_months,
    p.compression_type,
    p.target_tablespace,
    p.priority,
    COUNT(q.queue_id) AS pending_actions,
    MAX(e.execution_end) AS last_execution
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_evaluation_queue q
    ON q.policy_id = p.policy_id
    AND q.execution_status = 'PENDING'
LEFT JOIN cmr.dwh_ilm_execution_log e
    ON e.policy_id = p.policy_id
    AND e.status = 'SUCCESS'
WHERE p.enabled = 'Y'
GROUP BY
    p.policy_id, p.policy_name, p.table_owner, p.table_name,
    p.policy_type, p.action_type, p.age_days, p.age_months,
    p.compression_type, p.target_tablespace, p.priority
ORDER BY p.priority, p.policy_name;


-- -----------------------------------------------------------------------------
-- Execution Statistics View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_execution_stats AS
SELECT
    table_owner,
    table_name,
    policy_name,
    action_type,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(duration_seconds), 2) AS avg_duration_sec,
    ROUND(SUM(space_saved_mb), 2) AS total_space_saved_mb,
    MAX(execution_end) AS last_execution
FROM cmr.dwh_ilm_execution_log
GROUP BY table_owner, table_name, policy_name, action_type
ORDER BY total_space_saved_mb DESC NULLS LAST;


-- -----------------------------------------------------------------------------
-- Partition Temperature View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_ilm_partition_temperature AS
SELECT
    a.table_owner,
    a.table_name,
    a.partition_name,
    a.num_rows,
    a.size_mb,
    a.compression,
    a.tablespace_name,
    a.last_write_time,
    a.days_since_write,
    a.temperature,
    CASE
        WHEN a.days_since_write < 90 THEN 'No action needed'
        WHEN a.days_since_write BETWEEN 90 AND 365 THEN 'Compression candidate'
        WHEN a.days_since_write BETWEEN 365 AND 1095 THEN 'Archival candidate'
        WHEN a.days_since_write > 1095 THEN 'Purge candidate'
        ELSE 'Unknown'
    END AS recommendation
FROM cmr.dwh_ilm_partition_access a
ORDER BY a.days_since_write DESC;


-- -----------------------------------------------------------------------------
-- ILM Policy Summary Dashboard View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_policy_summary AS
SELECT
    p.table_owner,
    p.table_name,
    COUNT(DISTINCT p.policy_id) AS total_policies,
    SUM(CASE WHEN p.enabled = 'Y' THEN 1 ELSE 0 END) AS active_policies,
    SUM(CASE WHEN p.policy_type = 'COMPRESSION' THEN 1 ELSE 0 END) AS compression_policies,
    SUM(CASE WHEN p.policy_type = 'TIERING' THEN 1 ELSE 0 END) AS tiering_policies,
    SUM(CASE WHEN p.policy_type = 'ARCHIVAL' THEN 1 ELSE 0 END) AS archival_policies,
    SUM(CASE WHEN p.policy_type = 'PURGE' THEN 1 ELSE 0 END) AS purge_policies,
    COUNT(DISTINCT q.queue_id) AS pending_actions,
    COUNT(DISTINCT CASE WHEN q.eligible = 'Y' THEN q.partition_name END) AS eligible_partitions,
    MAX(e.execution_end) AS last_execution_time,
    SUM(CASE WHEN e.status = 'SUCCESS' AND e.execution_end > SYSTIMESTAMP - 7 THEN 1 ELSE 0 END) AS executions_last_7days,
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END), 2) AS total_space_saved_mb
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_evaluation_queue q
    ON q.policy_id = p.policy_id
    AND q.execution_status IN ('PENDING', 'SCHEDULED')
LEFT JOIN cmr.dwh_ilm_execution_log e
    ON e.policy_id = p.policy_id
GROUP BY p.table_owner, p.table_name
ORDER BY pending_actions DESC, total_space_saved_mb DESC;

COMMENT ON VIEW dwh_v_ilm_policy_summary IS 'Summary of ILM policies per table for dashboard';


-- -----------------------------------------------------------------------------
-- Upcoming ILM Actions View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_upcoming_actions AS
SELECT
    q.queue_id,
    q.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    q.partition_name,
    p.action_type,
    p.policy_type,
    q.evaluation_date,
    q.reason,
    q.scheduled_date,
    q.execution_status,
    a.size_mb AS partition_size_mb,
    a.num_rows AS partition_rows,
    a.days_since_write AS partition_age_days,
    a.temperature AS partition_temperature,
    a.compression AS current_compression,
    p.compression_type AS target_compression,
    p.target_tablespace,
    p.priority,
    CASE
        WHEN q.scheduled_date < SYSTIMESTAMP THEN 'Overdue'
        WHEN q.scheduled_date < SYSTIMESTAMP + INTERVAL '1' DAY THEN 'Today'
        WHEN q.scheduled_date < SYSTIMESTAMP + INTERVAL '7' DAY THEN 'This Week'
        WHEN q.scheduled_date < SYSTIMESTAMP + INTERVAL '30' DAY THEN 'This Month'
        ELSE 'Future'
    END AS urgency
FROM cmr.dwh_ilm_evaluation_queue q
JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
LEFT JOIN cmr.dwh_ilm_partition_access a
    ON a.table_owner = q.table_owner
    AND a.table_name = q.table_name
    AND a.partition_name = q.partition_name
WHERE q.eligible = 'Y'
AND q.execution_status IN ('PENDING', 'SCHEDULED')
AND q.scheduled_date < SYSTIMESTAMP + INTERVAL '30' DAY
ORDER BY q.scheduled_date, p.priority, q.table_name, q.partition_name;

COMMENT ON VIEW dwh_v_ilm_upcoming_actions IS 'Partitions scheduled for ILM actions in next 30 days';


-- -----------------------------------------------------------------------------
-- ILM Space Savings History View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_space_savings AS
SELECT
    e.table_owner,
    e.table_name,
    e.policy_name,
    e.action_type,
    TRUNC(e.execution_end) AS execution_date,
    COUNT(*) AS partitions_processed,
    SUM(e.size_before_mb) AS total_size_before_mb,
    SUM(e.size_after_mb) AS total_size_after_mb,
    SUM(e.space_saved_mb) AS total_space_saved_mb,
    ROUND(AVG(e.compression_ratio), 2) AS avg_compression_ratio,
    ROUND(AVG(e.duration_seconds), 2) AS avg_duration_seconds,
    SUM(e.rows_affected) AS total_rows_affected,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_operations,
    SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_operations
FROM cmr.dwh_ilm_execution_log e
WHERE e.status IN ('SUCCESS', 'FAILED')
GROUP BY e.table_owner, e.table_name, e.policy_name, e.action_type, TRUNC(e.execution_end)
ORDER BY execution_date DESC, total_space_saved_mb DESC;

COMMENT ON VIEW dwh_v_ilm_space_savings IS 'Historical space savings achieved by ILM policies';


-- -----------------------------------------------------------------------------
-- ILM Execution History Detail View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_execution_history AS
SELECT
    e.execution_id,
    e.policy_id,
    e.policy_name,
    e.table_owner,
    e.table_name,
    e.partition_name,
    e.action_type,
    e.status,
    e.execution_start,
    e.execution_end,
    e.duration_seconds,
    ROUND(e.duration_seconds / 60, 2) AS duration_minutes,
    e.size_before_mb,
    e.size_after_mb,
    e.space_saved_mb,
    CASE
        WHEN e.size_before_mb > 0 THEN ROUND((e.space_saved_mb / e.size_before_mb) * 100, 1)
        ELSE NULL
    END AS space_saved_pct,
    e.compression_ratio,
    e.rows_affected,
    e.error_code,
    CASE
        WHEN LENGTH(e.error_message) > 100 THEN SUBSTR(e.error_message, 1, 97) || '...'
        ELSE e.error_message
    END AS error_message_short,
    e.executed_by,
    e.execution_mode,
    CASE
        WHEN e.status = 'SUCCESS' AND e.duration_seconds < 300 THEN 'Fast'
        WHEN e.status = 'SUCCESS' AND e.duration_seconds < 1800 THEN 'Normal'
        WHEN e.status = 'SUCCESS' AND e.duration_seconds >= 1800 THEN 'Slow'
        WHEN e.status = 'FAILED' THEN 'Error'
        ELSE 'Unknown'
    END AS performance_category
FROM cmr.dwh_ilm_execution_log e
WHERE e.execution_start > SYSTIMESTAMP - INTERVAL '30' DAY
ORDER BY e.execution_start DESC;

COMMENT ON VIEW dwh_v_ilm_execution_history IS 'Detailed execution history for last 30 days';


-- -----------------------------------------------------------------------------
-- Partition Lifecycle Status View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_partition_lifecycle AS
SELECT
    tp.table_owner,
    tp.table_name,
    tp.partition_name,
    tp.partition_position,
    tp.high_value,
    tp.num_rows,
    ROUND(NVL(s.bytes, 0) / 1024 / 1024, 2) AS size_mb,
    tp.compression,
    tp.tablespace_name,
    tp.read_only,
    a.days_since_write,
    a.days_since_read,
    a.temperature,
    a.last_write_time,
    a.last_read_time,
    -- Calculate partition age from high_value for RANGE partitions
    CASE
        WHEN tp.high_value LIKE 'TO_DATE%' THEN
            TRUNC(SYSDATE - TO_DATE(
                REGEXP_SUBSTR(tp.high_value, '''[^'']+'''),
                'YYYY-MM-DD'
            ))
        ELSE NULL
    END AS partition_age_days_from_highvalue,
    -- Determine current lifecycle stage
    CASE
        WHEN NVL(a.days_since_write, 0) < 90 THEN 'HOT - Active'
        WHEN NVL(a.days_since_write, 0) BETWEEN 90 AND 365 THEN 'WARM - Aging'
        WHEN NVL(a.days_since_write, 0) BETWEEN 365 AND 1095 THEN 'COLD - Archive'
        WHEN NVL(a.days_since_write, 0) > 1095 THEN 'FROZEN - Purge Candidate'
        ELSE 'UNKNOWN'
    END AS lifecycle_stage,
    -- Next recommended action
    CASE
        WHEN tp.read_only = 'YES' THEN 'Already Read-Only'
        WHEN tp.compression LIKE '%ARCHIVE%' THEN 'Already Archived'
        WHEN NVL(a.days_since_write, 0) > 1095 AND tp.read_only = 'NO' THEN 'Make Read-Only / Drop'
        WHEN NVL(a.days_since_write, 0) BETWEEN 365 AND 1095 AND tp.compression NOT LIKE '%ARCHIVE%' THEN 'Archive Compression'
        WHEN NVL(a.days_since_write, 0) BETWEEN 90 AND 365 AND tp.compression IS NULL THEN 'Query Compression'
        ELSE 'No action needed'
    END AS recommended_action,
    -- Check if there's a pending ILM action
    (SELECT COUNT(*)
     FROM cmr.dwh_ilm_evaluation_queue q
     WHERE q.table_owner = tp.table_owner
     AND q.table_name = tp.table_name
     AND q.partition_name = tp.partition_name
     AND q.eligible = 'Y'
     AND q.execution_status IN ('PENDING', 'SCHEDULED')
    ) AS pending_ilm_actions,
    -- Last ILM execution
    (SELECT MAX(e.execution_end)
     FROM cmr.dwh_ilm_execution_log e
     WHERE e.table_owner = tp.table_owner
     AND e.table_name = tp.table_name
     AND e.partition_name = tp.partition_name
     AND e.status = 'SUCCESS'
    ) AS last_ilm_execution
FROM all_tab_partitions tp
LEFT JOIN all_segments s
    ON s.owner = tp.table_owner
    AND s.segment_name = tp.table_name
    AND s.partition_name = tp.partition_name
LEFT JOIN cmr.dwh_ilm_partition_access a
    ON a.table_owner = tp.table_owner
    AND a.table_name = tp.table_name
    AND a.partition_name = tp.partition_name
WHERE tp.table_owner = USER
ORDER BY tp.table_owner, tp.table_name, tp.partition_position;

COMMENT ON VIEW dwh_v_ilm_partition_lifecycle IS 'Current lifecycle status of all partitions with recommendations';


-- =============================================================================
-- SECTION 3: INITIALIZATION PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Initialize partition access tracking (full metadata refresh)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_refresh_partition_access_tracking(
    p_table_owner VARCHAR2 DEFAULT USER,
    p_table_name VARCHAR2 DEFAULT NULL
) AS
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
    v_cold_threshold NUMBER;
BEGIN
    -- Get thresholds from config
    SELECT TO_NUMBER(config_value) INTO v_hot_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_warm_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_cold_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'COLD_THRESHOLD_DAYS';

    -- Merge partition data
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            tp.table_owner,
            tp.table_name,
            tp.partition_name,
            SYSTIMESTAMP AS last_write_time,  -- Placeholder - would track actual access
            tp.num_rows,
            ROUND(NVL(s.bytes, 0)/1024/1024, 2) AS size_mb,
            tp.compression,
            s.tablespace_name
        FROM dba_tab_partitions tp
        LEFT JOIN all_segments s
            ON s.owner = tp.table_owner
            AND s.segment_name = tp.table_name
            AND s.partition_name = tp.partition_name
        WHERE tp.table_owner = p_table_owner
        AND (p_table_name IS NULL OR tp.table_name = p_table_name)
    ) src
    ON (a.table_owner = src.table_owner
        AND a.table_name = src.table_name
        AND a.partition_name = src.partition_name)
    WHEN MATCHED THEN UPDATE SET
        a.num_rows = src.num_rows,
        a.size_mb = src.size_mb,
        a.compression = src.compression,
        a.tablespace_name = src.tablespace_name,
        a.days_since_write = TRUNC(SYSDATE - NVL(a.last_write_time, SYSDATE - 10000)),
        a.days_since_read = TRUNC(SYSDATE - NVL(a.last_read_time, SYSDATE - 10000)),
        a.temperature = CASE
            WHEN TRUNC(SYSDATE - NVL(a.last_write_time, SYSDATE - 10000)) < v_hot_threshold THEN 'HOT'
            WHEN TRUNC(SYSDATE - NVL(a.last_write_time, SYSDATE - 10000)) < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        a.last_updated = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        table_owner, table_name, partition_name,
        last_write_time, num_rows, size_mb, compression, tablespace_name,
        days_since_write, days_since_read, temperature, last_updated
    ) VALUES (
        src.table_owner, src.table_name, src.partition_name,
        src.last_write_time, src.num_rows, src.size_mb, src.compression, src.tablespace_name,
        TRUNC(SYSDATE - src.last_write_time),
        TRUNC(SYSDATE - src.last_write_time),
        CASE
            WHEN TRUNC(SYSDATE - src.last_write_time) < v_hot_threshold THEN 'HOT'
            WHEN TRUNC(SYSDATE - src.last_write_time) < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        SYSTIMESTAMP
    );

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Partition access tracking refreshed: ' || SQL%ROWCOUNT || ' partitions');
END;
/


-- -----------------------------------------------------------------------------
-- Wrapper procedure for policy engine compatibility
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE refresh_partition_access_tracking(
    p_table_owner VARCHAR2 DEFAULT USER,
    p_table_name VARCHAR2 DEFAULT NULL
) AS
BEGIN
    dwh_refresh_partition_access_tracking(p_table_owner, p_table_name);
END;
/


-- -----------------------------------------------------------------------------
-- Initialize partition access tracking for newly migrated table
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_init_partition_access_tracking(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2
) AS
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
    v_partitions_initialized NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Initializing partition access tracking for ' ||
                        p_table_owner || '.' || p_table_name);

    -- Get thresholds from config
    SELECT TO_NUMBER(config_value) INTO v_hot_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_warm_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS';

    -- Initialize tracking for all partitions with age calculated from high_value
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            tp.table_owner,
            tp.table_name,
            tp.partition_name,
            tp.num_rows,
            ROUND(NVL(s.bytes, 0)/1024/1024, 2) AS size_mb,
            tp.compression,
            s.tablespace_name,
            -- Try to extract date from high_value for initial write time estimation
            CASE
                WHEN tp.high_value LIKE 'TO_DATE%' THEN
                    TO_DATE(
                        REGEXP_SUBSTR(tp.high_value, '''[^'']+'''),
                        'YYYY-MM-DD'
                    )
                ELSE SYSDATE
            END AS estimated_write_date
        FROM all_tab_partitions tp
        LEFT JOIN all_segments s
            ON s.owner = tp.table_owner
            AND s.segment_name = tp.table_name
            AND s.partition_name = tp.partition_name
        WHERE tp.table_owner = p_table_owner
        AND tp.table_name = p_table_name
    ) src
    ON (a.table_owner = src.table_owner
        AND a.table_name = src.table_name
        AND a.partition_name = src.partition_name)
    WHEN NOT MATCHED THEN INSERT (
        table_owner, table_name, partition_name,
        last_write_time, last_read_time, read_count, write_count,
        num_rows, size_mb, compression, tablespace_name,
        days_since_write, days_since_read, temperature, last_updated
    ) VALUES (
        src.table_owner, src.table_name, src.partition_name,
        src.estimated_write_date, src.estimated_write_date, 0, 0,
        src.num_rows, src.size_mb, src.compression, src.tablespace_name,
        TRUNC(SYSDATE - src.estimated_write_date),
        TRUNC(SYSDATE - src.estimated_write_date),
        CASE
            WHEN TRUNC(SYSDATE - src.estimated_write_date) < v_hot_threshold THEN 'HOT'
            WHEN TRUNC(SYSDATE - src.estimated_write_date) < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        SYSTIMESTAMP
    );

    v_partitions_initialized := SQL%ROWCOUNT;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('  Initialized tracking for ' || v_partitions_initialized || ' partition(s)');
    DBMS_OUTPUT.PUT_LINE('  Temperature calculated from partition high_value dates');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  ERROR initializing partition tracking: ' || SQLERRM);
        ROLLBACK;
END;
/


-- -----------------------------------------------------------------------------
-- Record partition access event (manual tracking)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_record_partition_access(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2,
    p_access_type VARCHAR2  -- 'READ' or 'WRITE'
) AS
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
BEGIN
    -- Get thresholds
    SELECT TO_NUMBER(config_value) INTO v_hot_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'HOT_THRESHOLD_DAYS';

    SELECT TO_NUMBER(config_value) INTO v_warm_threshold
    FROM cmr.dwh_ilm_config WHERE config_key = 'WARM_THRESHOLD_DAYS';

    -- Update access tracking
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            p_table_owner AS table_owner,
            p_table_name AS table_name,
            p_partition_name AS partition_name,
            p_access_type AS access_type
        FROM DUAL
    ) src
    ON (a.table_owner = src.table_owner
        AND a.table_name = src.table_name
        AND a.partition_name = src.partition_name)
    WHEN MATCHED THEN UPDATE SET
        a.last_read_time = CASE WHEN src.access_type = 'READ' THEN SYSTIMESTAMP ELSE a.last_read_time END,
        a.last_write_time = CASE WHEN src.access_type = 'WRITE' THEN SYSTIMESTAMP ELSE a.last_write_time END,
        a.read_count = CASE WHEN src.access_type = 'READ' THEN NVL(a.read_count, 0) + 1 ELSE a.read_count END,
        a.write_count = CASE WHEN src.access_type = 'WRITE' THEN NVL(a.write_count, 0) + 1 ELSE a.write_count END,
        a.days_since_read = CASE WHEN src.access_type = 'READ' THEN 0 ELSE a.days_since_read END,
        a.days_since_write = CASE WHEN src.access_type = 'WRITE' THEN 0 ELSE a.days_since_write END,
        a.temperature = CASE
            WHEN src.access_type IN ('READ', 'WRITE') THEN 'HOT'
            ELSE a.temperature
        END,
        a.last_updated = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        table_owner, table_name, partition_name,
        last_read_time, last_write_time, read_count, write_count,
        days_since_read, days_since_write, temperature, last_updated
    ) VALUES (
        src.table_owner, src.table_name, src.partition_name,
        CASE WHEN src.access_type = 'READ' THEN SYSTIMESTAMP ELSE NULL END,
        CASE WHEN src.access_type = 'WRITE' THEN SYSTIMESTAMP ELSE NULL END,
        CASE WHEN src.access_type = 'READ' THEN 1 ELSE 0 END,
        CASE WHEN src.access_type = 'WRITE' THEN 1 ELSE 0 END,
        0, 0, 'HOT', SYSTIMESTAMP
    );

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/


-- -----------------------------------------------------------------------------
-- Cleanup old execution logs
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE cleanup_execution_logs AS
    v_retention_days NUMBER;
    v_deleted NUMBER;
BEGIN
    SELECT TO_NUMBER(config_value) INTO v_retention_days
    FROM cmr.dwh_ilm_config WHERE config_key = 'LOG_RETENTION_DAYS';

    DELETE FROM cmr.dwh_ilm_execution_log
    WHERE execution_start < SYSTIMESTAMP - v_retention_days
    AND status IN ('SUCCESS', 'FAILED');

    v_deleted := SQL%ROWCOUNT;
    COMMIT;

    DBMS_OUTPUT.PUT_LINE('Cleaned up ' || v_deleted || ' old execution log entries');
END;
/


-- =============================================================================
-- SECTION 4: UTILITY FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Check if currently in execution window
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION is_execution_window_open
RETURN BOOLEAN
AS
    v_current_time VARCHAR2(5);
    v_start_time VARCHAR2(5);
    v_end_time VARCHAR2(5);
BEGIN
    SELECT TO_CHAR(SYSDATE, 'HH24:MI') INTO v_current_time FROM DUAL;

    SELECT config_value INTO v_start_time
    FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_START';

    SELECT config_value INTO v_end_time
    FROM cmr.dwh_ilm_config WHERE config_key = 'EXECUTION_WINDOW_END';

    -- Handle overnight windows
    IF v_start_time > v_end_time THEN
        RETURN (v_current_time >= v_start_time OR v_current_time <= v_end_time);
    ELSE
        RETURN (v_current_time >= v_start_time AND v_current_time <= v_end_time);
    END IF;
END;
/


-- -----------------------------------------------------------------------------
-- Get configuration value
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_dwh_ilm_config(
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
END;
/


-- =============================================================================
-- SECTION 5: GRANTS AND SYNONYMS (Optional)
-- =============================================================================

-- Grant access to other schemas if needed
-- GRANT SELECT, INSERT, UPDATE, DELETE ON cmr.dwh_ilm_policies TO <schema>;
-- GRANT SELECT ON v_ilm_active_policies TO <schema>;


-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'ILM Framework Setup Complete!' AS status FROM DUAL;

SELECT
    'Metadata Tables Created: ' ||
    (SELECT COUNT(*) FROM user_tables WHERE table_name LIKE 'ILM_%') AS info
FROM DUAL;

SELECT
    'Configuration Parameters: ' ||
    (SELECT COUNT(*) FROM cmr.dwh_ilm_config) AS info
FROM DUAL;
