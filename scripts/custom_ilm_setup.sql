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

-- Insert default configuration
INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('ENABLE_AUTO_EXECUTION', 'Y', 'Enable automatic policy execution via scheduler');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('EXECUTION_WINDOW_START', '22:00', 'Start time for ILM operations (HH24:MI)');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('EXECUTION_WINDOW_END', '06:00', 'End time for ILM operations (HH24:MI)');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('MAX_CONCURRENT_OPERATIONS', '4', 'Maximum number of concurrent partition operations');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('ACCESS_TRACKING_ENABLED', 'Y', 'Enable partition access tracking');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('HOT_THRESHOLD_DAYS', '90', 'Days threshold for HOT classification');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('WARM_THRESHOLD_DAYS', '365', 'Days threshold for WARM classification');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('COLD_THRESHOLD_DAYS', '1095', 'Days threshold for COLD classification (3 years)');

INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description) VALUES
    ('LOG_RETENTION_DAYS', '365', 'Days to retain execution logs');

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


-- =============================================================================
-- SECTION 3: INITIALIZATION PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Initialize partition access tracking
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
