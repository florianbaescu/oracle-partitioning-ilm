-- =============================================================================
-- Partition Utilities Setup
-- Creates logging table and helper views for partition utility operations
-- =============================================================================

-- =============================================================================
-- SECTION 1: LOGGING TABLE
-- =============================================================================

-- Drop existing table if rerunning
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_partition_utilities_log CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE cmr.dwh_partition_utilities_log (
    log_id              NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Operation details
    operation_name      VARCHAR2(100) NOT NULL,  -- e.g., 'precreate_hot_partitions', 'compress_partitions'
    operation_type      VARCHAR2(50),             -- e.g., 'PRECREATION', 'COMPRESSION', 'MOVE', 'HEALTH_CHECK'

    -- Target table
    table_owner         VARCHAR2(128),
    table_name          VARCHAR2(128),
    partition_name      VARCHAR2(128),

    -- Execution details
    start_time          TIMESTAMP DEFAULT SYSTIMESTAMP,
    end_time            TIMESTAMP,
    duration_seconds    NUMBER,
    status              VARCHAR2(20),              -- SUCCESS, WARNING, ERROR, SKIPPED

    -- Results
    partitions_created  NUMBER DEFAULT 0,
    partitions_modified NUMBER DEFAULT 0,
    partitions_skipped  NUMBER DEFAULT 0,
    rows_affected       NUMBER,

    -- Configuration used
    config_source       VARCHAR2(50),              -- ILM_TEMPLATE, AUTO_DETECTED, CONFIG_OVERRIDE, MANUAL
    interval_type       VARCHAR2(20),              -- DAILY, WEEKLY, MONTHLY, YEARLY

    -- Messages
    message             VARCHAR2(4000),
    error_message       VARCHAR2(4000),
    sql_statement       CLOB,

    -- Context
    session_user        VARCHAR2(128) DEFAULT USER,
    os_user             VARCHAR2(128) DEFAULT SYS_CONTEXT('USERENV', 'OS_USER'),
    host_name           VARCHAR2(128) DEFAULT SYS_CONTEXT('USERENV', 'HOST')
);

CREATE INDEX cmr.idx_part_util_log_time ON cmr.dwh_partition_utilities_log(start_time);
CREATE INDEX cmr.idx_part_util_log_table ON cmr.dwh_partition_utilities_log(table_owner, table_name);
CREATE INDEX cmr.idx_part_util_log_status ON cmr.dwh_partition_utilities_log(status);
CREATE INDEX cmr.idx_part_util_log_operation ON cmr.dwh_partition_utilities_log(operation_name);

COMMENT ON TABLE cmr.dwh_partition_utilities_log IS 'Audit log for partition utility operations';
COMMENT ON COLUMN cmr.dwh_partition_utilities_log.config_source IS 'Source of partition configuration: ILM_TEMPLATE, AUTO_DETECTED, CONFIG_OVERRIDE, MANUAL';
COMMENT ON COLUMN cmr.dwh_partition_utilities_log.status IS 'Operation status: SUCCESS, WARNING, ERROR, SKIPPED';

-- =============================================================================
-- SECTION 2: PARTITION CONFIGURATION OVERRIDE TABLE
-- =============================================================================

-- Optional table for manual configuration overrides
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_partition_precreation_config CASCADE CONSTRAINTS';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE cmr.dwh_partition_precreation_config (
    table_owner         VARCHAR2(128),
    table_name          VARCHAR2(128),

    -- Partition strategy
    partition_interval  VARCHAR2(20) NOT NULL,     -- DAILY, WEEKLY, MONTHLY, YEARLY

    -- Storage settings
    tablespace          VARCHAR2(30),
    compression         VARCHAR2(50),
    pctfree             NUMBER DEFAULT 10,

    -- Control
    enabled             VARCHAR2(1) DEFAULT 'Y' CHECK (enabled IN ('Y', 'N')),
    auto_precreate      VARCHAR2(1) DEFAULT 'Y' CHECK (auto_precreate IN ('Y', 'N')),

    -- Metadata
    date_created        DATE DEFAULT SYSDATE,
    created_by          VARCHAR2(128) DEFAULT USER,
    notes               VARCHAR2(500),

    PRIMARY KEY (table_owner, table_name)
);

COMMENT ON TABLE cmr.dwh_partition_precreation_config IS 'Manual configuration overrides for partition pre-creation (for non-framework tables)';

-- =============================================================================
-- SECTION 3: MONITORING VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW cmr.dwh_v_partition_utilities_summary AS
SELECT
    operation_name,
    operation_type,
    COUNT(*) as execution_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) as warning_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_count,
    SUM(partitions_created) as total_partitions_created,
    SUM(partitions_modified) as total_partitions_modified,
    ROUND(AVG(duration_seconds), 2) as avg_duration_seconds,
    MAX(start_time) as last_execution_time
FROM cmr.dwh_partition_utilities_log
GROUP BY operation_name, operation_type
ORDER BY last_execution_time DESC;

COMMENT ON VIEW cmr.dwh_v_partition_utilities_summary IS 'Summary of partition utility operations';

CREATE OR REPLACE VIEW cmr.dwh_v_partition_utilities_recent AS
SELECT
    log_id,
    operation_name,
    table_owner || '.' || table_name as table_full_name,
    partition_name,
    start_time,
    ROUND(duration_seconds, 2) as duration_sec,
    status,
    partitions_created,
    partitions_modified,
    config_source,
    SUBSTR(message, 1, 100) as message_short,
    session_user
FROM cmr.dwh_partition_utilities_log
ORDER BY start_time DESC
FETCH FIRST 50 ROWS ONLY;

COMMENT ON VIEW cmr.dwh_v_partition_utilities_recent IS 'Recent partition utility operations (last 50)';

CREATE OR REPLACE VIEW cmr.dwh_v_partition_utilities_errors AS
SELECT
    log_id,
    operation_name,
    table_owner || '.' || table_name as table_full_name,
    partition_name,
    start_time,
    status,
    message,
    error_message,
    session_user
FROM cmr.dwh_partition_utilities_log
WHERE status IN ('ERROR', 'WARNING')
ORDER BY start_time DESC;

COMMENT ON VIEW cmr.dwh_v_partition_utilities_errors IS 'Partition utility operations with errors or warnings';

-- =============================================================================
-- SECTION 4: GRANT PERMISSIONS
-- =============================================================================

-- Grant access to logging table
GRANT SELECT, INSERT, UPDATE ON cmr.dwh_partition_utilities_log TO PUBLIC;
GRANT SELECT ON cmr.dwh_v_partition_utilities_summary TO PUBLIC;
GRANT SELECT ON cmr.dwh_v_partition_utilities_recent TO PUBLIC;
GRANT SELECT ON cmr.dwh_v_partition_utilities_errors TO PUBLIC;

-- Grant access to config table
GRANT SELECT, INSERT, UPDATE, DELETE ON cmr.dwh_partition_precreation_config TO PUBLIC;

PROMPT ========================================
PROMPT Partition Utilities Setup Complete
PROMPT ========================================
PROMPT
PROMPT Objects Created:
PROMPT - cmr.dwh_partition_utilities_log (table)
PROMPT - cmr.dwh_partition_precreation_config (table)
PROMPT - cmr.dwh_v_partition_utilities_summary (view)
PROMPT - cmr.dwh_v_partition_utilities_recent (view)
PROMPT - cmr.dwh_v_partition_utilities_errors (view)
PROMPT
PROMPT ========================================
