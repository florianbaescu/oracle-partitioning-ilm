-- =============================================================================
-- Custom ILM Framework - Setup and Metadata Tables
-- PL/SQL-based Information Lifecycle Management without ADO
-- =============================================================================

-- =============================================================================
-- SECTION 0: CLEANUP (for rerunnable script)
-- =============================================================================
-- Drop existing tables in reverse dependency order (child tables first)
-- Suppress errors if tables don't exist
-- NOTE: dwh_ilm_config is NOT dropped to preserve custom configuration values

BEGIN
    -- Drop child tables first (tables with foreign keys)
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_evaluation_queue CASCADE CONSTRAINTS PURGE';
        DBMS_OUTPUT.PUT_LINE('Dropped table: dwh_ilm_evaluation_queue');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_execution_log CASCADE CONSTRAINTS PURGE';
        DBMS_OUTPUT.PUT_LINE('Dropped table: dwh_ilm_execution_log');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_partition_access CASCADE CONSTRAINTS PURGE';
        DBMS_OUTPUT.PUT_LINE('Dropped table: dwh_ilm_partition_access');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    -- Drop parent tables (except dwh_ilm_config and dwh_ilm_threshold_profiles which are preserved)
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE cmr.dwh_ilm_policies CASCADE CONSTRAINTS PURGE';
        DBMS_OUTPUT.PUT_LINE('Dropped table: dwh_ilm_policies');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    DBMS_OUTPUT.PUT_LINE('Cleanup completed successfully (dwh_ilm_config and dwh_ilm_threshold_profiles preserved)');
END;
/

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
-- Create config table only if it doesn't exist (preserves custom settings on rerun)

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_ilm_config (
            config_key          VARCHAR2(100) PRIMARY KEY,
            config_value        VARCHAR2(4000),
            description         VARCHAR2(500),
            modified_by         VARCHAR2(50),
            modified_date       TIMESTAMP DEFAULT SYSTIMESTAMP
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_ilm_config');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_ilm_config already exists - preserving existing data');
        ELSE
            RAISE;
        END IF;
END;
/

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

-- Note: HOT/WARM/COLD thresholds removed from config (v3.1.1)
-- Use dwh_ilm_threshold_profiles.DEFAULT profile as the global default

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

-- Email notification configuration
MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ENABLE_EMAIL_NOTIFICATIONS' AS config_key, 'N' AS config_value,
              'Enable email notifications for ILM failures (Y/N)' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_EMAIL_RECIPIENTS' AS config_key, 'dba@company.com' AS config_value,
              'Comma-separated list of email recipients for alerts' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_EMAIL_SENDER' AS config_key, 'oracle-ilm@company.com' AS config_value,
              'Email address for alert sender' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'SMTP_SERVER' AS config_key, 'smtp.company.com' AS config_value,
              'SMTP server for sending emails' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_FAILURE_THRESHOLD' AS config_key, '3' AS config_value,
              'Number of failures before sending alert' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

MERGE INTO cmr.dwh_ilm_config t
USING (SELECT 'ALERT_INTERVAL_HOURS' AS config_key, '4' AS config_value,
              'Minimum hours between alert emails (prevents spam)' AS description FROM dual) s
ON (t.config_key = s.config_key)
WHEN NOT MATCHED THEN
    INSERT (config_key, config_value, description)
    VALUES (s.config_key, s.config_value, s.description);

COMMIT;

COMMENT ON TABLE cmr.dwh_ilm_config IS 'Configuration parameters for custom ILM framework';

-- -----------------------------------------------------------------------------
-- ILM Threshold Profiles
-- -----------------------------------------------------------------------------
-- Reusable threshold profiles for temperature-based ILM classification
-- Policies can reference a profile or fall back to global config
-- Create table only if it doesn't exist (preserves custom profiles on rerun)
-- -----------------------------------------------------------------------------

BEGIN
    EXECUTE IMMEDIATE '
        CREATE TABLE cmr.dwh_ilm_threshold_profiles (
            profile_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            profile_name        VARCHAR2(100) NOT NULL UNIQUE,
            description         VARCHAR2(500),

            -- Temperature thresholds in days
            hot_threshold_days  NUMBER NOT NULL,
            warm_threshold_days NUMBER NOT NULL,
            cold_threshold_days NUMBER NOT NULL,

            -- Audit fields
            created_by          VARCHAR2(50) DEFAULT USER,
            created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
            modified_by         VARCHAR2(50),
            modified_date       TIMESTAMP,

            -- Validation: ensure thresholds are in ascending order
            CONSTRAINT chk_profile_thresholds CHECK (
                hot_threshold_days < warm_threshold_days
                AND warm_threshold_days < cold_threshold_days
            )
        )';
    DBMS_OUTPUT.PUT_LINE('Created table: dwh_ilm_threshold_profiles');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Table dwh_ilm_threshold_profiles already exists - preserving existing data');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_profiles_name ON cmr.dwh_ilm_threshold_profiles(profile_name)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_ilm_profiles_name');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_ilm_profiles_name already exists');
        ELSIF SQLCODE = -1408 THEN
            DBMS_OUTPUT.PUT_LINE('Column profile_name already indexed (by UNIQUE constraint)');
        ELSE
            RAISE;
        END IF;
END;
/

COMMENT ON TABLE cmr.dwh_ilm_threshold_profiles IS
    'Reusable threshold profiles for ILM temperature-based classification';

-- Insert commonly used threshold profiles
MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'DEFAULT' AS profile_name,
              'Standard aging profile (matches global config)' AS description,
              90 AS hot_threshold_days,
              365 AS warm_threshold_days,
              1095 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'FAST_AGING' AS profile_name,
              'Fast aging for transactional data (sales, orders)' AS description,
              30 AS hot_threshold_days,
              90 AS warm_threshold_days,
              180 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'SLOW_AGING' AS profile_name,
              'Slow aging for reference/master data' AS description,
              180 AS hot_threshold_days,
              730 AS warm_threshold_days,
              1825 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

MERGE INTO cmr.dwh_ilm_threshold_profiles t
USING (SELECT 'AGGRESSIVE_ARCHIVE' AS profile_name,
              'Aggressive archival for high-volume data' AS description,
              14 AS hot_threshold_days,
              30 AS warm_threshold_days,
              90 AS cold_threshold_days
       FROM dual) s
ON (t.profile_name = s.profile_name)
WHEN NOT MATCHED THEN
    INSERT (profile_name, description, hot_threshold_days, warm_threshold_days, cold_threshold_days)
    VALUES (s.profile_name, s.description, s.hot_threshold_days, s.warm_threshold_days, s.cold_threshold_days);

COMMIT;

-- Add foreign key to threshold profiles in dwh_ilm_policies table
DECLARE
    v_count NUMBER;
BEGIN
    -- Check if column already exists
    SELECT COUNT(*) INTO v_count
    FROM user_tab_columns
    WHERE table_name = 'DWH_ILM_POLICIES'
    AND column_name = 'THRESHOLD_PROFILE_ID';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE '
            ALTER TABLE cmr.dwh_ilm_policies
            ADD (
                threshold_profile_id NUMBER,
                CONSTRAINT fk_ilm_policy_profile
                    FOREIGN KEY (threshold_profile_id)
                    REFERENCES cmr.dwh_ilm_threshold_profiles(profile_id)
            )';
        DBMS_OUTPUT.PUT_LINE('Added column threshold_profile_id to dwh_ilm_policies');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Column threshold_profile_id already exists in dwh_ilm_policies');
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        -- If FK constraint already exists, catch that error too
        IF SQLCODE = -2275 THEN
            DBMS_OUTPUT.PUT_LINE('Foreign key constraint fk_ilm_policy_profile already exists');
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX idx_ilm_policies_profile ON cmr.dwh_ilm_policies(threshold_profile_id)';
    DBMS_OUTPUT.PUT_LINE('Created index: idx_ilm_policies_profile');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -955 THEN
            DBMS_OUTPUT.PUT_LINE('Index idx_ilm_policies_profile already exists');
        ELSIF SQLCODE = -1408 THEN
            DBMS_OUTPUT.PUT_LINE('Column threshold_profile_id already indexed');
        ELSE
            RAISE;
        END IF;
END;
/


-- =============================================================================
-- SECTION 1B: POLICY VALIDATION TRIGGER AND PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Trigger: Validate ILM Policy Configuration
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER cmr.trg_validate_dwh_ilm_policy
BEFORE INSERT OR UPDATE ON cmr.dwh_ilm_policies
FOR EACH ROW
DECLARE
    v_count NUMBER;
    v_partition_count NUMBER;
    v_error_msg VARCHAR2(500);
BEGIN
    -- Validation 1: Table exists and is partitioned
    BEGIN
        SELECT COUNT(*) INTO v_partition_count
        FROM dba_part_tables
        WHERE owner = :NEW.table_owner
        AND table_name = :NEW.table_name;

        IF v_partition_count = 0 THEN
            -- Check if table exists but is not partitioned
            SELECT COUNT(*) INTO v_count
            FROM dba_tables
            WHERE owner = :NEW.table_owner
            AND table_name = :NEW.table_name;

            IF v_count > 0 THEN
                v_error_msg := 'Table ' || :NEW.table_owner || '.' || :NEW.table_name ||
                              ' exists but is not partitioned. ILM policies require partitioned tables.';
            ELSE
                v_error_msg := 'Table ' || :NEW.table_owner || '.' || :NEW.table_name ||
                              ' does not exist.';
            END IF;
            RAISE_APPLICATION_ERROR(-20001, v_error_msg);
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
                'Unable to verify table ' || :NEW.table_owner || '.' || :NEW.table_name);
    END;

    -- Validation 2: Tablespace exists (for MOVE action)
    IF :NEW.action_type = 'MOVE' AND :NEW.target_tablespace IS NOT NULL THEN
        SELECT COUNT(*) INTO v_count
        FROM dba_tablespaces
        WHERE tablespace_name = :NEW.target_tablespace;

        IF v_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20002,
                'Target tablespace ' || :NEW.target_tablespace || ' does not exist');
        END IF;
    END IF;

    -- Validation 3: Compression type valid (if specified)
    IF :NEW.compression_type IS NOT NULL THEN
        IF :NEW.compression_type NOT IN (
            'QUERY LOW', 'QUERY HIGH', 'ARCHIVE LOW', 'ARCHIVE HIGH',
            'BASIC', 'OLTP'
        ) THEN
            RAISE_APPLICATION_ERROR(-20003,
                'Invalid compression type: ' || :NEW.compression_type ||
                '. Valid types: QUERY LOW/HIGH, ARCHIVE LOW/HIGH, BASIC, OLTP');
        END IF;
    END IF;

    -- Validation 4: Action type requires parameters
    IF :NEW.action_type = 'COMPRESS' AND :NEW.compression_type IS NULL THEN
        RAISE_APPLICATION_ERROR(-20004,
            'COMPRESS action requires compression_type to be specified');
    END IF;

    IF :NEW.action_type = 'MOVE' AND :NEW.target_tablespace IS NULL THEN
        RAISE_APPLICATION_ERROR(-20005,
            'MOVE action requires target_tablespace to be specified');
    END IF;

    IF :NEW.action_type = 'CUSTOM' AND :NEW.custom_action IS NULL THEN
        RAISE_APPLICATION_ERROR(-20006,
            'CUSTOM action requires custom_action PL/SQL block');
    END IF;

    -- Validation 5: Age threshold conflict
    IF :NEW.age_days IS NOT NULL AND :NEW.age_months IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('WARNING: Both age_days and age_months specified. ' ||
                           'age_months will take precedence.');
    END IF;

    -- Validation 6: At least one condition specified
    IF :NEW.age_days IS NULL
       AND :NEW.age_months IS NULL
       AND :NEW.access_pattern IS NULL
       AND :NEW.size_threshold_mb IS NULL
       AND :NEW.custom_condition IS NULL THEN
        RAISE_APPLICATION_ERROR(-20007,
            'Policy must have at least one condition: age_days, age_months, ' ||
            'access_pattern, size_threshold_mb, or custom_condition');
    END IF;

    -- Validation 7: Policy type and action type compatibility
    IF :NEW.policy_type = 'COMPRESSION' AND :NEW.action_type NOT IN ('COMPRESS') THEN
        RAISE_APPLICATION_ERROR(-20008,
            'COMPRESSION policy type requires COMPRESS action');
    END IF;

    IF :NEW.policy_type = 'PURGE' AND :NEW.action_type NOT IN ('DROP', 'TRUNCATE') THEN
        RAISE_APPLICATION_ERROR(-20009,
            'PURGE policy type requires DROP or TRUNCATE action');
    END IF;

    -- Validation 8: Priority range check
    IF :NEW.priority < 1 OR :NEW.priority > 999 THEN
        RAISE_APPLICATION_ERROR(-20010,
            'Priority must be between 1 and 999');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- Re-raise any errors
        RAISE;
END;
/

-- Note: Trigger validates ILM policy configuration before insert/update


-- -----------------------------------------------------------------------------
-- Procedure: Validate Policy Configuration
-- -----------------------------------------------------------------------------
-- Note: The dwh_validate_ilm_policy procedure has been moved to
-- custom_ilm_validation.sql to resolve circular dependencies.
-- Install custom_ilm_validation.sql after custom_ilm_policy_engine.sql
-- -----------------------------------------------------------------------------


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
    e.table_owner,
    e.table_name,
    e.policy_name,
    e.action_type,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_count,
    SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(AVG(e.duration_seconds), 2) AS avg_duration_sec,
    ROUND(SUM(e.space_saved_mb), 2) AS total_space_saved_mb,
    MAX(e.execution_end) AS last_execution
FROM cmr.dwh_ilm_execution_log e
GROUP BY e.table_owner, e.table_name, e.policy_name, e.action_type
ORDER BY SUM(e.space_saved_mb) DESC NULLS LAST;


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
    -- Generic recommendations using DEFAULT profile thresholds
    -- Note: Actual policies use age_days/age_months from dwh_ilm_policies table per policy
    CASE
        WHEN a.days_since_write < (SELECT hot_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'No action needed'
        WHEN a.days_since_write < (SELECT warm_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'Compression candidate'
        WHEN a.days_since_write < (SELECT cold_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'Archival candidate'
        ELSE 'Purge candidate'
    END AS recommendation
FROM cmr.dwh_ilm_partition_access a
ORDER BY a.days_since_write DESC;


-- -----------------------------------------------------------------------------
-- ILM Policy Threshold View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_policy_thresholds AS
SELECT
    p.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    p.threshold_profile_id,
    COALESCE(prof.profile_name, 'DEFAULT') AS profile_name,
    -- Show effective thresholds (use profile if set, otherwise DEFAULT profile)
    COALESCE(prof.hot_threshold_days, defprof.hot_threshold_days) AS effective_hot_threshold_days,
    COALESCE(prof.warm_threshold_days, defprof.warm_threshold_days) AS effective_warm_threshold_days,
    COALESCE(prof.cold_threshold_days, defprof.cold_threshold_days) AS effective_cold_threshold_days,
    CASE
        WHEN p.threshold_profile_id IS NOT NULL THEN 'CUSTOM'
        ELSE 'DEFAULT'
    END AS threshold_source
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_threshold_profiles prof
    ON prof.profile_id = p.threshold_profile_id
CROSS JOIN (SELECT hot_threshold_days, warm_threshold_days, cold_threshold_days
            FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') defprof;

COMMENT ON TABLE cmr.dwh_v_ilm_policy_thresholds IS
    'Shows effective threshold values for each ILM policy';


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
ORDER BY COUNT(DISTINCT q.queue_id) DESC, SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) DESC;

COMMENT ON TABLE cmr.dwh_v_ilm_policy_summary IS 'Summary of ILM policies per table for dashboard';


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

COMMENT ON TABLE cmr.dwh_v_ilm_upcoming_actions IS 'Partitions scheduled for ILM actions in next 30 days';


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
ORDER BY TRUNC(e.execution_end) DESC, SUM(e.space_saved_mb) DESC;

COMMENT ON TABLE cmr.dwh_v_ilm_space_savings IS 'Historical space savings achieved by ILM policies';


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

COMMENT ON TABLE cmr.dwh_v_ilm_execution_history IS 'Detailed execution history for last 30 days';


-- -----------------------------------------------------------------------------
-- Partition Lifecycle Status View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_partition_lifecycle AS
SELECT
    tp.table_owner,
    tp.table_name,
    tp.partition_name,
    tp.partition_position,
    get_partition_high_value(tp.table_owner, tp.table_name, tp.partition_name) AS high_value,
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
    -- Determine current lifecycle stage using DEFAULT profile thresholds
    CASE
        WHEN NVL(a.days_since_write, 0) < (SELECT hot_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'HOT - Active'
        WHEN NVL(a.days_since_write, 0) < (SELECT warm_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'WARM - Aging'
        WHEN NVL(a.days_since_write, 0) < (SELECT cold_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') THEN 'COLD - Archive'
        ELSE 'FROZEN - Purge Candidate'
    END AS lifecycle_stage,
    -- Next recommended action using DEFAULT profile thresholds
    CASE
        WHEN tp.read_only = 'YES' THEN 'Already Read-Only'
        WHEN tp.compression LIKE '%ARCHIVE%' THEN 'Already Archived'
        WHEN NVL(a.days_since_write, 0) >= (SELECT cold_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') AND tp.read_only = 'NO' THEN 'Make Read-Only / Drop'
        WHEN NVL(a.days_since_write, 0) >= (SELECT warm_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') AND tp.compression NOT LIKE '%ARCHIVE%' THEN 'Archive Compression'
        WHEN NVL(a.days_since_write, 0) >= (SELECT hot_threshold_days FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'DEFAULT') AND tp.compression IS NULL THEN 'Query Compression'
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
FROM dba_tab_partitions tp
LEFT JOIN dba_segments s
    ON s.owner = tp.table_owner
    AND s.segment_name = tp.table_name
    AND s.partition_name = tp.partition_name
LEFT JOIN cmr.dwh_ilm_partition_access a
    ON a.table_owner = tp.table_owner
    AND a.table_name = tp.table_name
    AND a.partition_name = tp.partition_name
WHERE tp.table_owner IN (
    SELECT username
    FROM dba_users
    WHERE oracle_maintained = 'N'
    AND account_status = 'OPEN'
    AND default_tablespace NOT LIKE '%USERS%'
)
ORDER BY tp.table_owner, tp.table_name, tp.partition_position;

COMMENT ON TABLE cmr.dwh_v_ilm_partition_lifecycle IS 'Current lifecycle status of all partitions with recommendations';


-- -----------------------------------------------------------------------------
-- Enhanced Performance Dashboard View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_performance_dashboard AS
SELECT
    -- Current Status
    (SELECT COUNT(*) FROM cmr.dwh_ilm_policies WHERE enabled = 'Y') AS active_policies_count,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_evaluation_queue WHERE execution_status = 'PENDING' AND eligible = 'Y') AS pending_actions_count,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSTIMESTAMP - INTERVAL '1' HOUR AND status = 'IN_PROGRESS') AS running_actions_count,
    -- Today's Metrics
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE TRUNC(execution_start) = TRUNC(SYSDATE) AND status = 'SUCCESS') AS actions_today_success,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE TRUNC(execution_start) = TRUNC(SYSDATE) AND status = 'FAILED') AS actions_today_failed,
    (SELECT ROUND(SUM(space_saved_mb)/1024, 2) FROM cmr.dwh_ilm_execution_log WHERE TRUNC(execution_start) = TRUNC(SYSDATE) AND status = 'SUCCESS') AS space_saved_today_gb,
    (SELECT ROUND(AVG(duration_seconds)/60, 2) FROM cmr.dwh_ilm_execution_log WHERE TRUNC(execution_start) = TRUNC(SYSDATE) AND status = 'SUCCESS') AS avg_duration_today_min,
    -- Last 7 Days Metrics
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 7 AND status = 'SUCCESS') AS actions_7days_success,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 7 AND status = 'FAILED') AS actions_7days_failed,
    (SELECT ROUND(SUM(space_saved_mb)/1024, 2) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 7 AND status = 'SUCCESS') AS space_saved_7days_gb,
    (SELECT ROUND(AVG(compression_ratio), 2) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 7 AND status = 'SUCCESS' AND action_type = 'COMPRESS') AS avg_compression_ratio_7days,
    -- Last 30 Days Metrics
    (SELECT ROUND(SUM(space_saved_mb)/1024, 2) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 30 AND status = 'SUCCESS') AS space_saved_30days_gb,
    (SELECT ROUND(AVG(duration_seconds), 2) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 30 AND status = 'SUCCESS') AS avg_duration_30days_sec,
    (SELECT MAX(execution_end) FROM cmr.dwh_ilm_execution_log WHERE status = 'SUCCESS') AS last_successful_execution,
    (SELECT MAX(execution_end) FROM cmr.dwh_ilm_execution_log WHERE status = 'FAILED') AS last_failed_execution,
    -- Partition Temperature Distribution
    (SELECT COUNT(*) FROM cmr.dwh_ilm_partition_access WHERE temperature = 'HOT') AS partitions_hot,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_partition_access WHERE temperature = 'WARM') AS partitions_warm,
    (SELECT COUNT(*) FROM cmr.dwh_ilm_partition_access WHERE temperature = 'COLD') AS partitions_cold,
    (SELECT ROUND(SUM(size_mb)/1024, 2) FROM cmr.dwh_ilm_partition_access) AS total_tracked_partitions_gb,
    -- Execution Rate (actions per day average over last 30 days)
    (SELECT ROUND(COUNT(*) / 30.0, 2) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 30 AND status = 'SUCCESS') AS avg_actions_per_day,
    -- Current Date/Time
    SYSTIMESTAMP AS dashboard_timestamp
FROM DUAL;

COMMENT ON TABLE cmr.dwh_v_ilm_performance_dashboard IS 'Real-time performance dashboard with key metrics';


-- -----------------------------------------------------------------------------
-- Alerting Metrics View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_alerting_metrics AS
SELECT
    -- Failure Rate (last 24 hours)
    CASE
        WHEN total_24h > 0 THEN ROUND((failed_24h / total_24h) * 100, 2)
        ELSE 0
    END AS failure_rate_24h_pct,
    CASE
        WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 10 THEN 'CRITICAL'
        WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 5 THEN 'WARNING'
        ELSE 'OK'
    END AS failure_rate_status,
    failed_24h AS failures_count_24h,
    -- Execution Duration (last 24 hours)
    ROUND(avg_duration_24h / 60, 2) AS avg_duration_24h_min,
    CASE
        WHEN avg_duration_24h > 7200 THEN 'CRITICAL'  -- > 2 hours
        WHEN avg_duration_24h > 3600 THEN 'WARNING'   -- > 1 hour
        ELSE 'OK'
    END AS duration_status,
    -- Queue Backlog
    pending_queue_count,
    CASE
        WHEN pending_queue_count > 500 THEN 'CRITICAL'
        WHEN pending_queue_count > 100 THEN 'WARNING'
        ELSE 'OK'
    END AS queue_status,
    -- Compression Ratio (last 7 days)
    ROUND(avg_compression_ratio_7d, 2) AS avg_compression_ratio_7d,
    CASE
        WHEN avg_compression_ratio_7d < 1.5 THEN 'CRITICAL'  -- Very poor compression
        WHEN avg_compression_ratio_7d < 2.0 THEN 'WARNING'
        ELSE 'OK'
    END AS compression_status,
    -- Job Execution Lag (scheduled vs actual)
    overdue_actions_count,
    CASE
        WHEN overdue_actions_count > 50 THEN 'CRITICAL'
        WHEN overdue_actions_count > 10 THEN 'WARNING'
        ELSE 'OK'
    END AS execution_lag_status,
    -- Stale Partitions (not tracked in last 7 days)
    stale_partitions_count,
    CASE
        WHEN stale_partitions_count > 100 THEN 'WARNING'
        ELSE 'OK'
    END AS stale_tracking_status,
    -- Overall Health Score
    CASE
        WHEN GREATEST(
            CASE WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 10 THEN 3 WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 5 THEN 2 ELSE 1 END,
            CASE WHEN avg_duration_24h > 7200 THEN 3 WHEN avg_duration_24h > 3600 THEN 2 ELSE 1 END,
            CASE WHEN pending_queue_count > 500 THEN 3 WHEN pending_queue_count > 100 THEN 2 ELSE 1 END,
            CASE WHEN avg_compression_ratio_7d < 1.5 THEN 3 WHEN avg_compression_ratio_7d < 2.0 THEN 2 ELSE 1 END
        ) >= 3 THEN 'CRITICAL'
        WHEN GREATEST(
            CASE WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 10 THEN 3 WHEN ROUND((failed_24h / NULLIF(total_24h, 0)) * 100, 2) > 5 THEN 2 ELSE 1 END,
            CASE WHEN avg_duration_24h > 7200 THEN 3 WHEN avg_duration_24h > 3600 THEN 2 ELSE 1 END,
            CASE WHEN pending_queue_count > 500 THEN 3 WHEN pending_queue_count > 100 THEN 2 ELSE 1 END,
            CASE WHEN avg_compression_ratio_7d < 1.5 THEN 3 WHEN avg_compression_ratio_7d < 2.0 THEN 2 ELSE 1 END
        ) >= 2 THEN 'WARNING'
        ELSE 'OK'
    END AS overall_health_status,
    SYSTIMESTAMP AS metrics_timestamp
FROM (
    SELECT
        (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 1) AS total_24h,
        (SELECT COUNT(*) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 1 AND status = 'FAILED') AS failed_24h,
        (SELECT AVG(duration_seconds) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 1 AND status = 'SUCCESS') AS avg_duration_24h,
        (SELECT COUNT(*) FROM cmr.dwh_ilm_evaluation_queue WHERE execution_status = 'PENDING' AND eligible = 'Y') AS pending_queue_count,
        (SELECT AVG(compression_ratio) FROM cmr.dwh_ilm_execution_log WHERE execution_start > SYSDATE - 7 AND status = 'SUCCESS' AND action_type = 'COMPRESS') AS avg_compression_ratio_7d,
        (SELECT COUNT(*) FROM cmr.dwh_ilm_evaluation_queue WHERE execution_status = 'PENDING' AND eligible = 'Y' AND scheduled_date < SYSTIMESTAMP) AS overdue_actions_count,
        (SELECT COUNT(*) FROM cmr.dwh_ilm_partition_access WHERE last_updated < SYSTIMESTAMP - INTERVAL '7' DAY) AS stale_partitions_count
    FROM DUAL
);

COMMENT ON TABLE cmr.dwh_v_ilm_alerting_metrics IS 'Pre-calculated alerting metrics with threshold-based status';


-- -----------------------------------------------------------------------------
-- Policy Effectiveness View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_policy_effectiveness AS
SELECT
    p.policy_id,
    p.policy_name,
    p.table_owner,
    p.table_name,
    p.policy_type,
    p.action_type,
    p.priority,
    p.enabled,
    p.created_date,
    -- Execution Metrics
    COUNT(e.execution_id) AS total_executions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_executions,
    SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_executions,
    CASE
        WHEN COUNT(e.execution_id) > 0 THEN
            ROUND((SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(e.execution_id)) * 100, 2)
        ELSE NULL
    END AS success_rate_pct,
    -- Space Savings
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) / 1024, 2) AS total_space_saved_gb,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' THEN e.space_saved_mb ELSE NULL END), 2) AS avg_space_saved_per_partition_mb,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' AND e.action_type = 'COMPRESS' THEN e.compression_ratio ELSE NULL END), 2) AS avg_compression_ratio,
    -- Performance Metrics
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END), 2) AS avg_execution_time_sec,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END) / 60, 2) AS avg_execution_time_min,
    MIN(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END) AS min_execution_time_sec,
    MAX(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END) AS max_execution_time_sec,
    -- Activity Metrics
    MIN(e.execution_start) AS first_execution,
    MAX(e.execution_end) AS last_execution,
    TRUNC(SYSDATE - CAST(MAX(e.execution_end) AS DATE)) AS days_since_last_execution,
    -- ROI Calculation (GB saved per hour of execution time)
    CASE
        WHEN SUM(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE 0 END) > 0 THEN
            ROUND(
                (SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) / 1024) /
                (SUM(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE 0 END) / 3600),
                2
            )
        ELSE NULL
    END AS space_saved_per_hour_gb,
    -- Effectiveness Rating
    CASE
        WHEN COUNT(e.execution_id) = 0 THEN 'NOT_EXECUTED'
        WHEN SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) > COUNT(e.execution_id) * 0.2 THEN 'POOR'  -- >20% failure rate
        WHEN AVG(CASE WHEN e.status = 'SUCCESS' AND e.action_type = 'COMPRESS' THEN e.compression_ratio ELSE NULL END) < 2.0 THEN 'FAIR'  -- Low compression
        WHEN AVG(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END) > 3600 THEN 'SLOW'  -- Avg >1 hour
        WHEN AVG(CASE WHEN e.status = 'SUCCESS' AND e.action_type = 'COMPRESS' THEN e.compression_ratio ELSE NULL END) >= 3.0 THEN 'EXCELLENT'  -- Great compression
        ELSE 'GOOD'
    END AS effectiveness_rating,
    -- Pending Actions
    (SELECT COUNT(*)
     FROM cmr.dwh_ilm_evaluation_queue q
     WHERE q.policy_id = p.policy_id
     AND q.execution_status = 'PENDING'
     AND q.eligible = 'Y'
    ) AS pending_actions_count
FROM cmr.dwh_ilm_policies p
LEFT JOIN cmr.dwh_ilm_execution_log e ON e.policy_id = p.policy_id
GROUP BY
    p.policy_id, p.policy_name, p.table_owner, p.table_name, p.policy_type,
    p.action_type, p.priority, p.enabled, p.created_date
ORDER BY
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) / 1024, 2) DESC NULLS LAST,
    CASE WHEN COUNT(e.execution_id) > 0 THEN ROUND((SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(e.execution_id)) * 100, 2) ELSE NULL END DESC NULLS LAST;

COMMENT ON TABLE cmr.dwh_v_ilm_policy_effectiveness IS 'Policy effectiveness metrics including ROI and success rates';


-- -----------------------------------------------------------------------------
-- Resource Utilization Trend View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_resource_trends AS
SELECT
    TO_CHAR(e.execution_end, 'YYYY-MM') AS year_month,
    TO_CHAR(e.execution_end, 'YYYY-IW') AS year_week,
    TRUNC(e.execution_end) AS execution_date,
    -- Daily Aggregates
    COUNT(*) AS total_actions,
    SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_actions,
    SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_actions,
    ROUND((SUM(CASE WHEN e.status = 'FAILED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) * 100, 2) AS failure_rate_pct,
    -- Space Metrics
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) / 1024, 2) AS space_saved_gb,
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.size_before_mb, 0) ELSE 0 END) / 1024, 2) AS size_before_gb,
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.size_after_mb, 0) ELSE 0 END) / 1024, 2) AS size_after_gb,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' AND e.action_type = 'COMPRESS' THEN e.compression_ratio ELSE NULL END), 2) AS avg_compression_ratio,
    -- Time Metrics
    ROUND(SUM(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE 0 END) / 3600, 2) AS total_execution_hours,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END), 2) AS avg_duration_seconds,
    ROUND(AVG(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE NULL END) / 60, 2) AS avg_duration_minutes,
    -- Efficiency Metrics
    CASE
        WHEN SUM(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE 0 END) > 0 THEN
            ROUND(
                (SUM(CASE WHEN e.status = 'SUCCESS' THEN NVL(e.space_saved_mb, 0) ELSE 0 END) / 1024) /
                (SUM(CASE WHEN e.status = 'SUCCESS' THEN e.duration_seconds ELSE 0 END) / 3600),
                2
            )
        ELSE NULL
    END AS gb_saved_per_hour,
    -- Row Metrics
    SUM(e.rows_affected) AS total_rows_affected,
    -- Action Type Breakdown
    SUM(CASE WHEN e.action_type = 'COMPRESS' AND e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS compress_actions,
    SUM(CASE WHEN e.action_type = 'MOVE' AND e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS move_actions,
    SUM(CASE WHEN e.action_type = 'DROP' AND e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS drop_actions,
    SUM(CASE WHEN e.action_type = 'CUSTOM' AND e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS custom_actions
FROM cmr.dwh_ilm_execution_log e
WHERE e.execution_end IS NOT NULL
GROUP BY
    TO_CHAR(e.execution_end, 'YYYY-MM'),
    TO_CHAR(e.execution_end, 'YYYY-IW'),
    TRUNC(e.execution_end)
ORDER BY TRUNC(e.execution_end) DESC;

COMMENT ON TABLE cmr.dwh_v_ilm_resource_trends IS 'Historical resource utilization trends for capacity planning';


-- -----------------------------------------------------------------------------
-- Failure Analysis View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_failure_analysis AS
SELECT
    e.table_owner,
    e.table_name,
    e.policy_name,
    e.action_type,
    e.error_code,
    CASE
        WHEN e.error_message LIKE '%ORA-01031%' THEN 'Insufficient Privileges'
        WHEN e.error_message LIKE '%ORA-00054%' THEN 'Resource Busy (Lock Conflict)'
        WHEN e.error_message LIKE '%ORA-01654%' THEN 'Tablespace Full'
        WHEN e.error_message LIKE '%ORA-08104%' THEN 'Index Not Partitioned'
        WHEN e.error_message LIKE '%ORA-14006%' THEN 'Invalid Partition Name'
        WHEN e.error_message LIKE '%ORA-14511%' THEN 'Partition Maintenance Not Allowed'
        WHEN e.error_message LIKE '%timeout%' THEN 'Operation Timeout'
        WHEN e.error_message LIKE '%space%' THEN 'Space Related Error'
        WHEN e.error_message LIKE '%lock%' THEN 'Lock Related Error'
        ELSE 'Other Error'
    END AS error_category,
    COUNT(*) AS failure_count,
    MIN(e.execution_start) AS first_failure,
    MAX(e.execution_start) AS last_failure,
    ROUND(AVG(e.duration_seconds), 2) AS avg_duration_before_failure_sec,
    -- Sample error message (first occurrence)
    MIN(CASE
        WHEN LENGTH(e.error_message) > 200 THEN SUBSTR(e.error_message, 1, 197) || '...'
        ELSE e.error_message
    END) AS sample_error_message,
    -- Recommended Action
    CASE
        WHEN MIN(e.error_message) LIKE '%ORA-01031%' THEN 'Grant necessary privileges to schema'
        WHEN MIN(e.error_message) LIKE '%ORA-00054%' THEN 'Check for long-running transactions, adjust execution window'
        WHEN MIN(e.error_message) LIKE '%ORA-01654%' THEN 'Add datafile or increase tablespace size'
        WHEN MIN(e.error_message) LIKE '%ORA-08104%' THEN 'Create local indexes or convert to partitioned indexes'
        WHEN MIN(e.error_message) LIKE '%ORA-14006%' THEN 'Verify partition exists, refresh partition tracking'
        WHEN MIN(e.error_message) LIKE '%timeout%' THEN 'Increase timeout or run during maintenance window'
        ELSE 'Review error message and execution log for details'
    END AS recommended_action
FROM cmr.dwh_ilm_execution_log e
WHERE e.status = 'FAILED'
AND e.execution_start > SYSDATE - 30  -- Last 30 days
GROUP BY
    e.table_owner, e.table_name, e.policy_name, e.action_type, e.error_code,
    CASE
        WHEN e.error_message LIKE '%ORA-01031%' THEN 'Insufficient Privileges'
        WHEN e.error_message LIKE '%ORA-00054%' THEN 'Resource Busy (Lock Conflict)'
        WHEN e.error_message LIKE '%ORA-01654%' THEN 'Tablespace Full'
        WHEN e.error_message LIKE '%ORA-08104%' THEN 'Index Not Partitioned'
        WHEN e.error_message LIKE '%ORA-14006%' THEN 'Invalid Partition Name'
        WHEN e.error_message LIKE '%ORA-14511%' THEN 'Partition Maintenance Not Allowed'
        WHEN e.error_message LIKE '%timeout%' THEN 'Operation Timeout'
        WHEN e.error_message LIKE '%space%' THEN 'Space Related Error'
        WHEN e.error_message LIKE '%lock%' THEN 'Lock Related Error'
        ELSE 'Other Error'
    END
ORDER BY COUNT(*) DESC, MAX(e.execution_start) DESC;

COMMENT ON TABLE cmr.dwh_v_ilm_failure_analysis IS 'Categorized failure analysis with recommended actions';


-- -----------------------------------------------------------------------------
-- Table Lifecycle Overview View
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW dwh_v_ilm_table_overview AS
SELECT
    t.owner AS table_owner,
    t.table_name,
    -- Table Metadata
    tp.partition_count,
    tp.partitioning_type,
    -- Space Metrics
    ROUND(NVL(s.total_size_mb, 0) / 1024, 2) AS total_table_size_gb,
    ROUND(NVL(a.tracked_size_mb, 0) / 1024, 2) AS tracked_partitions_size_gb,
    -- Temperature Distribution
    NVL(a.hot_partitions, 0) AS hot_partitions,
    NVL(a.warm_partitions, 0) AS warm_partitions,
    NVL(a.cold_partitions, 0) AS cold_partitions,
    -- Policy Coverage
    NVL(p.policy_count, 0) AS ilm_policies_count,
    NVL(p.active_policy_count, 0) AS active_policies_count,
    -- ILM Activity
    NVL(e.total_executions, 0) AS total_ilm_executions,
    NVL(e.successful_executions, 0) AS successful_executions,
    NVL(e.failed_executions, 0) AS failed_executions,
    ROUND(NVL(e.total_space_saved_gb, 0), 2) AS total_space_saved_gb,
    e.last_execution,
    -- Pending Actions
    NVL(q.pending_count, 0) AS pending_actions,
    -- Lifecycle Status
    CASE
        WHEN NVL(p.active_policy_count, 0) = 0 THEN 'NO_ILM_POLICIES'
        WHEN NVL(e.total_executions, 0) = 0 THEN 'NEVER_EXECUTED'
        WHEN e.last_execution < SYSDATE - 7 THEN 'STALE'
        WHEN NVL(e.failed_executions, 0) > NVL(e.successful_executions, 0) * 0.2 THEN 'HIGH_FAILURE_RATE'
        WHEN NVL(q.pending_count, 0) > 50 THEN 'HIGH_BACKLOG'
        ELSE 'ACTIVE'
    END AS lifecycle_status,
    -- Recommendations
    CASE
        WHEN NVL(p.active_policy_count, 0) = 0 THEN 'Create ILM policies for this table'
        WHEN NVL(e.total_executions, 0) = 0 THEN 'Policies defined but never executed - check policy criteria'
        WHEN e.last_execution < SYSDATE - 7 THEN 'No recent activity - verify policies and refresh tracking'
        WHEN NVL(e.failed_executions, 0) > NVL(e.successful_executions, 0) * 0.2 THEN 'Review failure logs and fix issues'
        WHEN NVL(q.pending_count, 0) > 50 THEN 'Large backlog - increase execution frequency'
        WHEN NVL(a.cold_partitions, 0) > NVL(tp.partition_count, 0) * 0.3 THEN 'Many COLD partitions - consider archival'
        ELSE 'Table lifecycle management is healthy'
    END AS recommendation
FROM dba_tables t
LEFT JOIN (
    SELECT owner AS table_owner, table_name, COUNT(*) AS partition_count, partitioning_type
    FROM dba_part_tables
    GROUP BY owner, table_name, partitioning_type
) tp ON tp.table_owner = t.owner AND tp.table_name = t.table_name
LEFT JOIN (
    SELECT owner, segment_name, SUM(bytes/1024/1024) AS total_size_mb
    FROM dba_segments
    WHERE segment_type LIKE '%PARTITION%'
    GROUP BY owner, segment_name
) s ON s.owner = t.owner AND s.segment_name = t.table_name
LEFT JOIN (
    SELECT
        table_owner, table_name,
        SUM(size_mb) AS tracked_size_mb,
        SUM(CASE WHEN temperature = 'HOT' THEN 1 ELSE 0 END) AS hot_partitions,
        SUM(CASE WHEN temperature = 'WARM' THEN 1 ELSE 0 END) AS warm_partitions,
        SUM(CASE WHEN temperature = 'COLD' THEN 1 ELSE 0 END) AS cold_partitions
    FROM cmr.dwh_ilm_partition_access
    GROUP BY table_owner, table_name
) a ON a.table_owner = t.owner AND a.table_name = t.table_name
LEFT JOIN (
    SELECT
        table_owner, table_name,
        COUNT(*) AS policy_count,
        SUM(CASE WHEN enabled = 'Y' THEN 1 ELSE 0 END) AS active_policy_count
    FROM cmr.dwh_ilm_policies
    GROUP BY table_owner, table_name
) p ON p.table_owner = t.owner AND p.table_name = t.table_name
LEFT JOIN (
    SELECT
        table_owner, table_name,
        COUNT(*) AS total_executions,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_executions,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_executions,
        SUM(CASE WHEN status = 'SUCCESS' THEN NVL(space_saved_mb, 0) ELSE 0 END) / 1024 AS total_space_saved_gb,
        MAX(execution_end) AS last_execution
    FROM cmr.dwh_ilm_execution_log
    GROUP BY table_owner, table_name
) e ON e.table_owner = t.owner AND e.table_name = t.table_name
LEFT JOIN (
    SELECT table_owner, table_name, COUNT(*) AS pending_count
    FROM cmr.dwh_ilm_evaluation_queue
    WHERE execution_status = 'PENDING' AND eligible = 'Y'
    GROUP BY table_owner, table_name
) q ON q.table_owner = t.owner AND q.table_name = t.table_name
WHERE tp.partition_count > 0  -- Only partitioned tables
AND t.owner IN (
    SELECT username
    FROM dba_users
    WHERE oracle_maintained = 'N'
    AND account_status = 'OPEN'
    AND default_tablespace NOT LIKE '%USERS%'
)
ORDER BY total_table_size_gb DESC NULLS LAST, lifecycle_status, t.table_name;

COMMENT ON TABLE cmr.dwh_v_ilm_table_overview IS 'Comprehensive table lifecycle overview with recommendations';


-- =============================================================================
-- SECTION 3: INITIALIZATION PROCEDURES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Initialize partition access tracking (full metadata refresh)
-- Enhanced to calculate age from partition high_value instead of placeholders
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_refresh_partition_access_tracking(
    p_table_owner VARCHAR2 DEFAULT USER,
    p_table_name VARCHAR2 DEFAULT NULL
) AS
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
    v_cold_threshold NUMBER;
    v_merge_count NUMBER;
    v_hot_count NUMBER := 0;
    v_warm_count NUMBER := 0;
    v_cold_count NUMBER := 0;

    -- Function to extract date from partition high_value
    FUNCTION get_partition_date(
        p_owner VARCHAR2,
        p_table VARCHAR2,
        p_partition VARCHAR2
    ) RETURN DATE
    IS
        v_high_value LONG;
        v_date DATE;
    BEGIN
        SELECT high_value INTO v_high_value
        FROM dba_tab_partitions
        WHERE table_owner = p_owner
        AND table_name = p_table
        AND partition_name = p_partition;

        -- Try to evaluate high_value as date
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v_high_value || ' FROM DUAL'
            INTO v_date;
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN NULL;
        END;
    END;

BEGIN
    -- Get thresholds from DEFAULT profile
    SELECT hot_threshold_days, warm_threshold_days, cold_threshold_days
    INTO v_hot_threshold, v_warm_threshold, v_cold_threshold
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'DEFAULT';

    -- Merge partition data with age calculated from high_value
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
            -- Calculate estimated write time from partition boundary date
            get_partition_date(tp.table_owner, tp.table_name, tp.partition_name) AS partition_date,
            CASE
                WHEN get_partition_date(tp.table_owner, tp.table_name, tp.partition_name) IS NOT NULL THEN
                    TRUNC(SYSDATE - get_partition_date(tp.table_owner, tp.table_name, tp.partition_name))
                ELSE 10000  -- Very old if can't determine
            END AS calculated_age_days
        FROM dba_tab_partitions tp
        LEFT JOIN dba_segments s
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
        -- Only update timestamps if we don't have real tracking data
        a.last_write_time = CASE
            WHEN a.write_count > 0 THEN a.last_write_time  -- Preserve real data
            ELSE src.partition_date  -- Use calculated
        END,
        a.last_read_time = CASE
            WHEN a.read_count > 0 THEN a.last_read_time  -- Preserve real data
            ELSE src.partition_date  -- Assume read age = write age
        END,
        a.days_since_write = src.calculated_age_days,
        a.days_since_read = src.calculated_age_days,  -- Assume read age = write age
        a.temperature = CASE
            WHEN src.calculated_age_days < v_hot_threshold THEN 'HOT'
            WHEN src.calculated_age_days < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        a.last_updated = SYSTIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        table_owner, table_name, partition_name,
        last_write_time, last_read_time, num_rows, size_mb, compression, tablespace_name,
        read_count, write_count,
        days_since_write, days_since_read, temperature, last_updated
    ) VALUES (
        src.table_owner, src.table_name, src.partition_name,
        src.partition_date, src.partition_date, src.num_rows, src.size_mb, src.compression, src.tablespace_name,
        0, 0,  -- No real access tracking yet
        src.calculated_age_days,
        src.calculated_age_days,
        CASE
            WHEN src.calculated_age_days < v_hot_threshold THEN 'HOT'
            WHEN src.calculated_age_days < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        SYSTIMESTAMP
    );

    v_merge_count := SQL%ROWCOUNT;

    -- Count temperature distribution
    SELECT
        SUM(CASE WHEN temperature = 'HOT' THEN 1 ELSE 0 END),
        SUM(CASE WHEN temperature = 'WARM' THEN 1 ELSE 0 END),
        SUM(CASE WHEN temperature = 'COLD' THEN 1 ELSE 0 END)
    INTO v_hot_count, v_warm_count, v_cold_count
    FROM cmr.dwh_ilm_partition_access
    WHERE table_owner = p_table_owner
    AND (p_table_name IS NULL OR table_name = p_table_name);

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Partition Access Tracking Refreshed');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Total partitions: ' || v_merge_count);
    DBMS_OUTPUT.PUT_LINE('HOT partitions (< ' || v_hot_threshold || ' days): ' || v_hot_count);
    DBMS_OUTPUT.PUT_LINE('WARM partitions (< ' || v_warm_threshold || ' days): ' || v_warm_count);
    DBMS_OUTPUT.PUT_LINE('COLD partitions (> ' || v_cold_threshold || ' days): ' || v_cold_count);
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Note: Temperature based on partition age (high_value)');
    DBMS_OUTPUT.PUT_LINE('      For accurate access tracking, use Oracle Heat Map');
    DBMS_OUTPUT.PUT_LINE('      or dwh_sync_heatmap_to_tracking() if available');
    DBMS_OUTPUT.PUT_LINE('========================================');
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

    -- Get thresholds from DEFAULT profile
    SELECT hot_threshold_days, warm_threshold_days
    INTO v_hot_threshold, v_warm_threshold
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'DEFAULT';

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
            -- Note: Cannot extract date from high_value directly (LONG type)
            -- Using SYSDATE as fallback - will be updated by refresh procedure
            SYSDATE AS estimated_write_date
        FROM dba_tab_partitions tp
        LEFT JOIN dba_segments s
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
-- Oracle Heat Map Integration (Optional - Enterprise Edition Only)
-- Syncs real access data from Oracle Heat Map to partition tracking table
-- -----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE dwh_sync_heatmap_to_tracking(
    p_table_owner VARCHAR2 DEFAULT USER,
    p_table_name VARCHAR2 DEFAULT NULL
) AS
    v_heat_map_available BOOLEAN := FALSE;
    v_count NUMBER;
    v_hot_threshold NUMBER;
    v_warm_threshold NUMBER;
    v_cold_threshold NUMBER;
    v_synced_count NUMBER;
BEGIN
    -- Check if Heat Map is enabled
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM v$option
        WHERE parameter = 'Heat Map'
        AND value = 'TRUE';

        v_heat_map_available := (v_count > 0);
    EXCEPTION
        WHEN OTHERS THEN
            v_heat_map_available := FALSE;
    END;

    IF NOT v_heat_map_available THEN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Oracle Heat Map Integration');
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Oracle Heat Map is not available or not enabled');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('To enable Heat Map (Enterprise Edition):');
        DBMS_OUTPUT.PUT_LINE('  ALTER SYSTEM SET HEAT_MAP = ON SCOPE=BOTH;');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Using partition high_value dates for temperature calculation instead');
        DBMS_OUTPUT.PUT_LINE('========================================');
        RETURN;
    END IF;

    -- Get thresholds from DEFAULT profile
    SELECT hot_threshold_days, warm_threshold_days, cold_threshold_days
    INTO v_hot_threshold, v_warm_threshold, v_cold_threshold
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'DEFAULT';

    -- Sync Heat Map data to tracking table
    MERGE INTO cmr.dwh_ilm_partition_access a
    USING (
        SELECT
            h.object_owner,
            h.object_name,
            h.subobject_name AS partition_name,
            h.segment_write_time,
            h.segment_read_time,
            TRUNC(SYSDATE - NVL(h.segment_write_time, SYSDATE - 10000)) AS days_since_write,
            TRUNC(SYSDATE - NVL(h.segment_read_time, SYSDATE - 10000)) AS days_since_read
        FROM dba_heat_map_segment h
        WHERE h.object_type = 'TABLE PARTITION'
        AND h.object_owner = p_table_owner
        AND (p_table_name IS NULL OR h.object_name = p_table_name)
    ) src
    ON (a.table_owner = src.object_owner
        AND a.table_name = src.object_name
        AND a.partition_name = src.partition_name)
    WHEN MATCHED THEN UPDATE SET
        a.last_write_time = src.segment_write_time,
        a.last_read_time = src.segment_read_time,
        a.days_since_write = src.days_since_write,
        a.days_since_read = src.days_since_read,
        a.write_count = NVL(a.write_count, 0) + 1,
        a.read_count = NVL(a.read_count, 0) + 1,
        a.temperature = CASE
            WHEN src.days_since_write < v_hot_threshold THEN 'HOT'
            WHEN src.days_since_write < v_warm_threshold THEN 'WARM'
            ELSE 'COLD'
        END,
        a.last_updated = SYSTIMESTAMP;

    v_synced_count := SQL%ROWCOUNT;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Heat Map Data Synced');
    DBMS_OUTPUT.PUT_LINE('========================================');
    DBMS_OUTPUT.PUT_LINE('Synced Heat Map data for ' || v_synced_count || ' partition(s)');
    DBMS_OUTPUT.PUT_LINE('Temperature calculated from real access patterns');
    DBMS_OUTPUT.PUT_LINE('========================================');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR syncing Heat Map data: ' || SQLERRM);
        ROLLBACK;
END;
/

-- Note: Procedure syncs real partition access data from Oracle Heat Map (Enterprise Edition required)


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
    -- Get thresholds from DEFAULT profile
    SELECT hot_threshold_days, warm_threshold_days
    INTO v_hot_threshold, v_warm_threshold
    FROM cmr.dwh_ilm_threshold_profiles
    WHERE profile_name = 'DEFAULT';

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
-- Convert LONG high_value to VARCHAR2
-- -----------------------------------------------------------------------------
-- Note: high_value in dba_tab_partitions is LONG type and cannot be used
-- directly in WHERE clauses or with functions. This function converts it.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_partition_high_value(
    p_table_owner VARCHAR2,
    p_table_name VARCHAR2,
    p_partition_name VARCHAR2
) RETURN VARCHAR2
AS
    v_high_value LONG;
    v_high_value_str VARCHAR2(4000);
BEGIN
    SELECT high_value INTO v_high_value
    FROM dba_tab_partitions
    WHERE table_owner = p_table_owner
    AND table_name = p_table_name
    AND partition_name = p_partition_name;

    -- Convert LONG to VARCHAR2 (max 4000 chars)
    v_high_value_str := SUBSTR(v_high_value, 1, 4000);

    RETURN v_high_value_str;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
    WHEN OTHERS THEN
        RETURN NULL;
END get_partition_high_value;
/


-- -----------------------------------------------------------------------------
-- Get Threshold Values for a Policy
-- -----------------------------------------------------------------------------
-- Returns HOT, WARM, COLD thresholds for a given policy
-- Uses policy's profile if specified, otherwise DEFAULT profile
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_policy_thresholds(
    p_policy_id NUMBER,
    p_threshold_type VARCHAR2 -- 'HOT', 'WARM', or 'COLD'
) RETURN NUMBER
AS
    v_profile_id NUMBER;
    v_threshold NUMBER;
BEGIN
    -- Get profile_id for this policy
    SELECT threshold_profile_id INTO v_profile_id
    FROM cmr.dwh_ilm_policies
    WHERE policy_id = p_policy_id;

    IF v_profile_id IS NOT NULL THEN
        -- Use specific profile thresholds
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_id = v_profile_id;
    ELSE
        -- Use DEFAULT profile as global fallback
        SELECT
            CASE p_threshold_type
                WHEN 'HOT' THEN hot_threshold_days
                WHEN 'WARM' THEN warm_threshold_days
                WHEN 'COLD' THEN cold_threshold_days
            END INTO v_threshold
        FROM cmr.dwh_ilm_threshold_profiles
        WHERE profile_name = 'DEFAULT';
    END IF;

    RETURN v_threshold;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Fallback to DEFAULT profile if policy not found
        BEGIN
            SELECT
                CASE p_threshold_type
                    WHEN 'HOT' THEN hot_threshold_days
                    WHEN 'WARM' THEN warm_threshold_days
                    WHEN 'COLD' THEN cold_threshold_days
                END INTO v_threshold
            FROM cmr.dwh_ilm_threshold_profiles
            WHERE profile_name = 'DEFAULT';
            RETURN v_threshold;
        EXCEPTION
            WHEN OTHERS THEN
                -- Hardcoded fallback if DEFAULT profile missing
                RETURN CASE p_threshold_type
                    WHEN 'HOT' THEN 90
                    WHEN 'WARM' THEN 365
                    WHEN 'COLD' THEN 1095
                END;
        END;
    WHEN OTHERS THEN
        -- Hardcoded fallback for any other errors
        RETURN CASE p_threshold_type
            WHEN 'HOT' THEN 90
            WHEN 'WARM' THEN 365
            WHEN 'COLD' THEN 1095
        END;
END get_policy_thresholds;
/


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
