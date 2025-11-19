-- =============================================================================
-- DWH Constants Package
-- =============================================================================
-- Centralized constants for the Data Warehouse ILM and Migration Framework
-- Single source of truth for all magic values, defaults, and configuration
-- =============================================================================

-- Package Specification
CREATE OR REPLACE PACKAGE pck_dwh_constants AS
    
    -- ==========================================================================
    -- Partition Boundaries
    -- ==========================================================================
    -- Maximum date for MAXVALUE partitions (future boundary)
    c_maxvalue_date CONSTANT DATE := TO_DATE('5999-01-01', 'YYYY-MM-DD');
    c_maxvalue_timestamp CONSTANT TIMESTAMP := TO_TIMESTAMP('5999-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    
    -- Minimum date for historical data (past boundary)
    c_minvalue_date CONSTANT DATE := TO_DATE('1900-01-01', 'YYYY-MM-DD');
    c_minvalue_timestamp CONSTANT TIMESTAMP := TO_TIMESTAMP('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    
    -- ==========================================================================
    -- Date Conversion
    -- ==========================================================================
    -- Average days per month for month-to-day conversion
    c_days_per_month CONSTANT NUMBER := 30.44;
    
    -- Average days per year
    c_days_per_year CONSTANT NUMBER := 365.25;
    
    -- ==========================================================================
    -- Default Thresholds
    -- ==========================================================================
    -- Default temperature thresholds (days)
    c_default_hot_threshold CONSTANT NUMBER := 730;   -- 2 years
    c_default_warm_threshold CONSTANT NUMBER := 1825; -- 5 years
    c_default_cold_threshold CONSTANT NUMBER := 1825; -- 5 years
    
    -- ==========================================================================
    -- Partition Naming
    -- ==========================================================================
    -- Prefix for partition names
    c_partition_prefix CONSTANT VARCHAR2(10) := 'P_';
    
    -- MAXVALUE partition name
    c_maxvalue_partition CONSTANT VARCHAR2(30) := 'P_MAXVALUE';
    
    -- ==========================================================================
    -- Tablespace Names
    -- ==========================================================================
    c_tablespace_hot CONSTANT VARCHAR2(30) := 'TBS_HOT';
    c_tablespace_warm CONSTANT VARCHAR2(30) := 'TBS_WARM';
    c_tablespace_cold CONSTANT VARCHAR2(30) := 'TBS_COLD';
    
    -- ==========================================================================
    -- Compression Types
    -- ==========================================================================
    c_compression_none CONSTANT VARCHAR2(20) := 'NONE';
    c_compression_basic CONSTANT VARCHAR2(20) := 'BASIC';
    c_compression_oltp CONSTANT VARCHAR2(20) := 'OLTP';
    c_compression_query_low CONSTANT VARCHAR2(20) := 'QUERY LOW';
    c_compression_query_high CONSTANT VARCHAR2(20) := 'QUERY HIGH';
    c_compression_archive_low CONSTANT VARCHAR2(20) := 'ARCHIVE LOW';
    c_compression_archive_high CONSTANT VARCHAR2(20) := 'ARCHIVE HIGH';
    
    -- ==========================================================================
    -- Temperature Classifications
    -- ==========================================================================
    c_temp_hot CONSTANT VARCHAR2(10) := 'HOT';
    c_temp_warm CONSTANT VARCHAR2(10) := 'WARM';
    c_temp_cold CONSTANT VARCHAR2(10) := 'COLD';
    
    -- ==========================================================================
    -- Status Values
    -- ==========================================================================
    c_status_pending CONSTANT VARCHAR2(20) := 'PENDING';
    c_status_in_progress CONSTANT VARCHAR2(20) := 'IN_PROGRESS';
    c_status_completed CONSTANT VARCHAR2(20) := 'COMPLETED';
    c_status_failed CONSTANT VARCHAR2(20) := 'FAILED';
    c_status_skipped CONSTANT VARCHAR2(20) := 'SKIPPED';
    c_status_success CONSTANT VARCHAR2(20) := 'SUCCESS';
    c_status_warning CONSTANT VARCHAR2(20) := 'WARNING';
    c_status_ready CONSTANT VARCHAR2(20) := 'READY';
    
    -- ==========================================================================
    -- ILM Actions
    -- ==========================================================================
    c_action_compress CONSTANT VARCHAR2(20) := 'COMPRESS';
    c_action_move CONSTANT VARCHAR2(20) := 'MOVE';
    c_action_read_only CONSTANT VARCHAR2(20) := 'READ_ONLY';
    c_action_drop CONSTANT VARCHAR2(20) := 'DROP';
    c_action_truncate CONSTANT VARCHAR2(20) := 'TRUNCATE';
    
    -- ==========================================================================
    -- Policy Types
    -- ==========================================================================
    c_policy_compression CONSTANT VARCHAR2(20) := 'COMPRESSION';
    c_policy_tiering CONSTANT VARCHAR2(20) := 'TIERING';
    c_policy_archival CONSTANT VARCHAR2(20) := 'ARCHIVAL';
    c_policy_purge CONSTANT VARCHAR2(20) := 'PURGE';
    c_policy_custom CONSTANT VARCHAR2(20) := 'CUSTOM';
    
    -- ==========================================================================
    -- Interval Types
    -- ==========================================================================
    c_interval_daily CONSTANT VARCHAR2(20) := 'DAILY';
    c_interval_weekly CONSTANT VARCHAR2(20) := 'WEEKLY';
    c_interval_monthly CONSTANT VARCHAR2(20) := 'MONTHLY';
    c_interval_yearly CONSTANT VARCHAR2(20) := 'YEARLY';
    
    -- ==========================================================================
    -- Configuration Keys
    -- ==========================================================================
    c_config_auto_merge CONSTANT VARCHAR2(50) := 'AUTO_MERGE_PARTITIONS';
    c_config_merge_timeout CONSTANT VARCHAR2(50) := 'MERGE_LOCK_TIMEOUT';
    c_config_log_retention CONSTANT VARCHAR2(50) := 'LOG_RETENTION_DAYS';
    
    -- ==========================================================================
    -- Threshold Profile Names
    -- ==========================================================================
    c_profile_default CONSTANT VARCHAR2(50) := 'DEFAULT';
    c_profile_fast_aging CONSTANT VARCHAR2(50) := 'FAST_AGING';
    c_profile_slow_aging CONSTANT VARCHAR2(50) := 'SLOW_AGING';
    c_profile_aggressive CONSTANT VARCHAR2(50) := 'AGGRESSIVE_ARCHIVE';
    
    -- ==========================================================================
    -- Other Constants
    -- ==========================================================================
    -- Very old age for partitions with unknown date
    c_unknown_age_days CONSTANT NUMBER := 10000;
    
    -- Suffix for auto-generated threshold profiles
    c_threshold_suffix CONSTANT VARCHAR2(20) := '_THRESHOLDS';
    
END pck_dwh_constants;
/

SHOW ERRORS PACKAGE pck_dwh_constants;

-- Package Body (empty for now, but allows future expansion)
CREATE OR REPLACE PACKAGE BODY pck_dwh_constants AS
    -- No implementation needed for constants-only package
    -- This body exists to allow future helper functions if needed
END pck_dwh_constants;
/

SHOW ERRORS PACKAGE BODY pck_dwh_constants;

-- Grant execute to public (or specific schemas as needed)
-- GRANT EXECUTE ON pck_dwh_constants TO <schema>;

PROMPT
PROMPT ========================================
PROMPT Constants Package Created Successfully
PROMPT ========================================
PROMPT
PROMPT Usage Examples:
PROMPT   - pck_dwh_constants.c_maxvalue_date
PROMPT   - pck_dwh_constants.c_temp_hot
PROMPT   - pck_dwh_constants.c_compression_basic
PROMPT
