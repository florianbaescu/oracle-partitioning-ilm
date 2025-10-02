-- =============================================================================
-- SCD2, Events, and Staging Table Partitioning Examples
-- Specialized patterns for slowly changing dimensions, event tables, and staging
-- =============================================================================

-- =============================================================================
-- SECTION 1: SCD TYPE 2 TABLES - EFFECTIVE_DATE PATTERN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Customer SCD2 with effective_date
-- Tracks customer history with single effective date column
-- -----------------------------------------------------------------------------

CREATE TABLE customer_dim_scd2 (
    customer_sk         NUMBER(18) NOT NULL,           -- Surrogate key
    customer_id         NUMBER(12) NOT NULL,           -- Business key
    customer_key        VARCHAR2(50),
    first_name          VARCHAR2(100),
    last_name           VARCHAR2(100),
    email               VARCHAR2(200),
    phone               VARCHAR2(30),
    address_line1       VARCHAR2(200),
    city                VARCHAR2(100),
    state_code          VARCHAR2(10),
    postal_code         VARCHAR2(20),
    customer_segment    VARCHAR2(50),
    loyalty_tier        VARCHAR2(30),

    -- SCD2 tracking columns
    effective_date      DATE NOT NULL,                 -- When this version became effective
    expiry_date         DATE DEFAULT DATE '9999-12-31',
    current_flag        CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),

    -- Audit columns
    created_by          VARCHAR2(50) DEFAULT USER,
    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_by          VARCHAR2(50),
    updated_date        TIMESTAMP,

    CONSTRAINT pk_customer_scd2 PRIMARY KEY (customer_sk)
)
PARTITION BY RANGE (effective_date)
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_history VALUES LESS THAN (DATE '2020-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes for SCD2 queries
CREATE INDEX idx_cust_scd2_id_eff ON customer_dim_scd2(customer_id, effective_date) LOCAL;
CREATE INDEX idx_cust_scd2_current ON customer_dim_scd2(customer_id, current_flag) LOCAL;
CREATE BITMAP INDEX idx_cust_scd2_flag ON customer_dim_scd2(current_flag) LOCAL;

COMMENT ON TABLE customer_dim_scd2 IS 'Customer SCD Type 2 - partitioned by effective_date';
COMMENT ON COLUMN customer_dim_scd2.customer_sk IS 'Surrogate key - unique for each version';
COMMENT ON COLUMN customer_dim_scd2.customer_id IS 'Business key - same across all versions';
COMMENT ON COLUMN customer_dim_scd2.effective_date IS 'Date this version became effective';
COMMENT ON COLUMN customer_dim_scd2.current_flag IS 'Y for current record, N for historical';


-- -----------------------------------------------------------------------------
-- Example 2: Product SCD2 with effective_date - Composite Partitioning
-- Large product catalog with frequent changes
-- -----------------------------------------------------------------------------

CREATE TABLE product_dim_scd2 (
    product_sk          NUMBER(18) NOT NULL,
    product_id          NUMBER(12) NOT NULL,
    product_key         VARCHAR2(50),
    product_name        VARCHAR2(200),
    category            VARCHAR2(50),
    subcategory         VARCHAR2(50),
    brand               VARCHAR2(100),
    unit_cost           NUMBER(12,2),
    list_price          NUMBER(12,2),
    product_status      VARCHAR2(20),

    -- SCD2 tracking
    effective_date      DATE NOT NULL,
    expiry_date         DATE DEFAULT DATE '9999-12-31',
    current_flag        CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),

    created_date        TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_product_scd2 PRIMARY KEY (product_sk)
)
PARTITION BY RANGE (effective_date)
SUBPARTITION BY HASH (product_id)
SUBPARTITIONS 8
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_2020 VALUES LESS THAN (DATE '2021-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_prod_scd2_id ON product_dim_scd2(product_id, effective_date, current_flag) LOCAL;
CREATE BITMAP INDEX idx_prod_scd2_current ON product_dim_scd2(current_flag) LOCAL;

COMMENT ON TABLE product_dim_scd2 IS 'Product SCD Type 2 - Range-Hash composite partitioning';


-- =============================================================================
-- SECTION 2: SCD TYPE 2 TABLES - VALID_FROM/VALID_TO PATTERN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 3: Employee SCD2 with valid_from_dttm/valid_to_dttm
-- Tracks employee history with precise timestamp ranges
-- -----------------------------------------------------------------------------

CREATE TABLE employee_dim_scd2 (
    employee_sk         NUMBER(18) NOT NULL,           -- Surrogate key
    employee_id         NUMBER(10) NOT NULL,           -- Business key
    employee_number     VARCHAR2(20),
    first_name          VARCHAR2(100),
    last_name           VARCHAR2(100),
    full_name           VARCHAR2(200),
    email               VARCHAR2(200),
    job_title           VARCHAR2(100),
    department          VARCHAR2(100),
    division            VARCHAR2(100),
    manager_id          NUMBER(10),
    location_id         NUMBER(8),
    salary_grade        VARCHAR2(20),
    employment_status   VARCHAR2(20),

    -- SCD2 validity tracking with timestamps
    valid_from_dttm     TIMESTAMP NOT NULL,            -- Start of validity period
    valid_to_dttm       TIMESTAMP DEFAULT TIMESTAMP '9999-12-31 23:59:59',  -- End of validity period
    is_current          CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),

    -- Audit
    load_dttm           TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_system       VARCHAR2(50),

    CONSTRAINT pk_employee_scd2 PRIMARY KEY (employee_sk)
)
PARTITION BY RANGE (valid_from_dttm)
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2020-01-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes optimized for temporal queries
CREATE INDEX idx_emp_scd2_id_valid ON employee_dim_scd2(employee_id, valid_from_dttm, valid_to_dttm) LOCAL;
CREATE INDEX idx_emp_scd2_current ON employee_dim_scd2(employee_id, is_current) LOCAL;
CREATE BITMAP INDEX idx_emp_scd2_is_current ON employee_dim_scd2(is_current) LOCAL;

COMMENT ON TABLE employee_dim_scd2 IS 'Employee SCD Type 2 - partitioned by valid_from_dttm';
COMMENT ON COLUMN employee_dim_scd2.valid_from_dttm IS 'Timestamp when this version became valid';
COMMENT ON COLUMN employee_dim_scd2.valid_to_dttm IS 'Timestamp when this version ceased to be valid';
COMMENT ON COLUMN employee_dim_scd2.is_current IS 'Y for current version, N for historical';


-- -----------------------------------------------------------------------------
-- Example 4: Account SCD2 with valid_from_dttm/valid_to_dttm - Composite
-- Banking accounts with frequent status changes
-- -----------------------------------------------------------------------------

CREATE TABLE account_dim_scd2 (
    account_sk          NUMBER(18) NOT NULL,
    account_id          NUMBER(12) NOT NULL,
    account_number      VARCHAR2(20),
    account_type        VARCHAR2(50),
    account_status      VARCHAR2(20),
    customer_id         NUMBER(12),
    branch_id           NUMBER(8),
    opening_date        DATE,
    credit_limit        NUMBER(15,2),
    interest_rate       NUMBER(5,4),

    -- SCD2 temporal tracking
    valid_from_dttm     TIMESTAMP NOT NULL,
    valid_to_dttm       TIMESTAMP DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current          CHAR(1) DEFAULT 'Y',
    record_version      NUMBER(5),

    load_dttm           TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_account_scd2 PRIMARY KEY (account_sk)
)
PARTITION BY RANGE (valid_from_dttm)
SUBPARTITION BY LIST (account_status)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp_active VALUES ('ACTIVE', 'OPEN'),
    SUBPARTITION sp_inactive VALUES ('INACTIVE', 'DORMANT'),
    SUBPARTITION sp_closed VALUES ('CLOSED', 'TERMINATED'),
    SUBPARTITION sp_other VALUES (DEFAULT)
)
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_2020 VALUES LESS THAN (TIMESTAMP '2021-01-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_acct_scd2_id_valid ON account_dim_scd2(account_id, valid_from_dttm, valid_to_dttm) LOCAL;
CREATE INDEX idx_acct_scd2_current ON account_dim_scd2(account_id, is_current) LOCAL;

COMMENT ON TABLE account_dim_scd2 IS 'Account SCD Type 2 - Range-List composite partitioning';


-- =============================================================================
-- SECTION 3: EVENTS TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 5: Application Events - High Volume Clickstream
-- Partitioned by event timestamp with short retention
-- -----------------------------------------------------------------------------

CREATE TABLE app_events (
    event_id            NUMBER(18) NOT NULL,
    event_dttm          TIMESTAMP NOT NULL,
    event_type          VARCHAR2(50) NOT NULL,
    event_category      VARCHAR2(30),
    session_id          VARCHAR2(100),
    user_id             NUMBER(12),
    device_id           VARCHAR2(100),
    app_version         VARCHAR2(20),
    platform            VARCHAR2(30),
    event_properties    CLOB,                          -- JSON event data

    -- Context
    ip_address          VARCHAR2(45),
    user_agent          VARCHAR2(500),
    referrer            VARCHAR2(500),
    page_url            VARCHAR2(1000),

    -- Metadata
    ingestion_dttm      TIMESTAMP DEFAULT SYSTIMESTAMP,
    partition_key       DATE,                          -- Computed column for partitioning

    CONSTRAINT pk_app_events PRIMARY KEY (event_id, event_dttm)
)
PARTITION BY RANGE (event_dttm)
SUBPARTITION BY LIST (event_type)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp_page_events VALUES ('PAGE_VIEW', 'PAGE_LOAD', 'PAGE_EXIT'),
    SUBPARTITION sp_click_events VALUES ('CLICK', 'BUTTON_CLICK', 'LINK_CLICK'),
    SUBPARTITION sp_form_events VALUES ('FORM_START', 'FORM_SUBMIT', 'FORM_ERROR'),
    SUBPARTITION sp_transaction VALUES ('ADD_TO_CART', 'CHECKOUT', 'PURCHASE', 'PAYMENT'),
    SUBPARTITION sp_user_events VALUES ('LOGIN', 'LOGOUT', 'REGISTER', 'PROFILE_UPDATE'),
    SUBPARTITION sp_error_events VALUES ('ERROR', 'EXCEPTION', 'CRASH'),
    SUBPARTITION sp_other VALUES (DEFAULT)
)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))                   -- Daily partitions
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes for common event queries
CREATE INDEX idx_app_events_session ON app_events(session_id, event_dttm) LOCAL;
CREATE INDEX idx_app_events_user ON app_events(user_id, event_dttm) LOCAL;
CREATE INDEX idx_app_events_type ON app_events(event_type, event_dttm) LOCAL;

COMMENT ON TABLE app_events IS 'Application events - daily partitions with 90-day retention';
COMMENT ON COLUMN app_events.event_properties IS 'JSON containing event-specific data';


-- -----------------------------------------------------------------------------
-- Example 6: Audit Events - Compliance and Security
-- Longer retention, partitioned by audit timestamp
-- -----------------------------------------------------------------------------

CREATE TABLE audit_events (
    audit_id            NUMBER(18) NOT NULL,
    audit_dttm          TIMESTAMP NOT NULL,
    event_type          VARCHAR2(50) NOT NULL,         -- LOGIN, LOGOUT, DATA_ACCESS, DATA_CHANGE, etc.
    event_severity      VARCHAR2(20),                  -- INFO, WARNING, CRITICAL

    -- Who
    user_id             NUMBER(12),
    username            VARCHAR2(100),
    user_role           VARCHAR2(50),

    -- What
    object_type         VARCHAR2(50),                  -- TABLE, VIEW, PROCEDURE, etc.
    object_name         VARCHAR2(200),
    action_performed    VARCHAR2(50),                  -- SELECT, INSERT, UPDATE, DELETE, EXECUTE

    -- Where
    source_ip           VARCHAR2(45),
    source_host         VARCHAR2(200),
    application         VARCHAR2(100),
    database_name       VARCHAR2(30),

    -- Details
    sql_text            CLOB,
    rows_affected       NUMBER,
    success_flag        CHAR(1),
    error_code          NUMBER,
    error_message       VARCHAR2(4000),

    -- Context
    session_id          NUMBER,
    transaction_id      VARCHAR2(100),
    additional_info     CLOB,                          -- JSON for additional context

    ingestion_dttm      TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_audit_events PRIMARY KEY (audit_id, audit_dttm)
)
PARTITION BY RANGE (audit_dttm)
SUBPARTITION BY LIST (event_severity)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp_critical VALUES ('CRITICAL', 'SECURITY_VIOLATION'),
    SUBPARTITION sp_warning VALUES ('WARNING', 'SUSPICIOUS'),
    SUBPARTITION sp_info VALUES ('INFO', 'DEBUG'),
    SUBPARTITION sp_other VALUES (DEFAULT)
)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))                 -- Monthly partitions
(
    PARTITION p_2024_01 VALUES LESS THAN (TIMESTAMP '2024-02-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_audit_user ON audit_events(user_id, audit_dttm) LOCAL;
CREATE INDEX idx_audit_object ON audit_events(object_name, audit_dttm) LOCAL;
CREATE INDEX idx_audit_type ON audit_events(event_type, audit_dttm) LOCAL;
CREATE BITMAP INDEX idx_audit_severity ON audit_events(event_severity) LOCAL;

COMMENT ON TABLE audit_events IS 'Audit trail - monthly partitions with 7-year retention for compliance';


-- -----------------------------------------------------------------------------
-- Example 7: IoT Sensor Events - Time-series data
-- High-frequency sensor readings
-- -----------------------------------------------------------------------------

CREATE TABLE iot_sensor_events (
    reading_id          NUMBER(18) NOT NULL,
    event_dttm          TIMESTAMP NOT NULL,
    device_id           NUMBER(12) NOT NULL,
    sensor_id           VARCHAR2(50),
    sensor_type         VARCHAR2(30),

    -- Measurements
    metric_name         VARCHAR2(50),
    metric_value        NUMBER(15,6),
    unit_of_measure     VARCHAR2(20),
    quality_score       NUMBER(3),

    -- Location context
    location_id         NUMBER(8),
    latitude            NUMBER(10,7),
    longitude           NUMBER(10,7),

    -- Device state
    battery_level       NUMBER(3),
    signal_strength     NUMBER(3),
    device_status       VARCHAR2(20),

    ingestion_dttm      TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_iot_events PRIMARY KEY (reading_id, event_dttm, device_id)
)
PARTITION BY RANGE (event_dttm)
SUBPARTITION BY HASH (device_id)
SUBPARTITIONS 16
INTERVAL (NUMTODSINTERVAL(1,'DAY'))                   -- Daily partitions
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_iot_device ON iot_sensor_events(device_id, event_dttm) LOCAL;
CREATE INDEX idx_iot_sensor ON iot_sensor_events(sensor_id, event_dttm) LOCAL;
CREATE INDEX idx_iot_location ON iot_sensor_events(location_id, event_dttm) LOCAL;

COMMENT ON TABLE iot_sensor_events IS 'IoT sensor readings - daily partitions with 90-day retention';


-- =============================================================================
-- SECTION 4: STAGING TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 8: Transactional Staging - Daily Load Window
-- Staging area for fact table loads
-- -----------------------------------------------------------------------------

CREATE TABLE stg_sales_transactions (
    staging_id          NUMBER(18) NOT NULL,
    load_batch_id       NUMBER(12) NOT NULL,
    load_dttm           TIMESTAMP DEFAULT SYSTIMESTAMP,

    -- Source data
    transaction_id      NUMBER(18),
    transaction_date    DATE,
    customer_id         NUMBER(12),
    product_id          NUMBER(12),
    quantity            NUMBER(10,2),
    unit_price          NUMBER(12,2),
    total_amount        NUMBER(12,2),

    -- Staging metadata
    source_system       VARCHAR2(50),
    source_file_name    VARCHAR2(200),
    record_number       NUMBER,

    -- Processing status
    processing_status   VARCHAR2(20) DEFAULT 'NEW',    -- NEW, PROCESSING, LOADED, ERROR
    error_message       VARCHAR2(4000),
    processed_dttm      TIMESTAMP,

    CONSTRAINT pk_stg_sales PRIMARY KEY (staging_id, load_dttm)
)
PARTITION BY RANGE (load_dttm)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
)
TABLESPACE users
NOLOGGING                                             -- Faster loads, not critical data
PARALLEL 8;

CREATE INDEX idx_stg_sales_batch ON stg_sales_transactions(load_batch_id, processing_status) LOCAL;
CREATE INDEX idx_stg_sales_status ON stg_sales_transactions(processing_status, load_dttm) LOCAL;

COMMENT ON TABLE stg_sales_transactions IS 'Sales staging - daily partitions with 7-day retention';


-- -----------------------------------------------------------------------------
-- Example 9: CDC Staging - Change Data Capture
-- Captures changes from source systems
-- -----------------------------------------------------------------------------

CREATE TABLE stg_customer_cdc (
    cdc_id              NUMBER(18) NOT NULL,
    capture_dttm        TIMESTAMP NOT NULL,
    operation_type      VARCHAR2(10) NOT NULL,         -- INSERT, UPDATE, DELETE

    -- Source key
    source_system       VARCHAR2(50),
    customer_id         NUMBER(12),

    -- Changed attributes (all nullable to support deletes)
    first_name          VARCHAR2(100),
    last_name           VARCHAR2(100),
    email               VARCHAR2(200),
    phone               VARCHAR2(30),
    address_line1       VARCHAR2(200),
    city                VARCHAR2(100),
    state_code          VARCHAR2(10),

    -- CDC metadata
    source_timestamp    TIMESTAMP,
    source_scn          NUMBER,
    source_txn_id       VARCHAR2(100),

    -- Processing
    processing_status   VARCHAR2(20) DEFAULT 'PENDING', -- PENDING, PROCESSED, ERROR
    processed_dttm      TIMESTAMP,
    target_sk           NUMBER(18),                     -- Link to dimension surrogate key

    CONSTRAINT pk_stg_cdc PRIMARY KEY (cdc_id, capture_dttm)
)
PARTITION BY RANGE (capture_dttm)
SUBPARTITION BY LIST (operation_type)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp_insert VALUES ('INSERT'),
    SUBPARTITION sp_update VALUES ('UPDATE'),
    SUBPARTITION sp_delete VALUES ('DELETE'),
    SUBPARTITION sp_other VALUES (DEFAULT)
)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
)
TABLESPACE users
NOLOGGING
PARALLEL 4;

CREATE INDEX idx_stg_cdc_customer ON stg_customer_cdc(customer_id, capture_dttm) LOCAL;
CREATE INDEX idx_stg_cdc_status ON stg_customer_cdc(processing_status, capture_dttm) LOCAL;

COMMENT ON TABLE stg_customer_cdc IS 'CDC staging - daily partitions with 30-day retention';


-- -----------------------------------------------------------------------------
-- Example 10: Bulk Load Staging - Large File Imports
-- Partitioned by load batch for easy exchange partition
-- -----------------------------------------------------------------------------

CREATE TABLE stg_bulk_import (
    row_id              NUMBER(18) NOT NULL,
    load_batch_id       NUMBER(12) NOT NULL,
    load_date           DATE NOT NULL,

    -- Data columns (example for product import)
    product_code        VARCHAR2(50),
    product_name        VARCHAR2(200),
    category            VARCHAR2(50),
    unit_price          NUMBER(12,2),
    supplier_code       VARCHAR2(50),

    -- File metadata
    source_file         VARCHAR2(200),
    file_row_number     NUMBER,

    -- Validation
    validation_status   VARCHAR2(20) DEFAULT 'PENDING',
    validation_errors   VARCHAR2(4000),

    load_dttm           TIMESTAMP DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_stg_bulk PRIMARY KEY (row_id, load_batch_id)
)
PARTITION BY LIST (load_batch_id)
(
    PARTITION p_batch_default VALUES (DEFAULT)
)
TABLESPACE users
NOLOGGING;

COMMENT ON TABLE stg_bulk_import IS 'Bulk import staging - list partitioned by batch for exchange';


-- -----------------------------------------------------------------------------
-- Example 11: Error Staging/Quarantine Table
-- Stores rejected records for review and reprocessing
-- -----------------------------------------------------------------------------

CREATE TABLE stg_error_quarantine (
    error_id            NUMBER(18) NOT NULL,
    error_dttm          TIMESTAMP DEFAULT SYSTIMESTAMP,
    source_table        VARCHAR2(128),
    load_batch_id       NUMBER(12),

    -- Error classification
    error_type          VARCHAR2(50),                  -- VALIDATION, CONSTRAINT, TRANSFORM, LOOKUP
    error_severity      VARCHAR2(20),                  -- WARNING, ERROR, CRITICAL
    error_code          VARCHAR2(20),
    error_message       VARCHAR2(4000),

    -- Rejected record (stored as CLOB for flexibility)
    rejected_record     CLOB,                          -- Can be CSV, JSON, or XML

    -- Processing
    reprocess_flag      CHAR(1) DEFAULT 'N',
    reprocessed_dttm    TIMESTAMP,
    resolution_notes    VARCHAR2(2000),

    CONSTRAINT pk_stg_error PRIMARY KEY (error_id, error_dttm)
)
PARTITION BY RANGE (error_dttm)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2024-01-01 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY LOW;

CREATE INDEX idx_stg_error_source ON stg_error_quarantine(source_table, error_dttm) LOCAL;
CREATE INDEX idx_stg_error_batch ON stg_error_quarantine(load_batch_id) LOCAL;
CREATE INDEX idx_stg_error_reprocess ON stg_error_quarantine(reprocess_flag, error_dttm) LOCAL;

COMMENT ON TABLE stg_error_quarantine IS 'Error quarantine - monthly partitions with 1-year retention';


-- =============================================================================
-- SECTION 5: HISTORICAL/SNAPSHOT TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 12: Monthly Historical Snapshot Table
-- Stores end-of-month snapshots of account balances
-- -----------------------------------------------------------------------------

CREATE TABLE hist_account_balances (
    snapshot_id         NUMBER(18) NOT NULL,
    hist_date           DATE NOT NULL,                    -- Snapshot date (end of month)

    -- Account identification
    account_id          NUMBER(12) NOT NULL,
    account_number      VARCHAR2(50) NOT NULL,
    account_type        VARCHAR2(30),
    customer_id         NUMBER(12),

    -- Balance information
    opening_balance     NUMBER(15,2),
    closing_balance     NUMBER(15,2),
    average_balance     NUMBER(15,2),
    min_balance         NUMBER(15,2),
    max_balance         NUMBER(15,2),

    -- Transaction statistics
    total_deposits      NUMBER(15,2),
    total_withdrawals   NUMBER(15,2),
    transaction_count   NUMBER(8),

    -- Status
    account_status      VARCHAR2(20),
    is_active           CHAR(1),

    -- Audit
    created_dttm        TIMESTAMP DEFAULT SYSTIMESTAMP,
    snapshot_batch_id   NUMBER(12),

    CONSTRAINT pk_hist_acct_bal PRIMARY KEY (snapshot_id, hist_date)
)
PARTITION BY RANGE (hist_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2020 VALUES LESS THAN (DATE '2021-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_hist_acct_bal_acct ON hist_account_balances(account_id, hist_date) LOCAL;

COMMENT ON TABLE hist_account_balances IS 'Monthly snapshots of account balances - 3 year retention';


-- -----------------------------------------------------------------------------
-- Example 13: Yearly Historical Snapshot Table
-- Stores end-of-year financial snapshots for regulatory compliance
-- -----------------------------------------------------------------------------

CREATE TABLE hist_annual_financials (
    snapshot_id         NUMBER(18) NOT NULL,
    history_date        DATE NOT NULL,                    -- Year-end date (Dec 31)
    fiscal_year         NUMBER(4) NOT NULL,

    -- Entity identification
    company_code        VARCHAR2(10) NOT NULL,
    business_unit       VARCHAR2(50),
    cost_center         VARCHAR2(20),

    -- Financial metrics
    total_revenue       NUMBER(18,2),
    total_expenses      NUMBER(18,2),
    net_income          NUMBER(18,2),
    total_assets        NUMBER(18,2),
    total_liabilities   NUMBER(18,2),
    shareholders_equity NUMBER(18,2),

    -- Ratios and KPIs
    profit_margin_pct   NUMBER(5,2),
    roe_pct             NUMBER(5,2),
    debt_to_equity      NUMBER(5,2),

    -- Compliance flags
    audited             CHAR(1) DEFAULT 'N',
    certified           CHAR(1) DEFAULT 'N',
    filed_with_sec      CHAR(1) DEFAULT 'N',

    -- Audit trail
    snapshot_dttm       TIMESTAMP DEFAULT SYSTIMESTAMP,
    created_by          VARCHAR2(50) DEFAULT USER,
    source_system       VARCHAR2(50),

    CONSTRAINT pk_hist_annual_fin PRIMARY KEY (snapshot_id, history_date)
)
PARTITION BY RANGE (history_date)
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_2015 VALUES LESS THAN (DATE '2016-01-01')
)
TABLESPACE users
COMPRESS FOR ARCHIVE HIGH;

CREATE INDEX idx_hist_annual_company ON hist_annual_financials(company_code, history_date) LOCAL;

COMMENT ON TABLE hist_annual_financials IS 'Annual financial snapshots - 7+ year retention for compliance';


-- -----------------------------------------------------------------------------
-- Example 14: Daily Historical Archive Table
-- Archives daily state of inventory for supply chain analysis
-- -----------------------------------------------------------------------------

CREATE TABLE hist_daily_inventory (
    archive_id          NUMBER(18) NOT NULL,
    archive_date        DATE NOT NULL,                    -- Date of snapshot

    -- Item identification
    item_id             NUMBER(12) NOT NULL,
    item_sku            VARCHAR2(50) NOT NULL,
    warehouse_id        NUMBER(8) NOT NULL,
    location_code       VARCHAR2(30),

    -- Inventory metrics
    quantity_on_hand    NUMBER(12,2),
    quantity_reserved   NUMBER(12,2),
    quantity_available  NUMBER(12,2),
    reorder_point       NUMBER(12,2),
    reorder_quantity    NUMBER(12,2),

    -- Valuation
    unit_cost           NUMBER(12,4),
    total_value         NUMBER(15,2),

    -- Aging
    avg_age_days        NUMBER(5,2),
    oldest_receipt_date DATE,

    -- Status
    stock_status        VARCHAR2(20),
    needs_replenishment CHAR(1),

    -- Metadata
    snapshot_timestamp  TIMESTAMP NOT NULL,
    created_by          VARCHAR2(50) DEFAULT USER,

    CONSTRAINT pk_hist_daily_inv PRIMARY KEY (archive_id, archive_date)
)
PARTITION BY RANGE (archive_date)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))
(
    PARTITION p_2024_q1 VALUES LESS THAN (DATE '2024-04-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

CREATE INDEX idx_hist_daily_inv_item ON hist_daily_inventory(item_id, archive_date) LOCAL;
CREATE INDEX idx_hist_daily_inv_wh ON hist_daily_inventory(warehouse_id, archive_date) LOCAL;

COMMENT ON TABLE hist_daily_inventory IS 'Daily inventory snapshots - 90 day retention with rolling purge';


-- =============================================================================
-- SECTION 6: COMMON QUERIES FOR EACH TABLE TYPE
-- =============================================================================

-- SCD2 Queries - effective_date pattern
-- Get current version of a customer
SELECT * FROM customer_dim_scd2
WHERE customer_id = 12345
AND current_flag = 'Y';

-- Get customer as of specific date
SELECT * FROM customer_dim_scd2
WHERE customer_id = 12345
AND effective_date <= DATE '2023-06-15'
AND expiry_date > DATE '2023-06-15';

-- Get all versions of a customer
SELECT customer_id, effective_date, expiry_date, version_number, email, customer_segment
FROM customer_dim_scd2
WHERE customer_id = 12345
ORDER BY effective_date;


-- SCD2 Queries - valid_from/valid_to pattern
-- Get current version of an employee
SELECT * FROM employee_dim_scd2
WHERE employee_id = 54321
AND is_current = 'Y';

-- Get employee as of specific timestamp
SELECT * FROM employee_dim_scd2
WHERE employee_id = 54321
AND valid_from_dttm <= TIMESTAMP '2023-06-15 14:30:00'
AND valid_to_dttm > TIMESTAMP '2023-06-15 14:30:00';

-- Find all employees in a department during a time period
SELECT DISTINCT employee_id, full_name, job_title
FROM employee_dim_scd2
WHERE department = 'SALES'
AND valid_from_dttm <= TIMESTAMP '2023-12-31 23:59:59'
AND valid_to_dttm > TIMESTAMP '2023-01-01 00:00:00';


-- Events Queries
-- Recent events by user
SELECT event_type, event_dttm, page_url
FROM app_events
WHERE user_id = 789
AND event_dttm >= SYSTIMESTAMP - INTERVAL '1' HOUR
ORDER BY event_dttm DESC;

-- Event counts by type for today
SELECT event_type, COUNT(*) as event_count
FROM app_events
WHERE event_dttm >= TRUNC(SYSDATE)
GROUP BY event_type
ORDER BY event_count DESC;


-- Staging Queries
-- Check load status
SELECT load_batch_id, processing_status, COUNT(*) as record_count
FROM stg_sales_transactions
WHERE load_dttm >= TRUNC(SYSDATE)
GROUP BY load_batch_id, processing_status;

-- Find errors in staging
SELECT * FROM stg_sales_transactions
WHERE processing_status = 'ERROR'
AND load_dttm >= TRUNC(SYSDATE);
