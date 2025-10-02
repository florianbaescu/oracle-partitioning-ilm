-- =============================================================================
-- Fact Table Partitioning Examples
-- Data Warehouse Best Practices for Oracle
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Sales Fact Table - Range Partitioning by Month with Interval
-- -----------------------------------------------------------------------------

CREATE TABLE sales_fact (
    sale_id             NUMBER(18) NOT NULL,
    sale_date           DATE NOT NULL,
    customer_id         NUMBER(12) NOT NULL,
    product_id          NUMBER(12) NOT NULL,
    store_id            NUMBER(8) NOT NULL,
    quantity            NUMBER(10,2),
    unit_price          NUMBER(12,2),
    discount_amount     NUMBER(12,2),
    tax_amount          NUMBER(12,2),
    total_amount        NUMBER(12,2),
    currency_code       VARCHAR2(3),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_sales_fact PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2023_12 VALUES LESS THAN (DATE '2024-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH
PARALLEL 8;

-- Local indexes for partition key and frequent filters
CREATE INDEX idx_sales_customer ON sales_fact(customer_id, sale_date) LOCAL PARALLEL 8;
CREATE INDEX idx_sales_product ON sales_fact(product_id, sale_date) LOCAL PARALLEL 8;
CREATE INDEX idx_sales_store ON sales_fact(store_id, sale_date) LOCAL PARALLEL 8;

-- Bitmap index for low-cardinality column (consider compression)
CREATE BITMAP INDEX idx_sales_currency ON sales_fact(currency_code) LOCAL PARALLEL 8;

-- Gather statistics
EXEC DBMS_STATS.GATHER_TABLE_STATS('SALES_FACT', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE sales_fact IS 'Sales transactions fact table - partitioned monthly with automatic ILM';


-- -----------------------------------------------------------------------------
-- Example 2: Order Fact Table - Range-Hash Composite Partitioning
-- High volume, requires even distribution within time periods
-- -----------------------------------------------------------------------------

CREATE TABLE order_fact (
    order_id            NUMBER(18) NOT NULL,
    order_date          DATE NOT NULL,
    customer_id         NUMBER(12) NOT NULL,
    order_status        VARCHAR2(20),
    order_total         NUMBER(15,2),
    shipping_cost       NUMBER(10,2),
    payment_method      VARCHAR2(30),
    warehouse_id        NUMBER(8),
    shipping_address_id NUMBER(12),
    created_by          VARCHAR2(50),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_order_fact PRIMARY KEY (order_id, order_date)
)
PARTITION BY RANGE (order_date)
SUBPARTITION BY HASH (order_id)
SUBPARTITIONS 8
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2023_12 VALUES LESS THAN (DATE '2024-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH
PARALLEL 8;

-- Local indexes
CREATE INDEX idx_order_customer ON order_fact(customer_id, order_date) LOCAL PARALLEL 8;
CREATE INDEX idx_order_status ON order_fact(order_status, order_date) LOCAL PARALLEL 8;

-- Global index for order_id lookups (when date unknown)
CREATE INDEX idx_order_id_global ON order_fact(order_id) GLOBAL PARALLEL 8;

EXEC DBMS_STATS.GATHER_TABLE_STATS('ORDER_FACT', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE order_fact IS 'Order transactions - range-hash composite partitioning for even distribution';


-- -----------------------------------------------------------------------------
-- Example 3: Web Events Fact - Range-List Composite Partitioning
-- Partitioned by date, sub-partitioned by event type
-- -----------------------------------------------------------------------------

CREATE TABLE web_events_fact (
    event_id            NUMBER(18) NOT NULL,
    event_timestamp     TIMESTAMP NOT NULL,
    event_type          VARCHAR2(50) NOT NULL,
    session_id          VARCHAR2(100),
    user_id             NUMBER(12),
    page_url            VARCHAR2(500),
    referrer_url        VARCHAR2(500),
    user_agent          VARCHAR2(200),
    ip_address          VARCHAR2(45),
    country_code        VARCHAR2(2),
    device_type         VARCHAR2(20),
    event_data          CLOB,
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_web_events PRIMARY KEY (event_id, event_timestamp, event_type)
)
PARTITION BY RANGE (event_timestamp)
SUBPARTITION BY LIST (event_type)
SUBPARTITION TEMPLATE (
    SUBPARTITION sp_page_view VALUES ('PAGE_VIEW'),
    SUBPARTITION sp_click VALUES ('CLICK', 'BUTTON_CLICK', 'LINK_CLICK'),
    SUBPARTITION sp_form VALUES ('FORM_SUBMIT', 'FORM_ERROR'),
    SUBPARTITION sp_purchase VALUES ('ADD_TO_CART', 'CHECKOUT', 'PURCHASE'),
    SUBPARTITION sp_other VALUES (DEFAULT)
)
INTERVAL (NUMTODSINTERVAL(1,'DAY'))
(
    PARTITION p_2024_01_01 VALUES LESS THAN (TIMESTAMP '2024-01-02 00:00:00')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH
PARALLEL 8;

-- Local indexes on frequently queried columns
CREATE INDEX idx_web_session ON web_events_fact(session_id, event_timestamp) LOCAL PARALLEL 8;
CREATE INDEX idx_web_user ON web_events_fact(user_id, event_timestamp) LOCAL PARALLEL 8;
CREATE INDEX idx_web_country ON web_events_fact(country_code, event_timestamp) LOCAL PARALLEL 8;

EXEC DBMS_STATS.GATHER_TABLE_STATS('WEB_EVENTS_FACT', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE web_events_fact IS 'Web clickstream events - daily partitions with event type sub-partitions';


-- -----------------------------------------------------------------------------
-- Example 4: Financial Transactions - Range Partitioning by Quarter
-- Compliance requirements for 7 years retention with different storage tiers
-- -----------------------------------------------------------------------------

CREATE TABLE financial_transactions (
    transaction_id      NUMBER(18) NOT NULL,
    transaction_date    DATE NOT NULL,
    account_id          NUMBER(12) NOT NULL,
    transaction_type    VARCHAR2(50),
    debit_amount        NUMBER(18,2),
    credit_amount       NUMBER(18,2),
    balance_after       NUMBER(18,2),
    currency_code       VARCHAR2(3),
    description         VARCHAR2(500),
    reference_number    VARCHAR2(100),
    posted_by           VARCHAR2(50),
    approved_by         VARCHAR2(50),
    approval_timestamp  TIMESTAMP,
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_fin_trans PRIMARY KEY (transaction_id, transaction_date)
)
PARTITION BY RANGE (transaction_date)
INTERVAL (NUMTOYMINTERVAL(3,'MONTH'))
(
    PARTITION p_2024_q1 VALUES LESS THAN (DATE '2024-04-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH
PARALLEL 8;

-- Local indexes
CREATE INDEX idx_fin_account ON financial_transactions(account_id, transaction_date) LOCAL PARALLEL 8;
CREATE INDEX idx_fin_type ON financial_transactions(transaction_type, transaction_date) LOCAL PARALLEL 8;
CREATE INDEX idx_fin_reference ON financial_transactions(reference_number) LOCAL PARALLEL 8;

EXEC DBMS_STATS.GATHER_TABLE_STATS('FINANCIAL_TRANSACTIONS', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE financial_transactions IS 'Financial transactions - quarterly partitions for compliance and ILM';


-- -----------------------------------------------------------------------------
-- Example 5: IoT Sensor Data - Reference Partitioning
-- Child table inherits partitioning from parent device table
-- -----------------------------------------------------------------------------

-- Parent table: Device registry
CREATE TABLE iot_devices (
    device_id           NUMBER(12) NOT NULL,
    device_type         VARCHAR2(50),
    location_id         NUMBER(8),
    install_date        DATE,
    status              VARCHAR2(20),
    CONSTRAINT pk_iot_devices PRIMARY KEY (device_id)
)
PARTITION BY HASH (device_id)
PARTITIONS 16
TABLESPACE users;

-- Child table: Sensor readings (reference partitioned)
CREATE TABLE iot_sensor_readings (
    reading_id          NUMBER(18) NOT NULL,
    device_id           NUMBER(12) NOT NULL,
    reading_timestamp   TIMESTAMP NOT NULL,
    metric_name         VARCHAR2(50),
    metric_value        NUMBER(15,4),
    unit_of_measure     VARCHAR2(20),
    quality_flag        VARCHAR2(10),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    CONSTRAINT pk_iot_readings PRIMARY KEY (reading_id, device_id),
    CONSTRAINT fk_iot_device FOREIGN KEY (device_id)
        REFERENCES iot_devices(device_id)
)
PARTITION BY REFERENCE (fk_iot_device)
TABLESPACE users
COMPRESS FOR QUERY HIGH
PARALLEL 8;

-- Local index on timestamp for time-range queries
CREATE INDEX idx_iot_timestamp ON iot_sensor_readings(reading_timestamp) LOCAL PARALLEL 8;
CREATE INDEX idx_iot_metric ON iot_sensor_readings(metric_name, reading_timestamp) LOCAL PARALLEL 8;

EXEC DBMS_STATS.GATHER_TABLE_STATS('IOT_DEVICES', CASCADE => FALSE, DEGREE => 8);
EXEC DBMS_STATS.GATHER_TABLE_STATS('IOT_SENSOR_READINGS', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE iot_sensor_readings IS 'IoT sensor data - reference partitioning ensures co-location with device';


-- -----------------------------------------------------------------------------
-- Performance Optimization Settings
-- -----------------------------------------------------------------------------

-- Enable partition-wise joins
ALTER SESSION SET OPTIMIZER_FEATURES_ENABLE = '19.1.0';
ALTER SESSION SET PARALLEL_DEGREE_POLICY = 'AUTO';

-- Enable result cache for frequently accessed partitions
ALTER SYSTEM SET RESULT_CACHE_MODE = FORCE;
ALTER SYSTEM SET RESULT_CACHE_MAX_SIZE = 1G;
