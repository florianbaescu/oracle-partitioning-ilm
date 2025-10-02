-- =============================================================================
-- Dimension Table Partitioning Examples
-- Data Warehouse Best Practices for Oracle
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Example 1: Customer Dimension - Hash Partitioning
-- Large dimension table requiring even distribution
-- -----------------------------------------------------------------------------

CREATE TABLE customer_dim (
    customer_id         NUMBER(12) NOT NULL,
    customer_key        VARCHAR2(50) UNIQUE NOT NULL,
    first_name          VARCHAR2(100),
    last_name           VARCHAR2(100),
    email               VARCHAR2(200),
    phone               VARCHAR2(30),
    date_of_birth       DATE,
    gender              VARCHAR2(10),
    customer_segment    VARCHAR2(50),
    loyalty_tier        VARCHAR2(30),
    registration_date   DATE,
    country_code        VARCHAR2(2),
    state_code          VARCHAR2(10),
    city                VARCHAR2(100),
    postal_code         VARCHAR2(20),
    address_line1       VARCHAR2(200),
    address_line2       VARCHAR2(200),
    account_status      VARCHAR2(20),
    credit_limit        NUMBER(12,2),
    effective_date      DATE NOT NULL,
    expiry_date         DATE,
    current_flag        CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_customer_dim PRIMARY KEY (customer_id)
)
PARTITION BY HASH (customer_id)
PARTITIONS 16
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes for common lookups
CREATE INDEX idx_customer_key ON customer_dim(customer_key) LOCAL;
CREATE INDEX idx_customer_email ON customer_dim(email) LOCAL;
CREATE BITMAP INDEX idx_customer_segment ON customer_dim(customer_segment) LOCAL;
CREATE BITMAP INDEX idx_customer_tier ON customer_dim(loyalty_tier) LOCAL;
CREATE INDEX idx_customer_country ON customer_dim(country_code, state_code) LOCAL;

EXEC DBMS_STATS.GATHER_TABLE_STATS('CUSTOMER_DIM', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE customer_dim IS 'Customer dimension - hash partitioned for even distribution';


-- -----------------------------------------------------------------------------
-- Example 2: Product Dimension - List Partitioning by Category
-- Partitioned by major product categories for maintenance and query optimization
-- -----------------------------------------------------------------------------

CREATE TABLE product_dim (
    product_id          NUMBER(12) NOT NULL,
    product_key         VARCHAR2(50) UNIQUE NOT NULL,
    product_name        VARCHAR2(200),
    product_description VARCHAR2(2000),
    category            VARCHAR2(50) NOT NULL,
    subcategory         VARCHAR2(50),
    brand               VARCHAR2(100),
    manufacturer        VARCHAR2(100),
    unit_cost           NUMBER(12,2),
    list_price          NUMBER(12,2),
    unit_of_measure     VARCHAR2(20),
    weight              NUMBER(10,3),
    weight_uom          VARCHAR2(10),
    size                VARCHAR2(50),
    color               VARCHAR2(30),
    product_status      VARCHAR2(20),
    introduction_date   DATE,
    discontinue_date    DATE,
    effective_date      DATE NOT NULL,
    expiry_date         DATE,
    current_flag        CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_product_dim PRIMARY KEY (product_id, category)
)
PARTITION BY LIST (category)
(
    PARTITION p_electronics VALUES ('ELECTRONICS', 'COMPUTERS', 'MOBILE'),
    PARTITION p_clothing VALUES ('APPAREL', 'SHOES', 'ACCESSORIES'),
    PARTITION p_home VALUES ('FURNITURE', 'APPLIANCES', 'HOME_DECOR'),
    PARTITION p_sports VALUES ('SPORTS', 'FITNESS', 'OUTDOOR'),
    PARTITION p_books VALUES ('BOOKS', 'MAGAZINES', 'MEDIA'),
    PARTITION p_grocery VALUES ('FOOD', 'BEVERAGES', 'HOUSEHOLD'),
    PARTITION p_other VALUES (DEFAULT)
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes
CREATE INDEX idx_product_key ON product_dim(product_key) LOCAL;
CREATE INDEX idx_product_name ON product_dim(product_name) LOCAL;
CREATE INDEX idx_product_brand ON product_dim(brand) LOCAL;
CREATE BITMAP INDEX idx_product_status ON product_dim(product_status) LOCAL;

EXEC DBMS_STATS.GATHER_TABLE_STATS('PRODUCT_DIM', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE product_dim IS 'Product dimension - list partitioned by category';


-- -----------------------------------------------------------------------------
-- Example 3: Date Dimension - Range Partitioning by Year
-- Standard calendar/fiscal date dimension
-- -----------------------------------------------------------------------------

CREATE TABLE date_dim (
    date_key            NUMBER(8) NOT NULL,
    full_date           DATE NOT NULL,
    day_of_week         NUMBER(1),
    day_of_month        NUMBER(2),
    day_of_year         NUMBER(3),
    day_name            VARCHAR2(10),
    day_abbrev          VARCHAR2(3),
    weekday_flag        CHAR(1),
    week_of_year        NUMBER(2),
    week_of_month       NUMBER(1),
    month_number        NUMBER(2),
    month_name          VARCHAR2(10),
    month_abbrev        VARCHAR2(3),
    quarter_number      NUMBER(1),
    quarter_name        VARCHAR2(2),
    year_number         NUMBER(4),
    year_month          NUMBER(6),
    year_quarter        NUMBER(6),
    fiscal_year         NUMBER(4),
    fiscal_quarter      NUMBER(1),
    fiscal_period       NUMBER(2),
    holiday_flag        CHAR(1),
    holiday_name        VARCHAR2(50),
    business_day_flag   CHAR(1),
    last_day_month_flag CHAR(1),
    last_day_quarter_flag CHAR(1),
    last_day_year_flag  CHAR(1),
    CONSTRAINT pk_date_dim PRIMARY KEY (date_key)
)
PARTITION BY RANGE (date_key)
(
    PARTITION p_2020 VALUES LESS THAN (20210101),
    PARTITION p_2021 VALUES LESS THAN (20220101),
    PARTITION p_2022 VALUES LESS THAN (20230101),
    PARTITION p_2023 VALUES LESS THAN (20240101),
    PARTITION p_2024 VALUES LESS THAN (20250101),
    PARTITION p_2025 VALUES LESS THAN (20260101),
    PARTITION p_2026 VALUES LESS THAN (20270101),
    PARTITION p_2027 VALUES LESS THAN (20280101),
    PARTITION p_2028 VALUES LESS THAN (20290101),
    PARTITION p_2029 VALUES LESS THAN (20300101),
    PARTITION p_2030 VALUES LESS THAN (20310101),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
)
TABLESPACE users;

-- Indexes for common date lookups
CREATE UNIQUE INDEX idx_date_full ON date_dim(full_date) LOCAL;
CREATE INDEX idx_date_year_month ON date_dim(year_month) LOCAL;
CREATE INDEX idx_date_year_quarter ON date_dim(year_quarter) LOCAL;
CREATE BITMAP INDEX idx_date_day_of_week ON date_dim(day_of_week) LOCAL;
CREATE BITMAP INDEX idx_date_holiday ON date_dim(holiday_flag) LOCAL;

EXEC DBMS_STATS.GATHER_TABLE_STATS('DATE_DIM', CASCADE => TRUE, DEGREE => 4);

COMMENT ON TABLE date_dim IS 'Date dimension - partitioned by year for calendar operations';


-- -----------------------------------------------------------------------------
-- Example 4: Slowly Changing Dimension (SCD Type 2) - Range Partitioning
-- Employee dimension with historical tracking
-- -----------------------------------------------------------------------------

CREATE TABLE employee_dim (
    employee_sk         NUMBER(12) NOT NULL,
    employee_id         NUMBER(10) NOT NULL,
    employee_number     VARCHAR2(20),
    first_name          VARCHAR2(100),
    last_name           VARCHAR2(100),
    full_name           VARCHAR2(200),
    email               VARCHAR2(200),
    phone               VARCHAR2(30),
    job_title           VARCHAR2(100),
    department          VARCHAR2(100),
    division            VARCHAR2(100),
    manager_id          NUMBER(10),
    manager_name        VARCHAR2(200),
    hire_date           DATE,
    termination_date    DATE,
    employee_status     VARCHAR2(20),
    employment_type     VARCHAR2(30),
    location_id         NUMBER(8),
    cost_center         VARCHAR2(50),
    salary_grade        VARCHAR2(20),
    effective_date      DATE NOT NULL,
    expiry_date         DATE NOT NULL,
    current_flag        CHAR(1) DEFAULT 'Y',
    version_number      NUMBER(5),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_employee_dim PRIMARY KEY (employee_sk)
)
PARTITION BY RANGE (effective_date)
INTERVAL (NUMTOYMINTERVAL(1,'YEAR'))
(
    PARTITION p_2020 VALUES LESS THAN (DATE '2021-01-01')
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes for SCD Type 2 queries
CREATE INDEX idx_employee_id ON employee_dim(employee_id, effective_date, expiry_date) LOCAL;
CREATE INDEX idx_employee_current ON employee_dim(employee_id, current_flag) LOCAL;
CREATE INDEX idx_employee_dept ON employee_dim(department, effective_date) LOCAL;
CREATE BITMAP INDEX idx_employee_status ON employee_dim(employee_status) LOCAL;

EXEC DBMS_STATS.GATHER_TABLE_STATS('EMPLOYEE_DIM', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE employee_dim IS 'Employee SCD Type 2 dimension - partitioned by effective date for history';


-- -----------------------------------------------------------------------------
-- Example 5: Location Dimension - Composite List-List Partitioning
-- Hierarchical geographic dimension
-- -----------------------------------------------------------------------------

CREATE TABLE location_dim (
    location_id         NUMBER(8) NOT NULL,
    location_key        VARCHAR2(50) UNIQUE NOT NULL,
    location_name       VARCHAR2(200),
    location_type       VARCHAR2(30),
    address_line1       VARCHAR2(200),
    address_line2       VARCHAR2(200),
    city                VARCHAR2(100),
    state_code          VARCHAR2(10),
    state_name          VARCHAR2(100),
    postal_code         VARCHAR2(20),
    country_code        VARCHAR2(2) NOT NULL,
    country_name        VARCHAR2(100),
    region              VARCHAR2(50) NOT NULL,
    latitude            NUMBER(10,7),
    longitude           NUMBER(10,7),
    timezone            VARCHAR2(50),
    location_status     VARCHAR2(20),
    opening_date        DATE,
    closing_date        DATE,
    square_footage      NUMBER(10),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_location_dim PRIMARY KEY (location_id, region, country_code)
)
PARTITION BY LIST (region)
SUBPARTITION BY LIST (country_code)
(
    PARTITION p_north_america VALUES ('NORTH_AMERICA')
    (
        SUBPARTITION p_na_us VALUES ('US'),
        SUBPARTITION p_na_ca VALUES ('CA'),
        SUBPARTITION p_na_mx VALUES ('MX'),
        SUBPARTITION p_na_other VALUES (DEFAULT)
    ),
    PARTITION p_europe VALUES ('EUROPE')
    (
        SUBPARTITION p_eu_uk VALUES ('GB'),
        SUBPARTITION p_eu_de VALUES ('DE'),
        SUBPARTITION p_eu_fr VALUES ('FR'),
        SUBPARTITION p_eu_es VALUES ('ES'),
        SUBPARTITION p_eu_it VALUES ('IT'),
        SUBPARTITION p_eu_other VALUES (DEFAULT)
    ),
    PARTITION p_asia VALUES ('ASIA')
    (
        SUBPARTITION p_asia_cn VALUES ('CN'),
        SUBPARTITION p_asia_jp VALUES ('JP'),
        SUBPARTITION p_asia_in VALUES ('IN'),
        SUBPARTITION p_asia_sg VALUES ('SG'),
        SUBPARTITION p_asia_other VALUES (DEFAULT)
    ),
    PARTITION p_other VALUES (DEFAULT)
    (
        SUBPARTITION p_other_default VALUES (DEFAULT)
    )
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Indexes
CREATE INDEX idx_location_key ON location_dim(location_key) LOCAL;
CREATE INDEX idx_location_city ON location_dim(city, state_code) LOCAL;
CREATE INDEX idx_location_postal ON location_dim(postal_code) LOCAL;
CREATE BITMAP INDEX idx_location_type ON location_dim(location_type) LOCAL;

EXEC DBMS_STATS.GATHER_TABLE_STATS('LOCATION_DIM', CASCADE => TRUE, DEGREE => 8);

COMMENT ON TABLE location_dim IS 'Location dimension - composite list partitioning by region and country';


-- -----------------------------------------------------------------------------
-- Example 6: Small Dimension - No Partitioning Required
-- Account dimension (< 10M rows)
-- -----------------------------------------------------------------------------

CREATE TABLE account_dim (
    account_id          NUMBER(10) NOT NULL,
    account_number      VARCHAR2(20) UNIQUE NOT NULL,
    account_name        VARCHAR2(200),
    account_type        VARCHAR2(50),
    account_status      VARCHAR2(20),
    opening_date        DATE,
    closing_date        DATE,
    branch_id           NUMBER(8),
    account_manager_id  NUMBER(10),
    currency_code       VARCHAR2(3),
    credit_limit        NUMBER(15,2),
    current_balance     NUMBER(15,2),
    created_timestamp   TIMESTAMP DEFAULT SYSTIMESTAMP,
    updated_timestamp   TIMESTAMP,
    CONSTRAINT pk_account_dim PRIMARY KEY (account_id)
)
TABLESPACE users
COMPRESS FOR QUERY HIGH;

-- Standard B-tree indexes
CREATE UNIQUE INDEX idx_account_number ON account_dim(account_number);
CREATE INDEX idx_account_branch ON account_dim(branch_id);
CREATE BITMAP INDEX idx_account_type ON account_dim(account_type);
CREATE BITMAP INDEX idx_account_status ON account_dim(account_status);

EXEC DBMS_STATS.GATHER_TABLE_STATS('ACCOUNT_DIM', CASCADE => TRUE, DEGREE => 4);

COMMENT ON TABLE account_dim IS 'Account dimension - small table, no partitioning required';
