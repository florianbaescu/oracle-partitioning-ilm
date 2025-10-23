-- =============================================================================
-- Test Data Setup Script
-- =============================================================================
-- Description: Creates test tables, partitions, and sample data
-- Usage: Called by test_suite_runner.sql
-- =============================================================================

DECLARE
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Creating test tables and data...');

    -- Create test fact table with monthly partitions
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE cmr.test_sales_fact (
            sale_id NUMBER,
            sale_date DATE,
            customer_id NUMBER,
            product_id NUMBER,
            amount NUMBER(12,2),
            quantity NUMBER
        ) PARTITION BY RANGE (sale_date) (
            PARTITION p_2024_01 VALUES LESS THAN (TO_DATE(''2024-02-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_02 VALUES LESS THAN (TO_DATE(''2024-03-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_03 VALUES LESS THAN (TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_04 VALUES LESS THAN (TO_DATE(''2024-05-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_05 VALUES LESS THAN (TO_DATE(''2024-06-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_06 VALUES LESS THAN (TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_07 VALUES LESS THAN (TO_DATE(''2024-08-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_08 VALUES LESS THAN (TO_DATE(''2024-09-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_09 VALUES LESS THAN (TO_DATE(''2024-10-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_10 VALUES LESS THAN (TO_DATE(''2024-11-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_11 VALUES LESS THAN (TO_DATE(''2024-12-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_12 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))
        )';
        DBMS_OUTPUT.PUT_LINE('  ✓ Created test_sales_fact table');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -955 THEN
                EXECUTE IMMEDIATE 'DROP TABLE cmr.test_sales_fact PURGE';
                EXECUTE IMMEDIATE 'CREATE TABLE cmr.test_sales_fact (
                    sale_id NUMBER,
                    sale_date DATE,
                    customer_id NUMBER,
                    product_id NUMBER,
                    amount NUMBER(12,2),
                    quantity NUMBER
                ) PARTITION BY RANGE (sale_date) (
                    PARTITION p_2024_01 VALUES LESS THAN (TO_DATE(''2024-02-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_02 VALUES LESS THAN (TO_DATE(''2024-03-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_03 VALUES LESS THAN (TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_04 VALUES LESS THAN (TO_DATE(''2024-05-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_05 VALUES LESS THAN (TO_DATE(''2024-06-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_06 VALUES LESS THAN (TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_07 VALUES LESS THAN (TO_DATE(''2024-08-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_08 VALUES LESS THAN (TO_DATE(''2024-09-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_09 VALUES LESS THAN (TO_DATE(''2024-10-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_10 VALUES LESS THAN (TO_DATE(''2024-11-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_11 VALUES LESS THAN (TO_DATE(''2024-12-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_12 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))
                )';
                DBMS_OUTPUT.PUT_LINE('  ✓ Recreated test_sales_fact table');
            ELSE
                RAISE;
            END IF;
    END;

    -- Insert sample data into test table
    FOR i IN 1..12 LOOP
        INSERT INTO cmr.test_sales_fact
        SELECT
            ROWNUM + (i-1)*1000,
            TO_DATE('2024-' || LPAD(i, 2, '0') || '-' || LPAD(MOD(ROWNUM, 28)+1, 2, '0'), 'YYYY-MM-DD'),
            MOD(ROWNUM, 1000) + 1,
            MOD(ROWNUM, 100) + 1,
            ROUND(DBMS_RANDOM.VALUE(10, 1000), 2),
            ROUND(DBMS_RANDOM.VALUE(1, 10))
        FROM dual
        CONNECT BY LEVEL <= 100;  -- 100 rows per partition
    END LOOP;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('  ✓ Inserted 1,200 test rows');

    -- Create test SCD2 dimension table
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE cmr.test_customer_dim (
            customer_key NUMBER PRIMARY KEY,
            customer_id NUMBER,
            customer_name VARCHAR2(100),
            effective_date DATE,
            expiry_date DATE,
            current_flag CHAR(1)
        ) PARTITION BY RANGE (effective_date) (
            PARTITION p_2023 VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_q1 VALUES LESS THAN (TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_q2 VALUES LESS THAN (TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_q3 VALUES LESS THAN (TO_DATE(''2024-10-01'', ''YYYY-MM-DD'')),
            PARTITION p_2024_q4 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))
        )';
        DBMS_OUTPUT.PUT_LINE('  ✓ Created test_customer_dim table');
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -955 THEN
                EXECUTE IMMEDIATE 'DROP TABLE cmr.test_customer_dim PURGE';
                EXECUTE IMMEDIATE 'CREATE TABLE cmr.test_customer_dim (
                    customer_key NUMBER PRIMARY KEY,
                    customer_id NUMBER,
                    customer_name VARCHAR2(100),
                    effective_date DATE,
                    expiry_date DATE,
                    current_flag CHAR(1)
                ) PARTITION BY RANGE (effective_date) (
                    PARTITION p_2023 VALUES LESS THAN (TO_DATE(''2024-01-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_q1 VALUES LESS THAN (TO_DATE(''2024-04-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_q2 VALUES LESS THAN (TO_DATE(''2024-07-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_q3 VALUES LESS THAN (TO_DATE(''2024-10-01'', ''YYYY-MM-DD'')),
                    PARTITION p_2024_q4 VALUES LESS THAN (TO_DATE(''2025-01-01'', ''YYYY-MM-DD''))
                )';
                DBMS_OUTPUT.PUT_LINE('  ✓ Recreated test_customer_dim table');
            ELSE
                RAISE;
            END IF;
    END;

    -- Insert sample SCD2 data
    INSERT INTO cmr.test_customer_dim
    SELECT
        ROWNUM,
        MOD(ROWNUM, 100) + 1,
        'Customer ' || (MOD(ROWNUM, 100) + 1),
        ADD_MONTHS(SYSDATE, -ROWNUM),
        CASE WHEN MOD(ROWNUM, 5) = 0 THEN TO_DATE('9999-12-31', 'YYYY-MM-DD') ELSE ADD_MONTHS(SYSDATE, -ROWNUM + 90) END,
        CASE WHEN MOD(ROWNUM, 5) = 0 THEN 'Y' ELSE 'N' END
    FROM dual
    CONNECT BY LEVEL <= 500;
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('  ✓ Inserted 500 SCD2 test rows');

    -- Refresh partition tracking for test tables
    dwh_refresh_partition_access_tracking('CMR', 'TEST_SALES_FACT');
    dwh_refresh_partition_access_tracking('CMR', 'TEST_CUSTOMER_DIM');
    DBMS_OUTPUT.PUT_LINE('  ✓ Refreshed partition tracking');

    DBMS_OUTPUT.PUT_LINE('Test data setup completed successfully');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('ERROR in test data setup: ' || SQLERRM);
        RAISE;
END;
/
