-- =============================================================================
-- Custom ILM Policy Engine Package
-- Evaluates partitions against ILM policies and queues eligible actions
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_ilm_policy_engine AS
    -- Main evaluation procedures
    PROCEDURE evaluate_all_policies;

    PROCEDURE evaluate_policy(
        p_policy_id NUMBER
    );

    PROCEDURE evaluate_table(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    );

    -- Policy evaluation functions
    FUNCTION is_partition_eligible(
        p_policy_id NUMBER,
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_reason OUT VARCHAR2
    ) RETURN BOOLEAN;

    -- Queue management
    PROCEDURE clear_queue(
        p_policy_id NUMBER DEFAULT NULL
    );

    PROCEDURE refresh_queue;

END pck_dwh_ilm_policy_engine;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_ilm_policy_engine AS

    -- ==========================================================================
    -- Private Helper Functions
    -- ==========================================================================

    FUNCTION get_partition_high_value_date(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) RETURN DATE
    AS
        v_high_value LONG;
        v_date DATE;
        v_sql VARCHAR2(4000);
    BEGIN
        SELECT high_value INTO v_high_value
        FROM dba_tab_partitions
        WHERE table_owner = p_table_owner
        AND table_name = p_table_name
        AND partition_name = p_partition_name;

        -- Try to extract date from high_value
        v_sql := 'SELECT ' || v_high_value || ' FROM DUAL';

        BEGIN
            EXECUTE IMMEDIATE v_sql INTO v_date;
            RETURN v_date;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN NULL;
        END;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END get_partition_high_value_date;


    FUNCTION get_partition_age_days(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_partition_date DATE;
        v_age_days NUMBER;
    BEGIN
        -- Try to get age from high_value
        v_partition_date := get_partition_high_value_date(
            p_table_owner, p_table_name, p_partition_name
        );

        IF v_partition_date IS NOT NULL THEN
            v_age_days := TRUNC(SYSDATE - v_partition_date);
        ELSE
            -- Fall back to access tracking
            SELECT days_since_write INTO v_age_days
            FROM cmr.dwh_ilm_partition_access
            WHERE table_owner = p_table_owner
            AND table_name = p_table_name
            AND partition_name = p_partition_name;
        END IF;

        RETURN v_age_days;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN NULL;
    END get_partition_age_days;


    FUNCTION get_partition_size_mb(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) RETURN NUMBER
    AS
        v_size_mb NUMBER;
    BEGIN
        SELECT ROUND(bytes/1024/1024, 2)
        INTO v_size_mb
        FROM dba_segments
        WHERE owner = p_table_owner
        AND segment_name = p_table_name
        AND partition_name = p_partition_name;

        RETURN v_size_mb;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 0;
    END get_partition_size_mb;


    FUNCTION get_partition_temperature(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) RETURN VARCHAR2
    AS
        v_temperature VARCHAR2(10);
    BEGIN
        SELECT temperature INTO v_temperature
        FROM cmr.dwh_ilm_partition_access
        WHERE table_owner = p_table_owner
        AND table_name = p_table_name
        AND partition_name = p_partition_name;

        RETURN v_temperature;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'UNKNOWN';
    END get_partition_temperature;


    -- ==========================================================================
    -- Public Functions and Procedures
    -- ==========================================================================

    FUNCTION is_partition_eligible(
        p_policy_id NUMBER,
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_reason OUT VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_policy dwh_ilm_policies%ROWTYPE;
        v_age_days NUMBER;
        v_age_months NUMBER;
        v_size_mb NUMBER;
        v_temperature VARCHAR2(10);
        v_compression VARCHAR2(30);
        v_tablespace VARCHAR2(30);
        v_custom_result NUMBER;
        v_sql VARCHAR2(4000);
    BEGIN
        -- Get policy details
        SELECT * INTO v_policy
        FROM cmr.dwh_ilm_policies
        WHERE policy_id = p_policy_id;

        -- Check if policy is enabled
        IF v_policy.enabled != 'Y' THEN
            p_reason := 'Policy is disabled';
            RETURN FALSE;
        END IF;

        -- Get partition metrics
        v_age_days := get_partition_age_days(p_table_owner, p_table_name, p_partition_name);
        v_size_mb := get_partition_size_mb(p_table_owner, p_table_name, p_partition_name);
        v_temperature := get_partition_temperature(p_table_owner, p_table_name, p_partition_name);

        -- Get current partition state
        SELECT
            compression,
            tablespace_name
        INTO
            v_compression,
            v_tablespace
        FROM dba_tab_partitions
        WHERE table_owner = p_table_owner
        AND table_name = p_table_name
        AND partition_name = p_partition_name;

        -- Check age threshold (days)
        IF v_policy.age_days IS NOT NULL THEN
            IF v_age_days IS NULL OR v_age_days < v_policy.age_days THEN
                p_reason := 'Partition age ' || NVL(TO_CHAR(v_age_days), 'NULL') ||
                           ' days is less than threshold ' || v_policy.age_days || ' days';
                RETURN FALSE;
            END IF;
        END IF;

        -- Check age threshold (months)
        IF v_policy.age_months IS NOT NULL THEN
            v_age_months := TRUNC(v_age_days / 30);
            IF v_age_months < v_policy.age_months THEN
                p_reason := 'Partition age ' || v_age_months ||
                           ' months is less than threshold ' || v_policy.age_months || ' months';
                RETURN FALSE;
            END IF;
        END IF;

        -- Check size threshold
        IF v_policy.size_threshold_mb IS NOT NULL THEN
            IF v_size_mb < v_policy.size_threshold_mb THEN
                p_reason := 'Partition size ' || v_size_mb ||
                           ' MB is less than threshold ' || v_policy.size_threshold_mb || ' MB';
                RETURN FALSE;
            END IF;
        END IF;

        -- Check access pattern (temperature)
        IF v_policy.access_pattern IS NOT NULL THEN
            -- Calculate temperature using policy-specific thresholds
            DECLARE
                v_hot_threshold NUMBER;
                v_warm_threshold NUMBER;
                v_policy_temperature VARCHAR2(10);
            BEGIN
                v_hot_threshold := get_policy_thresholds(v_policy.policy_id, 'HOT');
                v_warm_threshold := get_policy_thresholds(v_policy.policy_id, 'WARM');

                v_policy_temperature := CASE
                    WHEN v_age_days < v_hot_threshold THEN 'HOT'
                    WHEN v_age_days < v_warm_threshold THEN 'WARM'
                    ELSE 'COLD'
                END;

                IF v_policy_temperature != v_policy.access_pattern THEN
                    p_reason := 'Partition temperature (' || v_policy_temperature ||
                               ') does not match required ' || v_policy.access_pattern ||
                               ' [thresholds: HOT<' || v_hot_threshold || ', WARM<' || v_warm_threshold || ']';
                    RETURN FALSE;
                END IF;
            END;
        END IF;

        -- Check if already in target state
        IF v_policy.action_type = 'COMPRESS' THEN
            IF v_compression = 'ENABLED' THEN
                p_reason := 'Partition already compressed';
                RETURN FALSE;
            END IF;
        ELSIF v_policy.action_type = 'MOVE' THEN
            IF v_tablespace = v_policy.target_tablespace THEN
                p_reason := 'Partition already in target tablespace';
                RETURN FALSE;
            END IF;
        END IF;

        -- Evaluate custom condition if specified
        IF v_policy.custom_condition IS NOT NULL THEN
            v_sql := 'SELECT COUNT(*) FROM ' ||
                    p_table_owner || '.' || p_table_name ||
                    ' PARTITION (' || p_partition_name || ')' ||
                    ' WHERE ' || v_policy.custom_condition;

            BEGIN
                EXECUTE IMMEDIATE v_sql INTO v_custom_result;
                IF v_custom_result = 0 THEN
                    p_reason := 'Custom condition not met';
                    RETURN FALSE;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    p_reason := 'Error evaluating custom condition: ' || SQLERRM;
                    RETURN FALSE;
            END;
        END IF;

        -- All checks passed
        p_reason := 'Partition meets all policy criteria';
        RETURN TRUE;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            p_reason := 'Policy or partition not found';
            RETURN FALSE;
        WHEN OTHERS THEN
            p_reason := 'Error: ' || SQLERRM;
            RETURN FALSE;
    END is_partition_eligible;


    PROCEDURE evaluate_policy(
        p_policy_id NUMBER
    ) AS
        v_policy dwh_ilm_policies%ROWTYPE;
        v_eligible CHAR(1);
        v_reason VARCHAR2(500);
        v_count NUMBER := 0;
    BEGIN
        -- Get policy
        SELECT * INTO v_policy
        FROM cmr.dwh_ilm_policies
        WHERE policy_id = p_policy_id;

        DBMS_OUTPUT.PUT_LINE('Evaluating policy: ' || v_policy.policy_name);

        -- Evaluate all partitions for this table
        FOR part IN (
            SELECT table_owner, table_name, partition_name
            FROM dba_tab_partitions
            WHERE table_owner = v_policy.table_owner
            AND table_name = v_policy.table_name
            ORDER BY partition_position
        ) LOOP
            -- Check if partition is eligible
            IF is_partition_eligible(
                p_policy_id,
                part.table_owner,
                part.table_name,
                part.partition_name,
                v_reason
            ) THEN
                v_eligible := 'Y';
                v_count := v_count + 1;

                -- Add to evaluation queue
                INSERT INTO cmr.dwh_ilm_evaluation_queue (
                    policy_id, table_owner, table_name, partition_name,
                    evaluation_date, eligible, reason, execution_status
                ) VALUES (
                    p_policy_id, part.table_owner, part.table_name, part.partition_name,
                    SYSTIMESTAMP, v_eligible, v_reason, 'PENDING'
                );
            END IF;
        END LOOP;

        COMMIT;

        DBMS_OUTPUT.PUT_LINE('  Found ' || v_count || ' eligible partition(s)');

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('  ERROR: Policy not found');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  ERROR: ' || SQLERRM);
            ROLLBACK;
    END evaluate_policy;


    PROCEDURE evaluate_table(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2
    ) AS
        v_count NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Evaluating all policies for table: ' ||
                           p_table_owner || '.' || p_table_name);

        FOR pol IN (
            SELECT policy_id, policy_name
            FROM cmr.dwh_ilm_policies
            WHERE table_owner = p_table_owner
            AND table_name = p_table_name
            AND enabled = 'Y'
            ORDER BY priority, policy_id
        ) LOOP
            v_count := v_count + 1;
            evaluate_policy(pol.policy_id);
        END LOOP;

        IF v_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('  No active policies found for this table');
        END IF;
    END evaluate_table;


    PROCEDURE evaluate_all_policies AS
        v_policy_count NUMBER := 0;
        v_total_eligible NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Evaluating All ILM Policies');
        DBMS_OUTPUT.PUT_LINE('========================================');

        -- Clear old evaluation queue
        DELETE FROM cmr.dwh_ilm_evaluation_queue
        WHERE execution_status = 'PENDING'
        AND evaluation_date < SYSTIMESTAMP - INTERVAL '7' DAY;

        COMMIT;

        -- Refresh partition access tracking
        refresh_partition_access_tracking;

        -- Evaluate each enabled policy
        FOR pol IN (
            SELECT policy_id, policy_name, table_owner, table_name
            FROM cmr.dwh_ilm_policies
            WHERE enabled = 'Y'
            ORDER BY priority, policy_id
        ) LOOP
            v_policy_count := v_policy_count + 1;
            evaluate_policy(pol.policy_id);
        END LOOP;

        -- Get total eligible count
        SELECT COUNT(*) INTO v_total_eligible
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE execution_status = 'PENDING'
        AND eligible = 'Y';

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Evaluation Complete');
        DBMS_OUTPUT.PUT_LINE('  Policies evaluated: ' || v_policy_count);
        DBMS_OUTPUT.PUT_LINE('  Eligible partitions: ' || v_total_eligible);
        DBMS_OUTPUT.PUT_LINE('========================================');
    END evaluate_all_policies;


    PROCEDURE clear_queue(
        p_policy_id NUMBER DEFAULT NULL
    ) AS
        v_deleted NUMBER;
    BEGIN
        IF p_policy_id IS NULL THEN
            DELETE FROM cmr.dwh_ilm_evaluation_queue
            WHERE execution_status = 'PENDING';
        ELSE
            DELETE FROM cmr.dwh_ilm_evaluation_queue
            WHERE policy_id = p_policy_id
            AND execution_status = 'PENDING';
        END IF;

        v_deleted := SQL%ROWCOUNT;
        COMMIT;

        DBMS_OUTPUT.PUT_LINE('Cleared ' || v_deleted || ' pending items from evaluation queue');
    END clear_queue;


    PROCEDURE refresh_queue AS
    BEGIN
        -- Clear old pending items
        clear_queue;

        -- Re-evaluate all policies
        evaluate_all_policies;
    END refresh_queue;

END pck_dwh_ilm_policy_engine;
/

-- Verification
SELECT 'ILM Policy Engine Package Created Successfully!' AS status FROM DUAL;
