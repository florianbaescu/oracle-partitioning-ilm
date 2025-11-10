-- =============================================================================
-- Custom ILM Execution Engine Package
-- Executes ILM actions on eligible partitions
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_ilm_execution_engine AUTHID CURRENT_USER AS
    -- Main execution procedures
    PROCEDURE execute_pending_actions(
        p_max_operations NUMBER DEFAULT NULL
    );

    PROCEDURE execute_policy(
        p_policy_id NUMBER,
        p_max_operations NUMBER DEFAULT NULL
    );

    PROCEDURE execute_single_action(
        p_queue_id NUMBER
    );

    -- Direct execution (bypass queue)
    PROCEDURE compress_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE
    );

    PROCEDURE move_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE
    );

    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    );

    PROCEDURE drop_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    );

    PROCEDURE truncate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    );

END pck_dwh_ilm_execution_engine;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_ilm_execution_engine AS

    -- ==========================================================================
    -- Private Helper Procedures
    -- ==========================================================================

    PROCEDURE log_execution(
        p_execution_id IN OUT NUMBER,
        p_policy_id NUMBER,
        p_policy_name VARCHAR2,
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_action_type VARCHAR2,
        p_action_sql CLOB,
        p_status VARCHAR2,
        p_execution_start TIMESTAMP,
        p_execution_end TIMESTAMP,
        p_size_before_mb NUMBER DEFAULT NULL,
        p_size_after_mb NUMBER DEFAULT NULL,
        p_error_code NUMBER DEFAULT NULL,
        p_error_message VARCHAR2 DEFAULT NULL
    ) AS
        v_duration NUMBER;
        v_space_saved NUMBER;
        v_compression_ratio NUMBER;
    BEGIN
        v_duration := EXTRACT(SECOND FROM (p_execution_end - p_execution_start)) +
                     EXTRACT(MINUTE FROM (p_execution_end - p_execution_start)) * 60 +
                     EXTRACT(HOUR FROM (p_execution_end - p_execution_start)) * 3600;

        IF p_size_before_mb IS NOT NULL AND p_size_after_mb IS NOT NULL THEN
            v_space_saved := p_size_before_mb - p_size_after_mb;
            IF p_size_before_mb > 0 THEN
                v_compression_ratio := p_size_before_mb / NULLIF(p_size_after_mb, 0);
            END IF;
        END IF;

        INSERT INTO cmr.dwh_ilm_execution_log (
            policy_id, policy_name, table_owner, table_name, partition_name,
            execution_start, execution_end, duration_seconds, status,
            action_type, action_sql,
            size_before_mb, size_after_mb, space_saved_mb, compression_ratio,
            error_code, error_message
        ) VALUES (
            p_policy_id, p_policy_name, p_table_owner, p_table_name, p_partition_name,
            p_execution_start, p_execution_end, v_duration, p_status,
            p_action_type, p_action_sql,
            p_size_before_mb, p_size_after_mb, v_space_saved, v_compression_ratio,
            p_error_code, p_error_message
        ) RETURNING execution_id INTO p_execution_id;

        COMMIT;
    END log_execution;


    FUNCTION get_partition_size(
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
        WHEN NO_DATA_FOUND THEN
            RETURN 0;
    END get_partition_size;


    PROCEDURE rebuild_local_indexes(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_tablespace VARCHAR2 DEFAULT NULL
    ) AS
        v_sql VARCHAR2(4000);
        v_tablespace_clause VARCHAR2(200) := '';
    BEGIN
        IF p_tablespace IS NOT NULL THEN
            v_tablespace_clause := ' TABLESPACE ' || p_tablespace;
        END IF;

        FOR idx IN (
            SELECT ip.index_owner, ip.index_name, ip.partition_name
            FROM dba_ind_partitions ip
            JOIN dba_indexes i
                ON i.owner = ip.index_owner
                AND i.index_name = ip.index_name
            JOIN dba_part_indexes pi
                ON pi.owner = i.owner
                AND pi.index_name = i.index_name
            WHERE i.table_owner = p_table_owner
            AND i.table_name = p_table_name
            AND pi.locality = 'LOCAL'
            AND ip.partition_name = p_partition_name
        ) LOOP
            v_sql := 'ALTER INDEX ' || idx.index_owner || '.' || idx.index_name ||
                    ' REBUILD PARTITION ' || idx.partition_name ||
                    v_tablespace_clause || ' PARALLEL 4';

            DBMS_OUTPUT.PUT_LINE('  Rebuilding index: ' || idx.index_name ||
                               '.' || idx.partition_name);

            EXECUTE IMMEDIATE v_sql;
        END LOOP;
    END rebuild_local_indexes;


    PROCEDURE gather_partition_stats(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) AS
    BEGIN
        DBMS_STATS.GATHER_TABLE_STATS(
            ownname => p_table_owner,
            tabname => p_table_name,
            partname => p_partition_name,
            granularity => 'PARTITION',
            cascade => TRUE,
            degree => 4,
            estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE
        );

        DBMS_OUTPUT.PUT_LINE('  Statistics gathered for partition: ' || p_partition_name);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  Warning: Statistics gathering failed - ' || SQLERRM);
    END gather_partition_stats;


    -- ==========================================================================
    -- Public Action Procedures
    -- ==========================================================================

    PROCEDURE compress_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE
    ) AS
        v_sql VARCHAR2(4000);
        v_size_before NUMBER;
        v_size_after NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Compressing partition: ' || p_table_owner || '.' ||
                           p_table_name || '.' || p_partition_name);

        v_size_before := get_partition_size(p_table_owner, p_table_name, p_partition_name);

        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                ' MOVE PARTITION ' || p_partition_name ||
                ' COMPRESS FOR ' || p_compression_type;

        IF p_pctfree IS NOT NULL THEN
            v_sql := v_sql || ' PCTFREE ' || p_pctfree;
        END IF;

        v_sql := v_sql || ' PARALLEL 4';

        EXECUTE IMMEDIATE v_sql;

        v_size_after := get_partition_size(p_table_owner, p_table_name, p_partition_name);

        DBMS_OUTPUT.PUT_LINE('  Size before: ' || v_size_before || ' MB');
        DBMS_OUTPUT.PUT_LINE('  Size after: ' || v_size_after || ' MB');
        DBMS_OUTPUT.PUT_LINE('  Space saved: ' || (v_size_before - v_size_after) || ' MB');

        IF p_rebuild_indexes THEN
            rebuild_local_indexes(p_table_owner, p_table_name, p_partition_name);
        END IF;

        IF p_gather_stats THEN
            gather_partition_stats(p_table_owner, p_table_name, p_partition_name);
        END IF;

        DBMS_OUTPUT.PUT_LINE('  Compression completed successfully');
    END compress_partition;


    PROCEDURE move_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE
    ) AS
        v_sql VARCHAR2(4000);
        v_compression_clause VARCHAR2(100) := '';
        v_pctfree_clause VARCHAR2(50) := '';
        v_size_before NUMBER;
        v_size_after NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Moving partition: ' || p_table_owner || '.' ||
                           p_table_name || '.' || p_partition_name ||
                           ' to ' || p_target_tablespace);

        v_size_before := get_partition_size(p_table_owner, p_table_name, p_partition_name);

        IF p_compression_type IS NOT NULL THEN
            v_compression_clause := ' COMPRESS FOR ' || p_compression_type;
        END IF;

        IF p_pctfree IS NOT NULL THEN
            v_pctfree_clause := ' PCTFREE ' || p_pctfree;
        END IF;

        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                ' MOVE PARTITION ' || p_partition_name ||
                ' TABLESPACE ' || p_target_tablespace ||
                v_compression_clause ||
                v_pctfree_clause ||
                ' PARALLEL 4';

        EXECUTE IMMEDIATE v_sql;

        v_size_after := get_partition_size(p_table_owner, p_table_name, p_partition_name);

        DBMS_OUTPUT.PUT_LINE('  Move completed');
        IF p_compression_type IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('  Size before: ' || v_size_before || ' MB');
            DBMS_OUTPUT.PUT_LINE('  Size after: ' || v_size_after || ' MB');
        END IF;

        IF p_rebuild_indexes THEN
            rebuild_local_indexes(p_table_owner, p_table_name, p_partition_name, p_target_tablespace);
        END IF;

        IF p_gather_stats THEN
            gather_partition_stats(p_table_owner, p_table_name, p_partition_name);
        END IF;

        DBMS_OUTPUT.PUT_LINE('  Move completed successfully');
    END move_partition;


    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Making partition read-only: ' || p_table_owner || '.' ||
                           p_table_name || '.' || p_partition_name);

        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                ' MODIFY PARTITION ' || p_partition_name || ' READ ONLY';

        EXECUTE IMMEDIATE v_sql;

        DBMS_OUTPUT.PUT_LINE('  Partition is now read-only');
    END make_partition_readonly;


    PROCEDURE drop_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
        v_size NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Dropping partition: ' || p_table_owner || '.' ||
                           p_table_name || '.' || p_partition_name);

        v_size := get_partition_size(p_table_owner, p_table_name, p_partition_name);

        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                ' DROP PARTITION ' || p_partition_name;

        EXECUTE IMMEDIATE v_sql;

        DBMS_OUTPUT.PUT_LINE('  Partition dropped (freed ' || v_size || ' MB)');
    END drop_partition;


    PROCEDURE truncate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2
    ) AS
        v_sql VARCHAR2(4000);
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Truncating partition: ' || p_table_owner || '.' ||
                           p_table_name || '.' || p_partition_name);

        v_sql := 'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
                ' TRUNCATE PARTITION ' || p_partition_name ||
                ' UPDATE INDEXES';

        EXECUTE IMMEDIATE v_sql;

        DBMS_OUTPUT.PUT_LINE('  Partition truncated');
    END truncate_partition;


    -- ==========================================================================
    -- Queue-based Execution
    -- ==========================================================================

    PROCEDURE execute_single_action(
        p_queue_id NUMBER
    ) AS
        v_queue dwh_ilm_evaluation_queue%ROWTYPE;
        v_policy dwh_ilm_policies%ROWTYPE;
        v_execution_id NUMBER;
        v_start_time TIMESTAMP;
        v_end_time TIMESTAMP;
        v_size_before NUMBER;
        v_size_after NUMBER;
        v_sql CLOB;
        v_status VARCHAR2(20);
        v_error_msg VARCHAR2(4000);
    BEGIN
        -- Get queue item
        SELECT * INTO v_queue
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE queue_id = p_queue_id
        FOR UPDATE;

        -- Get policy
        SELECT * INTO v_policy
        FROM cmr.dwh_ilm_policies
        WHERE policy_id = v_queue.policy_id;

        -- Update queue status
        UPDATE cmr.dwh_ilm_evaluation_queue
        SET execution_status = 'RUNNING'
        WHERE queue_id = p_queue_id;
        COMMIT;

        v_start_time := SYSTIMESTAMP;
        v_size_before := get_partition_size(
            v_queue.table_owner,
            v_queue.table_name,
            v_queue.partition_name
        );

        BEGIN
            -- Execute appropriate action
            CASE v_policy.action_type
                WHEN 'COMPRESS' THEN
                    compress_partition(
                        v_queue.table_owner,
                        v_queue.table_name,
                        v_queue.partition_name,
                        v_policy.compression_type,
                        v_policy.pct_free,
                        (v_policy.rebuild_indexes = 'Y'),
                        (v_policy.gather_stats = 'Y')
                    );

                WHEN 'MOVE' THEN
                    move_partition(
                        v_queue.table_owner,
                        v_queue.table_name,
                        v_queue.partition_name,
                        v_policy.target_tablespace,
                        v_policy.compression_type,
                        v_policy.pct_free,
                        (v_policy.rebuild_indexes = 'Y'),
                        (v_policy.gather_stats = 'Y')
                    );

                WHEN 'READ_ONLY' THEN
                    make_partition_readonly(
                        v_queue.table_owner,
                        v_queue.table_name,
                        v_queue.partition_name
                    );

                WHEN 'DROP' THEN
                    drop_partition(
                        v_queue.table_owner,
                        v_queue.table_name,
                        v_queue.partition_name
                    );

                WHEN 'TRUNCATE' THEN
                    truncate_partition(
                        v_queue.table_owner,
                        v_queue.table_name,
                        v_queue.partition_name
                    );

                WHEN 'CUSTOM' THEN
                    IF v_policy.custom_action IS NOT NULL THEN
                        EXECUTE IMMEDIATE v_policy.custom_action;
                    END IF;

                ELSE
                    RAISE_APPLICATION_ERROR(-20001, 'Unknown action type: ' || v_policy.action_type);
            END CASE;

            v_status := 'SUCCESS';
            v_end_time := SYSTIMESTAMP;
            v_size_after := get_partition_size(
                v_queue.table_owner,
                v_queue.table_name,
                v_queue.partition_name
            );

        EXCEPTION
            WHEN OTHERS THEN
                v_status := 'FAILED';
                v_end_time := SYSTIMESTAMP;
                v_error_msg := SQLERRM;
                v_size_after := v_size_before;
                DBMS_OUTPUT.PUT_LINE('  ERROR: ' || v_error_msg);
        END;

        -- Log execution
        log_execution(
            v_execution_id,
            v_policy.policy_id,
            v_policy.policy_name,
            v_queue.table_owner,
            v_queue.table_name,
            v_queue.partition_name,
            v_policy.action_type,
            v_sql,
            v_status,
            v_start_time,
            v_end_time,
            v_size_before,
            v_size_after,
            SQLCODE,
            v_error_msg
        );

        -- Update queue
        UPDATE cmr.dwh_ilm_evaluation_queue
        SET execution_status = 'EXECUTED',
            execution_id = v_execution_id
        WHERE queue_id = p_queue_id;

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            DECLARE
                v_error_msg_outer VARCHAR2(4000) := SQLERRM;
                v_error_code NUMBER := SQLCODE;
                v_end_time_outer TIMESTAMP := SYSTIMESTAMP;
            BEGIN
                ROLLBACK;

                -- Log critical error to execution log
                BEGIN
                    log_execution(
                        p_execution_id => v_execution_id,
                        p_policy_id => NVL(v_policy.policy_id, 0),
                        p_policy_name => NVL(v_policy.policy_name, 'UNKNOWN'),
                        p_table_owner => NVL(v_queue.table_owner, 'UNKNOWN'),
                        p_table_name => NVL(v_queue.table_name, 'UNKNOWN'),
                        p_partition_name => NVL(v_queue.partition_name, 'UNKNOWN'),
                        p_action_type => 'ERROR',
                        p_action_sql => TO_CLOB('Critical error during action execution'),
                        p_status => 'FAILED',
                        p_execution_start => NVL(v_start_time, v_end_time_outer),
                        p_execution_end => v_end_time_outer,
                        p_error_code => v_error_code,
                        p_error_message => v_error_msg_outer
                    );
                EXCEPTION
                    WHEN OTHERS THEN
                        NULL; -- If logging fails, continue with status update
                END;

                -- Update queue status
                UPDATE cmr.dwh_ilm_evaluation_queue
                SET execution_status = 'FAILED'
                WHERE queue_id = p_queue_id;

                COMMIT;

                -- Log error but don't raise exception
                DBMS_OUTPUT.PUT_LINE('ERROR: Action execution failed for queue_id ' || p_queue_id || ' - ' || v_error_msg_outer);
            END;
    END execute_single_action;


    PROCEDURE execute_policy(
        p_policy_id NUMBER,
        p_max_operations NUMBER DEFAULT NULL
    ) AS
        v_count NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Executing policy ID: ' || p_policy_id);

        FOR queue_item IN (
            SELECT queue_id
            FROM cmr.dwh_ilm_evaluation_queue
            WHERE policy_id = p_policy_id
            AND execution_status = 'PENDING'
            AND eligible = 'Y'
            ORDER BY evaluation_date
        ) LOOP
            BEGIN
                execute_single_action(queue_item.queue_id);
                v_count := v_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('ERROR executing queue_id ' || queue_item.queue_id || ': ' || SQLERRM);
            END;

            EXIT WHEN p_max_operations IS NOT NULL AND v_count >= p_max_operations;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('Executed ' || v_count || ' action(s)');
    END execute_policy;


    PROCEDURE execute_pending_actions(
        p_max_operations NUMBER DEFAULT NULL
    ) AS
        v_max_concurrent NUMBER;
        v_total_count NUMBER := 0;
        v_policy_count NUMBER;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Executing Pending ILM Actions');
        DBMS_OUTPUT.PUT_LINE('========================================');

        -- Check if execution window is open
        IF NOT is_execution_window_open THEN
            DBMS_OUTPUT.PUT_LINE('Outside execution window - skipping');
            RETURN;
        END IF;

        -- Get max concurrent operations
        v_max_concurrent := TO_NUMBER(get_dwh_ilm_config('MAX_CONCURRENT_OPERATIONS'));

        -- Execute pending actions by policy priority
        FOR pol IN (
            SELECT DISTINCT
                q.policy_id,
                p.policy_name,
                p.priority
            FROM cmr.dwh_ilm_evaluation_queue q
            JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
            WHERE q.execution_status = 'PENDING'
            AND q.eligible = 'Y'
            AND p.enabled = 'Y'
            ORDER BY p.priority, p.policy_id
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Processing policy: ' || pol.policy_name);

            v_policy_count := 0;

            FOR queue_item IN (
                SELECT queue_id
                FROM cmr.dwh_ilm_evaluation_queue
                WHERE policy_id = pol.policy_id
                AND execution_status = 'PENDING'
                AND eligible = 'Y'
                ORDER BY evaluation_date
            ) LOOP
                BEGIN
                    execute_single_action(queue_item.queue_id);
                    v_policy_count := v_policy_count + 1;
                    v_total_count := v_total_count + 1;
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('  ERROR executing queue_id ' || queue_item.queue_id || ': ' || SQLERRM);
                END;

                EXIT WHEN p_max_operations IS NOT NULL AND v_total_count >= p_max_operations;
            END LOOP;

            DBMS_OUTPUT.PUT_LINE('  Executed ' || v_policy_count || ' action(s)');

            EXIT WHEN p_max_operations IS NOT NULL AND v_total_count >= p_max_operations;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('Total actions executed: ' || v_total_count);
        DBMS_OUTPUT.PUT_LINE('========================================');
    END execute_pending_actions;

END pck_dwh_ilm_execution_engine;
/

-- Verification
SELECT 'ILM Execution Engine Package Created Successfully!' AS status FROM DUAL;
