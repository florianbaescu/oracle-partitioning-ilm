-- =============================================================================
-- Package: pck_dwh_ilm_execution_engine (Enhanced with Scheduler)
-- Description: ILM execution engine with continuous execution and checkpointing
-- Dependencies: scheduler_enhancement_setup.sql must be run first
-- =============================================================================

CREATE OR REPLACE PACKAGE pck_dwh_ilm_execution_engine AUTHID CURRENT_USER AS

    -- =========================================================================
    -- TYPES
    -- =========================================================================

    TYPE schedule_rec IS RECORD (
        schedule_id             NUMBER,
        schedule_name           VARCHAR2(100),
        enabled                 CHAR(1),
        monday_hours            VARCHAR2(11),
        tuesday_hours           VARCHAR2(11),
        wednesday_hours         VARCHAR2(11),
        thursday_hours          VARCHAR2(11),
        friday_hours            VARCHAR2(11),
        saturday_hours          VARCHAR2(11),
        sunday_hours            VARCHAR2(11),
        batch_cooldown_minutes  NUMBER,
        enable_checkpointing    CHAR(1),
        checkpoint_frequency    NUMBER
    );

    -- =========================================================================
    -- MAIN EXECUTION (Continuous Execution with Checkpointing)
    -- =========================================================================

    -- Execute pending ILM actions continuously during window
    PROCEDURE execute_pending_actions(
        p_schedule_name VARCHAR2 DEFAULT 'DEFAULT_SCHEDULE',
        p_resume_batch_id VARCHAR2 DEFAULT NULL,  -- Resume specific batch
        p_force_run BOOLEAN DEFAULT FALSE         -- Bypass day/time checks
    );

    -- Execute all actions for a specific policy
    PROCEDURE execute_policy(
        p_policy_id NUMBER,
        p_max_operations NUMBER DEFAULT NULL
    );

    -- Execute a single queued action
    PROCEDURE execute_single_action(
        p_queue_id NUMBER
    );

    -- =========================================================================
    -- SCHEDULING HELPERS
    -- =========================================================================

    -- Get schedule configuration
    FUNCTION get_schedule_config(
        p_schedule_name VARCHAR2
    ) RETURN schedule_rec;

    -- Get today's execution hours
    FUNCTION get_today_hours(
        p_schedule schedule_rec
    ) RETURN VARCHAR2;

    -- Check if currently in execution window
    FUNCTION is_in_execution_window(
        p_schedule schedule_rec
    ) RETURN BOOLEAN;

    -- Check if should execute now (work-based)
    FUNCTION should_execute_now(
        p_schedule_name VARCHAR2
    ) RETURN BOOLEAN;

    -- =========================================================================
    -- DIRECT PARTITION OPERATIONS (Bypass Queue)
    -- =========================================================================

    PROCEDURE compress_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    PROCEDURE move_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    PROCEDURE drop_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    PROCEDURE truncate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    );

    -- Merge monthly into yearly partitions
    PROCEDURE merge_monthly_into_yearly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_monthly_partition VARCHAR2
    );

END pck_dwh_ilm_execution_engine;
/

CREATE OR REPLACE PACKAGE BODY pck_dwh_ilm_execution_engine AS

    -- =========================================================================
    -- PRIVATE HELPER PROCEDURES
    -- =========================================================================

    PROCEDURE log_info(p_message VARCHAR2) AS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('[INFO] ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') || ' - ' || p_message);
    END;

    PROCEDURE log_error(p_message VARCHAR2) AS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('[ERROR] ' || TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') || ' - ' || p_message);
    END;

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
        v_duration := EXTRACT(DAY FROM (p_execution_end - p_execution_start)) * 86400 +
                     EXTRACT(HOUR FROM (p_execution_end - p_execution_start)) * 3600 +
                     EXTRACT(MINUTE FROM (p_execution_end - p_execution_start)) * 60 +
                     EXTRACT(SECOND FROM (p_execution_end - p_execution_start));

        IF p_size_before_mb IS NOT NULL AND p_size_after_mb IS NOT NULL THEN
            v_space_saved := p_size_before_mb - p_size_after_mb;
            IF p_size_before_mb > 0 THEN
                v_compression_ratio := ROUND(p_size_before_mb / NULLIF(p_size_after_mb, 0), 2);
            END IF;
        END IF;

        INSERT INTO cmr.dwh_ilm_execution_log (
            policy_id, policy_name,
            table_owner, table_name, partition_name,
            action_type, action_sql,
            execution_start, execution_end, duration_seconds,
            size_before_mb, size_after_mb, space_saved_mb, compression_ratio,
            status, error_code, error_message
        ) VALUES (
            p_policy_id, p_policy_name,
            p_table_owner, p_table_name, p_partition_name,
            p_action_type, p_action_sql,
            p_execution_start, p_execution_end, v_duration,
            p_size_before_mb, p_size_after_mb, v_space_saved, v_compression_ratio,
            p_status, p_error_code, p_error_message
        ) RETURNING execution_id INTO p_execution_id;

        COMMIT;
    END log_execution;

    -- =========================================================================
    -- SCHEDULING HELPERS
    -- =========================================================================

    FUNCTION get_schedule_config(
        p_schedule_name VARCHAR2
    ) RETURN schedule_rec
    AS
        v_schedule schedule_rec;
    BEGIN
        SELECT
            schedule_id, schedule_name, enabled,
            monday_hours, tuesday_hours, wednesday_hours, thursday_hours,
            friday_hours, saturday_hours, sunday_hours,
            batch_cooldown_minutes,
            enable_checkpointing, checkpoint_frequency
        INTO v_schedule
        FROM cmr.dwh_ilm_execution_schedules
        WHERE schedule_name = p_schedule_name
        AND enabled = 'Y';

        RETURN v_schedule;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001, 'Schedule ''' || p_schedule_name || ''' not found or disabled');
    END get_schedule_config;

    FUNCTION get_today_hours(
        p_schedule schedule_rec
    ) RETURN VARCHAR2
    AS
        v_day_of_week VARCHAR2(10);
    BEGIN
        v_day_of_week := TRIM(TO_CHAR(SYSDATE, 'DAY'));

        RETURN CASE v_day_of_week
            WHEN 'MONDAY'    THEN p_schedule.monday_hours
            WHEN 'TUESDAY'   THEN p_schedule.tuesday_hours
            WHEN 'WEDNESDAY' THEN p_schedule.wednesday_hours
            WHEN 'THURSDAY'  THEN p_schedule.thursday_hours
            WHEN 'FRIDAY'    THEN p_schedule.friday_hours
            WHEN 'SATURDAY'  THEN p_schedule.saturday_hours
            WHEN 'SUNDAY'    THEN p_schedule.sunday_hours
            ELSE NULL
        END;
    END get_today_hours;

    FUNCTION is_in_execution_window(
        p_schedule schedule_rec
    ) RETURN BOOLEAN
    AS
        v_today_hours VARCHAR2(11);
        v_start_time VARCHAR2(5);
        v_end_time VARCHAR2(5);
        v_start_hour NUMBER;
        v_start_min NUMBER;
        v_end_hour NUMBER;
        v_end_min NUMBER;
        v_current_time NUMBER;  -- Minutes since midnight
        v_start_minutes NUMBER; -- Window start in minutes since midnight
        v_end_minutes NUMBER;   -- Window end in minutes since midnight
    BEGIN
        -- Get today's hours (e.g., '22:00-06:00' or NULL)
        v_today_hours := get_today_hours(p_schedule);

        -- If NULL, no execution today
        IF v_today_hours IS NULL THEN
            RETURN FALSE;
        END IF;

        -- Parse 'HH24:MI-HH24:MI' format
        v_start_time := SUBSTR(v_today_hours, 1, 5);   -- '22:00'
        v_end_time := SUBSTR(v_today_hours, 7, 5);     -- '06:00'

        v_start_hour := TO_NUMBER(SUBSTR(v_start_time, 1, 2));
        v_start_min := TO_NUMBER(SUBSTR(v_start_time, 4, 2));
        v_end_hour := TO_NUMBER(SUBSTR(v_end_time, 1, 2));
        v_end_min := TO_NUMBER(SUBSTR(v_end_time, 4, 2));

        -- Convert to minutes since midnight
        v_current_time := TO_NUMBER(TO_CHAR(SYSDATE, 'HH24')) * 60 + TO_NUMBER(TO_CHAR(SYSDATE, 'MI'));
        v_start_minutes := v_start_hour * 60 + v_start_min;
        v_end_minutes := v_end_hour * 60 + v_end_min;

        -- Handle windows that cross midnight (e.g., '22:00-06:00')
        IF v_start_minutes > v_end_minutes THEN
            RETURN v_current_time >= v_start_minutes OR v_current_time < v_end_minutes;
        ELSE
            RETURN v_current_time >= v_start_minutes AND v_current_time < v_end_minutes;
        END IF;
    END is_in_execution_window;

    FUNCTION should_execute_now(
        p_schedule_name VARCHAR2
    ) RETURN BOOLEAN
    AS
        v_schedule schedule_rec;
        v_running_count NUMBER;
        v_pending_work NUMBER;
    BEGIN
        v_schedule := get_schedule_config(p_schedule_name);

        -- Priority 1: Prevent concurrent execution
        SELECT COUNT(*) INTO v_running_count
        FROM cmr.dwh_ilm_execution_state
        WHERE schedule_id = v_schedule.schedule_id
        AND status = 'RUNNING';

        IF v_running_count > 0 THEN
            RETURN FALSE;  -- Already running
        END IF;

        -- Priority 2: Check if work exists
        SELECT COUNT(*) INTO v_pending_work
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE execution_status = 'PENDING'
        AND eligible = 'Y';

        IF v_pending_work = 0 THEN
            RETURN FALSE;  -- No work to do
        END IF;

        -- Priority 3: Check execution window (also checks if should run today)
        IF NOT is_in_execution_window(v_schedule) THEN
            RETURN FALSE;  -- Either not scheduled today (NULL) or outside time window
        END IF;

        -- All checks passed
        RETURN TRUE;
    END should_execute_now;

    -- =========================================================================
    -- BATCH EXECUTION (with Checkpointing)
    -- =========================================================================

    PROCEDURE checkpoint_batch(
        p_batch_id VARCHAR2,
        p_last_queue_id NUMBER,
        p_ops_completed NUMBER
    ) AS
    BEGIN
        UPDATE cmr.dwh_ilm_execution_state
        SET last_checkpoint = SYSTIMESTAMP,
            last_queue_id = p_last_queue_id,
            operations_completed = p_ops_completed,
            elapsed_seconds = EXTRACT(DAY FROM (SYSTIMESTAMP - start_time)) * 86400 +
                            EXTRACT(HOUR FROM (SYSTIMESTAMP - start_time)) * 3600 +
                            EXTRACT(MINUTE FROM (SYSTIMESTAMP - start_time)) * 60 +
                            EXTRACT(SECOND FROM (SYSTIMESTAMP - start_time))
        WHERE execution_batch_id = p_batch_id;
        COMMIT;
    END checkpoint_batch;

    PROCEDURE execute_single_batch(
        p_batch_id VARCHAR2,
        p_schedule schedule_rec,
        p_max_operations NUMBER
    ) AS
        v_ops_count NUMBER := 0;
        v_checkpoint_counter NUMBER := 0;
    BEGIN
        log_info('Executing batch ' || p_batch_id || ' (max_ops=' || p_max_operations || ')');

        -- Get queue items ordered by policy priority
        FOR queue_item IN (
            SELECT q.queue_id, q.policy_id, p.priority
            FROM cmr.dwh_ilm_evaluation_queue q
            JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
            WHERE q.execution_status = 'PENDING'
            AND q.eligible = 'Y'
            AND p.enabled = 'Y'
            ORDER BY p.priority, q.queue_id
            FETCH FIRST p_max_operations ROWS ONLY
        ) LOOP
            -- Tag queue item with batch ID
            UPDATE cmr.dwh_ilm_evaluation_queue
            SET execution_batch_id = p_batch_id,
                batch_sequence = v_ops_count + 1
            WHERE queue_id = queue_item.queue_id;
            COMMIT;

            -- Execute the action
            BEGIN
                execute_single_action(queue_item.queue_id);
                v_ops_count := v_ops_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    log_error('Failed to execute queue_id=' || queue_item.queue_id || ': ' || SQLERRM);
            END;

            -- Checkpoint if configured
            v_checkpoint_counter := v_checkpoint_counter + 1;
            IF p_schedule.enable_checkpointing = 'Y' AND
               v_checkpoint_counter >= p_schedule.checkpoint_frequency THEN
                checkpoint_batch(p_batch_id, queue_item.queue_id, v_ops_count);
                v_checkpoint_counter := 0;
            END IF;
        END LOOP;

        -- Final checkpoint
        IF p_schedule.enable_checkpointing = 'Y' THEN
            checkpoint_batch(p_batch_id, NULL, v_ops_count);
        END IF;

        log_info('Batch ' || p_batch_id || ' completed: ' || v_ops_count || ' operations');
    END execute_single_batch;

    -- =========================================================================
    -- MAIN CONTINUOUS EXECUTION
    -- =========================================================================

    PROCEDURE execute_pending_actions(
        p_schedule_name VARCHAR2 DEFAULT 'DEFAULT_SCHEDULE',
        p_resume_batch_id VARCHAR2 DEFAULT NULL,
        p_force_run BOOLEAN DEFAULT FALSE
    ) AS
        v_schedule schedule_rec;
        v_batch_id VARCHAR2(50);
        v_pending_count NUMBER;
        v_batch_count NUMBER := 0;
        v_max_operations NUMBER;
    BEGIN
        log_info('========================================');
        log_info('ILM Continuous Execution Starting');
        log_info('Schedule: ' || p_schedule_name);
        log_info('========================================');

        -- Get schedule configuration
        v_schedule := get_schedule_config(p_schedule_name);

        -- Get batch size from global config
        BEGIN
            SELECT TO_NUMBER(config_value) INTO v_max_operations
            FROM cmr.dwh_ilm_config
            WHERE config_key = 'MAX_CONCURRENT_OPERATIONS';
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_max_operations := 10;  -- Default fallback
                log_info('MAX_CONCURRENT_OPERATIONS not found, using default: 10');
        END;

        -- Check if in execution window (unless force run)
        IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
            log_info('Outside execution window - exiting');
            RETURN;
        END IF;

        log_info('Starting continuous execution (cooldown=' || v_schedule.batch_cooldown_minutes ||
                 ' min, batch_size=' || v_max_operations || ')');

        -- ================================================================
        -- ⭐ CONTINUOUS EXECUTION LOOP
        -- ================================================================
        LOOP
            -- Check if still in window (unless force run)
            IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
                log_info('Window closed - ending execution');
                EXIT;
            END IF;

            -- Check if work exists
            SELECT COUNT(*) INTO v_pending_count
            FROM cmr.dwh_ilm_evaluation_queue q
            JOIN cmr.dwh_ilm_policies p ON p.policy_id = q.policy_id
            WHERE q.execution_status = 'PENDING'
            AND q.eligible = 'Y'
            AND p.enabled = 'Y';

            IF v_pending_count = 0 THEN
                log_info('No more work in queue - ending execution');
                EXIT;
            END IF;

            -- Generate new batch ID
            v_batch_count := v_batch_count + 1;
            v_batch_id := 'BATCH_' || TO_CHAR(SYSTIMESTAMP, 'YYYYMMDD_HH24MISS') ||
                          '_' || LPAD(v_batch_count, 3, '0');

            log_info('Batch ' || v_batch_count || ': ' || v_pending_count || ' operations pending');

            -- Create execution state record
            INSERT INTO cmr.dwh_ilm_execution_state (
                execution_batch_id, schedule_id, start_time, status, operations_total
            ) VALUES (
                v_batch_id, v_schedule.schedule_id, SYSTIMESTAMP, 'RUNNING',
                LEAST(v_pending_count, v_max_operations)
            );
            COMMIT;

            -- Execute one batch
            BEGIN
                execute_single_batch(
                    p_batch_id => v_batch_id,
                    p_schedule => v_schedule,
                    p_max_operations => v_max_operations
                );

                UPDATE cmr.dwh_ilm_execution_state
                SET status = 'COMPLETED',
                    end_time = SYSTIMESTAMP,
                    elapsed_seconds = EXTRACT(DAY FROM (SYSTIMESTAMP - start_time)) * 86400 +
                                    EXTRACT(HOUR FROM (SYSTIMESTAMP - start_time)) * 3600 +
                                    EXTRACT(MINUTE FROM (SYSTIMESTAMP - start_time)) * 60 +
                                    EXTRACT(SECOND FROM (SYSTIMESTAMP - start_time))
                WHERE execution_batch_id = v_batch_id;
                COMMIT;

            EXCEPTION
                WHEN OTHERS THEN
                    UPDATE cmr.dwh_ilm_execution_state
                    SET status = 'FAILED', end_time = SYSTIMESTAMP
                    WHERE execution_batch_id = v_batch_id;
                    COMMIT;
                    log_error('Batch failed: ' || SQLERRM);
            END;

            -- ⭐ Cooldown between batches (if configured)
            IF v_schedule.batch_cooldown_minutes > 0 THEN
                log_info('Cooldown: ' || v_schedule.batch_cooldown_minutes || ' minutes');
                -- Use DBMS_SESSION.SLEEP (available in 18c+) or busy wait for older versions
                BEGIN
                    EXECUTE IMMEDIATE 'BEGIN DBMS_SESSION.SLEEP(' || v_schedule.batch_cooldown_minutes * 60 || '); END;';
                EXCEPTION
                    WHEN OTHERS THEN
                        -- Fallback for pre-18c: busy wait
                        DECLARE
                            v_end_time TIMESTAMP := SYSTIMESTAMP + NUMTODSINTERVAL(v_schedule.batch_cooldown_minutes, 'MINUTE');
                        BEGIN
                            WHILE SYSTIMESTAMP < v_end_time LOOP
                                NULL;  -- Busy wait
                            END LOOP;
                        END;
                END;

                -- Check if window closed during cooldown
                IF NOT p_force_run AND NOT is_in_execution_window(v_schedule) THEN
                    log_info('Window closed during cooldown - ending execution');
                    EXIT;
                END IF;
            END IF;

        END LOOP;
        -- ================================================================

        log_info('========================================');
        log_info('Continuous execution ended');
        log_info('Total batches: ' || v_batch_count);
        log_info('========================================');

    EXCEPTION
        WHEN OTHERS THEN
            log_error('Fatal error in continuous execution: ' || SQLERRM);
            RAISE;
    END execute_pending_actions;

    -- =========================================================================
    -- EXECUTE POLICY (Legacy - Kept for Compatibility)
    -- =========================================================================

    PROCEDURE execute_policy(
        p_policy_id NUMBER,
        p_max_operations NUMBER DEFAULT NULL
    ) AS
        v_ops_count NUMBER := 0;
        v_max_ops NUMBER := NVL(p_max_operations, 999999);
    BEGIN
        log_info('Executing policy_id=' || p_policy_id);

        FOR queue_item IN (
            SELECT queue_id
            FROM cmr.dwh_ilm_evaluation_queue
            WHERE policy_id = p_policy_id
            AND execution_status = 'PENDING'
            AND eligible = 'Y'
            ORDER BY queue_id
            FETCH FIRST v_max_ops ROWS ONLY
        ) LOOP
            BEGIN
                execute_single_action(queue_item.queue_id);
                v_ops_count := v_ops_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    log_error('Failed queue_id=' || queue_item.queue_id || ': ' || SQLERRM);
            END;
        END LOOP;

        log_info('Policy execution completed: ' || v_ops_count || ' operations');
    END execute_policy;

    -- =========================================================================
    -- EXECUTE SINGLE ACTION
    -- =========================================================================

    PROCEDURE execute_single_action(
        p_queue_id NUMBER
    ) AS
        v_queue_rec cmr.dwh_ilm_evaluation_queue%ROWTYPE;
        v_policy_rec cmr.dwh_ilm_policies%ROWTYPE;
        v_action_sql CLOB;
        v_status VARCHAR2(50);
        v_error_msg VARCHAR2(4000);
        v_execution_id NUMBER;
        v_start_time TIMESTAMP;
        v_end_time TIMESTAMP;
    BEGIN
        -- Get queue item
        SELECT * INTO v_queue_rec
        FROM cmr.dwh_ilm_evaluation_queue
        WHERE queue_id = p_queue_id;

        -- Get policy
        SELECT * INTO v_policy_rec
        FROM cmr.dwh_ilm_policies
        WHERE policy_id = v_queue_rec.policy_id;

        v_start_time := SYSTIMESTAMP;

        -- Route to appropriate action handler based on policy action_type
        -- All procedures now return OUT parameters for SQL executed, status, and error message
        CASE v_policy_rec.action_type
            WHEN 'COMPRESS' THEN
                compress_partition(
                    p_table_owner => v_queue_rec.table_owner,
                    p_table_name => v_queue_rec.table_name,
                    p_partition_name => v_queue_rec.partition_name,
                    p_compression_type => v_policy_rec.compression_type,
                    p_sql_executed => v_action_sql,
                    p_status => v_status,
                    p_error_message => v_error_msg
                );

            WHEN 'MOVE' THEN
                move_partition(
                    p_table_owner => v_queue_rec.table_owner,
                    p_table_name => v_queue_rec.table_name,
                    p_partition_name => v_queue_rec.partition_name,
                    p_target_tablespace => v_policy_rec.target_tablespace,
                    p_compression_type => v_policy_rec.compression_type,
                    p_sql_executed => v_action_sql,
                    p_status => v_status,
                    p_error_message => v_error_msg
                );

            WHEN 'READ_ONLY' THEN
                make_partition_readonly(
                    p_table_owner => v_queue_rec.table_owner,
                    p_table_name => v_queue_rec.table_name,
                    p_partition_name => v_queue_rec.partition_name,
                    p_sql_executed => v_action_sql,
                    p_status => v_status,
                    p_error_message => v_error_msg
                );

            WHEN 'DROP' THEN
                drop_partition(
                    p_table_owner => v_queue_rec.table_owner,
                    p_table_name => v_queue_rec.table_name,
                    p_partition_name => v_queue_rec.partition_name,
                    p_sql_executed => v_action_sql,
                    p_status => v_status,
                    p_error_message => v_error_msg
                );

            WHEN 'TRUNCATE' THEN
                truncate_partition(
                    p_table_owner => v_queue_rec.table_owner,
                    p_table_name => v_queue_rec.table_name,
                    p_partition_name => v_queue_rec.partition_name,
                    p_sql_executed => v_action_sql,
                    p_status => v_status,
                    p_error_message => v_error_msg
                );

            ELSE
                RAISE_APPLICATION_ERROR(-20002, 'Unknown action type: ' || v_policy_rec.action_type);
        END CASE;

        v_end_time := SYSTIMESTAMP;

        -- Log execution with OUT parameters captured from utility
        log_execution(
            p_policy_id => v_policy_rec.policy_id,
            p_policy_name => v_policy_rec.policy_name,
            p_table_owner => v_queue_rec.table_owner,
            p_table_name => v_queue_rec.table_name,
            p_partition_name => v_queue_rec.partition_name,
            p_action_type => v_policy_rec.action_type,
            p_action_sql => v_action_sql,  -- SQL executed by utility
            p_execution_start => v_start_time,
            p_execution_end => v_end_time,
            p_status => v_status,  -- Status from utility (SUCCESS, WARNING, ERROR, SKIPPED)
            p_error_message => v_error_msg  -- Error message from utility
        );

        -- Update queue status based on utility status
        UPDATE cmr.dwh_ilm_evaluation_queue
        SET execution_status = CASE
                WHEN v_status IN ('SUCCESS', 'WARNING', 'SKIPPED') THEN 'COMPLETED'
                ELSE 'FAILED'
            END
        WHERE queue_id = p_queue_id;

        COMMIT;

        log_info('Action completed: ' || v_policy_rec.action_type ||
                 ' on ' || v_queue_rec.table_name || '.' || v_queue_rec.partition_name ||
                 ' (Status: ' || v_status || ')');

    EXCEPTION
        WHEN OTHERS THEN
            v_end_time := SYSTIMESTAMP;
            v_error_msg := SUBSTR(SQLERRM, 1, 4000);

            -- Log failed execution
            log_execution(
                p_policy_id => v_policy_rec.policy_id,
                p_policy_name => v_policy_rec.policy_name,
                p_table_owner => v_queue_rec.table_owner,
                p_table_name => v_queue_rec.table_name,
                p_partition_name => v_queue_rec.partition_name,
                p_action_type => v_policy_rec.action_type,
                p_action_sql => v_action_sql,
                p_execution_start => v_start_time,
                p_execution_end => v_end_time,
                p_status => 'ERROR',
                p_error_message => v_error_msg
            );

            UPDATE cmr.dwh_ilm_evaluation_queue
            SET execution_status = 'FAILED'
            WHERE queue_id = p_queue_id;

            COMMIT;

            log_error('Action failed: ' || v_error_msg);
            RAISE;
    END execute_single_action;

    -- =========================================================================
    -- DIRECT PARTITION OPERATIONS (Implementation stubs - to be completed)
    -- =========================================================================

    PROCEDURE compress_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT 'QUERY HIGH',
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
    BEGIN
        -- Call partition utilities package
        pck_dwh_partition_utilities.compress_single_partition(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_compression_type => p_compression_type,
            p_sql_executed => p_sql_executed,
            p_status => p_status,
            p_error_message => p_error_message
        );

        log_info('Compressed partition: ' || p_table_name || '.' || p_partition_name || ' (Status: ' || p_status || ')');
    END compress_partition;

    PROCEDURE move_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_target_tablespace VARCHAR2,
        p_compression_type VARCHAR2 DEFAULT NULL,
        p_pctfree NUMBER DEFAULT NULL,
        p_rebuild_indexes BOOLEAN DEFAULT TRUE,
        p_gather_stats BOOLEAN DEFAULT TRUE,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
    BEGIN
        -- Call partition utilities package
        pck_dwh_partition_utilities.move_single_partition(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_target_tablespace => p_target_tablespace,
            p_compression_type => p_compression_type,
            p_sql_executed => p_sql_executed,
            p_status => p_status,
            p_error_message => p_error_message
        );

        log_info('Moved partition to ' || p_target_tablespace || ': ' || p_table_name || '.' || p_partition_name || ' (Status: ' || p_status || ')');
    END move_partition;

    PROCEDURE make_partition_readonly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
    BEGIN
        -- Call partition utilities package
        pck_dwh_partition_utilities.make_partition_readonly(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_sql_executed => p_sql_executed,
            p_status => p_status,
            p_error_message => p_error_message
        );

        log_info('Made partition read-only: ' || p_table_name || '.' || p_partition_name || ' (Status: ' || p_status || ')');
    END make_partition_readonly;

    PROCEDURE drop_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
    BEGIN
        -- Call partition utilities package
        pck_dwh_partition_utilities.drop_single_partition(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_sql_executed => p_sql_executed,
            p_status => p_status,
            p_error_message => p_error_message
        );

        log_info('Dropped partition: ' || p_table_name || '.' || p_partition_name || ' (Status: ' || p_status || ')');
    END drop_partition;

    PROCEDURE truncate_partition(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_partition_name VARCHAR2,
        p_sql_executed OUT CLOB,
        p_status OUT VARCHAR2,
        p_error_message OUT VARCHAR2
    ) AS
    BEGIN
        -- Call partition utilities package
        pck_dwh_partition_utilities.truncate_single_partition(
            p_table_owner => p_table_owner,
            p_table_name => p_table_name,
            p_partition_name => p_partition_name,
            p_sql_executed => p_sql_executed,
            p_status => p_status,
            p_error_message => p_error_message
        );

        log_info('Truncated partition: ' || p_table_name || '.' || p_partition_name || ' (Status: ' || p_status || ')');
    END truncate_partition;

    PROCEDURE merge_monthly_into_yearly(
        p_table_owner VARCHAR2,
        p_table_name VARCHAR2,
        p_monthly_partition VARCHAR2
    ) AS
    BEGIN
        -- Stub - to be implemented based on partition naming patterns
        log_info('Merge monthly into yearly: ' || p_table_name || '.' || p_monthly_partition);
        RAISE_APPLICATION_ERROR(-20003, 'merge_monthly_into_yearly not yet implemented');
    END merge_monthly_into_yearly;

END pck_dwh_ilm_execution_engine;
/

PROMPT ========================================
PROMPT Execution Engine Package Created!
PROMPT ========================================
PROMPT
PROMPT Package includes:
PROMPT - Continuous execution with LOOP
PROMPT - Per-day scheduling support
PROMPT - Checkpointing and resumability
PROMPT - Work-based execution checks
PROMPT - Cooldown between batches
PROMPT
PROMPT Next: Run scheduler_enhancement_scheduler.sql
PROMPT
