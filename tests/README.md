# ILM Framework Test Suite

Comprehensive automated test suite for the Oracle Custom ILM Framework.

## Overview

This test suite validates the functionality, reliability, and correctness of the ILM framework through:
- **Unit Tests**: Individual component testing (policy validation, partition tracking, policy evaluation)
- **Integration Tests**: End-to-end workflow testing
- **Regression Tests**: Verification of bug fixes and edge cases

## Test Structure

```
tests/
├── test_suite_runner.sql              # Main test runner (execute this)
├── test_data_setup.sql                # Creates test tables and data
├── unit_tests_policy_validation.sql   # 7 policy validation tests
├── unit_tests_partition_tracking.sql  # 3 partition tracking tests
├── unit_tests_policy_evaluation.sql   # 2 policy evaluation tests
├── integration_tests_ilm_workflow.sql # 2 end-to-end workflow tests
├── regression_tests.sql               # 3 regression tests for bug fixes
├── test_cleanup.sql                   # Cleanup test data
└── README.md                          # This file
```

## Prerequisites

1. **ILM Framework Installed**: Complete ILM framework must be installed in the `cmr` schema
2. **Database Privileges**:
   - `CREATE TABLE` privilege
   - `SELECT`, `INSERT`, `UPDATE`, `DELETE` on all ILM framework tables
   - `EXECUTE` on all ILM framework procedures
3. **Tablespace**: Sufficient space in the default tablespace for test tables

## Running the Test Suite

### Option 1: Run All Tests (Recommended)

```sql
-- Connect as CMR schema owner
CONNECT cmr/<password>

-- Run complete test suite
@test_suite_runner.sql
```

### Option 2: Run Individual Test Categories

```sql
-- Setup test data
@test_data_setup.sql

-- Run specific test category
@unit_tests_policy_validation.sql
@unit_tests_partition_tracking.sql
@unit_tests_policy_evaluation.sql
@integration_tests_ilm_workflow.sql
@regression_tests.sql

-- Cleanup
@test_cleanup.sql
```

## Test Categories

### Unit Tests: Policy Validation (7 tests)

Tests the policy validation trigger and procedure:

1. **Valid compression policy accepted** - Validates that correctly formed compression policies are accepted
2. **Invalid table name rejected** - Ensures policies for non-existent tables are rejected
3. **Invalid compression type rejected** - Validates compression type checking
4. **Negative age_days rejected** - Ensures age constraints are enforced
5. **Valid tiering policy accepted** - Validates tablespace move policies
6. **Policy with age_days and age_months accepted** - Tests dual age criteria
7. **Duplicate policy name rejected** - Ensures unique policy names

### Unit Tests: Partition Tracking (3 tests)

Tests partition access tracking functionality:

1. **Tracking refresh populates data** - Verifies tracking table is populated correctly
2. **Temperature calculation valid** - Ensures HOT/WARM/COLD classification works
3. **Size tracking captures data** - Validates partition size capture

### Unit Tests: Policy Evaluation (2 tests)

Tests policy evaluation and queue management:

1. **Evaluate identifies eligible partitions** - Ensures policies correctly identify eligible partitions
2. **Disabled policies not evaluated** - Verifies disabled policies are skipped

### Integration Tests: ILM Workflow (2 tests)

Tests complete end-to-end workflows:

1. **End-to-end compression workflow** - Validates policy creation → evaluation → execution → logging
2. **Monitoring views accessible** - Ensures all monitoring views can be queried

### Regression Tests (3 tests)

Tests for specific bug fixes:

1. **SQLERRM not used in DML statements** - Regression test for ORA-00904 bug (v1.0 fix)
2. **Config table accessible** - Validates framework independence (v1.0 fix)
3. **Parallel degree configuration valid** - Ensures parallel degree is applied (v2.0 fix)

## Test Results

### Viewing Test Results

```sql
-- View all test results from last run
SELECT
    test_category,
    test_name,
    test_status,
    error_message,
    execution_time_ms,
    test_date
FROM cmr.dwh_test_results
ORDER BY test_date DESC, test_category, test_name;

-- View summary by category
SELECT
    test_category,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN test_status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
    SUM(CASE WHEN test_status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
    ROUND(AVG(execution_time_ms), 2) AS avg_time_ms
FROM cmr.dwh_test_results
WHERE test_date > SYSDATE - 1
GROUP BY test_category
ORDER BY test_category;

-- View only failures
SELECT
    test_category,
    test_name,
    error_message,
    test_date
FROM cmr.dwh_test_results
WHERE test_status = 'FAILED'
ORDER BY test_date DESC;
```

### Expected Output

```
=============================================================================
ILM FRAMEWORK TEST SUITE
Started: 2025-10-23 14:30:00
=============================================================================

Setting up test data...
  ✓ Created test_sales_fact table
  ✓ Inserted 1,200 test rows
  ✓ Created test_customer_dim table
  ✓ Inserted 500 SCD2 test rows
  ✓ Refreshed partition tracking
Test data setup completed successfully

--- UNIT TESTS: Policy Validation ---
  [PASSED] Valid compression policy accepted
  [PASSED] Invalid table name rejected
  [PASSED] Invalid compression type rejected
  [PASSED] Negative age_days rejected
  [PASSED] Valid tiering policy accepted
  [PASSED] Policy with age_days and age_months accepted
  [PASSED] Duplicate policy name rejected

--- UNIT TESTS: Partition Tracking ---
  [PASSED] Tracking refresh populates data (12 partitions)
  [PASSED] Temperature calculation valid
  [PASSED] Size tracking captures data

--- UNIT TESTS: Policy Evaluation ---
  [PASSED] Policy evaluation found 10 eligible partitions
  [PASSED] Disabled policies not evaluated

--- INTEGRATION TESTS ---
  [PASSED] End-to-end workflow completed
  [PASSED] All monitoring views accessible

--- REGRESSION TESTS ---
  [PASSED] SQLERRM not in DML
  [PASSED] Config table accessible
  [PASSED] Parallel degree config valid

Cleaning up test data...
  ✓ Dropped test_sales_fact
  ✓ Dropped test_customer_dim
  ✓ Removed 17 partition tracking records
Test cleanup completed successfully

=============================================================================
TEST SUITE SUMMARY
=============================================================================
Total Tests:   17
Passed:        17 (100.0%)
Failed:        0 (0.0%)
Duration:      12.45 seconds
=============================================================================
✓ ALL TESTS PASSED!
=============================================================================
```

## Troubleshooting

### Common Issues

**Issue**: `ORA-00942: table or view does not exist`
- **Cause**: ILM framework not installed or not accessible
- **Solution**: Ensure ILM framework is installed and connected as correct schema

**Issue**: `ORA-01031: insufficient privileges`
- **Cause**: Missing privileges for test execution
- **Solution**: Grant necessary privileges (CREATE TABLE, EXECUTE on procedures)

**Issue**: Tests fail with "table already exists"
- **Cause**: Previous test run didn't cleanup properly
- **Solution**: Run `@test_cleanup.sql` before running tests again

**Issue**: All tests timeout
- **Cause**: Tables are locked from previous operations
- **Solution**: Check for blocking sessions and kill if necessary

### Cleanup Failed Test Run

```sql
-- Manually cleanup if test_cleanup.sql fails
@test_cleanup.sql

-- Or manual cleanup
DROP TABLE cmr.test_sales_fact PURGE;
DROP TABLE cmr.test_customer_dim PURGE;

DELETE FROM cmr.dwh_ilm_partition_access WHERE table_name LIKE 'TEST_%';
DELETE FROM cmr.dwh_ilm_policies WHERE policy_name LIKE 'TEST_%';
DELETE FROM cmr.dwh_ilm_evaluation_queue WHERE policy_id IN (SELECT policy_id FROM cmr.dwh_ilm_policies WHERE policy_name LIKE 'TEST_%');
DELETE FROM cmr.dwh_ilm_execution_log WHERE policy_name LIKE 'TEST_%';
COMMIT;
```

### Disable Specific Tests

To skip specific test categories, comment out the corresponding line in `test_suite_runner.sql`:

```sql
-- @unit_tests_policy_validation.sql  -- Skip policy validation tests
@unit_tests_partition_tracking.sql
@unit_tests_policy_evaluation.sql
-- @integration_tests_ilm_workflow.sql  -- Skip integration tests
@regression_tests.sql
```

## Test Maintenance

### Adding New Tests

1. Choose appropriate test category file (or create new one)
2. Follow existing test pattern:
   ```sql
   BEGIN
       -- Test logic here
       IF condition THEN
           INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, execution_time_ms)
           VALUES ('CATEGORY', 'Test description', 'PASSED', 0);
       ELSE
           INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
           VALUES ('CATEGORY', 'Test description', 'FAILED', 'Error message', 0);
       END IF;
       COMMIT;
   EXCEPTION
       WHEN OTHERS THEN
           ROLLBACK;
           INSERT INTO cmr.dwh_test_results (test_category, test_name, test_status, error_message, execution_time_ms)
           VALUES ('CATEGORY', 'Test description', 'FAILED', SQLERRM, 0);
           COMMIT;
   END;
   ```
3. Add test file to `test_suite_runner.sql` if new category

### Clearing Test History

```sql
-- Clear all test results
TRUNCATE TABLE cmr.dwh_test_results;

-- Or clear results older than 30 days
DELETE FROM cmr.dwh_test_results WHERE test_date < SYSDATE - 30;
COMMIT;
```

## Continuous Integration

For automated testing in CI/CD pipelines:

```bash
#!/bin/bash
# run_ilm_tests.sh

# Connect and run tests
sqlplus -S cmr/password@database <<EOF
@test_suite_runner.sql
EXIT
EOF

# Check exit code
if [ $? -eq 0 ]; then
    echo "Tests completed"

    # Query results and check for failures
    FAILURES=$(sqlplus -S cmr/password@database <<EOSQL
    SET HEADING OFF FEEDBACK OFF
    SELECT COUNT(*) FROM cmr.dwh_test_results
    WHERE test_date > SYSDATE - 1 AND test_status = 'FAILED';
    EXIT
EOSQL
)

    if [ "$FAILURES" -gt 0 ]; then
        echo "FAILED: $FAILURES test(s) failed"
        exit 1
    else
        echo "SUCCESS: All tests passed"
        exit 0
    fi
else
    echo "ERROR: Test execution failed"
    exit 1
fi
```

## Support

For issues or questions about the test suite:
- Review test output and `dwh_test_results` table
- Check `docs/operations/OPERATIONS_RUNBOOK.md` for operational guidance
- Verify ILM framework installation is complete and current

---

**Test Suite Version**: 1.0
**Last Updated**: 2025-10-23
**Compatible with ILM Framework**: v2.0+
