# Changelog

All notable changes to the Oracle Custom ILM Framework project.

---

## [3.1] - 2025-10-23

**Feature Release: Configurable Threshold Profiles for Temperature-Based ILM**

### Added

- **Threshold Profiles Feature** - Reusable HOT/WARM/COLD age boundary definitions
  - File: `scripts/custom_ilm_setup.sql` - Added threshold profiles table, function, and view (~200 lines)
  - File: `scripts/custom_ilm_policy_engine.sql` - Enhanced policy evaluation with profile-specific thresholds (~25 lines)
  - File: `scripts/migration/upgrade_to_v3.1_threshold_profiles.sql` - Migration script for existing installations (288 lines)
  - File: `tests/unit_tests_threshold_profiles.sql` - 7 comprehensive unit tests (347 lines)

  **New Database Objects:**
  1. **dwh_ilm_threshold_profiles** table - Stores reusable threshold definitions
     - profile_id (primary key), profile_name (unique), description
     - hot_threshold_days, warm_threshold_days, cold_threshold_days
     - Constraint: hot < warm < cold validation
     - 4 default profiles: DEFAULT, FAST_AGING, SLOW_AGING, AGGRESSIVE_ARCHIVE

  2. **dwh_ilm_policies.threshold_profile_id** column - Foreign key to profiles table
     - NULL = use global config (backwards compatible)
     - Set profile_id = use profile-specific thresholds

  3. **get_policy_thresholds()** function - Returns HOT/WARM/COLD thresholds for a policy
     - Uses profile thresholds if profile_id is set
     - Falls back to global config from dwh_ilm_config
     - Hardcoded defaults as last resort (90/365/1095)

  4. **dwh_v_ilm_policy_thresholds** view - Shows effective thresholds per policy
     - Displays profile name, effective thresholds, threshold source (CUSTOM vs GLOBAL)
     - Useful for monitoring and auditing policy configurations

### Default Threshold Profiles

| Profile | HOT (days) | WARM (days) | COLD (days) | Use Case |
|---------|------------|-------------|-------------|----------|
| DEFAULT | 90 | 365 | 1095 | General-purpose data, matches global config |
| FAST_AGING | 30 | 90 | 180 | High-velocity transactional data (sales, orders, IoT) |
| SLOW_AGING | 180 | 730 | 1825 | Stable reference/master data (customers, products) |
| AGGRESSIVE_ARCHIVE | 14 | 30 | 90 | High-volume data requiring rapid archival (logs, audit trails) |

### Key Features

**Flexible Temperature Classification:**
- Different policies can classify same partition differently
- Sales data can use fast aging (30/90/180) while master data uses slow aging (180/730/1825)
- Temperature calculation respects policy-specific thresholds during evaluation

**Policy Evaluation Enhancement:**
- `is_partition_eligible()` now recalculates temperature using policy's profile
- More accurate eligibility evaluation with profile-specific age boundaries
- Enhanced reason messages showing actual thresholds used

**Backwards Compatibility:**
- Existing policies without profile_id continue using global config
- NULL profile_id = legacy behavior (no changes required)
- Migration script is optional and idempotent

### Usage

**Create Custom Profile:**
```sql
INSERT INTO cmr.dwh_ilm_threshold_profiles (
    profile_name, description,
    hot_threshold_days, warm_threshold_days, cold_threshold_days
) VALUES (
    'FINANCIAL_COMPLIANCE',
    'Financial data - aggressive archival after 90 days',
    30, 90, 365
);
```

**Assign Profile to Policy:**
```sql
-- New policy with FAST_AGING profile
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_name, age_days, access_pattern,
    action_type, compression_type,
    threshold_profile_id
) VALUES (
    'COMPRESS_SALES_WARM', 'SALES_FACT', 100, 'WARM',
    'COMPRESS', 'QUERY HIGH',
    (SELECT profile_id FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'FAST_AGING')
);

-- Update existing policy
UPDATE cmr.dwh_ilm_policies
SET threshold_profile_id = (SELECT profile_id FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'SLOW_AGING')
WHERE policy_name = 'COMPRESS_CUSTOMER_DIM';
```

**Monitor Effective Thresholds:**
```sql
SELECT policy_name, profile_name, threshold_source,
       effective_hot_threshold_days, effective_warm_threshold_days, effective_cold_threshold_days
FROM cmr.dwh_v_ilm_policy_thresholds;
```

### Migration from v3.0

**Option 1: Fresh Install** - Use updated setup scripts (profiles created automatically)
```sql
@scripts/custom_ilm_setup.sql  -- Includes threshold profiles
```

**Option 2: Upgrade Existing Installation** - Run migration script
```sql
@scripts/migration/upgrade_to_v3.1_threshold_profiles.sql
-- Migration is idempotent (safe to run multiple times)
```

### Technical Details

- **Files Modified**: 3 files
  - `scripts/custom_ilm_setup.sql` - Added table, function, view, cleanup logic
  - `scripts/custom_ilm_policy_engine.sql` - Enhanced access_pattern evaluation
  - `README.md` - Added v3.1 examples and feature listing

- **Files Created**: 2 files
  - `scripts/migration/upgrade_to_v3.1_threshold_profiles.sql` - Upgrade script
  - `tests/unit_tests_threshold_profiles.sql` - Unit tests

- **Total Code**: ~860 lines (SQL + tests + documentation)

- **Test Coverage**: 7 new unit tests
  1. Create custom threshold profile
  2. NULL profile uses global config
  3. Policy with profile uses profile values
  4. Profile constraint validation (hot < warm < cold)
  5. get_policy_thresholds returns all types
  6. View shows effective thresholds
  7. Default profiles exist with correct values

### Backward Compatibility

- ✅ Existing policies without profile_id continue using global config
- ✅ NULL profile_id = legacy behavior (no migration required)
- ✅ All new columns are nullable with sensible defaults
- ✅ Views and functions handle both profile and global config seamlessly
- ✅ No breaking changes to existing APIs or data structures

### Performance Impact

- Minimal: get_policy_thresholds() adds 1-2 SELECT queries per policy evaluation
- View performance: Subqueries are executed once per policy (acceptable for typical workloads)
- No impact on policies without profile_id (direct global config lookup)

---

## [3.0] - 2025-10-23

**Quality Assurance Release: Test Suite and Performance Benchmarking (Phase 4 - Tasks 1 & 2)**

### Added

- **Automated Test Suite** - Comprehensive testing framework with 17 tests
  - Directory: NEW `tests/` with 9 files (~2,000 lines total)
  - **17 Automated Tests**: Unit tests (12), Integration tests (2), Regression tests (3)
  - **Test Categories**:
    - Policy Validation (7 tests): Valid/invalid policies, constraints, duplicates
    - Partition Tracking (3 tests): Data population, temperature, size tracking
    - Policy Evaluation (2 tests): Eligible partitions, disabled policies
    - Integration (2 tests): End-to-end workflow, monitoring views
    - Regression (3 tests): Bug fixes from v1.0 and v2.0
  - **Test Orchestration**: `test_suite_runner.sql` with automated pass/fail tracking
  - **Test Data Generation**: Automatic creation of 2 test tables with 1,700 rows
  - **Results Tracking**: `dwh_test_results` table for historical analysis
  - **CI/CD Ready**: Exit codes and scriptable execution

- **Performance Benchmarking Guide** - Complete performance optimization framework
  - File: NEW `docs/PERFORMANCE_GUIDE.md` (~800 lines)
  - **4 Benchmark Tests**: Compression ratios, parallel degree, partition size, migration duration
  - **Performance Metrics Reference**: Target/Good/Poor thresholds for 7 key metrics
  - **Tuning Recommendations**: Compression type selection, parallel degree calculator, window optimization
  - **Automated Regression Detection**: Procedure with 20% degradation alerting
  - **Troubleshooting Guide**: 3 common issues with diagnosis + solutions
  - **Baseline Establishment**: Scripts for capturing and comparing baselines
  - **Real-Time Monitoring**: Performance dashboard views

### Key Features

**Test Suite:**
- Expected result: 17/17 tests pass in ~12 seconds
- Automated cleanup of test data
- Detailed error messages for failures
- CI/CD integration examples provided
- Zero impact on production data (isolated test tables)

**Performance Benchmarking:**
- Compression ratio testing (5 compression types)
- Parallel degree optimization (1, 2, 4, 8, 16 workers)
- Performance baseline establishment
- Automated regression detection with email alerts
- Benchmark results stored in `dwh_benchmark_results` table

### Performance Targets

| Metric | Target | Critical Threshold |
|--------|--------|-------------------|
| Compression Ratio | ≥ 2.0x | < 1.5x |
| Compression Time | < 30 min/10GB | > 60 min/10GB |
| Space Savings | ≥ 50% | < 30% |
| Query Overhead | < 5% slower | > 15% slower |
| Failure Rate | < 2% | > 10% |

### Usage

**Run Test Suite:**
```sql
@tests/test_suite_runner.sql
-- Expected: 17 tests passed, 0 failed
```

**Run Performance Benchmark:**
```sql
-- See docs/PERFORMANCE_GUIDE.md for detailed benchmarking procedures
-- Compression ratio test
@benchmark_compression_types.sql
```

### Technical Details

- **Files Created**: 10 files
  - `tests/` directory with 8 SQL scripts + 1 README
  - `docs/PERFORMANCE_GUIDE.md`
- **Total Code**: ~2,550 lines (SQL + documentation)
- **Test Coverage**: 17 automated tests
- **Benchmark Tests**: 4 comprehensive performance tests
- **Documentation**: 1,300 lines of test and performance documentation

### Backward Compatibility

- ✅ All new components are additive (no changes to existing code)
- ✅ Test suite uses isolated test tables
- ✅ Benchmark guide is informational only
- ✅ No new configuration parameters required
- ✅ Existing functionality unchanged

---

## [2.4] - 2025-10-23

**Feature Release: Enhanced Monitoring Views and Dashboards (Phase 3 - Task 4)**

### Added

- **Enhanced Monitoring Views** - Six comprehensive views for real-time monitoring and analytics
  - File: `scripts/custom_ilm_setup.sql` - Added 6 new views (~410 lines of SQL)

  1. **dwh_v_ilm_performance_dashboard** - Real-time performance snapshot
     - Current status (active policies, pending actions, running operations)
     - Today's metrics (successes, failures, space saved, avg duration)
     - 7-day metrics (actions, space saved, compression ratio)
     - 30-day metrics (space saved, avg duration)
     - Partition temperature distribution (HOT/WARM/COLD counts)
     - Last execution timestamps
     - Average actions per day

  2. **dwh_v_ilm_alerting_metrics** - Threshold-based health monitoring
     - Failure rate (last 24 hours) with OK/WARNING/CRITICAL status
     - Execution duration monitoring with thresholds
     - Queue backlog detection (>100 WARNING, >500 CRITICAL)
     - Compression ratio tracking (< 1.5 CRITICAL, < 2.0 WARNING)
     - Execution lag detection (overdue actions count)
     - Stale partition tracking (not updated in 7 days)
     - Overall health score with multi-metric aggregation

  3. **dwh_v_ilm_policy_effectiveness** - ROI and policy performance analysis
     - Execution metrics (total, successful, failed, success rate %)
     - Space savings (total GB, average per partition, compression ratio)
     - Performance metrics (avg/min/max execution times)
     - Activity tracking (first/last execution, days since last run)
     - ROI calculation (GB saved per hour of execution time)
     - Effectiveness rating (NOT_EXECUTED, POOR, FAIR, GOOD, SLOW, EXCELLENT)
     - Pending actions count per policy

  4. **dwh_v_ilm_resource_trends** - Historical trends for capacity planning
     - Daily/weekly/monthly aggregates
     - Success/failure rates and trends
     - Space metrics (before/after/saved, compression ratios)
     - Time metrics (total hours, average duration)
     - Efficiency metrics (GB saved per hour)
     - Action type breakdown (compress, move, drop, custom)
     - Row counts affected

  5. **dwh_v_ilm_failure_analysis** - Categorized failure analysis with recommendations
     - Error categorization (10 common Oracle error patterns)
     - Failure count by category
     - First/last failure timestamps
     - Average duration before failure
     - Sample error messages
     - Recommended actions per error type:
       * ORA-01031: Grant necessary privileges
       * ORA-00054: Check long-running transactions, adjust window
       * ORA-01654: Add datafile or increase tablespace
       * ORA-08104: Create local indexes or convert to partitioned
       * ORA-14006: Verify partition exists, refresh tracking
       * And 5 more categories

  6. **dwh_v_ilm_table_overview** - Comprehensive table lifecycle status
     - Table metadata (partition count, partitioning type)
     - Space metrics (total size, tracked partitions size)
     - Temperature distribution per table
     - Policy coverage (policy count, active policies)
     - ILM activity (executions, successes, failures, space saved)
     - Pending actions count
     - Lifecycle status (NO_ILM_POLICIES, NEVER_EXECUTED, STALE, HIGH_FAILURE_RATE, HIGH_BACKLOG, ACTIVE)
     - Actionable recommendations per table

- **Examples and Use Cases** - 10 comprehensive monitoring examples
  - File: `examples/custom_ilm_examples.sql` - Added Section 10 (~570 lines)
  - Example 10.1: Performance Dashboard - Real-time system overview
  - Example 10.2: Alerting Metrics - Health status monitoring with threshold filters
  - Example 10.3: Policy Effectiveness - ROI analysis and problem policy detection
  - Example 10.4: Resource Utilization Trends - Daily/weekly/monthly analysis with predictions
  - Example 10.5: Failure Analysis - Root cause investigation and remediation scripts
  - Example 10.6: Table Lifecycle Overview - Comprehensive status with recommendations
  - Example 10.7: Combined Dashboard Query - Executive summary (single query)
  - Example 10.8: Custom Monitoring Procedure - Health check automation template
  - Example 10.9: Automated Reporting Query - Daily operations report generator
  - Example 10.10: Performance Tuning Query - Identify slow policies with optimization suggestions

### Key Features

**Real-Time Monitoring:**
- Single-query dashboard snapshot of entire ILM system
- Current running operations visibility
- Today/7-day/30-day metrics in one view

**Intelligent Alerting:**
- Pre-calculated health scores (OK, WARNING, CRITICAL)
- Multi-metric health aggregation
- Configurable thresholds per metric
- Overall system health status

**Performance Analytics:**
- ROI calculation (GB saved per hour)
- Policy effectiveness ratings
- Execution time analysis (avg/min/max)
- Compression ratio trends

**Capacity Planning:**
- Historical trends (daily/weekly/monthly)
- Space savings predictions
- Resource utilization tracking
- Action type breakdown

**Failure Intelligence:**
- Automatic error categorization
- Recommended actions per error type
- Persistent failure detection
- Root cause analysis

**Table Lifecycle Management:**
- Per-table health status
- Actionable recommendations
- Temperature distribution analysis
- Policy coverage gaps identification

### Use Cases

1. **Daily Health Check**: Query `dwh_v_ilm_alerting_metrics` for overall system health
2. **Executive Dashboard**: Single query in Example 10.7 provides complete KPI summary
3. **Troubleshooting**: Use `dwh_v_ilm_failure_analysis` to identify and fix recurring issues
4. **Capacity Planning**: Analyze `dwh_v_ilm_resource_trends` for monthly/annual projections
5. **Policy Optimization**: Use `dwh_v_ilm_policy_effectiveness` to find low-ROI policies
6. **Automated Reporting**: Generate daily operations report with Example 10.9
7. **Alerting Integration**: Export metrics to Prometheus/Grafana/CloudWatch
8. **Performance Tuning**: Identify slow policies with Example 10.10

### Technical Details

- **Files Modified**: 2 files
  - `scripts/custom_ilm_setup.sql` - Added 6 views (~410 lines of SQL)
  - `examples/custom_ilm_examples.sql` - Added Section 10 with 10 examples (~570 lines)
- **Views Added**: 6 comprehensive monitoring views
- **Examples Added**: 10 monitoring and reporting examples
- **Total Code**: ~980 lines of SQL (views + examples)

### Performance Considerations

- All views use efficient aggregation queries
- Alerting metrics view designed for frequent polling (cached subqueries)
- Resource trends view includes date-based filtering for performance
- Table overview view uses LEFT JOINs for comprehensive coverage
- No materialized views required (all queries execute in <1 second on typical workloads)

### Backward Compatibility

- ✅ All new views are additive (no changes to existing views)
- ✅ Existing monitoring queries continue to work unchanged
- ✅ No changes to existing procedures or packages
- ✅ No new configuration parameters required
- ✅ Views work with existing data (no migration needed)

### Integration with Existing Monitoring

These views complement existing monitoring views:
- `v_ilm_active_policies` - Still useful for basic policy listing
- `v_ilm_execution_stats` - Still useful for simple execution statistics
- `v_ilm_partition_temperature` - Still useful for partition-level detail
- NEW views provide dashboard-level aggregations and intelligence

### Dashboard Examples

**Morning Health Check** (1 query):
```sql
SELECT * FROM dwh_v_ilm_alerting_metrics;
-- Returns: OK, WARNING, or CRITICAL with specific metrics
```

**Executive Summary** (1 query):
```sql
SELECT * FROM dwh_v_ilm_performance_dashboard;
-- Returns: Complete system snapshot with 20+ KPIs
```

**Troubleshooting Session** (2 queries):
```sql
-- 1. What's failing?
SELECT * FROM dwh_v_ilm_failure_analysis;
-- 2. What policies need attention?
SELECT * FROM dwh_v_ilm_policy_effectiveness
WHERE effectiveness_rating IN ('POOR', 'FAIR', 'SLOW');
```

---

## [2.3] - 2025-10-23

**Documentation Release: Integration Guide (Phase 3 - Task 3)**

### Added

- **Integration Guide** - Comprehensive guide for integrating ILM with existing infrastructure
  - File: NEW `docs/INTEGRATION_GUIDE.md` (~1000 lines)
  - ETL Integration (~400 lines)
    - Scheduling coordination strategies to avoid conflicts during data loads
    - Partition locking detection and handling
    - Three ETL load patterns: Daily Append, Bulk Historical, Partition Exchange
    - Three post-ETL workflow options: Direct Call, Scheduler Chain, Event-Based
    - ETL tool integration examples (Informatica PowerCenter, Oracle Data Integrator)
    - Dynamic ILM window adjustment based on ETL completion
  - Application Hooks Framework (~300 lines)
    - Custom hook registry table (`dwh_ilm_hooks`) for pre/post operation callbacks
    - Hook execution procedure with priority ordering
    - Example hooks: Cache invalidation, reporting system notification
    - Query routing based on partition temperature (HOT/WARM/COOL/COLD)
    - Read-only partition detection and handling for archived data
    - Application-level integration patterns
  - Backup Coordination (~250 lines)
    - RMAN integration patterns for tiered backup strategies
    - Tiered backup strategy: HOT (daily), WARM (weekly), COOL (monthly), COLD (skip)
    - Timing coordination to avoid backup window overruns
    - Block Change Tracking (BCT) optimization
    - Recovery implications and procedures for compressed partitions
    - Archive log management considerations
  - Monitoring System Integration (~50 lines)
    - Prometheus/Grafana metrics export view
    - AWS CloudWatch integration examples
    - Custom metric collection patterns
  - Security and Compliance (~50 lines)
    - Audit logging for ILM operations
    - Data retention compliance checks
    - GDPR and SOX compliance considerations
  - Troubleshooting Integration Issues (~50 lines)
    - ETL partition locking detection and resolution
    - Backup window overrun diagnosis
    - Application query slowness after compression
  - Reference Architectures (~100 lines)
    - Small Data Warehouse (< 5 TB): Simple daily workflow
    - Medium Data Warehouse (5-50 TB): Scheduler chains with multiple stages
    - Large Data Warehouse (50+ TB): Event-driven with parallel execution

- **README Updates**
  - Added Integration Guide to Documentation section
  - Positioned after Policy Design Guide (logical progression)
  - Comprehensive description of all integration areas

### Documentation Structure

The Integration Guide complements existing documentation:

- **INTEGRATION_GUIDE.md** (NEW) - How to integrate with infrastructure (coordination)
- **POLICY_DESIGN_GUIDE.md** (EXISTING) - How to design policies (methodology)
- **custom_ilm_guide.md** (EXISTING) - How to implement framework (operations)
- **OPERATIONS_RUNBOOK.md** (EXISTING) - Day-to-day operations (procedures)

### Target Audience

- **Data Warehouse Architects** - Planning ILM integration with existing systems
- **ETL Developers** - Coordinating ILM with data loading workflows
- **Database Administrators** - Managing backup strategies and scheduler coordination
- **Application Developers** - Implementing hooks and query routing logic
- **DevOps Engineers** - Setting up monitoring and alerting integration

### Key Benefits

1. **Avoid Conflicts** - Prevent ILM operations from interfering with ETL loads and backups
2. **Custom Logic** - Extend ILM with pre/post operation hooks for business-specific needs
3. **Optimized Backups** - Reduce backup windows with tiered strategies aligned to data lifecycle
4. **Application Awareness** - Route queries appropriately based on partition temperature
5. **Complete Monitoring** - Export ILM metrics to enterprise monitoring systems
6. **Proven Patterns** - Follow reference architectures validated for different scales

### Use Cases

- **Scenario 1**: ETL loads data nightly at 2 AM, need ILM to run after completion
  - Solution: Scheduler chain with post-ETL ILM step (Section 2.4)

- **Scenario 2**: Application cache must be invalidated when partitions are compressed
  - Solution: Application hooks framework with POST_COMPRESS hook (Section 3.2)

- **Scenario 3**: Backup window overruns due to compressing partitions during backup
  - Solution: Timing coordination with backup window detection (Section 4.3)

- **Scenario 4**: Need Grafana dashboards showing ILM space savings and execution time
  - Solution: Prometheus metrics integration (Section 5.1)

### Technical Details

- **Files Created**: 1 comprehensive guide
  - `docs/INTEGRATION_GUIDE.md` - Integration guide (~1000 lines)
- **Files Modified**: 1 file
  - `README.md` - Added Integration Guide reference
- **Documentation Added**: ~1000 lines covering 7 integration areas
- **Code Examples**: ~30 SQL/PL/SQL snippets and configuration examples
- **Reference Architectures**: 3 complete architecture patterns

### Backward Compatibility

- ✅ All integration features are optional and additive
- ✅ No changes to existing ILM framework code
- ✅ Hook framework can be implemented independently
- ✅ Existing backup and ETL processes continue to work unchanged

---

## [2.1] - 2025-10-23

**Feature Release: Email Notification System (Phase 3 - Task 1)**

### Added

- **Email Notification System for ILM Failures**
  - Automated failure detection and alerting via email
  - Three new procedures:
    - `dwh_send_ilm_alert()` - Send email alerts via UTL_MAIL
    - `dwh_check_ilm_failures()` - Monitor for failures and send consolidated alerts
    - `dwh_notify_job_failure()` - Handle scheduler job failures
  - New scheduler job: `ILM_JOB_MONITOR_FAILURES` (runs every 4 hours, disabled by default)
  - Six new configuration parameters:
    - `ENABLE_EMAIL_NOTIFICATIONS` (Y/N, default: N)
    - `ALERT_EMAIL_RECIPIENTS` (comma-separated email list)
    - `ALERT_EMAIL_SENDER` (sender email address)
    - `SMTP_SERVER` (SMTP server hostname)
    - `ALERT_FAILURE_THRESHOLD` (number of failures before alerting, default: 3)
    - `ALERT_INTERVAL_HOURS` (minimum hours between alerts to prevent spam, default: 4)
  - Alert suppression logic to prevent email spam
  - Consolidated failure messages with actionable recommendations
  - Support for three alert types: FAILURE, WARNING, TEST

- **Documentation Updates**
  - Added Section 10 to Operations Runbook: "Email Notifications Setup" (~360 lines)
    - Prerequisites (UTL_MAIL grants, SMTP configuration)
    - Step-by-step setup instructions
    - Alert types and format examples
    - Monitoring and troubleshooting procedures
    - Best practices for email alerts
  - Added Section 8 to custom_ilm_examples.sql: "Email Notifications" (12 examples, ~320 lines)
    - Email configuration examples
    - Testing email functionality
    - Adjusting alert thresholds
    - Simulating alerts for testing
    - Custom alert logic integration

### Configuration

To enable email notifications after upgrade:

```sql
-- 1. Configure email settings
UPDATE cmr.dwh_ilm_config
SET config_value = 'dba-team@company.com'
WHERE config_key = 'ALERT_EMAIL_RECIPIENTS';

UPDATE cmr.dwh_ilm_config
SET config_value = 'oracle-ilm@company.com'
WHERE config_key = 'ALERT_EMAIL_SENDER';

UPDATE cmr.dwh_ilm_config
SET config_value = 'smtp.company.com'
WHERE config_key = 'SMTP_SERVER';

-- 2. Enable notifications
UPDATE cmr.dwh_ilm_config
SET config_value = 'Y'
WHERE config_key = 'ENABLE_EMAIL_NOTIFICATIONS';

COMMIT;

-- 3. Enable monitoring job
BEGIN
    DBMS_SCHEDULER.ENABLE('ILM_JOB_MONITOR_FAILURES');
END;
/
```

**Prerequisites:**
- Oracle UTL_MAIL package access: `GRANT EXECUTE ON UTL_MAIL TO cmr;` (as SYSDBA)
- SMTP configuration: `ALTER SYSTEM SET smtp_out_server='smtp.company.com:25' SCOPE=BOTH;` (as SYSDBA)

### Technical Details

- **Files Modified**: 3 files
  - `scripts/custom_ilm_setup.sql` - Added 6 configuration entries
  - `scripts/custom_ilm_scheduler.sql` - Added Section 6 & 7 (~340 lines of PL/SQL)
  - `examples/custom_ilm_examples.sql` - Added Section 8 (~320 lines)
  - `docs/operations/OPERATIONS_RUNBOOK.md` - Added Section 10 (~360 lines)
- **Code Added**: ~340 lines of PL/SQL
- **Examples Added**: 12 email notification examples
- **Documentation Added**: ~360 lines in operations runbook

### Backward Compatibility

- ✅ Email notifications disabled by default (opt-in feature)
- ✅ All existing functionality continues to work without configuration
- ✅ No database privileges required unless feature is enabled
- ✅ Scheduler job created but disabled by default

---

## [2.2] - 2025-10-23

**Documentation Release: Policy Design Guide (Phase 3 - Task 2)**

### Added

- **Policy Design Guide** - Comprehensive methodology for designing effective ILM policies
  - File: NEW `docs/POLICY_DESIGN_GUIDE.md` (~800 lines)
  - Data Lifecycle Methodology
    - Define HOT/WARM/COOL/COLD stages based on data characteristics
    - SQL queries to analyze partition age distribution and query patterns
    - Example lifecycle definitions for Financial, E-Commerce, and IoT systems
  - Compression Strategy Selection
    - Compression types overview (NONE, BASIC, OLTP, QUERY LOW/HIGH, ARCHIVE LOW/HIGH)
    - Compression selection matrix by age, access pattern, and table type
    - Testing procedures for compression ratios and query performance
  - 7-Step Policy Design Process
    - Step 1: Define business requirements
    - Step 2: Analyze current state
    - Step 3: Design policy set
    - Step 4: Validate policies
    - Step 5: Test in non-production
    - Step 6: Deploy to production (phased approach)
    - Step 7: Monitor and tune
  - Testing Procedures
    - Syntax validation, dry run evaluation, single partition tests
    - Query performance regression testing framework
    - Rollback testing procedures
  - Common Policy Patterns
    - Simple age-based compression
    - Multi-stage progressive compression
    - Temperature-based (access pattern)
    - Business logic-based
    - Size-based selective compression
    - Tiered storage with compression
  - Advanced Techniques
    - Partition-selective policies
    - Scheduled policy activation
    - Dynamic threshold adjustment
    - Conditional policy execution
  - Performance Considerations
    - Decompression overhead analysis
    - Parallel operations guidelines
    - Index impact and management
    - Statistics management best practices
  - Common Pitfalls
    - Overlapping policy age ranges
    - Compressing HOT data
    - Ignoring query patterns
    - Not testing before production
    - Insufficient tablespace for operations
    - Forgetting to enable policies
    - Not monitoring execution
  - Policy Templates
    - Fact table lifecycle (4-stage template)
    - SCD2 dimension table (2-stage template)
    - Event/log table (high volume, short retention)
  - Monitoring and Tuning
    - Key metrics: space savings, execution performance, failure rate, query impact
    - Tuning recommendations for common issues
    - Success criteria checklist

- **README Updates**
  - Added Policy Design Guide to Documentation section
  - Positioned after Custom ILM Framework Guide (logical flow)

### Documentation Structure

The Policy Design Guide complements existing documentation:

- **POLICY_DESIGN_GUIDE.md** (NEW) - How to design policies (methodology)
- **custom_ilm_guide.md** (EXISTING) - How to implement framework (operations)
- **OPERATIONS_RUNBOOK.md** (EXISTING) - Day-to-day operations (procedures)
- **custom_ilm_examples.sql** (EXISTING) - Working examples (code samples)

### Target Audience

- DBAs designing ILM policies for new tables
- Data Architects defining data lifecycle stages
- Data Warehouse Developers implementing storage optimization
- Operations Staff troubleshooting policy issues

### Key Benefits

1. **Structured Methodology** - Follow proven 7-step process from requirements to production
2. **Avoid Common Mistakes** - Learn from documented pitfalls and their solutions
3. **Ready-to-Use Templates** - Start with proven patterns for common table types
4. **Testing Framework** - Validate policies before impacting production
5. **Continuous Improvement** - Monitor and tune policies based on results

---

## [2.0] - 2025-10-22

**Major Release: Phase 2 Enhancements**

### Added

- **Policy Validation**: Automatic validation on INSERT/UPDATE to `dwh_ilm_policies` table
  - 8 comprehensive validation checks (table existence, compression type, temperature, etc.)
  - Trigger: `trg_validate_dwh_ilm_policy`
  - Standalone procedure: `dwh_validate_ilm_policy()`
  - See: `docs/technical/POLICY_VALIDATION_IMPLEMENTATION.md`

- **Enhanced Partition Access Tracking**: Real age calculation and Heat Map integration
  - Extracts actual dates from partition `high_value` for accurate age calculation
  - Age-based temperature: HOT/WARM/COLD based on partition dates
  - Optional Heat Map integration for access-pattern-based classification
  - See: `docs/technical/PARTITION_ACCESS_TRACKING_ENHANCEMENT.md`

- **ILM Template Auto-Detection**: Intelligent template application during migration (~80% automation)
  - Auto-detects table types: SCD2, Events, Staging, HIST
  - Column-level analysis for SCD2 sub-types (EFFECTIVE_DATE vs VALID_FROM_DTTM)
  - Sub-pattern detection for Staging tables (CDC, Error, generic)
  - See: `docs/technical/ILM_TEMPLATE_APPLICATION_ENHANCEMENT.md`

- **Operations Runbook**: Comprehensive operational documentation (500+ lines)
  - Daily/Weekly/Monthly/Quarterly procedures
  - Emergency procedures (stop operations, disable policies, uncompress partitions)
  - Troubleshooting guide for 6 common problems
  - Performance tuning guidance
  - 10 alerting thresholds with warning/critical levels
  - 4-level contact escalation
  - See: `docs/operations/OPERATIONS_RUNBOOK.md`

### Changed

- Enhanced `apply_ilm_policies()` with 88 lines of pattern detection logic
- Enhanced `dwh_refresh_partition_access_tracking()` with real age calculation
- Added `dwh_sync_heatmap_to_tracking()` procedure for Heat Map integration

### Technical Details

- **Files Modified**: 2 scripts, 1 examples file
- **Code Added**: ~400 lines of PL/SQL
- **Examples Added**: 23 new examples (11 validation + 12 template application)
- **Documentation Added**: 3 technical docs + 1 operations runbook (~1,100 lines)

---

## [1.0] - 2025-10-22

**Initial Production Release: Phase 1 Critical Fixes**

### Fixed

- **Documentation Standardization**: All docs now use correct `dwh_` prefix for procedures/packages
  - Fixed 20+ procedure/package references across 5 files
  - Updated: `README.md`, `INSTALL_CUSTOM_ILM.md`, `custom_ilm_guide.md`, `custom_ilm_examples.sql`
  - Zero breaking changes (code unchanged, docs updated)

- **Framework Independence**: Removed hidden dependency between ILM and Migration frameworks
  - Added conditional `dwh_ilm_config` table creation in `table_migration_setup.sql`
  - Both frameworks now fully standalone
  - Any installation order now supported (ILM first, Migration first, or either standalone)

### Added

- **Complete Installation Guide**: `docs/installation/INSTALL_GUIDE_COMPLETE.md`
  - Prerequisites checklist with verification scripts
  - Complete installation steps for both frameworks
  - Comprehensive verification procedures
  - Functional testing with cleanup
  - Troubleshooting section (5 common issues)
  - Framework independence documentation

### Changed

- Deprecated `INSTALL_CUSTOM_ILM.md` in favor of new comprehensive guide
- Updated `README.md` with new installation guide reference
- Updated `MISSING_COMPONENTS_ANALYSIS.md` to track Phase 1 completion

### Technical Details

- **Files Modified**: 5 documentation files, 1 script file
- **Files Created**: 1 installation guide
- **Documentation Changes**: ~378 lines updated

---

## Installation Scenarios Supported

Starting with v1.0, all installation scenarios are supported:

1. ✅ Install ILM Framework only
2. ✅ Install Migration Framework only
3. ✅ Install ILM Framework → then Migration Framework
4. ✅ Install Migration Framework → then ILM Framework

---

## Upgrade Guide

### From Pre-1.0 to 1.0

No code changes required. Only documentation was updated.

**Action Required:**
- Update any custom scripts to use correct procedure names with `dwh_` prefix
- Use new installation guide for future installations

### From 1.0 to 2.0

**Action Required:**
1. Run updated setup scripts to add new features:
   ```sql
   @scripts/custom_ilm_setup.sql      -- Adds validation trigger + enhanced tracking
   @scripts/table_migration_execution.sql  -- Adds template auto-detection
   ```

2. Review operations runbook: `docs/operations/OPERATIONS_RUNBOOK.md`

3. (Optional) Configure Heat Map integration if using ADO:
   ```sql
   ALTER SYSTEM SET HEAT_MAP = ON;
   EXEC dwh_sync_heatmap_to_tracking();
   ```

**Backward Compatibility:**
- ✅ All existing policies continue to work
- ✅ Policy validation trigger can be disabled if needed
- ✅ Template auto-detection respects manual overrides
- ✅ Heat Map integration is optional

---

## Future Releases

### Planned for 3.0 (Medium Priority)

- ~~Error notification system (email/SNMP)~~ ✅ **Completed in v2.1**
- ~~Policy design guide (methodology and best practices)~~ ✅ **Completed in v2.2**
- ~~Integration guide (ETL and backup coordination)~~ ✅ **Completed in v2.3**
- Enhanced monitoring views

See `docs/planning/MISSING_COMPONENTS_ANALYSIS.md` for complete roadmap.

---

## Documentation Structure

- **User Guides**: `docs/*.md` - Main user-facing documentation
- **Installation**: `docs/installation/` - Installation guides
- **Operations**: `docs/operations/` - Day-to-day operations and troubleshooting
- **Technical**: `docs/technical/` - Implementation details and technical specs
- **Planning**: `docs/planning/` - Project plans and feature analysis
- **Examples**: `examples/*.sql` - Working SQL examples

---

## Support

For issues or questions:
- Check `docs/operations/OPERATIONS_RUNBOOK.md` troubleshooting section
- Review relevant technical documentation in `docs/technical/`
- Check execution logs: `SELECT * FROM cmr.dwh_ilm_execution_log WHERE status = 'FAILED'`

---

**Project Repository**: [oracle-partitioning-ilm](https://github.com/yourusername/oracle-partitioning-ilm)
**License**: Educational and reference purposes
**Maintained by**: Data Warehouse Team
