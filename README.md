# Oracle Data Warehouse Partitioning and ILM

A comprehensive guide and implementation toolkit for Oracle database partitioning strategies and Information Lifecycle Management (ILM) in data warehouse environments.

## Overview

This repository provides:
- **Strategy Documentation**: Best practices for partitioning and ILM
- **DDL Examples**: Ready-to-use table creation scripts
- **Custom ILM Framework**: Complete PL/SQL-based lifecycle management (no ADO required)
- **Table Migration Framework**: Convert non-partitioned tables to partitioned with ILM
- **Management Scripts**: Automation for partition lifecycle operations
- **Monitoring Tools**: Diagnostics and health check queries

## ILM Approaches

This repository includes **two approaches** to Information Lifecycle Management:

### 1. Custom PL/SQL ILM Framework (Recommended)
- ✅ **No ADO license required**
- ✅ **Full control over policies and execution**
- ✅ **Metadata-driven with complete audit trail**
- ✅ **Customizable business logic**
- ✅ **Scheduler-based automation**
- ✅ **Configurable threshold profiles** (v3.1+) - Define custom HOT/WARM/COLD aging boundaries per data type

**Quick Start**: See [Custom ILM Guide](docs/custom_ilm_guide.md)

### 2. Oracle ADO (Automatic Data Optimization)
- Requires Oracle Enterprise Edition + ADO option
- Built-in Heat Map tracking
- Pre-defined ILM policies
- Maintenance window execution

**See**: [ILM Strategy](docs/ilm_strategy.md) for ADO approach

---

## Quick Start - Custom ILM Framework

**📖 Complete Installation Guide**: See [docs/installation/INSTALL_GUIDE_COMPLETE.md](docs/installation/INSTALL_GUIDE_COMPLETE.md) for step-by-step installation with prerequisites, verification, and troubleshooting.

**📋 Operations Runbook**: See [docs/operations/OPERATIONS_RUNBOOK.md](docs/operations/OPERATIONS_RUNBOOK.md) for daily operations, troubleshooting, and maintenance procedures.

**📝 What's New**: See [CHANGELOG.md](CHANGELOG.md) for version history, release notes, and upgrade instructions.

### 1. Install Framework

```sql
@scripts/custom_ilm_setup.sql
@scripts/custom_ilm_policy_engine.sql
@scripts/custom_ilm_execution_engine.sql
@scripts/custom_ilm_scheduler.sql
```

### 2. Create a Partitioned Fact Table

```sql
-- Example: Monthly partitioned sales fact table
CREATE TABLE sales_fact (
    sale_id NUMBER(18) NOT NULL,
    sale_date DATE NOT NULL,
    customer_id NUMBER(12) NOT NULL,
    product_id NUMBER(12) NOT NULL,
    total_amount NUMBER(12,2),
    CONSTRAINT pk_sales_fact PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (sale_date)
INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
(
    PARTITION p_2023_12 VALUES LESS THAN (DATE '2024-01-01')
)
COMPRESS FOR QUERY HIGH
PARALLEL 8
ENABLE ROW MOVEMENT;
```

### 3. Define ILM Policy

```sql
-- Basic policy: Compress partitions older than 90 days
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled
) VALUES (
    'COMPRESS_SALES_90D', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 90,
    'QUERY HIGH', 100, 'Y'
);

-- Advanced (v3.1+): Use threshold profiles for custom aging behavior
-- Fast aging for transactional data (compress at 30 days, archive at 90 days)
INSERT INTO cmr.dwh_ilm_policies (
    policy_name, table_owner, table_name,
    policy_type, action_type, age_days,
    compression_type, priority, enabled,
    threshold_profile_id
) VALUES (
    'COMPRESS_SALES_FAST', USER, 'SALES_FACT',
    'COMPRESSION', 'COMPRESS', 30,
    'QUERY HIGH', 100, 'Y',
    (SELECT profile_id FROM cmr.dwh_ilm_threshold_profiles WHERE profile_name = 'FAST_AGING')
);
COMMIT;
```

### 4. Run ILM Cycle

```sql
-- Manual execution
EXEC dwh_run_ilm_cycle();

-- Or enable automatic execution
UPDATE cmr.dwh_ilm_config SET config_value = 'Y'
WHERE config_key = 'ENABLE_AUTO_EXECUTION';
COMMIT;

EXEC dwh_start_ilm_jobs();
```

## Repository Structure

```
oracle-partitioning-ilm/
├── README.md                            # Main entry point (you are here)
├── CHANGELOG.md                         # Version history and release notes
├── docs/
│   ├── # User Guides
│   ├── partitioning_strategy.md         # Partitioning best practices
│   ├── ilm_strategy.md                  # Oracle ADO ILM guide
│   ├── custom_ilm_guide.md              # Custom PL/SQL ILM framework guide
│   ├── table_migration_guide.md         # Table migration framework guide
│   ├── quick_reference.md               # Command cheat sheet
│   │
│   ├── installation/                    # Installation Documentation
│   │   ├── INSTALL_GUIDE_COMPLETE.md    # Complete installation guide (recommended)
│   │   └── INSTALL_CUSTOM_ILM.md        # Legacy installation guide (deprecated)
│   │
│   ├── operations/                      # Operations Documentation
│   │   └── OPERATIONS_RUNBOOK.md        # Daily operations, troubleshooting, maintenance
│   │
│   ├── technical/                       # Technical Documentation
│   │   ├── POLICY_VALIDATION_IMPLEMENTATION.md      # Policy validation details
│   │   ├── PARTITION_ACCESS_TRACKING_ENHANCEMENT.md # Partition tracking enhancement
│   │   ├── ILM_TEMPLATE_APPLICATION_ENHANCEMENT.md  # Template auto-detection
│   │   ├── MIGRATION_WORKFLOW.md        # Migration workflow details
│   │   ├── COLUMN_SELECTION_LOGIC.md    # Column selection algorithm
│   │   ├── DISTINCT_DATES_EXPLAINED.md  # Date handling explanation
│   │   └── REFACTORING_PLAN.md          # Date column detection refactoring
│   │
│   └── planning/                        # Project Planning
│       └── MISSING_COMPONENTS_ANALYSIS.md           # Feature roadmap and status
│
├── examples/
│   ├── fact_table_partitioning.sql      # Fact table DDL examples
│   ├── dimension_table_partitioning.sql # Dimension table DDL
│   ├── scd2_events_staging_partitioning.sql # SCD2, Events, and Staging tables
│   ├── ilm_policies.sql                 # Oracle ADO ILM examples
│   ├── custom_ilm_examples.sql          # Custom ILM usage examples
│   └── table_migration_examples.sql     # Migration framework examples
│
└── scripts/
    ├── partition_management.sql         # Partition operations
    ├── monitoring_diagnostics.sql       # Health checks and monitoring
    ├── custom_ilm_setup.sql             # Custom ILM metadata tables
    ├── custom_ilm_policy_engine.sql     # Policy evaluation engine
    ├── custom_ilm_execution_engine.sql  # Action execution engine
    ├── custom_ilm_scheduler.sql         # Automation scheduler jobs
    ├── table_migration_setup.sql        # Migration framework setup
    ├── table_migration_analysis.sql     # Table analysis package
    └── table_migration_execution.sql    # Migration execution package
```

## Documentation

### Partitioning Strategy
[docs/partitioning_strategy.md](docs/partitioning_strategy.md)

Learn about:
- Partitioning types (Range, List, Hash, Composite)
- Partition key selection guidelines
- Recommended strategies by table type
- Interval partitioning for automatic management
- Performance considerations

### Custom ILM Framework Guide (Recommended)
[docs/custom_ilm_guide.md](docs/custom_ilm_guide.md)

Complete guide for the PL/SQL-based ILM framework:
- Installation and setup
- Policy definition and configuration
- Execution modes (automatic and manual)
- Monitoring and troubleshooting
- Advanced features and customization
- No ADO license required

### Policy Design Guide (New!)
[docs/POLICY_DESIGN_GUIDE.md](docs/POLICY_DESIGN_GUIDE.md)

Methodology for designing effective ILM policies:
- **Data Lifecycle Methodology**: Define HOT/WARM/COOL/COLD stages for your data
- **Compression Strategy Selection**: Choose the right compression type for each stage
- **7-Step Policy Design Process**: Structured approach from requirements to production
- **Testing Procedures**: Validate policies before deployment
- **Common Patterns**: Proven policy templates for different table types
- **Performance Considerations**: Balance storage savings with query performance
- **Pitfalls to Avoid**: Learn from common mistakes

### Integration Guide (New!)
[docs/INTEGRATION_GUIDE.md](docs/INTEGRATION_GUIDE.md)

Comprehensive guide for integrating ILM with existing infrastructure:
- **ETL Integration**: Coordinate ILM operations with data loading workflows
  - Scheduling coordination (avoid conflicts during data loads)
  - Partition locking detection and handling
  - Post-ETL workflow patterns (Direct Call, Scheduler Chains, Event-Based)
  - ETL tool integration (Informatica, Oracle Data Integrator)
- **Application Hooks**: Custom pre/post operation callbacks
  - Hook framework for cache invalidation, notifications, custom logic
  - Query routing based on partition temperature (HOT/WARM/COOL/COLD)
  - Read-only partition detection and handling
- **Backup Coordination**: Align backups with ILM lifecycle stages
  - RMAN integration patterns and tiered backup strategies
  - Timing coordination to avoid backup window conflicts
  - Block Change Tracking optimization
  - Recovery implications for compressed partitions
- **Monitoring Integration**: Export metrics to Prometheus, Grafana, CloudWatch
- **Security & Compliance**: Audit logging, data retention, GDPR/SOX compliance
- **Reference Architectures**: Small/Medium/Large data warehouse configurations

### Oracle ADO ILM Strategy
[docs/ilm_strategy.md](docs/ilm_strategy.md)

For those using Oracle ADO:
- Data lifecycle stages (Hot, Warm, Cool, Cold)
- Automatic Data Optimization (ADO)
- Multi-tier storage policies
- Compression strategies
- Retention and compliance

### Table Migration Framework (New!)
[docs/table_migration_guide.md](docs/table_migration_guide.md)

Migrate non-partitioned tables to partitioned:
- **Automated Analysis**: Recommends optimal partitioning strategy
- **Date Column Conversion**: Automatically converts NUMBER/VARCHAR dates to DATE type
- **Multiple Methods**: CTAS, Online Redefinition, Exchange Partition
- **ILM Integration**: Automatically applies ILM policies after migration
- **Project Management**: Organize and track multiple migrations
- **Validation & Rollback**: Verify success and rollback if needed
- **Complete Audit Trail**: Detailed logging of all steps

Quick example (automatic workflow):
```sql
-- 1. Identify candidates
SELECT * FROM cmr.dwh_v_migration_candidates;

-- 2. Create migration task
INSERT INTO cmr.dwh_migration_tasks (task_name, source_table, migration_method)
VALUES ('Migrate SALES_FACT', 'SALES_FACT', 'CTAS');

-- 3. Analyze table (framework recommends partition strategy)
EXEC pck_dwh_table_migration_analyzer.analyze_table(1);

-- 4. Apply recommendations automatically
EXEC pck_dwh_table_migration_executor.apply_recommendations(1);

-- 5. Execute migration
EXEC pck_dwh_table_migration_executor.execute_migration(1);
```

Or use manual control:
```sql
-- After analysis, review recommendations
SELECT recommendation_summary, partition_type, partition_key
FROM cmr.dwh_migration_analysis WHERE task_id = 1;

-- Manually set your preferences (overrides recommendations)
UPDATE cmr.dwh_migration_tasks
SET partition_type = 'RANGE',
    partition_key = 'sale_date',
    partition_interval = 'MONTHLY'
WHERE task_id = 1;

-- Then execute migration
EXEC pck_dwh_table_migration_executor.execute_migration(1);
```

## Examples

### Fact Table Partitioning
[examples/fact_table_partitioning.sql](examples/fact_table_partitioning.sql)

Includes examples for:
- Range partitioning by month with interval
- Range-Hash composite partitioning for high volume
- Range-List for event-driven data
- Reference partitioning for parent-child relationships
- Financial transactions with compliance requirements

### Dimension Table Partitioning
[examples/dimension_table_partitioning.sql](examples/dimension_table_partitioning.sql)

Examples include:
- Hash partitioning for large dimensions
- List partitioning by category
- Range partitioning for date dimensions
- SCD Type 2 with historical tracking
- Composite list-list for geographic hierarchies

### SCD2, Events, and Staging Tables
[examples/scd2_events_staging_partitioning.sql](examples/scd2_events_staging_partitioning.sql)

Specialized examples for:
- **SCD2 Tables**: Two patterns (effective_date and valid_from_dttm/valid_to_dttm)
- **Events Tables**: Clickstream, audit/compliance, IoT sensors
- **Staging Tables**: Transactional staging, CDC, bulk import, error quarantine
- Integrated ILM policies for each table type
- Automatic detection and migration support

### ILM Policies
[examples/ilm_policies.sql](examples/ilm_policies.sql)

Demonstrates:
- Basic compression policies
- Multi-tier storage tiering
- Partition-specific policies
- Custom function-based policies
- Automated partition archival and purging

## Management Scripts

### Partition Management
[scripts/partition_management.sql](scripts/partition_management.sql)

Includes procedures for:

**Creation and Maintenance:**
- `create_future_partitions` - Pre-create partitions
- `split_partition` - Split for reorganization
- `merge_partitions` - Merge adjacent partitions

**Data Loading:**
- `exchange_partition_load` - Fast bulk loading
- `parallel_partition_load` - Parallel direct path insert

**Maintenance:**
- `truncate_old_partitions` - Remove old data
- `compress_partitions` - Apply compression
- `move_partitions_to_tablespace` - Storage tiering

**Statistics:**
- `gather_partition_statistics` - Incremental stats
- `gather_stale_partition_stats` - Update stale stats

**Health Checks:**
- `check_partition_health` - Identify issues
- `validate_partition_constraints` - Verify integrity
- `partition_size_report` - Size analysis

### Monitoring and Diagnostics
[scripts/monitoring_diagnostics.sql](scripts/monitoring_diagnostics.sql)

Provides queries for:
- Partition distribution and skew analysis
- Partition pruning verification
- Compression effectiveness reporting
- ILM policy execution monitoring
- Heat map analysis
- Index health checks
- Statistics validation
- Space utilization tracking
- Performance monitoring
- Automated alerting

## Common Use Cases

### Use Case 1: Create Monthly Partitioned Table

```sql
-- See examples/fact_table_partitioning.sql
-- Sales fact table with automatic monthly partitions
```

### Use Case 2: Implement Multi-Tier Storage

```sql
-- See examples/ilm_policies.sql
-- Move data through hot → warm → cold → archive tiers
```

### Use Case 3: Bulk Load via Partition Exchange

```sql
-- Load staging table
INSERT INTO sales_staging SELECT * FROM external_source;

-- Exchange with target partition
EXEC exchange_partition_load('SALES_FACT', 'SALES_STAGING', 'P_2024_10');
```

### Use Case 4: Compress Old Partitions

```sql
-- Compress partitions older than 1 year with archive compression
EXEC compress_partitions('SALES_FACT', 'ARCHIVE HIGH', 365);
```

### Use Case 5: Health Check and Monitoring

```sql
-- Run comprehensive health check
EXEC check_partition_health('SALES_FACT');

-- Generate size report
EXEC partition_size_report('SALES_FACT');
```

## Best Practices

### Partitioning

1. **Choose the Right Partition Key**
   - Use columns that appear in WHERE clauses
   - Ensure queries can prune 80%+ of partitions
   - Consider data growth and access patterns

2. **Optimize Partition Count**
   - Recommended: 12-365 partitions for range-partitioned tables
   - Avoid >1000 partitions (metadata overhead)
   - Use interval partitioning for automatic creation

3. **Use Local Indexes**
   - Create local indexes on partition keys
   - Consider global indexes only for non-partition key lookups
   - Maintain indexes during partition operations

### ILM

1. **Enable Heat Map First**
   - Wait 30-90 days to gather access patterns
   - Analyze before implementing policies
   - Use data to inform policy decisions

2. **Start Conservative**
   - Test on non-production first
   - Begin with oldest partitions
   - Monitor compression ratios and performance

3. **Plan for Multiple Tiers**
   - Hot: 0-3 months, minimal compression
   - Warm: 3-12 months, query compression
   - Cold: 1-3 years, archive compression
   - Archive: 3+ years, read-only

4. **Document Retention Requirements**
   - Legal and regulatory requirements
   - Business operational needs
   - Recovery time objectives (RTO)

## Maintenance Schedule

### Daily
- Monitor ILM policy execution
- Check for unusable indexes
- Review partition load operations

### Weekly
- Gather statistics on active partitions
- Review compression effectiveness
- Analyze query performance

### Monthly
- Run partition health checks
- Create future partitions (if not using interval)
- Review and adjust ILM policies
- Analyze space utilization

### Quarterly
- Validate retention compliance
- Review partitioning strategy
- Optimize underperforming queries
- Plan for data growth

## Performance Tuning

### Query Optimization

1. **Verify Partition Pruning**
```sql
-- Check execution plan
EXPLAIN PLAN FOR
SELECT * FROM sales_fact WHERE sale_date = DATE '2024-01-15';

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'PARTITION'));
```

2. **Enable Partition-Wise Joins**
```sql
ALTER SESSION SET OPTIMIZER_FEATURES_ENABLE = '19.1.0';
ALTER SESSION SET PARALLEL_DEGREE_POLICY = 'AUTO';
```

3. **Use Partition-Extended Names**
```sql
-- Direct partition access
SELECT * FROM sales_fact PARTITION (p_2024_01);
```

### Load Optimization

1. **Use Direct Path Insert**
```sql
INSERT /*+ APPEND */ INTO sales_fact SELECT * FROM staging;
```

2. **Leverage Partition Exchange**
```sql
-- Fastest method for bulk partition loads
ALTER TABLE sales_fact EXCHANGE PARTITION p_2024_01 WITH TABLE staging;
```

3. **Disable Constraints During Load**
```sql
ALTER TABLE sales_fact MODIFY CONSTRAINT pk_sales_fact DISABLE;
-- Load data
ALTER TABLE sales_fact MODIFY CONSTRAINT pk_sales_fact ENABLE NOVALIDATE;
```

## Troubleshooting

### Issue: Partition Pruning Not Working

**Check:**
- Is partition key in WHERE clause?
- Are there implicit type conversions?
- Is the query using bind variables incorrectly?

**Solution:**
```sql
-- Bad: implicit conversion
WHERE sale_date = '2024-01-15'  -- String

-- Good: explicit date
WHERE sale_date = DATE '2024-01-15'
```

### Issue: ILM Policy Not Executing

**Check:**
```sql
-- Verify Heat Map is enabled
SELECT * FROM V$OPTION WHERE PARAMETER = 'Heat Map';

-- Check policy status
SELECT * FROM USER_ILMPOLICIES WHERE OBJECT_NAME = 'SALES_FACT';

-- Force execution
EXEC DBMS_ILM.FLUSH_ALL_SEGMENTS;
```

### Issue: Unusable Index Partitions

**Solution:**
```sql
-- Rebuild all unusable index partitions
BEGIN
    FOR idx IN (
        SELECT index_name, partition_name
        FROM user_ind_partitions
        WHERE status = 'UNUSABLE'
    ) LOOP
        EXECUTE IMMEDIATE
            'ALTER INDEX ' || idx.index_name ||
            ' REBUILD PARTITION ' || idx.partition_name;
    END LOOP;
END;
/
```

## Requirements

- Oracle Database 12c or higher (19c+ recommended for full ILM features)
- SYSTEM privileges to enable Heat Map
- Sufficient storage for multiple tiers (optional)
- AWR license for advanced monitoring (optional)

## Contributing

This is a reference implementation. Customize for your specific:
- Data volumes and growth rates
- Query patterns and SLAs
- Compliance and retention requirements
- Hardware and storage infrastructure
- Backup and recovery strategies

## License

These scripts and documentation are provided as-is for educational and reference purposes.

## References

- Oracle Database VLDB and Partitioning Guide
- Oracle Database Administrator's Guide
- Oracle Automatic Data Optimization White Papers
- Oracle Information Lifecycle Management Best Practices

---

**Author**: Data Warehouse Team
**Last Updated**: 2024-10-02
**Version**: 1.0
