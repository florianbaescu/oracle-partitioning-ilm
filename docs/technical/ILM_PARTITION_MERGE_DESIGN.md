# ILM Partition Merge Design Document

## Overview

This document describes the design for automatic partition merging in the tiered partitioning ILM system. The goal is to consolidate monthly partitions into yearly partitions as they age through tiers, maintaining optimal partition counts while preserving the tiered storage strategy.

## Problem Statement

### Current Behavior
When a table is created with tiered partitioning:
- **COLD tier**: YEARLY partitions (data > 60 months old)
- **WARM tier**: YEARLY partitions (data 24-60 months old)
- **HOT tier**: MONTHLY partitions (data < 24 months old)

### The Issue
As time passes, monthly HOT partitions age and ILM policies move them to WARM:
- Month 0: `P_2023_11` created in HOT (TBS_HOT, no compression)
- Month 24: ILM moves `P_2023_11` to WARM (TBS_WARM, BASIC compression)
- Month 25: ILM moves `P_2023_12` to WARM (TBS_WARM, BASIC compression)

**Result**: WARM tier accumulates monthly partitions over time, defeating the purpose of yearly intervals in WARM/COLD tiers.

After 5 years, WARM could have:
- Initial yearly partitions: `P_2022`, `P_2023` (partial)
- Accumulated monthly: `P_2023_11`, `P_2023_12`, `P_2024_01`, ..., `P_2025_10` (24+ partitions)

## Solution: Automatic Partition Merge

### Design Principles

1. **Conservative**: Only merge when safe and beneficial
2. **Non-blocking**: Merges should not interfere with DML operations
3. **Idempotent**: Can be run multiple times safely
4. **Auditable**: All merge operations logged
5. **Tier-aware**: Merge logic respects tier boundaries and intervals

### Merge Strategy

#### When to Merge

A monthly partition should be merged into a yearly partition when:

1. ✅ Monthly partition has been moved to WARM/COLD tier
2. ✅ A yearly partition exists for the same year
3. ✅ The monthly partition is **adjacent** to the yearly partition (Oracle requirement)
4. ✅ Both partitions are in the same tablespace
5. ✅ No active transactions on either partition
6. ✅ Merge would not violate partition bounds

#### Merge Scenarios

**Scenario 1: Incremental Merge (Preferred)**
```
Initial state:
  P_2023: VALUES LESS THAN (2023-11-01) -- Yearly in WARM
  P_2023_11: VALUES LESS THAN (2023-12-01) -- Monthly just moved to WARM

Action: MERGE PARTITIONS P_2023, P_2023_11 INTO PARTITION P_2023
Result: P_2023: VALUES LESS THAN (2023-12-01) -- Extended by one month
```

**Scenario 2: Complete Year**
```
State after all 12 months merged:
  P_2023: VALUES LESS THAN (2024-01-01) -- Full year

Next month arrives:
  P_2024_01: moves to WARM

Result: P_2024_01 starts a new yearly partition for 2024
```

**Scenario 3: Multiple Monthly Partitions**
```
Initial state:
  P_2023: VALUES LESS THAN (2023-11-01)
  P_2023_11: VALUES LESS THAN (2023-12-01)
  P_2023_12: VALUES LESS THAN (2024-01-01)

If both monthly partitions moved on same day:
  1. MERGE P_2023, P_2023_11 → P_2023 (2023-01-01 to 2023-12-01)
  2. MERGE P_2023, P_2023_12 → P_2023 (2023-01-01 to 2024-01-01)
```

## Implementation Design

### Component 1: Merge Detection Function

**Function**: `can_merge_partition(p_partition_info, p_tier_config) RETURN BOOLEAN`

**Logic**:
```sql
1. Get partition details (name, table, high_value, tablespace)
2. Check if partition is monthly format (P_YYYY_MM)
3. Extract year from partition name
4. Look for yearly partition (P_YYYY) in same table
5. Check if both partitions are adjacent
6. Check if both in same tablespace
7. Return TRUE if all checks pass
```

**Returns**:
- `TRUE`: Partition can and should be merged
- `FALSE`: Partition should remain standalone

### Component 2: Merge Execution Procedure

**Procedure**: `merge_monthly_into_yearly(p_table_owner, p_table_name, p_monthly_partition)`

**Steps**:
```sql
1. Validate inputs
2. Get yearly partition name (P_YYYY from P_YYYY_MM)
3. Check if yearly partition exists
4. Verify partitions are adjacent (Oracle requirement)
5. Execute: ALTER TABLE owner.table
   MERGE PARTITIONS yearly_part, monthly_part
   INTO PARTITION yearly_part;
6. Log merge operation in dwh_ilm_execution_log
7. Update partition access tracking
8. Handle errors with rollback
```

**Safety Checks**:
- Lock timeout to prevent hanging
- Verify table is not being dropped
- Check no DDL in progress
- Validate partition bounds
- Ensure compression and storage are compatible

### Component 3: Integration with ILM Engine

**Modified**: `move_partition` procedure in `pck_dwh_ilm_execution_engine`

**New Flow**:
```sql
PROCEDURE move_partition(...) AS
BEGIN
    -- Existing move logic
    v_sql := 'ALTER TABLE ... MOVE PARTITION ... TABLESPACE ...';
    EXECUTE IMMEDIATE v_sql;

    -- NEW: Check if merge is needed
    IF partition_is_monthly(p_partition_name) THEN
        IF can_merge_partition(p_table_owner, p_table_name, p_partition_name) THEN
            BEGIN
                merge_monthly_into_yearly(
                    p_table_owner,
                    p_table_name,
                    p_partition_name
                );

                DBMS_OUTPUT.PUT_LINE('  └─> Merged into yearly partition');
            EXCEPTION
                WHEN OTHERS THEN
                    -- Log merge failure but don't fail the move
                    log_merge_failure(p_partition_name, SQLERRM);
            END;
        END IF;
    END IF;

    -- Existing post-move logic (rebuild indexes, stats)
END move_partition;
```

### Component 4: Merge Tracking

**New Table**: `dwh_ilm_partition_merges`

```sql
CREATE TABLE cmr.dwh_ilm_partition_merges (
    merge_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merge_date          TIMESTAMP DEFAULT SYSTIMESTAMP,
    table_owner         VARCHAR2(128) NOT NULL,
    table_name          VARCHAR2(128) NOT NULL,
    source_partition    VARCHAR2(128) NOT NULL, -- Monthly partition (P_2023_11)
    target_partition    VARCHAR2(128) NOT NULL, -- Yearly partition (P_2023)
    merge_status        VARCHAR2(20) NOT NULL,  -- SUCCESS, FAILED, SKIPPED
    error_message       VARCHAR2(4000),
    duration_seconds    NUMBER,
    rows_merged         NUMBER,

    CONSTRAINT chk_merge_status CHECK (merge_status IN ('SUCCESS', 'FAILED', 'SKIPPED'))
);
```

**Purpose**:
- Audit trail of all merge operations
- Track merge success/failure rates
- Debug merge issues
- Analyze partition consolidation progress

## Oracle MERGE PARTITIONS Syntax

### Basic Syntax
```sql
ALTER TABLE schema.table_name
MERGE PARTITIONS partition1, partition2
INTO PARTITION target_partition
[TABLESPACE tablespace_name]
[UPDATE INDEXES];
```

### Requirements
1. Partitions must be **adjacent** (consecutive bounds)
2. Target partition name can be either source partition or new name
3. Merged partition inherits target partition's storage attributes
4. Indexes must be rebuilt or updated

### Example
```sql
-- Before: P_2023 (Jan-Oct), P_2023_11 (Nov)
ALTER TABLE sales_part
MERGE PARTITIONS P_2023, P_2023_11
INTO PARTITION P_2023
UPDATE INDEXES;

-- After: P_2023 (Jan-Nov)
```

## Edge Cases and Handling

### Edge Case 1: Non-Adjacent Partitions
**Scenario**: P_2023 (Jan-Oct), P_2024_01 (January next year)
**Handling**: Cannot merge (not adjacent). P_2024_01 will start new year.

### Edge Case 2: Missing Yearly Partition
**Scenario**: All partitions for 2023 are monthly, no yearly P_2023 exists
**Handling**:
- Option A: Create yearly partition first, then merge
- Option B: Keep monthly partitions (simpler, recommended)

### Edge Case 3: Partial Year at Tier Boundary
**Scenario**: P_2023 ends at 2023-11-01 (partial year)
**Handling**: Merge P_2023_11 extends it to 2023-12-01. This is correct behavior.

### Edge Case 4: Multiple Monthly Partitions Move Simultaneously
**Scenario**: Bulk ILM run moves P_2023_11 and P_2023_12 together
**Handling**:
- First merge: P_2023 + P_2023_11 → P_2023
- Second merge: P_2023 + P_2023_12 → P_2023
- Execute sequentially, not in parallel

### Edge Case 5: Active Transactions
**Scenario**: DML in progress on partition during merge
**Handling**:
- Set lock timeout
- Skip merge if table locked
- Retry on next ILM run

### Edge Case 6: Different Compression
**Scenario**: P_2023 has BASIC compression, P_2023_11 has NONE
**Handling**: Merge anyway - result inherits target (P_2023) compression

## Configuration

### New ILM Config Settings

```sql
-- Enable/disable automatic partition merging
INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description)
VALUES ('AUTO_MERGE_PARTITIONS', 'Y', 'Enable automatic partition merging when moving to WARM/COLD tiers');

-- Merge lock timeout (seconds)
INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description)
VALUES ('MERGE_LOCK_TIMEOUT', '30', 'Lock timeout for partition merge operations (seconds)');

-- Retry failed merges
INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description)
VALUES ('MERGE_RETRY_FAILED', 'Y', 'Retry failed partition merges on next ILM run');

-- Maximum merges per ILM run
INSERT INTO cmr.dwh_ilm_config (config_key, config_value, description)
VALUES ('MAX_MERGES_PER_RUN', '50', 'Maximum number of partition merges per ILM execution');
```

### Template Enhancement

Templates should indicate merge behavior:

```json
{
  "tier_config": {
    "enabled": true,
    "auto_merge": true,  // NEW: Enable merge for this template
    "hot": { ... },
    "warm": {
      "interval": "YEARLY",
      "merge_monthly": true  // NEW: Merge monthly into yearly
    },
    "cold": {
      "interval": "YEARLY",
      "merge_monthly": true
    }
  }
}
```

## Performance Considerations

### Impact of MERGE PARTITIONS

**Operation Cost**:
- **Low**: Metadata-only operation if partitions empty
- **Medium**: Quick if small amount of data
- **High**: Can be slow for large partitions

**Best Practices**:
1. Merge during low-activity periods
2. Use UPDATE INDEXES to avoid index invalidation
3. Consider online redefinition for very large tables
4. Set DDL_LOCK_TIMEOUT to prevent hanging

### Scheduling Strategy

**Recommended**:
- Run merge operations separately from main ILM moves
- Schedule merges during maintenance windows
- Batch merges by table to reduce overhead

**Implementation**:
```sql
-- Separate merge job
BEGIN
  pck_dwh_ilm_execution_engine.execute_pending_merges(
    p_max_operations => 50
  );
END;
```

## Testing Strategy

### Test Case 1: Basic Merge
1. Create table with tiered partitioning
2. Wait 24 months (or manipulate dates)
3. Run ILM to move P_2023_11 to WARM
4. Verify P_2023_11 merged into P_2023
5. Verify P_2023 bounds extended

### Test Case 2: Complete Year
1. Create table with 3 years data
2. Move all 12 months of 2023 to WARM
3. Verify all merged into single P_2023
4. Verify P_2023 covers full year

### Test Case 3: Non-Adjacent Skip
1. Create P_2023 (Jan-Jun), P_2024_01 (next year Jan)
2. Move P_2024_01 to WARM
3. Verify NO merge attempted (not adjacent)
4. Verify P_2024_01 remains standalone

### Test Case 4: Failed Merge Recovery
1. Lock table during merge
2. Verify merge fails gracefully
3. Verify retry on next ILM run
4. Verify eventual success

### Test Case 5: Bulk Merge
1. Move 5 monthly partitions simultaneously
2. Verify all merge successfully in sequence
3. Verify audit log shows all 5 merges

## Monitoring and Observability

### Key Metrics

1. **Merge Success Rate**:
```sql
SELECT
  COUNT(*) FILTER (WHERE merge_status = 'SUCCESS') * 100.0 / COUNT(*) as success_rate
FROM cmr.dwh_ilm_partition_merges
WHERE merge_date > SYSDATE - 7;
```

2. **Partition Count by Tier**:
```sql
SELECT
  tablespace_name,
  COUNT(*) as partition_count,
  COUNT(*) FILTER (WHERE partition_name LIKE 'P____') as yearly_count,
  COUNT(*) FILTER (WHERE partition_name LIKE 'P_____\__%' ESCAPE '\') as monthly_count
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
AND table_name LIKE '%_PART'
GROUP BY tablespace_name;
```

3. **Merge Performance**:
```sql
SELECT
  table_name,
  AVG(duration_seconds) as avg_merge_time,
  MAX(duration_seconds) as max_merge_time
FROM cmr.dwh_ilm_partition_merges
WHERE merge_status = 'SUCCESS'
GROUP BY table_name;
```

### Alerts

**Recommended Alerts**:
1. Merge failure rate > 10%
2. Merge duration > 5 minutes
3. WARM tier monthly partition count > 100
4. Failed merge not retried in 7 days

## Rollback and Recovery

### If Merge Fails

The partition remains unmerged but moved to correct tier:
- Data is safe (move completed successfully)
- Partition is functional in new tablespace
- Only optimization (consolidation) didn't occur
- Will be retried on next ILM run

### Manual Merge

If automatic merge fails repeatedly:
```sql
-- Manual merge procedure
BEGIN
  pck_dwh_ilm_execution_engine.merge_monthly_into_yearly(
    p_table_owner => 'CMR',
    p_table_name => 'SALES_PART',
    p_monthly_partition => 'P_2023_11'
  );
END;
/
```

### Undo a Merge

**Cannot be undone directly**. Options:
1. Use backup table if available
2. Use EXCHANGE PARTITION to split again
3. Accept the merged state (recommended - it's the goal)

## Implementation Phases

### Phase 1: Core Merge Logic (Week 1)
- [ ] Create merge detection function
- [ ] Implement merge execution procedure
- [ ] Add merge tracking table
- [ ] Basic unit tests

### Phase 2: ILM Integration (Week 2)
- [ ] Integrate with move_partition
- [ ] Add configuration settings
- [ ] Implement retry logic
- [ ] Integration tests

### Phase 3: Monitoring & Safety (Week 3)
- [ ] Add performance metrics
- [ ] Implement lock timeout handling
- [ ] Create monitoring views
- [ ] Load testing

### Phase 4: Documentation & Rollout (Week 4)
- [ ] User guide
- [ ] Operations runbook
- [ ] Gradual rollout to production
- [ ] Monitor and tune

## Success Criteria

1. ✅ Monthly partitions in WARM tier < 20% of total WARM partitions
2. ✅ Merge success rate > 95%
3. ✅ Average merge time < 30 seconds
4. ✅ No data loss or corruption
5. ✅ No impact on query performance
6. ✅ Zero unplanned downtime

## Risks and Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Merge locks table | High | Medium | Set DDL_LOCK_TIMEOUT, skip if locked |
| Merge fails silently | Medium | Low | Comprehensive logging and monitoring |
| Performance degradation | High | Low | Test with production-size data first |
| Wrong partitions merged | Critical | Very Low | Multiple validation checks before merge |
| Index corruption | High | Very Low | Use UPDATE INDEXES, validate post-merge |

## Open Questions

1. **Should we merge immediately after move, or in a separate batch job?**
   - Recommendation: Separate batch job for better control

2. **What if yearly partition doesn't exist initially?**
   - Recommendation: Keep monthly, don't create yearly artificially

3. **Should merge be configurable per table or per template?**
   - Recommendation: Per template (more flexible)

4. **How to handle subpartitioned tables?**
   - Recommendation: Phase 2 feature, not in initial release

5. **Should we merge COLD tier partitions into multi-year partitions?**
   - Recommendation: No, yearly is fine for COLD. Consider if partition count becomes issue.

## Alternatives Considered

### Alternative 1: Don't Merge - Keep All Monthly
**Pros**: Simple, no risk
**Cons**: High partition count in old data (defeats tiered design)

### Alternative 2: Create All Partitions as Yearly From Start
**Pros**: No merge needed
**Cons**: Loss of granularity in HOT tier, complicates ILM logic

### Alternative 3: Manual Consolidation Job (No Auto-Merge)
**Pros**: More control, safer
**Cons**: Requires manual intervention, easy to forget

**Decision**: Proceed with automatic merge (Option 3 from original design) as it achieves the design goals while remaining manageable.

## Conclusion

The ILM partition merge feature is essential for maintaining the tiered partitioning strategy's effectiveness. By automatically consolidating monthly partitions into yearly partitions as they age, we achieve:

- Optimal partition counts in WARM/COLD tiers
- Consistent granularity per tier (yearly in WARM/COLD, monthly in HOT)
- Reduced metadata overhead
- Improved query performance on historical data

The implementation is complex but manageable with proper safety checks, monitoring, and phased rollout.

## Next Steps

1. Review and approve this design document
2. Create implementation tasks with estimates
3. Set up development environment for testing
4. Begin Phase 1 implementation

---

**Document Version**: 1.0
**Author**: Claude Code
**Date**: 2025-11-12
**Status**: Draft - Awaiting Review
