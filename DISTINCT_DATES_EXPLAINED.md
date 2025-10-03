# distinct_dates Field - Explanation

## Overview

The `distinct_dates` field in the date column analysis shows the **number of unique calendar dates** (ignoring time) that exist in a DATE/TIMESTAMP column. This metric helps determine the optimal partitioning granularity.

## When is it Populated?

The `distinct_dates` field is **only calculated** when:

- **Condition**: `has_time_component = 'Y'`
- **Meaning**: The column contains timestamp data with time components (hours, minutes, seconds)
- **Calculation**:
  ```sql
  SELECT COUNT(DISTINCT TRUNC(column_name))
  FROM table
  WHERE column_name IS NOT NULL
  ```

## When is it NULL?

The `distinct_dates` field is **set to NULL** when:

- **Condition**: `has_time_component = 'N'`
- **Meaning**: All values in the column are at midnight (00:00:00)
- **Reason**: The information is redundant - the column already stores pure dates without time

## Why This Matters

### Scenario 1: Sparse Data Distribution

**Example:**
- Column: `UPLOAD_DATE`
- Total rows: 1,000,000
- Date range: 2020-01-01 to 2025-10-03 (2,101 days)
- `has_time_component`: Y
- `distinct_dates`: 52

**Analysis:**
- Data spans ~2,100 days
- But only 52 distinct days have data (weekly uploads)
- Daily partitioning would create:
  - **52 partitions with data** (~19,230 rows each)
  - **2,049 empty partitions** (wasted overhead)

**Recommendation:** Use **weekly** or **monthly** partitioning instead of daily

### Scenario 2: Dense Data Distribution

**Example:**
- Column: `TRANSACTION_DATE`
- Total rows: 10,000,000
- Date range: 2020-01-01 to 2025-10-03 (2,101 days)
- `has_time_component`: Y
- `distinct_dates`: 2,095

**Analysis:**
- Data spans ~2,100 days
- Has data on 2,095 distinct days (99.7% coverage - nearly every day)
- Daily partitioning would create:
  - **2,095 partitions with data** (~4,775 rows each)
  - **6 empty partitions** (minimal overhead)

**Recommendation:** Daily partitioning is appropriate

### Scenario 3: Pure Date Column

**Example:**
- Column: `EFFECTIVE_DATE`
- Total rows: 5,000,000
- Date range: 2015-01-01 to 2025-10-03
- `has_time_component`: N
- `distinct_dates`: NULL

**Analysis:**
- All values are at midnight (pure dates, no time)
- Every non-null row represents a distinct calendar date
- `distinct_dates` would be redundant information

**Recommendation:** Use the date range and row count directly for partitioning decisions

## Use Cases

### 1. Partition Granularity Decision

Compare `distinct_dates` to the total `range_days`:

| Ratio | Interpretation | Suggested Granularity |
|-------|---------------|----------------------|
| > 90% | Dense - data almost every day | DAILY |
| 50-90% | Moderate density | DAILY or WEEKLY |
| 10-50% | Sparse - many gaps | WEEKLY or MONTHLY |
| < 10% | Very sparse - mostly empty days | MONTHLY or YEARLY |

### 2. Identifying Data Load Patterns

**Weekly loads:**
```
distinct_dates: 260 (5 years × 52 weeks)
range_days: 1,825 (5 years × 365 days)
Ratio: 14% → suggests weekly loading pattern
```

**Monthly loads:**
```
distinct_dates: 60 (5 years × 12 months)
range_days: 1,825
Ratio: 3% → suggests monthly loading pattern
```

**Daily loads:**
```
distinct_dates: 1,800
range_days: 1,825
Ratio: 98% → data loaded nearly every day
```

### 3. Estimating Partition Sizes

If you partition daily:
- **Partitions with data**: `distinct_dates`
- **Average rows per partition**: `total_rows / distinct_dates`
- **Empty partitions**: `range_days - distinct_dates`

## Code Location

**File:** `scripts/table_migration_analysis.sql`

**Calculation logic (lines 327-346):**
```plsql
-- Check if column has time component (not all midnight)
v_sql := 'SELECT COUNT(*) FROM (' ||
        '  SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ ' || p_column_name ||
        '  FROM ' || p_owner || '.' || p_table_name ||
        '  WHERE ' || p_column_name || ' IS NOT NULL' ||
        '    AND ' || p_column_name || ' != TRUNC(' || p_column_name || ')' ||
        '  AND ROWNUM <= 1' ||
        ')';

EXECUTE IMMEDIATE v_sql INTO v_time_sample;

IF v_time_sample > 0 OR v_min_time != '00:00:00' OR v_max_time != '00:00:00' THEN
    p_has_time_component := 'Y';

    -- Get distinct date count (without time) with parallel hint
    v_sql := 'SELECT /*+ PARALLEL(' || p_parallel_degree || ') */ COUNT(DISTINCT TRUNC(' || p_column_name || ')) ' ||
            'FROM ' || p_owner || '.' || p_table_name ||
            ' WHERE ' || p_column_name || ' IS NOT NULL';
    EXECUTE IMMEDIATE v_sql INTO p_distinct_dates;
ELSE
    p_has_time_component := 'N';
    p_distinct_dates := NULL;  -- Not needed if no time component
END IF;
```

## JSON Output Example

**With time component:**
```json
{
  "column_name": "UPLOAD_DATE",
  "data_type": "DATE",
  "min_date": "2020-01-01",
  "max_date": "2025-10-02",
  "range_days": 2101.4487,
  "range_years": 5.7534,
  "null_percentage": 0.001,
  "has_time_component": "Y",
  "distinct_dates": 52,
  "usage_score": 0,
  "data_quality_issue": "N"
}
```

**Without time component:**
```json
{
  "column_name": "EFFECTIVE_DATE",
  "data_type": "DATE",
  "min_date": "2015-01-01",
  "max_date": "2025-10-03",
  "range_days": 3928,
  "range_years": 10.7534,
  "null_percentage": 0,
  "has_time_component": "N",
  "distinct_dates": null,
  "usage_score": 23,
  "data_quality_issue": "N"
}
```

## Performance Impact

**Why we skip calculation for pure dates:**

Calculating `COUNT(DISTINCT)` on a large table is expensive:
- Full table scan required
- Hash aggregation in memory or temp space
- Can take minutes on multi-million row tables

For pure date columns (no time component):
- The calculation provides no new information
- Every row already represents a distinct date
- Skipping saves significant analysis time

## Related Fields

| Field | Description | When Populated |
|-------|-------------|---------------|
| `has_time_component` | Does column have HH:MI:SS values? | Always |
| `distinct_dates` | Unique calendar dates | Only when has_time_component = 'Y' |
| `range_days` | Total days from min to max | Always |
| `null_count` | Number of NULL values | Always |
| `non_null_count` | Number of non-NULL values | Always |

## Summary

- **Purpose**: Shows data density across calendar dates
- **Populated**: Only for timestamp columns with time components
- **NULL**: For pure date columns (no time component)
- **Use**: Helps choose between DAILY, WEEKLY, MONTHLY partitioning
- **Calculation**: `COUNT(DISTINCT TRUNC(column_name))`
- **Performance**: Expensive query - only run when needed
