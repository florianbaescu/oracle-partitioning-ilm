# Column Selection Logic - Complete Explanation

This document explains the complete logic for selecting the optimal date/timestamp column for table partitioning.

## Selection Logic Flow

### **Phase 1: Stereotype Pattern Detection** (lines 1448-1479)

The system first tries to detect known column naming patterns in this order:

1. **SCD2 Pattern**: Columns like `VALID_FROM`, `VALID_TO`, `EFFECTIVE_DATE`, `EFF_FROM_DATE`
2. **Events Pattern**: Columns like `EVENT_DATE`, `TRANSACTION_DATE`, `TXN_DATE`, **`DATA_TRANZACTIEI`** (hardcoded at line 474)
3. **Staging Pattern**: Columns like `LOAD_DATE`, `UPLOAD_DATE`, `INSERT_DATE`
4. **HIST Pattern**: Columns ending in `_HIST`

**Behavior:**
- If ANY pattern matches → sets `v_date_found = TRUE` and `v_date_column = <detected_column>`
- This column is tentatively selected as the partition key
- Proceeds to Phase 2 for validation

---

### **Phase 2: Validate Stereotype Column for Data Quality** (lines 1481-1511)

**Purpose:** Prevent stereotype-detected columns with bad data from being blindly accepted.

If a stereotype was detected in Phase 1 (`v_date_found = TRUE`):

1. **Analyze the detected column** via `analyze_date_column()`:
   - Checks min/max date years
   - Sets `p_data_quality_issue = 'Y'` if any year is outside 1900-2100 range

2. **If data quality issue found:**
   ```
   IF v_temp_quality = 'Y' THEN
       -- Reset the selection to allow quality-based evaluation
       v_date_found := FALSE;
       v_date_column := NULL;
   END IF;
   ```

3. **Prints warning:**
   ```
   WARNING: Stereotype-detected column DATA_TRANZACTIEI has data quality issues (years outside 1900-2100)
   Will evaluate all date columns for better alternatives...
   ```

**Result:** Allows Phase 3 to run and select a better column based on data quality.

---

### **Phase 3: Analyze ALL Date Columns** (lines 1522-1687)

Loops through **every** DATE/TIMESTAMP column in the table, regardless of whether a stereotype was found.

#### **For Each Column in Loop:**

**Step 1: Analysis** (lines 1523-1529)
- Calls `analyze_date_column()` function
- Returns: min/max dates, range, null percentage, usage score, **data_quality_issue flag**
- `v_data_quality_issue = 'Y'` if years outside 1900-2100
- `v_data_quality_issue = 'N'` if all years are 1900-2100

**Step 2: Apply Score Penalty** (lines 1531-1533)
```plsql
IF v_data_quality_issue = 'Y' THEN
    v_usage_score := GREATEST(0, v_usage_score - 50);  -- Heavy penalty
    DBMS_OUTPUT.PUT_LINE('  *** PENALIZED: Score reduced by 50 points...');
END IF;
```

**Step 3: Print Year Warnings** (lines 1536-1602)
- If MIN year < 1900 → print warning, add to warnings JSON
- If MIN year > 2100 → print warning, add to warnings JSON
- If MAX year < 1900 → print warning, add to warnings JSON
- If MAX year > 2100 → print warning, add to warnings JSON

**Step 4: Print Column Summary** (lines 1631-1636)
```
  - DATA_DECONTARII: 2020-01-03 (year 2020) to 2025-10-02 (year 2025) (2099 days, 0% NULLs, usage score: 23)
  - DATA_TRANZACTIEI: 0021-06-07 (year 21) to 2025-10-02 (year 2025) (2638 days, 0% NULLs, usage score: 0)
  - UPLOAD_DATE: 2020-01-01 (year 2020) to 2025-10-02 (year 2025) (2101.4487 days, .001% NULLs, has time component, usage score: 0)
```

**Step 5: Selection Decision** (lines 1649-1684)

**IMPORTANT:** This logic **ONLY runs** if `v_date_found = FALSE`
- i.e., no stereotype was found, OR stereotype column was rejected in Phase 2

**Selection Algorithm:**

**First Column** (`v_date_column IS NULL`):
```plsql
-- Select first column as baseline regardless of data quality
v_date_column := v_date_columns(i);
v_best_has_quality_issue := v_data_quality_issue;
```

**Subsequent Columns** - Three Cases:

**Case 1:** Current column is clean, best column is dirty → **ALWAYS replace**
```plsql
(v_data_quality_issue = 'N' AND v_best_has_quality_issue = 'Y')
```
→ Clean data always wins over dirty data

**Case 2:** Both have same data quality → Apply additional criteria in priority order
```plsql
(v_data_quality_issue = v_best_has_quality_issue AND (
    -- Priority 1: Significantly fewer NULLs (>10% difference)
    (v_null_percentage < v_selected_null_pct - 10) OR

    -- Priority 2: No time component (when NULL% similar)
    (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
     v_has_time_component = 'N' AND v_selected_has_time = 'Y') OR

    -- Priority 3: Higher usage score (when NULL% and time same)
    (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
     v_has_time_component = v_selected_has_time AND
     v_usage_score > v_max_usage_score) OR

    -- Priority 4: Wider range (final tiebreaker)
    (ABS(v_null_percentage - v_selected_null_pct) <= 10 AND
     v_has_time_component = v_selected_has_time AND
     v_usage_score >= v_max_usage_score * 0.8 AND
     v_range_days > v_max_range)
))
```
→ Replace if any condition matches in priority order

**Case 3:** Current column is dirty, best column is clean → **NEVER replace** (implicit)
```plsql
-- No condition matches, so no replacement happens
```

---

## Priority Order (Enforced)

The selection logic enforces this strict priority:

1. **Data Quality** (highest priority)
   - Clean columns (years 1900-2100) always preferred over dirty columns

2. **NULL Percentage** (high priority)
   - Columns with significantly fewer NULLs (>10% difference) are strongly preferred
   - Fewer NULLs means better partition distribution and fewer DEFAULT partition rows

3. **Time Component** (medium-high priority)
   - Columns without time component (pure DATE) preferred over TIMESTAMP-like columns
   - Avoids need for TRUNC() in partition key expressions

4. **Usage Score** (medium priority)
   - Higher score indicates column is used more in queries/indexes
   - Calculated from: primary key membership, foreign key membership, index membership

5. **Date Range** (lowest priority - tiebreaker only)
   - Wider range preferred when all other factors are similar (within 20%)

---

## Example Scenario: Columns with Different NULL Percentages

**Given your table has these columns (in discovery order):**

| Column | Min Year | Max Year | Range (days) | NULL % | Time? | Score | Quality |
|--------|----------|----------|--------------|--------|-------|-------|---------|
| FIRST_DT | 2023 | 2025 | 912 | 0.0062% | No | 0 | N (clean) |
| SECOND_DT | 2023 | 2025 | 912.9444 | 60.7849% | Yes | 0 | N (clean) |

**Expected Execution Flow (with corrected logic):**

### Phase 1: Stereotype Detection
- No stereotype patterns detected (assumes FIRST_DT and SECOND_DT don't match standard patterns)
- **Result:** `v_date_found = FALSE` → Proceeds to quality-based selection

### Phase 2: Loop Through All Columns

**Iteration 1: FIRST_DT**
- Analyze: quality='N', null%=0.0062%, time='N', score=0, range=912
- Score penalty: None (clean data)
- Warnings: None (years 2023-2025 are valid)
- **Prints:** `"  - FIRST_DT: 2023-04-06 (year 2023) to 2025-10-04 (year 2025) (912 days, 0.0062% NULLs, usage score: 0)"`
- **Selection:** `v_date_column IS NULL` → Select as baseline
  - `v_date_column = 'FIRST_DT'`
  - `v_selected_null_pct = 0.0062`
  - `v_selected_has_time = 'N'`
  - `v_max_usage_score = 0`
  - `v_best_has_quality_issue = 'N'`

**Iteration 2: SECOND_DT**
- Analyze: quality='N', null%=60.7849%, time='Y', score=0, range=912.9444
- Score penalty: None (clean data)
- **Prints:** `"  - SECOND_DT: 2023-04-06 (year 2023) to 2025-10-04 (year 2025) (912.9444 days, 60.7849% NULLs, has time component, usage score: 0)"`
- **Selection Check (NEW LOGIC):**
  - Case 1: Both clean (`'N' = 'N'`) → FALSE
  - Case 2a: Fewer NULLs? `60.7849 < 0.0062 - 10` → FALSE (has MORE NULLs!)
  - Case 2b: No time component? `'Y' = 'N' AND 'N' = 'Y'` → FALSE (has time!)
  - Case 2c: Higher score? `0 > 0` → FALSE
  - Case 2d: Wider range? `0 >= 0 * 0.8 AND 912.9444 > 912` → FALSE (NULL% diff >10%, earlier condition blocks)
  - **No replacement** - FIRST_DT stays selected ✅

**Final Selection:**
- `v_date_found = FALSE` and `v_date_column = 'FIRST_DT'`
- **Prints:** `"Selected date column: FIRST_DT (usage score: 0, range: 912 days)"`

---

## Expected Result (CORRECTED)

**FIRST_DT is correctly selected** because:
1. Both columns have clean data (years 2023-2025)
2. FIRST_DT has **significantly fewer NULLs** (0.0062% vs 60.78%)
3. FIRST_DT has **no time component** (pure DATE vs TIMESTAMP-like)
4. The NULL percentage difference (60.78%) far exceeds the 10% threshold
5. Even though SECOND_DT has slightly wider range, it's blocked by NULL% priority

**SECOND_DT is NOT selected** because:
1. It has 60.78% NULL values (extremely high)
2. It has a time component requiring TRUNC() in partition key
3. The new logic prioritizes NULL percentage and time component before range
4. Range is only used as a tiebreaker when other factors are similar

---

## Code Location Reference

| Phase | Line Range | Key Variables |
|-------|------------|---------------|
| Stereotype Detection | 1448-1479 | `v_date_found`, `v_date_column` |
| Stereotype Validation | 1481-1511 | `v_temp_quality`, resets `v_date_found` |
| Column Loop | 1522-1687 | `v_data_quality_issue`, `v_usage_score` |
| Selection Logic | 1649-1684 | `v_best_has_quality_issue`, comparison cases |
| Final Assignment | 1690-1692 | Sets `v_date_found = TRUE` |

File: `scripts/table_migration_analysis.sql`
