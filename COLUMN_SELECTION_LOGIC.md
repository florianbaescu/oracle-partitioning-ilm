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

**Case 2:** Both have same data quality → Use score and range
```plsql
(v_data_quality_issue = v_best_has_quality_issue AND (
    v_usage_score > v_max_usage_score OR
    (v_usage_score >= v_max_usage_score * 0.8 AND v_range_days > v_max_range)
))
```
→ Replace if:
- New score is higher, OR
- New score is ≥ 80% of best score AND has wider range

**Case 3:** Current column is dirty, best column is clean → **NEVER replace** (implicit)
```plsql
-- No condition matches, so no replacement happens
```

---

## Priority Order (Enforced)

The selection logic enforces this strict priority:

1. **Data Quality** (highest priority)
   - Clean columns (years 1900-2100) always preferred over dirty columns

2. **Usage Score** (medium priority)
   - Higher score indicates column is used more in queries/indexes
   - Calculated from: primary key membership, foreign key membership, index membership

3. **Date Range** (lowest priority - tiebreaker only)
   - Wider range preferred when scores are similar (within 20%)

---

## Example Scenario: Your Case

**Given your table has these columns (in discovery order):**

| Column | Min Year | Max Year | Range (days) | Score | Quality |
|--------|----------|----------|--------------|-------|---------|
| DATA_DECONTARII | 2020 | 2025 | 2099 | 23 | N (clean) |
| DATA_TRANZACTIEI | 21 | 2025 | 2638 | 0 | Y (dirty) |
| UPLOAD_DATE | 2020 | 2025 | 2101 | 0 | N (clean) |

**Expected Execution Flow:**

### Phase 1: Stereotype Detection
- `detect_events_table()` scans for event patterns
- Finds `DATA_TRANZACTIEI` (hardcoded in line 474)
- **Sets:** `v_date_column = 'DATA_TRANZACTIEI'`, `v_date_found = TRUE`
- **Prints:** `"Detected Events date column: DATA_TRANZACTIEI"`

### Phase 2: Validate Stereotype
- Analyzes DATA_TRANZACTIEI
- Finds: min_year = 21 (< 1900)
- **Sets:** `v_temp_quality = 'Y'`
- **Resets:** `v_date_found = FALSE`, `v_date_column = NULL`
- **Prints:**
  ```
  WARNING: Stereotype-detected column DATA_TRANZACTIEI has data quality issues (years outside 1900-2100)
  Will evaluate all date columns for better alternatives...
  ```

### Phase 3: Loop Through All Columns

**Iteration 1: DATA_DECONTARII**
- Analyze: quality='N', score=23, range=2099
- Score penalty: None (clean data)
- Warnings: None (years 2020-2025 are valid)
- **Prints:** `"  - DATA_DECONTARII: 2020-01-03 (year 2020) to 2025-10-02 (year 2025) (2099 days, 0% NULLs, usage score: 23)"`
- **Selection:** `v_date_column IS NULL` → Select as baseline
  - `v_date_column = 'DATA_DECONTARII'`
  - `v_best_has_quality_issue = 'N'`
  - `v_max_usage_score = 23`

**Iteration 2: DATA_TRANZACTIEI**
- Analyze: quality='Y', score=0 (before penalty), range=2638
- Score penalty: `0 - 50 = 0` (already at minimum)
- **Prints:**
  ```
  *** PENALIZED: Score reduced by 50 points due to data quality issue
  *** WARNING: MIN date has year 21 (< 1900) - possible data quality issue
  - DATA_TRANZACTIEI: 0021-06-07 (year 21) to 2025-10-02 (year 2025) (2638 days, 0% NULLs, usage score: 0)
  ```
- **Selection Check:**
  - Case 1: `'Y' AND 'N'` → FALSE (dirty can't replace clean)
  - Case 2: `'Y' != 'N'` → FALSE
  - **No replacement** - DATA_DECONTARII stays selected

**Iteration 3: UPLOAD_DATE**
- Analyze: quality='N', score=0, range=2101
- Score penalty: None (clean data)
- Warnings: None (years 2020-2025 are valid)
- **Prints:** `"  - UPLOAD_DATE: 2020-01-01 (year 2020) to 2025-10-02 (year 2025) (2101.4487 days, .001% NULLs, has time component, usage score: 0)"`
- **Selection Check:**
  - Case 1: `'N' AND 'N'` → FALSE (both clean)
  - Case 2: `'N' = 'N' AND (0 > 23 OR ...)` → FALSE (score too low)
  - **No replacement** - DATA_DECONTARII stays selected

**Final Selection:** (line 1690-1692)
- `v_date_found = FALSE` and `v_date_column = 'DATA_DECONTARII'`
- **Prints:** `"Selected date column: DATA_DECONTARII (usage score: 23, range: 2099 days)"`

---

## Expected Result

**DATA_DECONTARII should be selected** because:
1. It has clean data (years 2020-2025)
2. It has the highest usage score (23 vs 0)
3. It was selected in Phase 3 using quality-first logic

**DATA_TRANZACTIEI should NOT be selected** because:
1. Although it matches Events stereotype pattern
2. It was rejected in Phase 2 due to data quality issues (year 21)
3. In Phase 3 loop, it has dirty data and cannot replace a clean column

---

## Diagnostic Questions

When you run the analysis, please check these outputs:

1. **Phase 1 Output** - Is there a line saying:
   ```
   Detected Events date column: DATA_TRANZACTIEI
   ```

2. **Phase 2 Output** - Is there a warning saying:
   ```
   WARNING: Stereotype-detected column DATA_TRANZACTIEI has data quality issues (years outside 1900-2100)
   Will evaluate all date columns for better alternatives...
   ```

3. **Phase 3 Output** - What is printed at the end:
   ```
   Selected date column: ??? (usage score: ???, range: ??? days)
   ```

4. **Final Result** - What column appears in the `DATE_COLUMN_NAME` field of `dwh_migration_analysis` table?

Please share all of these outputs so we can identify where the logic is breaking down.

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
