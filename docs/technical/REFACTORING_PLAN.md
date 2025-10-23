# Main Loop Refactoring Plan

## Current Structure (Lines 2075-2365)

The current analysis has these phases:

### Phase 1: Stereotype Detection (Lines 2102-2140)
- detect_scd2_pattern() - looks for SCD2 columns
- detect_events_table() - looks for event date columns
- detect_staging_table() - looks for staging load dates
- detect_hist_table() - looks for historical table dates
- **Result**: Sets v_date_column if pattern matches

### Phase 2: Stereotype Validation (Lines 2147-2177)
- Validates the stereotype-detected column has good data quality
- If bad quality, resets v_date_found to allow Phase 3 to override

### Phase 3: Analyze ALL DATE Columns (Lines 2180-2238)
- Calls get_date_columns() - gets DATE/TIMESTAMP columns only
- Loops through each DATE column
- Calls analyze_date_column() for each
- Applies selection logic based on:
  - Data quality
  - NULL percentage
  - Time component
  - Usage score
  - Date range
- **Result**: Selects best DATE column

### Phase 4: Analyze NUMBER/VARCHAR Alternatives (Lines 2284-2357)
- Only adds to JSON analysis (not selection)
- Doesn't go through comprehensive analysis

### Phase 5: Detection for Missing Columns (Lines 2365-2433)
- detect_numeric_date_column() - name-pattern based
- detect_varchar_date_column() - name-pattern based
- detect_date_column_by_content() - fallback sampling
- **Only runs if no DATE column selected**

## Proposed New Structure

### Phase 1: Collect ALL Potential Date Columns
```plsql
-- Get DATE columns
v_date_columns := get_date_columns(...)

-- Get NUMBER columns by name pattern
FOR rec IN (SELECT column_name FROM ... WHERE data_type='NUMBER' AND name patterns) LOOP
    -- Detect format
    IF detect_format(...) THEN
        Add to candidates list with (column_name, 'NUMBER', format)
    END IF
END LOOP

-- Get VARCHAR columns by name pattern
FOR rec IN (SELECT column_name FROM ... WHERE data_type='VARCHAR2' AND name patterns) LOOP
    -- Detect format
    IF detect_format(...) THEN
        Add to candidates list with (column_name, 'VARCHAR2', format)
    END IF
END LOOP

-- FALLBACK: Content-based detection if no candidates found
IF no candidates THEN
    detect_date_column_by_content(...)
    Add to candidates list
END IF
```

### Phase 2: Unified Analysis Loop
```plsql
FOR each candidate in ALL candidates (DATE + NUMBER + VARCHAR) LOOP
    -- Analyze using unified function
    IF analyze_any_date_column(
        column_name, data_type, format,
        v_min_date, v_max_date, v_range, v_null_pct,
        v_has_time, v_usage_score, v_quality) THEN

        -- Apply penalty for data quality issues
        IF v_quality = 'Y' THEN
            v_usage_score := v_usage_score - 50
        END IF

        -- Add to JSON analysis
        DBMS_LOB.APPEND(v_all_date_analysis, json_for_column)

        -- Selection logic (UNIFIED for all types)
        IF v_date_column IS NULL THEN
            -- First column - select as baseline
            v_date_column := column_name
            v_date_type := data_type
            ... track all metrics
        ELSIF should_replace_current_selection() THEN
            -- Better column found - replace
            v_date_column := column_name
            ... update all metrics
        END IF
    END IF
END LOOP
```

### Phase 3: Stereotype Priority Override
```plsql
-- After unified loop, check if a stereotype was found
IF stereotype_column IS NOT NULL AND stereotype in candidates THEN
    -- Prefer stereotype unless quality is bad
    IF stereotype_quality = 'N' OR v_date_column IS NULL THEN
        v_date_column := stereotype_column
    END IF
END IF
```

## Benefits

1. ✅ All column types analyzed with same metrics
2. ✅ Fair comparison in selection logic
3. ✅ NUMBER/VARCHAR compete equally with DATE columns
4. ✅ Content-based fallback integrated naturally
5. ✅ Simpler, more maintainable code
6. ✅ User can see ALL candidates with full analysis in JSON

## Implementation Steps

1. ✅ Create analyze_any_date_column() - DONE
2. ✅ Create detect_date_column_by_content() - DONE
3. ✅ Build unified candidate collection - DONE (collect_all_date_candidates)
4. ✅ Replace DATE-only loop with unified loop - DONE (lines 2168-2476)
5. ✅ Integrate stereotype detection - DONE (Phase 1 + Phase 4 override)
6. 🔄 Test with all scenarios - READY FOR TESTING

## Variables Needed

```plsql
TYPE t_date_candidate IS RECORD (
    column_name VARCHAR2(128),
    data_type VARCHAR2(30),      -- 'DATE', 'NUMBER', 'VARCHAR2'
    date_format VARCHAR2(50),     -- 'YYYYMMDD', 'YYYYMM', etc.
    is_stereotype VARCHAR2(1),    -- 'Y' if from stereotype detection
    stereotype_type VARCHAR2(30)  -- 'SCD2', 'EVENTS', etc.
);
TYPE t_candidate_list IS TABLE OF t_date_candidate;
v_all_candidates t_candidate_list := t_candidate_list();
```
