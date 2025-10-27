# Tablespace Allocation Strategy for Oracle Partitioning & ILM

**Meeting Date:** TBD
**Audience:** DBA Team
**Topic:** Tablespace allocation strategy for partitioned tables and Information Lifecycle Management

---

## Executive Summary

This document outlines the tablespace allocation strategy for implementing table partitioning and Information Lifecycle Management (ILM) in the data warehouse. The strategy focuses on optimizing storage costs, performance, and manageability through tiered storage and automated lifecycle policies.

**Key Areas Covered:**
- **Tiered Storage Architecture:** 4-tier approach (HOT/WARM/COLD/ARCHIVE) for tables, indexes, and LOB segments
- **Compression Strategy:** Tier-specific compression ratios achieving 60-90% storage reduction
- **LOB Management:** SECUREFILE LOBs with compression and deduplication for Large Object columns
- **ILM Automation:** Automated partition movement and compression based on age and access patterns
- **Cost Optimization:** 70-85% storage cost reduction through intelligent data lifecycle management
- **Naming Standards:** Consistent tablespace naming conventions (DWH_TIER_TYPE)
- **Monitoring & Maintenance:** Ready-to-use queries for space tracking and performance analysis

**Expected Benefits:**
- Storage savings: 60-90% (higher with LOB compression and deduplication)
- Backup window reduction: 50%+
- Query performance improvement: 2-10x (partition pruning)
- Automated lifecycle management reduces DBA overhead

---

## 1. Tablespace Strategy for Partitioned Tables

### 1.1 Current Challenges
- Non-partitioned tables reside in general-purpose tablespaces
- No data temperature differentiation (hot/warm/cold)
- Uniform storage characteristics regardless of access patterns
- Inefficient space utilization for historical data

### 1.2 Proposed Tiered Tablespace Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ HOT TIER - Recent/Active Data (3-12 months)                │
│ - High performance storage (SSD/Flash)                      │
│ - No compression or minimal compression                     │
│ - Frequent DML operations                                   │
│ Tablespaces: DWH_HOT_DATA, DWH_HOT_IDX, DWH_HOT_LOB        │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ WARM TIER - Recent Historical (1-3 years)                  │
│ - Standard storage (SAS)                                    │
│ - OLTP/Query Low compression                                │
│ - Read-mostly, occasional updates                           │
│ Tablespaces: DWH_WARM_DATA, DWH_WARM_IDX, DWH_WARM_LOB     │
│ LOB Compression: MEDIUM (3-5x ratio)                        │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ COLD TIER - Archived Data (3+ years)                       │
│ - Low-cost storage (SATA)                                   │
│ - Query High/Archive High compression                       │
│ - Read-only access                                          │
│ Tablespaces: DWH_COLD_DATA, DWH_COLD_IDX, DWH_COLD_LOB     │
│ LOB Compression: HIGH (5-10x ratio)                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ ARCHIVE TIER - Long-term Retention (7+ years)              │
│ - Cheapest storage available                                │
│ - Maximum compression (Archive High/Hybrid Columnar)        │
│ - Rare access, compliance/audit only                        │
│ Tablespaces: DWH_ARCHIVE_DATA, DWH_ARCHIVE_IDX,            │
│              DWH_ARCHIVE_LOB                                │
│ LOB Compression: HIGH + DEDUPLICATION (10-15x ratio)        │
└─────────────────────────────────────────────────────────────┘
```

### 1.3 Partition-to-Tablespace Mapping

**Range Partitioning Strategy:**
- **Current Month:** `DWH_HOT_DATA`
- **Last 12 months:** `DWH_HOT_DATA`
- **13-36 months:** `DWH_WARM_DATA`
- **37+ months:** `DWH_COLD_DATA`
- **84+ months (7 years):** `DWH_ARCHIVE_DATA`

**Example:**
```sql
-- Partition allocation for a table partitioned by month
CREATE TABLE cmr.fact_sales (
    sale_id NUMBER,
    sale_date DATE,
    amount NUMBER
)
PARTITION BY RANGE (sale_date) (
    PARTITION p_2025_01 VALUES LESS THAN (TO_DATE('2025-02-01','YYYY-MM-DD')) TABLESPACE DWH_HOT_DATA,
    PARTITION p_2024_01 VALUES LESS THAN (TO_DATE('2024-02-01','YYYY-MM-DD')) TABLESPACE DWH_WARM_DATA,
    PARTITION p_2022_01 VALUES LESS THAN (TO_DATE('2022-02-01','YYYY-MM-DD')) TABLESPACE DWH_COLD_DATA,
    PARTITION p_2018_01 VALUES LESS THAN (TO_DATE('2018-02-01','YYYY-MM-DD')) TABLESPACE DWH_ARCHIVE_DATA
);
```

---

## 2. ILM-Related Tablespace Tiering

### 2.1 Automated Partition Movement

The ILM framework (`pck_dwh_ilm_policy_engine` and `pck_dwh_ilm_execution_engine`) automates partition movement between tablespaces based on:

1. **Age-based policies** (stored in `cmr.dwh_ilm_policies`)
2. **Access patterns** (tracked in `cmr.dwh_ilm_partition_access`)
3. **Compression objectives** (defined in policy rules)

### 2.2 ILM Policy Actions

| Action | Source Tier | Target Tier | Trigger |
|--------|-------------|-------------|---------|
| COMPRESS_LOW | HOT | WARM | 12 months old |
| COMPRESS_HIGH | WARM | COLD | 36 months old |
| ARCHIVE | COLD | ARCHIVE | 84 months old |
| MOVE_TABLESPACE | Any | Any | Manual/Custom |

### 2.3 Example ILM Policy

```sql
-- Policy: Move partitions to WARM tier after 12 months
INSERT INTO cmr.dwh_ilm_policies (
    policy_id, policy_name, object_owner, object_name,
    policy_type, condition_type, condition_value,
    action_type, action_params, enabled
) VALUES (
    dwh_ilm_policy_seq.NEXTVAL,
    'COMPRESS_LOW_12M',
    'CMR',
    'FACT_SALES',
    'PARTITION',
    'AGE_DAYS',
    '365',
    'COMPRESS_LOW',
    '{"target_tablespace": "DWH_WARM_DATA", "cascade_indexes": true}',
    'Y'
);
```

### 2.4 Scheduler Job Integration

The ILM scheduler (`DWH_ILM_DAILY_JOB`) automatically:
1. Evaluates all enabled policies
2. Identifies partitions meeting criteria
3. Queues compression/movement actions
4. Executes during maintenance window
5. Logs all operations to `cmr.dwh_ilm_execution_log`

---

## 3. Tablespace Sizing Requirements

### 3.1 Sizing Methodology

**Current State Analysis:**
```sql
-- Get current table sizes
SELECT
    owner,
    table_name,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS size_gb
FROM dba_segments
WHERE owner = 'CMR'
  AND segment_type = 'TABLE'
GROUP BY owner, table_name
ORDER BY 3 DESC;
```

**Partition Size Estimation:**
- Average partition size = Total table size / Number of expected partitions
- Growth factor = 20% annual growth (adjust based on business forecast)

### 3.2 Storage Tier Calculations

**Example: 1TB Table Partitioned Monthly (10 years retention)**

| Tier | Months | Compression | Raw Size | Compressed Size | Tablespace |
|------|--------|-------------|----------|-----------------|------------|
| HOT | 12 | None | 120 GB | 120 GB | DWH_HOT_DATA |
| WARM | 24 | 2x (OLTP) | 240 GB | 120 GB | DWH_WARM_DATA |
| COLD | 48 | 4x (Query High) | 480 GB | 120 GB | DWH_COLD_DATA |
| ARCHIVE | 36 | 10x (Archive High) | 360 GB | 36 GB | DWH_ARCHIVE_DATA |
| **TOTAL** | **120** | **-** | **1,200 GB** | **396 GB** | **67% savings** |

### 3.3 Recommended Tablespace Sizes

**Initial Allocation (Production):**
- `DWH_HOT_DATA`: 500 GB (autoextend, maxsize unlimited)
- `DWH_HOT_IDX`: 200 GB (autoextend, maxsize unlimited)
- `DWH_HOT_LOB`: 100 GB (autoextend, maxsize unlimited)
- `DWH_WARM_DATA`: 1 TB (autoextend, maxsize unlimited)
- `DWH_WARM_IDX`: 300 GB (autoextend, maxsize unlimited)
- `DWH_WARM_LOB`: 200 GB (autoextend, maxsize unlimited)
- `DWH_COLD_DATA`: 2 TB (autoextend, maxsize unlimited)
- `DWH_COLD_IDX`: 500 GB (autoextend, maxsize unlimited)
- `DWH_COLD_LOB`: 300 GB (autoextend, maxsize unlimited)
- `DWH_ARCHIVE_DATA`: 1 TB (autoextend, maxsize unlimited)
- `DWH_ARCHIVE_IDX`: 200 GB (autoextend, maxsize unlimited)
- `DWH_ARCHIVE_LOB`: 200 GB (autoextend, maxsize unlimited)

**Growth Monitoring:**
- Set AUTOEXTEND ON with reasonable increments (1GB for data, 256MB for indexes)
- Implement alerting at 80% capacity
- Review quarterly and adjust based on actual growth

---

## 4. Compression and Storage Parameters

### 4.1 Compression by Tier

| Tier | Compression Type | Ratio | License Required | Use Case |
|------|------------------|-------|------------------|----------|
| HOT | None or BASIC | 1-2x | No | Current data, high DML |
| WARM | OLTP Compression | 2-3x | Advanced Compression | Recent historical, read-mostly |
| COLD | Query High | 4-6x | Advanced Compression | Old data, read-only |
| ARCHIVE | Archive High / HCC | 10-15x | Advanced Compression / Exadata | Compliance, rare access |

### 4.2 Tablespace Storage Clauses

**HOT Tier (High Performance):**
```sql
CREATE TABLESPACE DWH_HOT_DATA
DATAFILE '/oradata/prod/dwh_hot_data_01.dbf' SIZE 100G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;
```

**WARM Tier (Compressed):**
```sql
CREATE TABLESPACE DWH_WARM_DATA
DATAFILE '/oradata/prod/dwh_warm_data_01.dbf' SIZE 100G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING
DEFAULT COMPRESS FOR OLTP;
```

**COLD Tier (Highly Compressed):**
```sql
CREATE TABLESPACE DWH_COLD_DATA
DATAFILE '/oradata/prod/dwh_cold_data_01.dbf' SIZE 100G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING
DEFAULT COMPRESS FOR QUERY HIGH;
```

**ARCHIVE Tier (Maximum Compression):**
```sql
CREATE TABLESPACE DWH_ARCHIVE_DATA
DATAFILE '/oradata/prod/dwh_archive_data_01.dbf' SIZE 50G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING
DEFAULT COMPRESS FOR ARCHIVE HIGH;
-- Note: For Exadata, use: DEFAULT COMPRESS FOR QUERY HIGH
```

### 4.3 Index Compression

```sql
-- Local indexes on partitioned tables
CREATE INDEX idx_fact_sales_date ON cmr.fact_sales(sale_date)
LOCAL
COMPRESS ADVANCED LOW  -- For HOT/WARM tiers
TABLESPACE DWH_HOT_IDX;

-- For COLD/ARCHIVE tiers
CREATE INDEX idx_fact_sales_customer ON cmr.fact_sales(customer_id)
LOCAL
COMPRESS ADVANCED HIGH  -- Higher compression for cold data
TABLESPACE DWH_COLD_IDX;
```

### 4.4 LOB Segment Management and Tiering

LOB (Large Object) columns (CLOB, BLOB, NCLOB) can consume significant storage and benefit greatly from ILM and compression strategies. This section outlines LOB-specific tablespace allocation, compression, and lifecycle management.

#### 4.4.1 LOB Storage Fundamentals

**BASICFILE vs SECUREFILE LOBs:**

| Feature | BASICFILE (Legacy) | SECUREFILE (11g+) | Recommendation |
|---------|-------------------|-------------------|----------------|
| Compression | None | LOW, MEDIUM, HIGH | SECUREFILE only |
| Deduplication | No | Yes | SECUREFILE only |
| Encryption | No | Yes | SECUREFILE only |
| Performance | Moderate | High | Use SECUREFILE for all new tables |
| License | None | Advanced Compression | Required for compression |

**Key Decisions:**
- **Inline vs Out-of-line:** LOBs < 4KB can be stored inline (within table row), larger LOBs stored separately
- **ENABLE STORAGE IN ROW:** Stores small LOBs inline for performance
- **CHUNK size:** Optimal chunk size = DB_BLOCK_SIZE * integer (e.g., 8KB * 4 = 32KB)

#### 4.4.1.1 Inline vs Out-of-Line LOB Storage - Detailed Explanation

**🔑 KEY POINT: Oracle's Automatic Storage Decision**

With `ENABLE STORAGE IN ROW` (the **default**), Oracle **automatically** decides for each row whether to store the LOB inline or out-of-line based on the LOB's size at INSERT/UPDATE time. **You don't need to do anything** - Oracle handles this transparently and intelligently per row.

**Storage Behavior:**

Oracle LOBs can be stored in two ways:

1. **Inline (ENABLE STORAGE IN ROW) - DEFAULT and AUTOMATIC:**
   - **Oracle automatically checks LOB size for each row**
   - If LOB ≤ 4000 bytes (~4KB): **Automatically stored inline** in the table row
   - If LOB > 4000 bytes: **Automatically stored out-of-line** in LOB segment
   - Decision is made **per row, per operation** (INSERT/UPDATE)
   - **No manual intervention required** - Oracle optimizes for you
   - Stored in the table's data block (when inline)
   - Faster access for small LOBs (single I/O operation)
   - **Recommended default for most use cases**

2. **Out-of-Line (DISABLE STORAGE IN ROW) - MANUAL OVERRIDE:**
   - LOB values **always** stored in separate LOB segment (regardless of size)
   - **Oracle never stores inline, even for small LOBs**
   - LOB locator (pointer) stored in table row
   - Requires separate I/O to fetch LOB data (even for small values)
   - Use when you **know** all LOBs will be large (> 10KB)
   - Prevents Oracle from checking size threshold

**Visual Comparison:**

```
ENABLE STORAGE IN ROW (Default):
┌─────────────────────────────────────────┐
│ TABLE ROW (in DWH_HOT_DATA)             │
│ ┌─────────┬──────────┬───────────────┐ │
│ │ ID: 123 │ Date: .. │ Small LOB     │ │  ← LOB data inline (< 4KB)
│ │         │          │ "Hello World" │ │     Single I/O, fast access
│ └─────────┴──────────┴───────────────┘ │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ TABLE ROW (in DWH_HOT_DATA)             │
│ ┌─────────┬──────────┬───────────────┐ │
│ │ ID: 124 │ Date: .. │ LOB Locator   │ │  ← Large LOB: pointer only
│ │         │          │ (pointer)     │ │
│ └─────────┴──────────┴───────────────┘ │
└─────────────────────────────────────────┘
         │
         │ Points to LOB segment
         ↓
┌─────────────────────────────────────────┐
│ LOB SEGMENT (in DWH_HOT_LOB)            │  ← Actual LOB data (> 4KB)
│ ┌─────────────────────────────────────┐ │     Separate I/O required
│ │ Large document content...           │ │
│ │ [50KB of data]                      │ │
│ └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘

DISABLE STORAGE IN ROW:
┌─────────────────────────────────────────┐
│ TABLE ROW (in DWH_HOT_DATA)             │
│ ┌─────────┬──────────┬───────────────┐ │
│ │ ID: 125 │ Date: .. │ LOB Locator   │ │  ← ALL LOBs: pointer only
│ │         │          │ (pointer)     │ │     (even small ones)
│ └─────────┴──────────┴───────────────┘ │
└─────────────────────────────────────────┘
         │
         │ Always points to LOB segment
         ↓
┌─────────────────────────────────────────┐
│ LOB SEGMENT (in DWH_HOT_LOB)            │  ← Even small LOBs stored here
│ ┌─────────────────────────────────────┐ │     Extra I/O for small values
│ │ "Hello World" (50 bytes)            │ │
│ └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

**When to Use Each Approach:**

| Scenario | Recommendation | Reason |
|----------|----------------|--------|
| Mix of small and large LOBs | ENABLE STORAGE IN ROW | Small LOBs benefit from inline storage |
| Mostly small LOBs (< 1KB) | ENABLE STORAGE IN ROW | Single I/O access, better performance |
| All LOBs are large (> 10KB) | DISABLE STORAGE IN ROW | Prevents row chaining, cleaner storage |
| Variable-length documents | ENABLE STORAGE IN ROW | Automatic optimization by size |
| Binary files (PDFs, images) | DISABLE STORAGE IN ROW | Always large, out-of-line makes sense |
| Short text notes/comments | ENABLE STORAGE IN ROW | Typically < 4KB, inline is faster |
| JSON/XML documents | ENABLE STORAGE IN ROW | Mixed sizes, let Oracle decide |

**Performance Impact:**

```sql
-- Benchmark Example: 1 million rows with mixed LOB sizes

-- ENABLE STORAGE IN ROW (Default):
-- Small LOBs (< 4KB): 1 I/O per row (fast)
-- Large LOBs (> 4KB): 2 I/Os per row (table + LOB segment)
-- Average for 70% small / 30% large: 1.3 I/Os per row

-- DISABLE STORAGE IN ROW:
-- Small LOBs: 2 I/Os per row (slower for small values)
-- Large LOBs: 2 I/Os per row (same as default)
-- Average: 2.0 I/Os per row

-- Performance difference: ~35% slower for mixed workloads with DISABLE
```

**Automatic Behavior Example:**

```sql
-- Create table with default ENABLE STORAGE IN ROW
CREATE TABLE cmr.test_lobs (
    id NUMBER,
    content CLOB
)
LOB (content) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW   -- This is the DEFAULT, can be omitted
);

-- Insert different sized LOBs - Oracle automatically decides storage location

-- Small LOB (50 bytes) → Oracle stores INLINE automatically
INSERT INTO cmr.test_lobs VALUES (1, 'This is a short text message');

-- Medium LOB (2KB) → Oracle stores INLINE automatically
INSERT INTO cmr.test_lobs VALUES (2, LPAD('X', 2000, 'X'));

-- At threshold (4000 bytes) → Oracle stores INLINE automatically
INSERT INTO cmr.test_lobs VALUES (3, LPAD('Y', 4000, 'Y'));

-- Just over threshold (5KB) → Oracle stores OUT-OF-LINE automatically
INSERT INTO cmr.test_lobs VALUES (4, LPAD('Z', 5000, 'Z'));

-- Large LOB (1MB) → Oracle stores OUT-OF-LINE automatically
INSERT INTO cmr.test_lobs VALUES (5, LPAD('A', 1000000, 'A'));

-- Result (automatic, no action required):
-- Row 1: Stored inline in table (1 I/O to read)
-- Row 2: Stored inline in table (1 I/O to read)
-- Row 3: Stored inline in table (1 I/O to read)
-- Row 4: Pointer in table, data in LOB segment (2 I/Os to read)
-- Row 5: Pointer in table, data in LOB segment (2 I/Os to read)

-- Query to verify Oracle's automatic decisions:
SELECT
    id,
    LENGTH(content) AS size_bytes,
    CASE
        WHEN LENGTH(content) <= 4000 THEN 'Inline (1 I/O)'
        ELSE 'Out-of-line (2 I/Os)'
    END AS automatic_storage
FROM cmr.test_lobs;

-- Output:
-- ID  SIZE_BYTES  AUTOMATIC_STORAGE
-- 1   29          Inline (1 I/O)
-- 2   2000        Inline (1 I/O)
-- 3   4000        Inline (1 I/O)
-- 4   5000        Out-of-line (2 I/Os)
-- 5   1000000     Out-of-line (2 I/Os)
```

**Key Takeaway:** You only specify `ENABLE STORAGE IN ROW` once at table creation. After that, Oracle automatically makes the inline vs out-of-line decision for every INSERT and UPDATE based on the LOB size. This is transparent and requires no application code changes.

**Practical Examples:**

**Example 1: Document Management System (Mixed Sizes)**
```sql
-- Scenario: Mix of short notes (500 bytes) and large attachments (5MB)
-- Recommendation: ENABLE STORAGE IN ROW

CREATE TABLE cmr.documents (
    doc_id NUMBER PRIMARY KEY,
    doc_date DATE,
    doc_title VARCHAR2(200),
    doc_notes CLOB,        -- Usually < 2KB (short notes)
    doc_attachment BLOB    -- Usually > 100KB (PDFs, images)
)
PARTITION BY RANGE (doc_date) (
    PARTITION p_2025_01 VALUES LESS THAN (TO_DATE('2025-02-01','YYYY-MM-DD'))
        TABLESPACE DWH_HOT_DATA
)
LOB (doc_notes) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW   -- Small notes stored inline, faster access
    CHUNK 8192              -- Small chunk for small LOBs
    CACHE
)
LOB (doc_attachment) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    DISABLE STORAGE IN ROW  -- Large files always out-of-line, prevents row chaining
    CHUNK 65536             -- Large chunk for large files
    NOCACHE LOGGING
);

-- Result:
-- doc_notes: 80% stored inline (< 4KB), 1 I/O
-- doc_notes: 20% stored out-of-line (> 4KB), 2 I/Os
-- doc_attachment: 100% stored out-of-line, 2 I/Os
-- Average performance: Optimal for this workload
```

**Example 2: Application Logs (Mostly Small)**
```sql
-- Scenario: Log entries, typically 500-2000 bytes
-- Recommendation: ENABLE STORAGE IN ROW

CREATE TABLE cmr.application_logs (
    log_id NUMBER PRIMARY KEY,
    log_date TIMESTAMP,
    log_level VARCHAR2(20),
    log_message CLOB       -- Usually < 2KB
)
PARTITION BY RANGE (log_date) (
    PARTITION p_2025_01 VALUES LESS THAN (TIMESTAMP '2025-02-01 00:00:00')
        TABLESPACE DWH_HOT_DATA
)
LOB (log_message) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW   -- 95% of logs fit inline
    CHUNK 8192
    CACHE READS
);

-- Result:
-- 95% of log_message values stored inline (< 4KB)
-- 5% overflow to LOB segment (stack traces, large errors)
-- Query performance: Excellent (mostly single I/O)
```

**Example 3: Binary File Repository (All Large)**
```sql
-- Scenario: Video files, always > 10MB
-- Recommendation: DISABLE STORAGE IN ROW

CREATE TABLE cmr.media_files (
    file_id NUMBER PRIMARY KEY,
    file_date DATE,
    file_name VARCHAR2(500),
    file_content BLOB      -- Always > 10MB (videos)
)
PARTITION BY RANGE (file_date) (
    PARTITION p_2025_01 VALUES LESS THAN (TO_DATE('2025-02-01','YYYY-MM-DD'))
        TABLESPACE DWH_HOT_DATA
)
LOB (file_content) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    DISABLE STORAGE IN ROW  -- Always large, no point checking size
    CHUNK 131072            -- 128KB chunks for large files
    NOCACHE                 -- Don't pollute buffer cache
);

-- Result:
-- 100% of file_content stored out-of-line
-- No overhead checking for inline storage eligibility
-- Clean row structure, no row chaining
```

**Example 4: E-commerce Product Descriptions (Variable)**
```sql
-- Scenario: Product descriptions: 200 bytes to 20KB
-- Recommendation: ENABLE STORAGE IN ROW (let Oracle optimize)

CREATE TABLE cmr.products (
    product_id NUMBER PRIMARY KEY,
    product_name VARCHAR2(200),
    short_description CLOB,     -- 200-500 bytes (fits inline)
    long_description CLOB,       -- 2KB-20KB (mixed)
    html_content CLOB            -- 5KB-50KB (mostly out-of-line)
)
TABLESPACE DWH_HOT_DATA
LOB (short_description) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW   -- Almost always inline
    CHUNK 8192
    CACHE
)
LOB (long_description) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW   -- Mixed: Oracle decides per row
    CHUNK 16384
    CACHE READS
)
LOB (html_content) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    DISABLE STORAGE IN ROW  -- Usually large, always out-of-line
    CHUNK 32768
    CACHE READS
);

-- Result:
-- short_description: ~99% inline
-- long_description: ~60% inline, 40% out-of-line
-- html_content: 100% out-of-line
-- Optimal for this use case
```

**Checking If LOBs Are BASICFILE or SECUREFILE:**

```sql
-- Quick check: Identify BASICFILE vs SECUREFILE LOBs
SELECT
    owner,
    table_name,
    column_name,
    CASE
        WHEN securefile = 'YES' THEN 'SECUREFILE ✓'
        ELSE 'BASICFILE (Legacy - Needs Migration)'
    END AS lob_type,
    compression,
    deduplication,
    in_row,
    tablespace_name
FROM dba_lobs
WHERE owner = 'CMR'
ORDER BY securefile, table_name, column_name;

-- Sample output:
-- OWNER  TABLE_NAME  COLUMN_NAME    LOB_TYPE                              COMPRESSION  DEDUPLICATION  IN_ROW
-- CMR    OLD_DOCS    CONTENT        BASICFILE (Legacy - Needs Migration)  NO           NO             YES
-- CMR    DOCUMENTS   DOC_NOTES      SECUREFILE ✓                          NOCOMPRESS   NO             YES
-- CMR    DOCUMENTS   DOC_ATTACH     SECUREFILE ✓                          MEDIUM       NO             NO
```

```sql
-- Detailed view with LOB sizes - Identify migration candidates
SELECT
    l.owner,
    l.table_name,
    l.column_name,
    CASE
        WHEN l.securefile = 'YES' THEN 'SECUREFILE'
        ELSE 'BASICFILE'
    END AS lob_type,
    l.compression,
    l.deduplication,
    ROUND(s.bytes/1024/1024/1024, 2) AS size_gb,
    CASE
        WHEN l.securefile = 'NO' AND s.bytes/1024/1024/1024 > 10 THEN 'HIGH PRIORITY MIGRATION'
        WHEN l.securefile = 'NO' AND s.bytes/1024/1024/1024 > 1 THEN 'MEDIUM PRIORITY MIGRATION'
        WHEN l.securefile = 'NO' THEN 'LOW PRIORITY MIGRATION'
        ELSE 'OK (Already SECUREFILE)'
    END AS migration_status,
    l.tablespace_name
FROM dba_lobs l
LEFT JOIN dba_segments s
    ON l.segment_name = s.segment_name
    AND l.owner = s.owner
WHERE l.owner = 'CMR'
ORDER BY
    CASE WHEN l.securefile = 'NO' THEN 0 ELSE 1 END,
    s.bytes DESC NULLS LAST;

-- Sample output:
-- OWNER  TABLE_NAME  COLUMN_NAME  LOB_TYPE    SIZE_GB  MIGRATION_STATUS           TABLESPACE_NAME
-- CMR    OLD_DOCS    CONTENT      BASICFILE   15.3     HIGH PRIORITY MIGRATION    USERS
-- CMR    ARCHIVE     OLD_DATA     BASICFILE   2.1      MEDIUM PRIORITY MIGRATION  USERS
-- CMR    DOCUMENTS   DOC_NOTES    SECUREFILE  0.5      OK (Already SECUREFILE)    DWH_HOT_LOB
-- CMR    DOCUMENTS   DOC_ATTACH   SECUREFILE  12.8     OK (Already SECUREFILE)    DWH_HOT_LOB
```

```sql
-- Summary statistics: BASICFILE vs SECUREFILE by schema
SELECT
    owner,
    CASE
        WHEN securefile = 'YES' THEN 'SECUREFILE'
        ELSE 'BASICFILE (Needs Migration)'
    END AS lob_type,
    COUNT(*) AS lob_count,
    ROUND(SUM(s.bytes)/1024/1024/1024, 2) AS total_gb,
    SUM(CASE WHEN compression != 'NO' THEN 1 ELSE 0 END) AS compressed_count,
    SUM(CASE WHEN deduplication = 'YES' THEN 1 ELSE 0 END) AS dedupe_count
FROM dba_lobs l
LEFT JOIN dba_segments s
    ON l.segment_name = s.segment_name
    AND l.owner = s.owner
WHERE owner = 'CMR'
GROUP BY owner, securefile
ORDER BY securefile;

-- Sample output:
-- OWNER  LOB_TYPE                       LOB_COUNT  TOTAL_GB  COMPRESSED_COUNT  DEDUPE_COUNT
-- CMR    BASICFILE (Needs Migration)    8          45.2      0                 0
-- CMR    SECUREFILE                     15         23.7      10                3
```

```sql
-- Find all BASICFILE LOBs that need migration (for DBA meeting)
SELECT
    'ALTER TABLE ' || owner || '.' || table_name || ' MOVE LOB (' || column_name ||
    ') STORE AS SECUREFILE (TABLESPACE DWH_HOT_LOB COMPRESS MEDIUM);' AS migration_ddl,
    ROUND(s.bytes/1024/1024/1024, 2) AS current_size_gb,
    ROUND(s.bytes/1024/1024/1024 / 4, 2) AS estimated_compressed_gb
FROM dba_lobs l
LEFT JOIN dba_segments s
    ON l.segment_name = s.segment_name
    AND l.owner = s.owner
WHERE l.owner = 'CMR'
  AND l.securefile = 'NO'
ORDER BY s.bytes DESC NULLS LAST;

-- Sample output (ready to execute migration statements):
-- MIGRATION_DDL                                                          CURRENT_GB  ESTIMATED_COMPRESSED_GB
-- ALTER TABLE CMR.OLD_DOCS MOVE LOB (CONTENT) STORE AS...              15.3        3.8
-- ALTER TABLE CMR.ARCHIVE MOVE LOB (OLD_DATA) STORE AS...              2.1         0.5
```

**Checking Current LOB Storage Configuration:**

```sql
-- Query to see how LOBs are configured (all details)
SELECT
    table_name,
    column_name,
    securefile,                -- YES = SECUREFILE, NO = BASICFILE
    in_row,                    -- YES = ENABLE STORAGE IN ROW
    chunk,                     -- CHUNK size in bytes
    compression,
    deduplication,
    tablespace_name
FROM dba_lobs
WHERE owner = 'CMR'
ORDER BY table_name, column_name;

-- Sample output:
-- TABLE_NAME    COLUMN_NAME    SECUREFILE  IN_ROW  CHUNK   COMPRESSION  DEDUPLICATION  TABLESPACE_NAME
-- DOCUMENTS     DOC_NOTES      YES         YES     8192    NOCOMPRESS   NO             DWH_HOT_LOB
-- DOCUMENTS     DOC_ATTACHMENT YES         NO      65536   MEDIUM       NO             DWH_WARM_LOB
-- MEDIA_FILES   FILE_CONTENT   YES         NO      131072  HIGH         YES            DWH_ARCHIVE_LOB
-- OLD_DOCS      CONTENT        NO          YES     8192    NO           NO             USERS
-- PRODUCTS      SHORT_DESC     YES         YES     8192    NOCOMPRESS   NO             DWH_HOT_LOB
```

**Monitoring Inline vs Out-of-Line Storage:**

```sql
-- Check how much LOB data is actually stored inline vs out-of-line
SELECT
    l.table_name,
    l.column_name,
    l.in_row,
    COUNT(*) AS total_rows,
    -- This query shows segment usage; inline LOBs don't appear in segments
    ROUND(SUM(s.bytes)/1024/1024, 2) AS lob_segment_mb
FROM dba_lobs l
LEFT JOIN dba_segments s
    ON l.segment_name = s.segment_name
    AND l.owner = s.owner
WHERE l.owner = 'CMR'
GROUP BY l.table_name, l.column_name, l.in_row
ORDER BY l.table_name, l.column_name;

-- If lob_segment_mb is small relative to table size, most LOBs are inline
```

**Migration Between Inline and Out-of-Line:**

```sql
-- Change from ENABLE to DISABLE STORAGE IN ROW
-- (Requires table move, can be done online in 12c+)

ALTER TABLE cmr.documents MOVE ONLINE
    LOB (doc_notes) STORE AS SECUREFILE (
        TABLESPACE DWH_HOT_LOB
        DISABLE STORAGE IN ROW  -- Change from default ENABLE
        CHUNK 8192
    );

-- Change from DISABLE to ENABLE STORAGE IN ROW
ALTER TABLE cmr.media_files MOVE ONLINE
    LOB (file_content) STORE AS SECUREFILE (
        TABLESPACE DWH_HOT_LOB
        ENABLE STORAGE IN ROW   -- Allow inline storage
        CHUNK 65536
    );
```

**Common Misconceptions:**

❌ **WRONG:** "I need to write application code to decide where to store LOBs"
✅ **CORRECT:** Oracle automatically decides based on size. No application changes needed.

❌ **WRONG:** "ENABLE STORAGE IN ROW means all LOBs are stored inline"
✅ **CORRECT:** Only LOBs ≤ 4KB are stored inline. Larger LOBs automatically go out-of-line.

❌ **WRONG:** "I need to manually move LOBs between inline and out-of-line storage"
✅ **CORRECT:** Oracle handles this automatically on INSERT/UPDATE based on new size.

❌ **WRONG:** "DISABLE STORAGE IN ROW is faster because it's predictable"
✅ **CORRECT:** ENABLE is usually faster for mixed sizes (35% faster for typical workloads).

**What Happens on UPDATE:**

```sql
-- Initial insert: Small LOB stored inline
INSERT INTO cmr.test_lobs VALUES (100, 'Short text');  -- 10 bytes → Inline

-- Update to larger value: Oracle automatically moves to out-of-line
UPDATE cmr.test_lobs SET content = LPAD('X', 10000, 'X') WHERE id = 100;
-- 10KB → Out-of-line (automatic migration by Oracle)

-- Update back to small value: Oracle automatically moves back to inline
UPDATE cmr.test_lobs SET content = 'Short again' WHERE id = 100;
-- 11 bytes → Inline again (automatic migration by Oracle)

-- No manual intervention needed - Oracle handles everything!
```

**Best Practice Summary:**

1. **Default to ENABLE STORAGE IN ROW** for most use cases (it's the DEFAULT anyway)
   - Oracle automatically optimizes per row
   - Small LOBs benefit from inline storage (1 I/O)
   - Large LOBs automatically go out-of-line (2 I/Os)
   - **No application code changes required**
   - **No manual storage management needed**

2. **Use DISABLE STORAGE IN ROW when:**
   - All LOBs are known to be large (> 10KB)
   - Want predictable storage layout (always 2 I/Os)
   - Avoiding row chaining is critical
   - **Only about 10-20% of use cases**

3. **Consider data patterns:**
   - Analyze actual LOB sizes in your tables
   - Use statistics to guide decision
   - Monitor performance after migration
   - Let Oracle do the work unless you have a specific reason not to

4. **Partitioned tables with LOBs:**
   - Can use different strategies per partition
   - HOT tier: ENABLE (fast access for small LOBs, Oracle auto-optimizes)
   - ARCHIVE tier: Can use DISABLE (all compressed anyway, predictable)

#### 4.4.2 LOB Tablespace Strategy

**Dedicated LOB Tablespaces (Recommended):**

```
┌──────────────────────────────────────────────────────┐
│ TABLE DATA          │  LOB SEGMENTS                  │
├──────────────────────────────────────────────────────┤
│ DWH_HOT_DATA        →  DWH_HOT_LOB                   │
│ DWH_WARM_DATA       →  DWH_WARM_LOB (compressed)    │
│ DWH_COLD_DATA       →  DWH_COLD_LOB (high compress) │
│ DWH_ARCHIVE_DATA    →  DWH_ARCHIVE_LOB (dedupe)     │
└──────────────────────────────────────────────────────┘
```

**Benefits:**
- Independent space management for LOBs
- Apply different compression/deduplication per tier
- Easier monitoring and troubleshooting
- Flexible backup strategies (LOBs can be backed up less frequently)

#### 4.4.3 LOB Tablespace Creation

**HOT Tier (No Compression):**
```sql
CREATE TABLESPACE DWH_HOT_LOB
DATAFILE '/oradata/prod/dwh_hot_lob_01.dbf' SIZE 50G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;
```

**WARM Tier (MEDIUM Compression):**
```sql
CREATE TABLESPACE DWH_WARM_LOB
DATAFILE '/oradata/prod/dwh_warm_lob_01.dbf' SIZE 50G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;
-- Note: Compression specified at LOB column level, not tablespace
```

**COLD Tier (HIGH Compression):**
```sql
CREATE TABLESPACE DWH_COLD_LOB
DATAFILE '/oradata/prod/dwh_cold_lob_01.dbf' SIZE 50G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;
```

**ARCHIVE Tier (HIGH Compression + Deduplication):**
```sql
CREATE TABLESPACE DWH_ARCHIVE_LOB
DATAFILE '/oradata/prod/dwh_archive_lob_01.dbf' SIZE 25G
AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED
BLOCKSIZE 8K
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;
```

#### 4.4.4 LOB Column Definitions by Tier

**HOT Tier (Active Data - No Compression):**
```sql
CREATE TABLE cmr.documents (
    doc_id NUMBER PRIMARY KEY,
    doc_date DATE,
    doc_content CLOB,
    doc_binary BLOB
)
PARTITION BY RANGE (doc_date) (
    PARTITION p_2025_01 VALUES LESS THAN (TO_DATE('2025-02-01','YYYY-MM-DD'))
        TABLESPACE DWH_HOT_DATA
)
LOB (doc_content) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    ENABLE STORAGE IN ROW
    CHUNK 32768
    CACHE
    NOCOMPRESS
    KEEP_DUPLICATES
)
LOB (doc_binary) STORE AS SECUREFILE (
    TABLESPACE DWH_HOT_LOB
    DISABLE STORAGE IN ROW
    CHUNK 32768
    NOCACHE LOGGING
    NOCOMPRESS
    KEEP_DUPLICATES
);
```

**WARM Tier (Recent Historical - MEDIUM Compression):**
```sql
-- For existing table, add partition with different LOB parameters
ALTER TABLE cmr.documents ADD PARTITION p_2024_01
    VALUES LESS THAN (TO_DATE('2024-02-01','YYYY-MM-DD'))
    TABLESPACE DWH_WARM_DATA
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_WARM_LOB
        COMPRESS MEDIUM
        CACHE READS
        KEEP_DUPLICATES
    )
    LOB (doc_binary) STORE AS SECUREFILE (
        TABLESPACE DWH_WARM_LOB
        COMPRESS MEDIUM
        NOCACHE
        KEEP_DUPLICATES
    );
```

**COLD Tier (Archived - HIGH Compression):**
```sql
ALTER TABLE cmr.documents ADD PARTITION p_2022_01
    VALUES LESS THAN (TO_DATE('2022-02-01','YYYY-MM-DD'))
    TABLESPACE DWH_COLD_DATA
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_COLD_LOB
        COMPRESS HIGH
        CACHE READS
        KEEP_DUPLICATES
    )
    LOB (doc_binary) STORE AS SECUREFILE (
        TABLESPACE DWH_COLD_LOB
        COMPRESS HIGH
        NOCACHE
        KEEP_DUPLICATES
    );
```

**ARCHIVE Tier (Long-term - HIGH Compression + Deduplication):**
```sql
ALTER TABLE cmr.documents ADD PARTITION p_2018_01
    VALUES LESS THAN (TO_DATE('2018-02-01','YYYY-MM-DD'))
    TABLESPACE DWH_ARCHIVE_DATA
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_ARCHIVE_LOB
        COMPRESS HIGH
        DEDUPLICATE
        CACHE READS
    )
    LOB (doc_binary) STORE AS SECUREFILE (
        TABLESPACE DWH_ARCHIVE_LOB
        COMPRESS HIGH
        DEDUPLICATE
        NOCACHE
    );
```

#### 4.4.5 LOB Compression Ratios and Benefits

| Compression Level | Compression Ratio | CPU Overhead | Use Case |
|-------------------|-------------------|--------------|----------|
| NOCOMPRESS | 1x (baseline) | None | HOT tier, active DML |
| COMPRESS LOW | 2-3x | Low | Not typically used for LOBs |
| COMPRESS MEDIUM | 3-5x | Moderate | WARM tier, good balance |
| COMPRESS HIGH | 5-10x | Higher | COLD/ARCHIVE tier, read-only |

**Deduplication Benefits:**
- Eliminates duplicate LOB chunks across the segment
- Particularly effective for:
  - Standard document templates (e.g., contracts with boilerplate text)
  - Repeated attachments (e.g., same PDF attached to multiple records)
  - Log files with repetitive patterns
- Typical savings: 20-50% additional space reduction
- Best used in ARCHIVE tier where data is stable

**Example Compression Savings:**

| Scenario | LOB Type | Size (Uncompressed) | HIGH + DEDUPE | Savings |
|----------|----------|---------------------|---------------|---------|
| Contract documents (CLOB) | Text | 500 GB | 75 GB | 85% |
| PDF attachments (BLOB) | Binary | 1 TB | 200 GB | 80% |
| Log data (CLOB) | Text | 800 GB | 80 GB | 90% |

#### 4.4.6 ILM Policies for LOB Segments

**Automated LOB Compression and Movement:**

```sql
-- Policy: Compress LOBs after 12 months
INSERT INTO cmr.dwh_ilm_policies (
    policy_id, policy_name, object_owner, object_name,
    policy_type, condition_type, condition_value,
    action_type, action_params, enabled
) VALUES (
    dwh_ilm_policy_seq.NEXTVAL,
    'LOB_COMPRESS_MEDIUM_12M',
    'CMR',
    'DOCUMENTS',
    'PARTITION',
    'AGE_DAYS',
    '365',
    'MOVE_PARTITION',
    '{
        "target_tablespace": "DWH_WARM_DATA",
        "lob_params": {
            "doc_content": {
                "tablespace": "DWH_WARM_LOB",
                "compress": "MEDIUM"
            },
            "doc_binary": {
                "tablespace": "DWH_WARM_LOB",
                "compress": "MEDIUM"
            }
        }
    }',
    'Y'
);

-- Policy: Enable deduplication after 7 years
INSERT INTO cmr.dwh_ilm_policies (
    policy_id, policy_name, object_owner, object_name,
    policy_type, condition_type, condition_value,
    action_type, action_params, enabled
) VALUES (
    dwh_ilm_policy_seq.NEXTVAL,
    'LOB_ARCHIVE_DEDUPE_7Y',
    'CMR',
    'DOCUMENTS',
    'PARTITION',
    'AGE_DAYS',
    '2555',
    'MOVE_PARTITION',
    '{
        "target_tablespace": "DWH_ARCHIVE_DATA",
        "lob_params": {
            "doc_content": {
                "tablespace": "DWH_ARCHIVE_LOB",
                "compress": "HIGH",
                "deduplicate": true
            },
            "doc_binary": {
                "tablespace": "DWH_ARCHIVE_LOB",
                "compress": "HIGH",
                "deduplicate": true
            }
        }
    }',
    'Y'
);
```

**Manual LOB Partition Movement:**
```sql
-- Move LOB segment for a specific partition
ALTER TABLE cmr.documents MOVE PARTITION p_2022_01
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_COLD_LOB
        COMPRESS HIGH
        KEEP_DUPLICATES
    )
    LOB (doc_binary) STORE AS SECUREFILE (
        TABLESPACE DWH_COLD_LOB
        COMPRESS HIGH
        KEEP_DUPLICATES
    );
```

#### 4.4.7 LOB Monitoring and Space Analysis

**LOB Size by Tablespace:**
```sql
SELECT
    tablespace_name,
    COUNT(*) AS lob_segment_count,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS total_gb,
    ROUND(AVG(bytes)/1024/1024, 2) AS avg_lob_mb
FROM dba_segments
WHERE segment_type = 'LOBSEGMENT'
  AND owner = 'CMR'
GROUP BY tablespace_name
ORDER BY 3 DESC;
```

**LOB Details per Table:**
```sql
SELECT
    t.table_name,
    l.column_name,
    l.segment_name,
    l.tablespace_name,
    l.securefile,
    l.compression,
    l.deduplication,
    l.in_row,
    s.bytes/1024/1024 AS size_mb
FROM dba_lobs l
JOIN dba_tables t ON l.table_name = t.table_name AND l.owner = t.owner
LEFT JOIN dba_segments s ON l.segment_name = s.segment_name AND l.owner = s.owner
WHERE l.owner = 'CMR'
  AND t.table_name = 'DOCUMENTS'
ORDER BY s.bytes DESC NULLS LAST;
```

**LOB Compression Effectiveness:**
```sql
-- Compare partition LOB sizes (requires partitioned LOBs)
SELECT
    lp.table_name,
    lp.lob_name,
    lp.partition_name,
    lp.tablespace_name,
    lp.compression,
    lp.deduplication,
    ROUND(s.bytes/1024/1024, 2) AS size_mb
FROM dba_lob_partitions lp
LEFT JOIN dba_segments s
    ON lp.lob_partition_name = s.segment_name
    AND s.owner = 'CMR'
WHERE lp.table_owner = 'CMR'
  AND lp.table_name = 'DOCUMENTS'
ORDER BY lp.partition_position DESC;
```

**Deduplication Ratio:**
```sql
-- Check deduplication effectiveness (SECUREFILE only)
SELECT
    table_name,
    column_name,
    segment_name,
    securefile,
    ROUND(fs_bytes/1024/1024, 2) AS allocated_mb,
    ROUND(used_bytes/1024/1024, 2) AS used_mb,
    ROUND((1 - (used_bytes/NULLIF(fs_bytes,0))) * 100, 2) AS dedupe_pct
FROM dba_lobs
WHERE owner = 'CMR'
  AND deduplication = 'YES'
  AND securefile = 'YES';
```

#### 4.4.8 LOB Migration Considerations

**Converting BASICFILE to SECUREFILE:**
```sql
-- Option 1: Online move (12c+)
ALTER TABLE cmr.documents MOVE ONLINE
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_HOT_LOB
        COMPRESS MEDIUM
    );

-- Option 2: Offline move with ALTER TABLE
ALTER TABLE cmr.documents MOVE
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_HOT_LOB
        COMPRESS MEDIUM
    );

-- Option 3: DBMS_REDEFINITION for zero-downtime (large tables)
-- See Oracle documentation for full procedure
```

**Enabling Compression on Existing LOBs:**
```sql
-- Cannot alter compression in-place; must move the LOB
ALTER TABLE cmr.documents MOVE
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_WARM_LOB
        COMPRESS HIGH
    );
```

**Enabling Deduplication:**
```sql
-- Enable deduplication (requires rebuild)
ALTER TABLE cmr.documents MODIFY LOB (doc_content) (DEDUPLICATE);

-- Note: This only affects new LOB data. To deduplicate existing:
ALTER TABLE cmr.documents MOVE
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_ARCHIVE_LOB
        COMPRESS HIGH
        DEDUPLICATE
    );
```

#### 4.4.9 LOB Tablespace Sizing

**Initial Allocation Recommendations:**

| Tablespace | Size | Purpose | Growth Rate |
|------------|------|---------|-------------|
| DWH_HOT_LOB | 100 GB | Active LOBs, no compression | High (20-30%/year) |
| DWH_WARM_LOB | 200 GB | Recent LOBs, medium compression | Moderate (10-15%/year) |
| DWH_COLD_LOB | 300 GB | Archived LOBs, high compression | Low (5%/year) |
| DWH_ARCHIVE_LOB | 200 GB | Long-term LOBs, dedupe enabled | Minimal (<5%/year) |

**Sizing Formula:**
```
LOB Tablespace Size = (Total LOB Data / Compression Ratio) * 1.2 (buffer)

Example:
- 500 GB of CLOB data
- MEDIUM compression (4x ratio)
- Buffer: 20%
Size = (500 GB / 4) * 1.2 = 150 GB
```

#### 4.4.10 LOB Performance Best Practices

**CACHE Settings:**
```sql
-- HOT tier: Frequently accessed LOBs
LOB (doc_content) STORE AS SECUREFILE (
    CACHE                    -- Keep in buffer cache
)

-- WARM tier: Occasionally accessed
LOB (doc_content) STORE AS SECUREFILE (
    CACHE READS             -- Cache on read, not write
)

-- COLD/ARCHIVE tier: Rarely accessed
LOB (doc_content) STORE AS SECUREFILE (
    NOCACHE                 -- Don't cache (save buffer pool)
)
```

**CHUNK Size Optimization:**
```sql
-- Small LOBs (< 100KB average)
CHUNK 8192  -- 8KB, matches DB_BLOCK_SIZE

-- Medium LOBs (100KB - 10MB)
CHUNK 32768  -- 32KB

-- Large LOBs (> 10MB)
CHUNK 65536  -- 64KB or higher
```

**Inline Storage Threshold:**
```sql
-- Enable inline storage for small LOBs (< 4KB)
LOB (doc_content) STORE AS SECUREFILE (
    ENABLE STORAGE IN ROW   -- Faster access for small LOBs
)

-- Disable for large LOBs (always out-of-line)
LOB (doc_binary) STORE AS SECUREFILE (
    DISABLE STORAGE IN ROW  -- Prevents row chaining
)
```

#### 4.4.11 LOB-Specific Discussion Points

**For DBA Meeting:**

1. **Current LOB Usage:**
   - Which tables contain LOBs (CLOB, BLOB, NCLOB)?
   - What is the current total LOB storage consumption?
   - Are LOBs currently BASICFILE or SECUREFILE?

2. **Compression Requirements:**
   - Do we have Advanced Compression licenses for SECUREFILE?
   - What compression level is acceptable for each tier?
   - Expected compression ratios based on data type (text vs binary)?

3. **Deduplication Candidates:**
   - Which tables have repetitive LOB content?
   - Are there standard templates or attachments?
   - Storage savings from deduplication (run analysis query)?

4. **Performance Considerations:**
   - LOB access patterns (read-heavy vs write-heavy)?
   - Acceptable query latency for compressed LOBs?
   - Buffer cache size for LOB caching?

5. **Migration Strategy:**
   - Convert BASICFILE to SECUREFILE first or during partitioning?
   - Acceptable downtime for LOB migration?
   - Online redefinition required for zero-downtime?

**Quick Assessment Query:**
```sql
-- Identify all tables with LOBs
SELECT
    t.table_name,
    l.column_name,
    l.segment_name,
    l.securefile,
    l.compression,
    l.deduplication,
    ROUND(s.bytes/1024/1024/1024, 2) AS size_gb,
    CASE
        WHEN s.bytes/1024/1024/1024 > 10 THEN 'HIGH PRIORITY'
        WHEN s.bytes/1024/1024/1024 > 1 THEN 'MEDIUM PRIORITY'
        ELSE 'LOW PRIORITY'
    END AS migration_priority
FROM dba_lobs l
JOIN dba_tables t ON l.table_name = t.table_name AND l.owner = t.owner
LEFT JOIN dba_segments s ON l.segment_name = s.segment_name AND l.owner = s.owner
WHERE l.owner = 'CMR'
ORDER BY s.bytes DESC NULLS LAST;
```

---

## 5. Naming Conventions

### 5.1 Tablespace Naming Standard

**Format:** `<PROJECT>_<TIER>_<TYPE>`

**Components:**
- `<PROJECT>`: `DWH` (Data Warehouse)
- `<TIER>`: `HOT`, `WARM`, `COLD`, `ARCHIVE`
- `<TYPE>`: `DATA`, `IDX` (Index), `LOB` (Large Objects)

**Examples:**
- `DWH_HOT_DATA` - Hot tier data tablespace
- `DWH_HOT_IDX` - Hot tier index tablespace
- `DWH_HOT_LOB` - Hot tier LOB tablespace
- `DWH_WARM_DATA` - Warm tier data tablespace
- `DWH_WARM_LOB` - Warm tier LOB tablespace (compressed)
- `DWH_COLD_IDX` - Cold tier index tablespace
- `DWH_COLD_LOB` - Cold tier LOB tablespace (high compression)
- `DWH_ARCHIVE_LOB` - Archive tier LOB tablespace (dedupe enabled)

### 5.2 Datafile Naming Standard

**Format:** `<tablespace_name>_<sequence>.dbf`

**Examples:**
- `/oradata/prod/dwh_hot_data_01.dbf`
- `/oradata/prod/dwh_hot_data_02.dbf`
- `/oradata/prod/dwh_warm_data_01.dbf`
- `/oradata/prod/dwh_cold_idx_01.dbf`

### 5.3 ASM Diskgroup Mapping (if applicable)

| Tier | Diskgroup | Redundancy | Storage Type |
|------|-----------|------------|--------------|
| HOT | `+DATA_FLASH` | HIGH | SSD/NVMe |
| WARM | `+DATA_SAS` | NORMAL | SAS 10K/15K |
| COLD | `+DATA_SATA` | NORMAL | SATA 7.2K |
| ARCHIVE | `+DATA_ARCH` | NORMAL | SATA/Object Storage |

---

## 6. Migration Considerations

### 6.1 Initial Migration Approach

**Option 1: All-at-once (Recommended for smaller tables)**
```sql
-- Create partitioned table in target tablespace
-- Use CTAS or Data Pump
-- Swap table names
-- Drop old table
```

**Option 2: Phased Migration (Recommended for large tables)**
```sql
-- Create partitioned table with partitions in appropriate tiers
-- Migrate data in batches by date range
-- Validate and switch over
```

### 6.2 Tablespace Allocation During Migration

**Temporary Migration Tablespace:**
- `DWH_MIGRATION_TEMP` - For intermediate objects during migration
- Size: 150% of largest table being migrated
- Drop after migration completes

**Example:**
```sql
-- Stored in cmr.dwh_migration_analysis
SELECT
    task_id,
    table_name,
    recommended_partition_strategy,
    JSON_VALUE(partition_strategy_details, '$.initial_tablespace') AS initial_tbs
FROM cmr.dwh_migration_analysis
WHERE project_id = 1
  AND analysis_status = 'COMPLETED';
```

### 6.3 Rollback Strategy

- Keep original non-partitioned tables for 30 days after migration
- Store in `DWH_ROLLBACK_TBS` tablespace
- Rename with suffix `_OLD`
- Monitor partitioned table performance
- Drop after validation period

---

## 7. Monitoring and Maintenance

### 7.1 Space Monitoring Queries

**Tablespace Usage:**
```sql
SELECT
    tablespace_name,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS allocated_gb,
    ROUND(SUM(CASE WHEN status = 'ACTIVE' THEN bytes ELSE 0 END)/1024/1024/1024, 2) AS used_gb,
    ROUND((SUM(CASE WHEN status = 'ACTIVE' THEN bytes ELSE 0 END) / SUM(bytes)) * 100, 2) AS pct_used
FROM dba_data_files
WHERE tablespace_name LIKE 'DWH_%'
GROUP BY tablespace_name
ORDER BY tablespace_name;
```

**Partition Distribution:**
```sql
SELECT
    tablespace_name,
    COUNT(*) AS partition_count,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS total_gb
FROM dba_segments
WHERE segment_type LIKE 'TABLE%PARTITION'
  AND owner = 'CMR'
GROUP BY tablespace_name
ORDER BY tablespace_name;
```

### 7.2 ILM Execution Monitoring

```sql
-- Recent ILM actions
SELECT
    execution_date,
    policy_name,
    object_name,
    partition_name,
    action_type,
    execution_status,
    error_message
FROM cmr.dwh_ilm_execution_log
WHERE execution_date >= SYSDATE - 7
ORDER BY execution_date DESC;
```

### 7.3 Compression Effectiveness

```sql
-- Compare uncompressed vs compressed sizes
SELECT
    table_name,
    partition_name,
    compression,
    ROUND(bytes/1024/1024, 2) AS size_mb
FROM dba_tab_partitions
WHERE table_owner = 'CMR'
  AND table_name = 'FACT_SALES'
ORDER BY partition_position DESC;
```

---

## 8. Performance Considerations

### 8.1 I/O Distribution

**Goal:** Balance I/O across storage tiers
- HOT tier: 60-70% of read I/O (recent data queries)
- WARM tier: 20-30% of read I/O (historical analysis)
- COLD tier: 5-10% of read I/O (compliance/audit)
- ARCHIVE tier: <5% of read I/O (rare access)

### 8.2 Query Performance Impact

**Partition Pruning Benefits:**
- Queries with date predicates will scan only relevant partitions
- Example: `WHERE sale_date >= '2024-01-01'` scans only 2024+ partitions
- Reduces I/O by 90%+ for time-series queries

**Compression Trade-offs:**
- HOT (no compression): Fastest DML, moderate query speed
- WARM (OLTP compress): Slight DML overhead, good query speed
- COLD (Query High): Read-only, excellent compression, good query speed
- ARCHIVE (Archive High): Read-only, maximum compression, slower queries

### 8.3 Index Strategy by Tier

| Tier | Index Type | Rebuild Frequency | Compression |
|------|------------|-------------------|-------------|
| HOT | Local B-tree | Never (active DML) | None or LOW |
| WARM | Local B-tree | Annually | ADVANCED LOW |
| COLD | Local B-tree | After compression | ADVANCED HIGH |
| ARCHIVE | Minimal indexes | As needed | ADVANCED HIGH |

---

## 9. Backup and Recovery

### 9.1 Backup Strategy by Tier

**HOT Tier:**
- RMAN incremental backup: Daily
- Archive log backup: Every 15 minutes
- Retention: 30 days

**WARM Tier:**
- RMAN incremental backup: Weekly
- Archive log backup: Every 15 minutes
- Retention: 60 days

**COLD/ARCHIVE Tier:**
- RMAN full backup: Monthly (after compression)
- Partitions rarely change (read-only)
- Retention: 1 year

### 9.2 Point-in-Time Recovery

```sql
-- Partition-level recovery (Oracle 12c+)
RECOVER TABLESPACE DWH_HOT_DATA
UNTIL TIME "TO_DATE('2025-01-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS')";
```

---

## 10. Cost Optimization

### 10.1 Storage Cost Comparison

**Example: 10TB Data Warehouse**

| Scenario | Storage Cost | Compression | Effective Size | Annual Cost | Savings |
|----------|--------------|-------------|----------------|-------------|---------|
| No ILM (all HOT) | $500/TB/year | None | 10 TB | $5,000 | Baseline |
| With ILM (tiered) | Blended | 3x avg | 3.3 TB | $1,200 | 76% |

**Breakdown:**
- HOT (1 TB @ $500/TB): $500
- WARM (1 TB @ $300/TB): $300
- COLD (1 TB @ $150/TB): $150
- ARCHIVE (0.3 TB @ $50/TB): $15
- **Total: $965/year vs $5,000/year**

### 10.2 ROI Calculation

**Investment:**
- Oracle Advanced Compression license: $X per core
- Storage hardware/cloud costs: Varies
- DBA time for implementation: Y hours

**Returns:**
- Storage cost reduction: 60-80%
- Backup window reduction: 50%+
- Query performance improvement: 2-10x (partition pruning)
- Operational efficiency: Automated lifecycle management

---

## 11. Discussion Points for DBA Meeting

### 11.1 Questions to Address

1. **Storage Infrastructure:**
   - What storage tiers are currently available?
   - Are we using ASM, filesystem, or cloud storage?
   - What are the current I/O performance characteristics?

2. **Licensing:**
   - Do we have Oracle Advanced Compression licenses?
   - Are we on Exadata (Hybrid Columnar Compression available)?
   - Any license restrictions to consider?

3. **Operational:**
   - What is the current backup window?
   - When can ILM jobs run (maintenance windows)?
   - Who will monitor tablespace growth and ILM execution?

4. **Migration Planning:**
   - Which tables to migrate first (pilot candidates)?
   - What is the acceptable downtime per table?
   - Rollback procedures and validation criteria?

5. **Performance:**
   - What are acceptable query SLAs by data age?
   - How to handle ad-hoc queries against archived data?
   - Index rebuild strategy during tier transitions?

### 11.2 Decisions Needed

- [ ] Finalize tablespace naming convention (including LOB tablespaces)
- [ ] Approve storage tier sizes and growth strategy
- [ ] Select compression types by tier (license-dependent)
- [ ] Define ILM policy thresholds (age, access patterns)
- [ ] Identify tables with LOB columns and assess compression candidates
- [ ] Decide on BASICFILE to SECUREFILE migration strategy
- [ ] Establish monitoring and alerting procedures
- [ ] Create migration schedule and priority list
- [ ] Define success metrics and KPIs

### 11.3 Action Items (Template)

| Action | Owner | Due Date | Status |
|--------|-------|----------|--------|
| Create tablespaces (DEV) | DBA Team | TBD | Pending |
| Test compression ratios | DBA/Dev | TBD | Pending |
| Document backup strategy | DBA Team | TBD | Pending |
| Create monitoring dashboard | Dev Team | TBD | Pending |
| Pilot migration (1 table) | Joint | TBD | Pending |
| Performance testing | QA/DBA | TBD | Pending |
| Production rollout plan | Project Team | TBD | Pending |

---

## 12. Appendix A: Quick Reference Commands

### Create All Tablespaces
```sql
-- HOT TIER
CREATE TABLESPACE DWH_HOT_DATA DATAFILE '/oradata/prod/dwh_hot_data_01.dbf' SIZE 100G AUTOEXTEND ON NEXT 1G;
CREATE TABLESPACE DWH_HOT_IDX DATAFILE '/oradata/prod/dwh_hot_idx_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 256M;
CREATE TABLESPACE DWH_HOT_LOB DATAFILE '/oradata/prod/dwh_hot_lob_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 1G;

-- WARM TIER
CREATE TABLESPACE DWH_WARM_DATA DATAFILE '/oradata/prod/dwh_warm_data_01.dbf' SIZE 100G AUTOEXTEND ON NEXT 1G DEFAULT COMPRESS FOR OLTP;
CREATE TABLESPACE DWH_WARM_IDX DATAFILE '/oradata/prod/dwh_warm_idx_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 256M;
CREATE TABLESPACE DWH_WARM_LOB DATAFILE '/oradata/prod/dwh_warm_lob_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 1G;

-- COLD TIER
CREATE TABLESPACE DWH_COLD_DATA DATAFILE '/oradata/prod/dwh_cold_data_01.dbf' SIZE 100G AUTOEXTEND ON NEXT 1G DEFAULT COMPRESS FOR QUERY HIGH;
CREATE TABLESPACE DWH_COLD_IDX DATAFILE '/oradata/prod/dwh_cold_idx_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 256M;
CREATE TABLESPACE DWH_COLD_LOB DATAFILE '/oradata/prod/dwh_cold_lob_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 1G;

-- ARCHIVE TIER
CREATE TABLESPACE DWH_ARCHIVE_DATA DATAFILE '/oradata/prod/dwh_archive_data_01.dbf' SIZE 50G AUTOEXTEND ON NEXT 1G DEFAULT COMPRESS FOR ARCHIVE HIGH;
CREATE TABLESPACE DWH_ARCHIVE_IDX DATAFILE '/oradata/prod/dwh_archive_idx_01.dbf' SIZE 20G AUTOEXTEND ON NEXT 256M;
CREATE TABLESPACE DWH_ARCHIVE_LOB DATAFILE '/oradata/prod/dwh_archive_lob_01.dbf' SIZE 25G AUTOEXTEND ON NEXT 1G;
```

### Move Partition Between Tablespaces
```sql
-- Regular table partition
ALTER TABLE cmr.fact_sales MOVE PARTITION p_2022_01 TABLESPACE DWH_COLD_DATA COMPRESS FOR QUERY HIGH;
ALTER INDEX cmr.idx_fact_sales_date REBUILD PARTITION p_2022_01 TABLESPACE DWH_COLD_IDX COMPRESS ADVANCED HIGH;

-- Partition with LOB columns
ALTER TABLE cmr.documents MOVE PARTITION p_2022_01
    TABLESPACE DWH_COLD_DATA
    LOB (doc_content) STORE AS SECUREFILE (
        TABLESPACE DWH_COLD_LOB
        COMPRESS HIGH
    );
```

### Monitor Space Usage
```sql
-- Overall tablespace usage
SELECT tablespace_name, ROUND(SUM(bytes)/1024/1024/1024,2) AS gb FROM dba_segments WHERE tablespace_name LIKE 'DWH_%' GROUP BY tablespace_name;

-- LOB segment breakdown
SELECT
    tablespace_name,
    segment_type,
    COUNT(*) AS segment_count,
    ROUND(SUM(bytes)/1024/1024/1024, 2) AS gb
FROM dba_segments
WHERE tablespace_name LIKE 'DWH_%'
GROUP BY tablespace_name, segment_type
ORDER BY tablespace_name, segment_type;

-- Check LOB compression status
SELECT table_name, column_name, securefile, compression, deduplication, tablespace_name
FROM dba_lobs
WHERE owner = 'CMR'
ORDER BY table_name, column_name;
```

---

## 13. Appendix B: References

- Oracle Database VLDB and Partitioning Guide
- Oracle Advanced Compression documentation
- ILM framework code: `/scripts/custom_ilm_*.sql`
- Migration framework code: `/scripts/table_migration_*.sql`
- Example queries: `/examples/table_migration_examples.sql`

---

**Document Version:** 1.0
**Last Updated:** 2025-10-27
**Next Review:** Post-DBA Meeting
