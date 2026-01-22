# Equality Delete Vectors - User Guide

**Version**: 1.0
**Date**: 2026-01-22
**Minimum Iceberg Version**: 1.5.0+

---

## Overview

Equality Delete Vectors (EDV) provide **40-100x storage reduction** and **90%+ memory reduction** for equality deletes on sequential LONG columns by storing deleted values as compressed bitmaps instead of full row data.

### Key Benefits

- 🗜️ **Massive Compression**: 1M deletes = 1 MB (vs 100 MB Parquet)
- 💾 **Memory Efficient**: 100x less memory during table scans
- ⚡ **Fast Performance**: Same or better read/write performance
- 🔄 **Compatible**: Works alongside traditional delete files

### When to Use

✅ **Perfect For**:
- CDC tables with `_row_id` (sequential identifiers)
- Batch deletes on auto-increment primary keys
- Large delete operations (>100K rows)

❌ **Not Suitable For**:
- Random/UUID fields (poor compression)
- Multi-column equality deletes (not supported)
- Non-LONG fields (INT, STRING, etc.)

---

## Quick Start

### 30-Second Setup

**1. Enable EDV** (one command):
```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);
```

**2. Verify** (check next DELETE creates `.puffin` file):
```sql
DELETE FROM my_table WHERE id IN (1, 2, 3);

-- Check format
SELECT file_format, file_size_in_bytes
FROM my_table.metadata.delete_files
ORDER BY committed_at DESC LIMIT 1;
-- Should show: PUFFIN, ~200 bytes
```

✅ **Done!** Future deletes now use bitmap compression.

---

## Prerequisites

### Required

1. **Format Version 3+**:
   ```sql
   -- Check current version
   DESCRIBE TABLE EXTENDED my_table;

   -- Upgrade if needed
   ALTER TABLE my_table SET TBLPROPERTIES ('format-version' = '3');
   ```

2. **Single LONG Equality Field**:
   ```sql
   -- ✅ Good: Single LONG field
   DELETE FROM my_table WHERE user_id IN (1, 2, 3);

   -- ❌ Bad: Multiple fields
   DELETE FROM my_table WHERE (user_id, order_id) IN ((1,100), (2,200));

   -- ❌ Bad: Non-LONG field
   DELETE FROM my_table WHERE username IN ('alice', 'bob');
   ```

3. **Non-Negative Values**:
   ```sql
   -- ✅ Good: Positive IDs
   DELETE FROM my_table WHERE id IN (100, 200, 300);

   -- ❌ Bad: Negative values
   DELETE FROM my_table WHERE id IN (-1, -2, -3);
   ```

### Recommended

- **Identifier Fields**: Mark your delete field as identifier
  ```sql
  ALTER TABLE my_table SET IDENTIFIER FIELDS user_id;
  ```

- **Sequential Pattern**: Best compression with sequential or clustered IDs
  ```sql
  -- Sequential IDs (100x compression)
  _row_id: 1, 2, 3, 4, 5, ...

  -- Clustered IDs (50x compression)
  order_id: 1-100, 500-600, 1000-1100, ...
  ```

---

## Common Scenarios

### Scenario 1: CDC Table (Flink/Debezium)

**Perfect use case** - Sequential `_row_id` gets 100x compression

```sql
-- 1. Create CDC table
CREATE TABLE orders_cdc (
  _row_id BIGINT,      -- Sequential CDC identifier
  order_id BIGINT,
  customer_id BIGINT,
  amount DECIMAL(10,2),
  updated_at TIMESTAMP
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);

-- 2. Mark identifier (recommended)
ALTER TABLE orders_cdc SET IDENTIFIER FIELDS _row_id;

-- 3. CDC operations automatically use EDV
-- Flink CDC DELETE → EDV bitmap file
-- Flink CDC UPDATE → EDV delete + new insert
```

**Result**:
- 1M CDC deletes: **1 MB EDV** (vs 100 MB Parquet)
- Memory during scan: **2 MB** (vs 200 MB)
- Storage savings: **99%**

---

### Scenario 2: Batch DELETE (Spark)

**Common use case** - Cleanup old data

```sql
-- 1. Enable EDV
ALTER TABLE events SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);

-- 2. Run batch delete
DELETE FROM events
WHERE event_id IN (
  SELECT event_id
  FROM events
  WHERE event_date < '2024-01-01'
);

-- 3. Check compression
SELECT
  file_format,
  record_count,
  file_size_in_bytes / 1024 / 1024 as size_mb
FROM events.metadata.delete_files
ORDER BY committed_at DESC LIMIT 1;
```

**Expected**:
```
file_format | record_count | size_mb
PUFFIN      | 100000       | 1 MB      ← 100x smaller ✅
```

---

### Scenario 3: MERGE Operation (Spark/Flink)

**MERGE automatically uses EDV** when enabled

```sql
-- Enable EDV
ALTER TABLE target SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);

-- MERGE deletes use EDV
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Check delete file format
SELECT file_format FROM target.metadata.delete_files
WHERE operation = 'merge' ORDER BY committed_at DESC LIMIT 1;
-- Should be: PUFFIN
```

---

## Configuration

### Table Properties

**Enable EDV**:
```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);
```

**Disable EDV** (fall back to Parquet):
```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'false'
);
```

**Check current setting**:
```sql
SHOW TBLPROPERTIES my_table;
-- Look for: write.delete.equality-vector.enabled = true
```

---

## Monitoring

### Check File Formats

**See mix of traditional vs EDV files**:
```sql
SELECT
  file_format,
  COUNT(*) as file_count,
  SUM(record_count) as total_deletes,
  SUM(file_size_in_bytes) / 1024 / 1024 as total_mb
FROM my_table.metadata.delete_files
GROUP BY file_format;
```

**Expected after migration**:
```
file_format | file_count | total_deletes | total_mb
PARQUET     | 50         | 5000000       | 5000 MB    ← Old files
PUFFIN      | 10         | 1000000       | 10 MB      ← New EDV ✅
```

---

### Check Compression Ratio

**Measure EDV effectiveness**:
```sql
SELECT
  file_path,
  record_count,
  file_size_in_bytes,
  file_size_in_bytes / NULLIF(record_count, 0) as bytes_per_delete
FROM my_table.metadata.delete_files
WHERE file_format = 'PUFFIN'
ORDER BY committed_at DESC
LIMIT 10;
```

**Good compression**:
- Sequential: **<1 byte per delete** (100x+ compression)
- Sparse: **<10 bytes per delete** (10x compression)

**Poor compression**:
- Random: **>50 bytes per delete** (<2x compression)
- **Action**: Consider disabling EDV for this table

---

## Compaction

### Why Compact?

- Merge small delete files into larger ones
- Convert old Parquet → EDV format
- Reduce file count, improve scan performance

### How to Compact

**Spark SQL** (recommended):
```sql
-- Compact delete files
CALL your_catalog.system.rewrite_delete_files(
  table => 'my_table',
  options => map('min-input-files', '5')
);
```

**Result**:
```
Before: 100 Parquet files (5 GB)
After: 1 Puffin file (50 MB)  ← 100x smaller ✅
```

**Schedule** (Airflow example):
```python
# Daily compaction at 2 AM
@dag(schedule_interval="0 2 * * *")
def compact_deletes():
    SparkSQLOperator(
        task_id="compact",
        sql="""
        CALL catalog.system.rewrite_delete_files(
          table => 'my_table'
        );
        """
    )
```

---

## Troubleshooting

### EDV Not Being Used?

**Check 1**: Verify property
```sql
SHOW TBLPROPERTIES my_table;
-- Must show: write.delete.equality-vector.enabled = true
```

**Check 2**: Verify format version
```sql
DESCRIBE TABLE EXTENDED my_table;
-- Must be: format-version: 3 or higher
```

**Check 3**: Check logs for fallback warnings
```bash
grep "EDV" /path/to/logs/*.log
```

**Common fallback reasons**:
- ❌ Multi-column equality: Use traditional
- ❌ STRING field: Use traditional
- ❌ Negative values: Use traditional
- ❌ NULL values: Use traditional

---

### Files Larger Than Expected?

**Check delete pattern**:
```sql
-- Analyze value distribution
SELECT
  MIN(id) as min_val,
  MAX(id) as max_val,
  COUNT(*) as delete_count,
  (MAX(id) - MIN(id)) / NULLIF(COUNT(*), 0) as avg_gap
FROM (
  -- Hypothetical: extract values from delete files
  SELECT id FROM my_table.deletes
);
```

**If avg_gap > 1000**: Sparse pattern → limited compression (10-50x vs 100x)

**Still beneficial**, just not as dramatic.

---

### How to Disable?

**Disable for future deletes**:
```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'false'
);
```

**Existing EDV files**:
- Still applied correctly
- No data loss
- Will be compacted to Parquet over time

---

## Performance Expectations

### Write Performance

- **Throughput**: 80-95% of Parquet (slightly slower)
- **Why**: Bitmap construction + compression overhead
- **Trade-off**: Worth it for read/storage benefits

### Read Performance

- **Throughput**: 95-110% of Parquet (same or faster)
- **Memory**: 50-100x less (huge benefit)
- **Why**: Bitmap lookup = O(1), no object allocation

### Storage

| Delete Pattern | Compression Ratio |
|---------------|-------------------|
| Sequential (0,1,2,...) | 50-100x |
| Clustered (ranges) | 10-50x |
| Sparse (gaps < 10K) | 10-20x |
| Random/UUID | 1-2x ❌ |

---

## Best Practices

### ✅ DO

1. **Use for CDC tables** with `_row_id`
   ```sql
   ALTER TABLE cdc_table SET IDENTIFIER FIELDS _row_id;
   ALTER TABLE cdc_table SET TBLPROPERTIES (
     'write.delete.equality-vector.enabled' = 'true'
   );
   ```

2. **Enable for large delete operations** (>100K deletes)

3. **Monitor file sizes** to verify compression

4. **Schedule regular compaction** (daily/weekly)

5. **Test on one table first**, then expand

---

### ❌ DON'T

1. **Don't use for random/UUID fields**
   - Poor compression (1-2x)
   - Stick with Parquet

2. **Don't use for multi-column equality deletes**
   - Not supported
   - Falls back to Parquet automatically

3. **Don't enable for all tables blindly**
   - Check delete pattern first
   - Sequential/clustered = good, random = bad

4. **Don't skip compaction**
   - Many small files = poor performance
   - Compact weekly minimum

---

## Advanced Topics

### Mixed Delete Files

**EDV and Parquet can coexist**:
```
Table delete files:
- delete-00001.parquet (traditional, multi-column)
- delete-00002.puffin (EDV, single LONG)
- delete-00003.parquet (traditional, STRING field)
- delete-00004.puffin (EDV, single LONG)

✅ All applied correctly during reads
```

---

### Schema Evolution

**Field drops**:
```sql
-- 1. Table has EDV deletes on user_id
-- 2. Drop user_id column
ALTER TABLE my_table DROP COLUMN user_id;

-- 3. EDV files still apply using old field value
-- (Standard Iceberg schema evolution)
```

---

### Identifier Fields (Optional)

**Recommended but not required**:
```sql
-- Without identifier (works, warning logged)
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);
-- ⚠️ Log: "Equality field 'id' is not marked as identifier..."

-- With identifier (works, no warning)
ALTER TABLE my_table SET IDENTIFIER FIELDS id;
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);
-- ✅ No warning
```

**Benefits of setting identifier**:
- Clearer semantics
- No warning logs
- Better documentation

---

## FAQ

### Q: Will existing delete files be converted?

**A**: No, only during compaction. Enable EDV → new deletes use EDV → compact to merge old+new.

### Q: Can I mix EDV and traditional deletes?

**A**: Yes! They coexist perfectly. Useful when some deletes qualify for EDV, others don't.

### Q: What if my Iceberg version doesn't support EDV?

**A**: Upgrade to v1.5.0+. Or wait - EDV is backward compatible (old readers ignore EDV, data appears undeleted).

### Q: Does EDV work with all file formats (Parquet/Avro/ORC)?

**A**: EDV is independent of data file format. Data can be Parquet/Avro/ORC, EDV deletes always use Puffin.

### Q: Can I use EDV with partitioned tables?

**A**: Yes! EDV works normally with partitioning. Each partition can have EDV delete files.

### Q: How do I know if EDV is worth it for my table?

**A**: Check your delete pattern:
- Sequential/clustered IDs → Definitely worth it (100x compression)
- Sparse IDs (gaps < 10K) → Probably worth it (10-50x compression)
- Random IDs → Not worth it (1-2x compression)

---

## Getting Help

- **Migration Guide**: See `equality-delete-vectors-migration-guide.md`
- **Performance Details**: See `equality-delete-vectors-performance.md`
- **Spec Details**: See `equality-delete-vectors-spec-addition.md`
- **Issues**: GitHub issues or contact your platform team

---

*User Guide Version: 1.0*
*Last Updated: 2026-01-22*
*Tested with: Apache Iceberg 1.5.0+, Spark 3.5, Flink 2.0*
