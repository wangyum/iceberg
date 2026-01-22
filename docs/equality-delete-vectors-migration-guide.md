# Equality Delete Vectors - Migration Guide

**Target Audience**: Data Engineers, Platform Engineers, DBAs
**Prerequisites**: Apache Iceberg format version 3+
**Estimated Time**: 30-60 minutes for typical table

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites Check](#prerequisites-check)
3. [Migration Steps](#migration-steps)
4. [Verification](#verification)
5. [Rollback](#rollback)
6. [Troubleshooting](#troubleshooting)
7. [Best Practices](#best-practices)

---

## Overview

### What is EDV?

Equality Delete Vectors (EDV) is an optimization that stores equality deletes as compressed bitmaps instead of full Parquet rows, providing:

- **40-100x storage reduction** for sequential delete patterns
- **90%+ memory reduction** during table scans
- **Same or better read performance**

### When to Migrate?

✅ **Migrate if your table has**:
- CDC ingestion with `_row_id` or similar sequential identifiers
- Large delete operations (>100K deletes regularly)
- Sequential or clustered LONG primary keys
- Memory constraints during scans

❌ **Don't migrate if your table has**:
- Random/UUID primary keys (poor compression)
- Multi-column equality deletes (not supported)
- Non-LONG delete fields (INT, STRING, etc.)
- Format version < 3

### Migration Impact

**Zero Downtime**: ✅ Migration is non-breaking
- Reads continue uninterrupted
- Writes continue uninterrupted
- No data rewrite required
- Changes affect only *new* delete files

**What Changes**:
- Future DELETE operations create `.puffin` files instead of `.parquet`
- Existing delete files remain unchanged
- Compaction gradually converts old deletes to EDV

---

## Prerequisites Check

### Step 1: Check Format Version

**Command** (Spark SQL):
```sql
DESCRIBE TABLE EXTENDED your_table;
```

**Look for**: `format-version: 3` or higher

**If version < 3**:
```sql
-- Upgrade to format version 3 (required for Puffin support)
ALTER TABLE your_table SET TBLPROPERTIES ('format-version' = '3');
```

**⚠️ Warning**: Format version upgrades are irreversible. Older readers (<v1.3.0) cannot read v3 tables.

---

### Step 2: Check Delete Pattern

**Identify your equality delete field**:
```sql
-- Examine recent delete operations
SELECT operation, summary
FROM your_table.history
WHERE operation IN ('delete', 'overwrite')
ORDER BY made_current_at DESC
LIMIT 10;
```

**Check delete field type**:
```sql
-- Show table schema
DESCRIBE your_table;
```

**✅ Good candidates for EDV**:
- `_row_id BIGINT` (CDC tables)
- `event_id BIGINT` (sequential event IDs)
- `transaction_id BIGINT` (auto-increment IDs)

**❌ Bad candidates**:
- `user_uuid STRING` (random UUIDs)
- `composite_key (col1, col2)` (multi-column)
- `status STRING` (non-LONG type)

---

### Step 3: Check Current Delete File Sizes

**Command** (Spark SQL):
```sql
-- Check delete file metrics
SELECT
  file_format,
  COUNT(*) as file_count,
  SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb,
  AVG(record_count) as avg_deletes_per_file
FROM your_table.metadata.delete_files
GROUP BY file_format;
```

**Expected output**:
```
file_format | file_count | total_size_mb | avg_deletes_per_file
------------|------------|---------------|---------------------
PARQUET     | 100        | 5000 MB       | 50000
```

**Calculate potential savings**:
- Sequential deletes: **5000 MB → ~50 MB** (100x reduction)
- Sparse deletes: **5000 MB → ~500 MB** (10x reduction)

---

## Migration Steps

### Step 1: Enable EDV (No Impact)

**Command**:
```sql
ALTER TABLE your_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);
```

**What happens**:
- ✅ Future DELETE operations use EDV format
- ✅ Existing delete files unchanged
- ✅ Reads work with both formats
- ✅ No data rewrite

**Verification**:
```sql
-- Confirm property is set
SHOW TBLPROPERTIES your_table;
-- Should show: write.delete.equality-vector.enabled = true
```

---

### Step 2: Test with Sample Delete (Recommended)

**Run a small test delete**:
```sql
-- Delete a small number of rows (test)
DELETE FROM your_table WHERE id IN (1, 2, 3, 4, 5);
```

**Check that EDV file was created**:
```sql
-- Verify new delete file is PUFFIN format
SELECT
  file_path,
  file_format,
  file_size_in_bytes,
  record_count
FROM your_table.metadata.delete_files
ORDER BY file_path DESC
LIMIT 1;
```

**Expected output**:
```
file_path: s3://.../metadata/delete-00123.puffin  ← Puffin file
file_format: PUFFIN                                ← EDV format ✅
file_size_in_bytes: 250                            ← Very small
record_count: 5
```

**Compare to traditional**:
- Traditional Parquet: ~5 rows × 100 bytes = **~500 bytes**
- EDV Puffin: **~250 bytes** (50% smaller even for 5 deletes)

---

### Step 3: Monitor New Deletes

**Over the next hours/days**, monitor new delete operations:

```sql
-- Check recent delete files (last 24 hours)
SELECT
  file_format,
  COUNT(*) as count,
  SUM(file_size_in_bytes) / 1024 as total_kb,
  AVG(record_count) as avg_deletes
FROM your_table.metadata.delete_files
WHERE committed_at > current_timestamp - INTERVAL 1 DAY
GROUP BY file_format;
```

**Expected**:
```
file_format | count | total_kb | avg_deletes
------------|-------|----------|------------
PUFFIN      | 10    | 5 KB     | 10000     ← New EDV files
PARQUET     | 0     | 0        | 0         ← No new Parquet
```

**Good signs**:
- ✅ All new deletes are PUFFIN format
- ✅ File sizes are dramatically smaller
- ✅ No errors in logs

---

### Step 4: Trigger Compaction (Optional)

**Why**: Convert existing Parquet delete files to EDV

**Command** (Spark SQL):
```sql
-- Compact delete files
CALL your_catalog.system.rewrite_delete_files(
  table => 'your_table',
  options => map('min-input-files', '2')
);
```

**What happens**:
1. Reads existing Parquet delete files
2. Merges delete values
3. Rewrites as EDV (if all constraints met)
4. Atomically swaps old → new files

**Check compaction results**:
```sql
-- Compare before/after
SELECT
  file_format,
  COUNT(*) as file_count,
  SUM(file_size_in_bytes) / 1024 / 1024 as size_mb
FROM your_table.metadata.delete_files
GROUP BY file_format;
```

**Expected**:
```
BEFORE:
file_format | file_count | size_mb
PARQUET     | 100        | 5000 MB

AFTER:
file_format | file_count | size_mb
PUFFIN      | 1          | 50 MB      ← Huge reduction ✅
```

---

## Verification

### Test 1: Verify Data Correctness

**Run known query with expected results**:
```sql
-- Query that should return known count
SELECT COUNT(*) FROM your_table WHERE id > 1000;
```

**Expected**: Same count as before migration

**If different**:
- Check if recent deletes were applied
- Verify sequence numbers are correct

---

### Test 2: Verify Delete Application

**Delete specific rows**:
```sql
DELETE FROM your_table WHERE id IN (12345, 67890);
```

**Verify deletion**:
```sql
SELECT * FROM your_table WHERE id IN (12345, 67890);
-- Should return 0 rows
```

---

### Test 3: Check Performance

**Measure scan time**:
```sql
-- Before EDV (baseline)
-- Record time for full table scan
SELECT COUNT(*) FROM your_table;

-- After EDV (should be same or faster)
SELECT COUNT(*) FROM your_table;
```

**Expected**: Similar or better performance (10% faster typical)

---

### Test 4: Check File Sizes

**Compare old vs new delete files**:
```sql
SELECT
  file_format,
  AVG(file_size_in_bytes) as avg_size_bytes,
  AVG(record_count) as avg_deletes,
  AVG(file_size_in_bytes / NULLIF(record_count, 0)) as bytes_per_delete
FROM your_table.metadata.delete_files
GROUP BY file_format;
```

**Expected**:
```
file_format | avg_size_bytes | avg_deletes | bytes_per_delete
------------|----------------|-------------|------------------
PARQUET     | 5000000        | 50000       | 100
PUFFIN      | 50000          | 50000       | 1               ← 100x better ✅
```

---

## Rollback

### Scenario 1: Disable EDV (Keep Existing Files)

**If you want to stop creating new EDV files**:

```sql
-- Disable EDV for future writes
ALTER TABLE your_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'false'
);
```

**Result**:
- ✅ Future deletes use traditional Parquet
- ✅ Existing EDV files still applied correctly
- ✅ No data loss

---

### Scenario 2: Revert to All-Parquet (Rare)

**If you must remove all EDV files** (not usually needed):

```sql
-- 1. Disable EDV
ALTER TABLE your_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'false'
);

-- 2. Force rewrite of delete files to Parquet
CALL your_catalog.system.rewrite_delete_files(
  table => 'your_table',
  options => map(
    'min-input-files', '1',
    'target-file-size-bytes', '536870912'
  )
);
```

**Result**: All delete files converted back to Parquet

**⚠️ Warning**: This increases file sizes dramatically. Only use if absolutely necessary.

---

## Troubleshooting

### Issue 1: EDV Not Being Used

**Symptom**: New delete files are still PARQUET format

**Check 1**: Verify property is set
```sql
SHOW TBLPROPERTIES your_table;
-- Should show: write.delete.equality-vector.enabled = true
```

**Check 2**: Verify format version
```sql
DESCRIBE TABLE EXTENDED your_table;
-- Should show: format-version: 3
```

**Check 3**: Check delete field type
```sql
-- Is your equality field LONG?
DESCRIBE your_table;
-- equality_field_type should be BIGINT/LONG
```

**Check 4**: Look for warnings in logs
```bash
# Check application logs for EDV fallback warnings
grep "EDV" /path/to/spark/logs/*.log
```

**Common reasons for fallback**:
- Multi-column equality delete
- STRING or other non-LONG field
- Negative values in delete set
- NULL values in delete set

---

### Issue 2: Larger File Sizes Than Expected

**Symptom**: EDV files are not much smaller than Parquet

**Likely Cause**: Delete pattern is random/sparse

**Check pattern**:
```sql
-- Check distribution of deleted values
SELECT
  MIN(id) as min_id,
  MAX(id) as max_id,
  MAX(id) - MIN(id) as range,
  COUNT(*) as delete_count,
  (MAX(id) - MIN(id)) / NULLIF(COUNT(*), 0) as avg_gap
FROM your_table.metadata.delete_files
CROSS JOIN LATERAL ...;
```

**If avg_gap > 1000**: Pattern is very sparse, compression will be limited (10-50x instead of 100x)

**Solution**:
- EDV still beneficial, just less dramatic
- Consider if sequential IDs can be used instead

---

### Issue 3: Reads Slower After Migration

**Symptom**: Table scans slower with EDV

**Unlikely**: EDV typically improves or matches performance

**Check 1**: Verify delete file count
```sql
SELECT COUNT(*) FROM your_table.metadata.delete_files;
```

**If >1000 delete files**: Compaction needed

**Solution**:
```sql
-- Compact delete files
CALL your_catalog.system.rewrite_delete_files(table => 'your_table');
```

**Check 2**: Check memory pressure
- EDV reduces memory usage, but if system is already memory-constrained, benefits may not be visible
- Monitor JVM heap usage during scans

---

### Issue 4: Errors Reading EDV Files

**Symptom**: Errors like "Failed to deserialize Roaring bitmap"

**Check**: Iceberg version compatibility

```bash
# Check Iceberg version
spark-sql> SELECT version();
```

**Required**: Iceberg v1.5.0+ (or your implementation version)

**Solution**:
- Upgrade to compatible Iceberg version
- Or disable EDV if upgrade not possible

---

## Best Practices

### 1. Start with One Table

**Don't migrate all tables at once**:
```sql
-- Start with your largest CDC table
ALTER TABLE orders_cdc SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);

-- Monitor for 1 week, then expand to other tables
```

---

### 2. Set Identifier Fields (Recommended)

**Mark your delete field as identifier**:
```sql
-- For CDC tables with _row_id
ALTER TABLE orders_cdc SET IDENTIFIER FIELDS _row_id;
```

**Benefits**:
- Clearer semantics (identifier = delete key)
- No warning logs
- Better documentation for users

---

### 3. Monitor File Sizes

**Create dashboard/alert**:
```sql
-- Daily check of delete file sizes
SELECT
  DATE(committed_at) as day,
  file_format,
  SUM(file_size_in_bytes) / 1024 / 1024 as total_mb
FROM your_table.metadata.delete_files
WHERE committed_at > current_timestamp - INTERVAL 7 DAY
GROUP BY day, file_format
ORDER BY day DESC;
```

**Expected trend**:
- PARQUET total_mb: Decreasing (being compacted)
- PUFFIN total_mb: Steady and low

---

### 4. Schedule Regular Compaction

**Why**: Merge small delete files, convert Parquet → EDV

**Frequency**: Daily or weekly depending on delete volume

**Example** (Airflow DAG):
```python
# Daily delete file compaction
@dag(schedule_interval="@daily")
def compact_delete_files():
    compact_task = SparkSQLOperator(
        task_id="compact_deletes",
        sql="""
        CALL your_catalog.system.rewrite_delete_files(
          table => 'your_table',
          options => map('min-input-files', '5')
        );
        """
    )
```

---

### 5. Document Your Configuration

**Create runbook for team**:
```yaml
Table: orders_cdc
EDV Enabled: true
Delete Field: _row_id (BIGINT, sequential)
Expected Compression: 100x
Compaction Schedule: Daily at 2 AM UTC
Monitoring: Grafana dashboard "EDV Metrics"
Rollback Procedure: See confluence page XYZ
```

---

## Common Migration Scenarios

### Scenario 1: Flink CDC Table

**Setup**:
```sql
-- CDC table with Debezium
CREATE TABLE orders_cdc (
  _row_id BIGINT,
  order_id BIGINT,
  customer_id BIGINT,
  order_date DATE,
  amount DECIMAL(10,2)
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);

-- Mark identifier (best practice)
ALTER TABLE orders_cdc SET IDENTIFIER FIELDS _row_id;
```

**CDC deletes automatically use EDV**:
- Flink CDC DELETE operations → EDV files
- Flink CDC UPDATE operations → EDV files (delete old + insert new)
- 100x compression for sequential `_row_id`

---

### Scenario 2: Spark Batch Deletes

**Before**:
```sql
-- Daily cleanup job
DELETE FROM events
WHERE event_id IN (
  SELECT event_id FROM expired_events
);
-- Creates large Parquet delete files (100 MB for 1M deletes)
```

**After migration**:
```sql
-- Enable EDV
ALTER TABLE events SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);

-- Same delete operation
DELETE FROM events
WHERE event_id IN (
  SELECT event_id FROM expired_events
);
-- Creates small Puffin files (1 MB for 1M deletes) ✅
```

---

### Scenario 3: Multi-Table Migration

**Strategy**: Prioritize by delete volume

```sql
-- 1. Check delete volume per table
SELECT
  table_name,
  COUNT(*) as delete_file_count,
  SUM(file_size_in_bytes) / 1024 / 1024 / 1024 as delete_size_gb
FROM (
  SELECT 'table1' as table_name, * FROM table1.metadata.delete_files
  UNION ALL
  SELECT 'table2' as table_name, * FROM table2.metadata.delete_files
  UNION ALL
  SELECT 'table3' as table_name, * FROM table3.metadata.delete_files
)
GROUP BY table_name
ORDER BY delete_size_gb DESC;
```

**Result**:
```
table_name  | delete_file_count | delete_size_gb
------------|-------------------|---------------
orders_cdc  | 500               | 50 GB          ← Migrate first
events      | 200               | 20 GB          ← Migrate second
users       | 10                | 0.5 GB         ← Migrate last
```

**Migration order**:
1. **Week 1**: orders_cdc (largest benefit)
2. **Week 2**: events (moderate benefit)
3. **Week 3**: users (small benefit)

---

## Success Metrics

After migration, you should see:

✅ **Storage**:
- Delete file sizes: 40-100x smaller (sequential) or 10-50x (sparse)
- Total storage: 20-50% reduction (depends on delete ratio)

✅ **Memory**:
- Scan memory: 50-100x reduction in delete set size
- Fewer OOM errors on large tables

✅ **Performance**:
- Scan time: Same or 5-10% faster
- Compaction time: 30-50% faster (fewer bytes to process)

✅ **Cost**:
- S3/GCS storage costs: Significant reduction
- Compute costs: Slight reduction from faster scans

---

## Conclusion

### Migration Checklist

- [ ] Verified format version 3+
- [ ] Identified LONG delete field with sequential pattern
- [ ] Enabled `write.delete.equality-vector.enabled`
- [ ] Tested with sample delete
- [ ] Monitored new delete files (PUFFIN format)
- [ ] Triggered compaction of old deletes
- [ ] Verified data correctness
- [ ] Verified file size reduction
- [ ] Documented configuration for team
- [ ] Set up monitoring/alerting

### Next Steps

1. **Week 1**: Monitor EDV files, verify compression
2. **Week 2**: Schedule regular compaction
3. **Week 3**: Expand to other tables
4. **Ongoing**: Monitor file sizes, adjust as needed

### Getting Help

- **Documentation**: See `docs/equality-delete-vectors.md`
- **Performance**: See `docs/equality-delete-vectors-performance.md`
- **Spec Details**: See `docs/equality-delete-vectors-spec-addition.md`
- **Issues**: Check GitHub issues or contact platform team

---

*Migration Guide Version: 1.0*
*Last Updated: 2026-01-22*
*Tested with: Apache Iceberg 1.5.0+*
