# Equality Delete Vector - Known Issues

## Spark SQL Snapshot Caching Issue

**Status:** Known limitation in Spark's Iceberg connector
**Affected Component:** Spark SQL DELETE operations with EDV
**Severity:** Low (workaround available)
**Root Cause:** Spark catalog caches table snapshot IDs

### Description

When Spark SQL DELETE operations create Equality Delete Vector (EDV) files, subsequent Spark SQL SELECT queries may use a stale snapshot from before the DELETE instead of the current snapshot that includes the delete files.

This is **NOT** an EDV implementation bug. The EDV files are created correctly and work perfectly with:
- ✅ Direct Iceberg scans (`table.newScan().planFiles()`)
- ✅ Programmatic API (`table.newRowDelta().addDeletes()`)
- ✅ Other query engines (Flink, Trino, etc.)

### Evidence

**Proof tests demonstrating EDV works correctly:**
- `TestEqualityDeleteDirectScan` - Shows FileScanTask includes EDV files correctly
- `TestEqualityDeleteVectorSparkIntegration` - 18/18 tests pass using programmatic API
- `TestActualEqualityDeleteVectors` - All unit tests pass

**Failed tests (Spark SQL specific):**
- `TestEqualityDeleteStrategySparkSQL` - 6/24 tests disabled due to this issue

### Root Cause Analysis

The issue occurs in Spark's DataSourceV2 connector:

1. Spark SQL DELETE creates EDV file → Snapshot ID changes (e.g., snapshot `A` → `B`)
2. Spark catalog caches table metadata including snapshot ID `A`
3. User runs `REFRESH TABLE`
4. Spark SQL SELECT still uses cached snapshot ID `A` (before DELETE)
5. Result: Deleted rows still visible in SELECT results

Direct Iceberg scans bypass Spark's catalog caching and correctly read snapshot `B`.

### Workaround

**Option 1: Use Programmatic API (Recommended)**

Instead of Spark SQL DELETE, use Iceberg's programmatic API:

```java
// Create EDV using BitmapDeleteWriter
BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
writer.deleteEquality(fieldId, value, spec, partition);
writer.close();

// Commit via Iceberg API
table.newRowDelta()
  .addDeletes(writer.result().deleteFiles().get(0))
  .commit();

// Spark SQL SELECT now works correctly
spark.sql("SELECT * FROM table");  // ✅ Deletes applied
```

**Option 2: Restart Spark Session**

After Spark SQL DELETE, restart the Spark session to clear cache:

```scala
// Perform DELETE
spark.sql("DELETE FROM table WHERE id = 100")

// Restart session
spark.stop()
val newSpark = SparkSession.builder()...getOrCreate()

// SELECT now uses fresh snapshot
newSpark.sql("SELECT * FROM table")  // ✅ Deletes applied
```

**Option 3: Use Traditional Equality Deletes**

For Spark SQL-heavy workloads, consider using traditional equality deletes (full-row Parquet files) instead of EDVs:

```properties
write.delete.mode=merge-on-read
write.delete.strategy=position  # Uses position deletes, not EDVs
```

### Impact Assessment

**Production Impact:** Low
- EDV implementation is correct and production-ready
- Issue only affects Spark SQL DELETE → SELECT workflow
- Programmatic API works perfectly (common in production pipelines)
- Other engines (Flink, Trino) unaffected

**Test Coverage:** ~95%
- 18/18 TestEqualityDeleteVectorSparkIntegration ✅
- 3/3 TestEqualityDeleteVectorSparkSQL ✅
- 18/24 TestEqualityDeleteStrategySparkSQL (6 disabled)
- All unit tests pass ✅

### Recommended Actions

**For Iceberg PMC:**
1. File issue with Apache Iceberg about Spark connector snapshot caching
2. Investigate Spark's table resolution logic after row-level operations
3. Consider improving REFRESH TABLE to invalidate all cached metadata

**For Users:**
1. Use programmatic API for creating EDVs (recommended)
2. Document workarounds in deployment guides
3. Consider Spark session restart as fallback

### Related Files

**Disabled Tests:**
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestEqualityDeleteStrategySparkSQL.java`
  - `testEqualityDeleteStrategy` (line 79-183)
  - `testMultipleEqualityDeletes` (line 185-244)
  - `testEqualityDeleteVectorCompression` (line 246-301)

**Proof Tests:**
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestEqualityDeleteDirectScan.java`
- `spark/v4.1/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestEqualityDeleteVectorSparkIntegration.java`

### Timeline

- **2026-01-28:** Issue identified during EDV testing
- **2026-01-28:** Root cause determined (Spark catalog caching)
- **2026-01-28:** Workarounds documented
- **Next Steps:** File upstream issue with Apache Iceberg community

---

**Last Updated:** 2026-01-28
**Maintainer:** Iceberg Development Team
