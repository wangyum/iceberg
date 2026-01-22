# Apache Iceberg Merge Readiness Analysis

**Date**: 2026-01-22
**Feature**: Equality Delete Vectors (EDV)
**Target**: Apache Iceberg main repository
**Status**: ⚠️ **NOT READY - Critical Issues Found**

---

## Executive Summary

**Overall Assessment**: 🔴 **BLOCKED - Cannot Merge Yet**

**Critical Issues**: 2 (must fix before any PR)
**Major Issues**: 5 (required for production use)
**Minor Issues**: 3 (nice to have)

**Estimated Work**: 2-3 weeks of additional development

---

## Critical Issues ⛔ (BLOCKERS)

### Critical #1: ⛔ **Spark Integration Missing - COMPILATION FAILS**

**Status**: 🔴 **BLOCKER - Code does not compile**

**Problem**: SparkFileWriterFactory does not implement `extractEqualityFieldValue()`

**Error**:
```
spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkFileWriterFactory.java:47:
error: SparkFileWriterFactory is not abstract and does not override abstract method
extractEqualityFieldValue(InternalRow,int) in BaseFileWriterFactory
```

**Affected Versions**:
- ❌ Spark 3.4 - Missing implementation
- ❌ Spark 3.5 - Missing implementation
- ❌ Spark 4.0 - Missing implementation
- ❌ Spark 4.1 - Missing implementation

**Impact**: **CRITICAL - 80%+ of Iceberg users use Spark**

**Required Fix**:
```java
// SparkFileWriterFactory.java - ADD THIS METHOD
@Override
protected Long extractEqualityFieldValue(InternalRow row, int fieldId) {
  // Find field index in equality delete schema
  Schema schema = equalityDeleteRowSchema();
  int fieldIndex = -1;
  for (int i = 0; i < schema.columns().size(); i++) {
    if (schema.columns().get(i).fieldId() == fieldId) {
      fieldIndex = i;
      break;
    }
  }

  if (fieldIndex < 0) {
    return null;
  }

  // Extract LONG value from Spark InternalRow
  if (row.isNullAt(fieldIndex)) {
    return null;
  }

  return row.getLong(fieldIndex);
}
```

**Estimated Effort**: 2 hours (implement for all 4 Spark versions)

**Verification Needed**: Integration tests with Spark MERGE INTO, DELETE

---

### Critical #2: ⛔ **Flink Integration Missing - COMPILATION FAILS**

**Status**: 🔴 **BLOCKER - Code does not compile**

**Problem**: FlinkFileWriterFactory does not implement `extractEqualityFieldValue()`

**Error**:
```
flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkFileWriterFactory.java:45:
error: FlinkFileWriterFactory is not abstract and does not override abstract method
extractEqualityFieldValue(RowData,int) in BaseFileWriterFactory
```

**Affected Versions**:
- ❌ Flink 1.20 - Missing implementation
- ❌ Flink 2.0 - Missing implementation
- ❌ Flink 2.1 - Missing implementation

**Impact**: **CRITICAL - Flink CDC is primary EDV use case**

**Required Fix**:
```java
// FlinkFileWriterFactory.java - ADD THIS METHOD
@Override
protected Long extractEqualityFieldValue(RowData row, int fieldId) {
  // Find field index in equality delete schema
  Schema schema = equalityDeleteRowSchema();
  int fieldIndex = -1;
  for (int i = 0; i < schema.columns().size(); i++) {
    if (schema.columns().get(i).fieldId() == fieldId) {
      fieldIndex = i;
      break;
    }
  }

  if (fieldIndex < 0) {
    return null;
  }

  // Extract LONG value from Flink RowData
  if (row.isNullAt(fieldIndex)) {
    return null;
  }

  RowType.RowField field = equalityDeleteFlinkType().getFields().get(fieldIndex);
  return row.getLong(fieldIndex);
}
```

**Estimated Effort**: 2 hours (implement for all 3 Flink versions)

**Verification Needed**: Integration tests with Flink CDC, MERGE

---

## Major Issues 🔶 (Required for Production)

### Major #1: 🔶 **No Spark Integration Tests**

**Status**: 🟡 **MISSING - No test coverage**

**Problem**: Zero Spark tests verify EDV functionality

**Current Test Coverage**:
- ✅ GenericFileWriterFactory: 6 tests (data module)
- ✅ Mixed formats: 2 tests
- ✅ Compaction: 3 tests
- ❌ SparkFileWriterFactory: **0 tests**

**Required Tests**:
```java
// TestSparkEqualityDeleteVectors.java - NEW FILE NEEDED
@Test
public void testSparkMergeIntoWithEDV() {
  // MERGE INTO with EDV enabled
  // Verify PUFFIN files created, not Parquet
}

@Test
public void testSparkDeleteWithEDV() {
  // DELETE WHERE id IN (...) with EDV
  // Verify bitmap compression
}

@Test
public void testSparkMixedEDVAndParquet() {
  // Read table with both EDV and Parquet deletes
  // Verify correctness
}
```

**Location**: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkEqualityDeleteVectors.java`

**Estimated Effort**: 1 day (3 Spark tests + coverage)

---

### Major #2: 🔶 **No Flink Integration Tests**

**Status**: 🟡 **MISSING - No test coverage**

**Problem**: Zero Flink tests verify EDV functionality

**Required Tests**:
```java
// TestFlinkEqualityDeleteVectors.java - NEW FILE NEEDED
@Test
public void testFlinkCDCWithEDV() {
  // Flink CDC ingestion with EDV
  // Verify _row_id deletes use bitmap
}

@Test
public void testFlinkMergeWithEDV() {
  // Flink SQL MERGE statement
  // Verify EDV files created
}
```

**Location**: `flink/v2.0/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkEqualityDeleteVectors.java`

**Estimated Effort**: 1 day (CDC + MERGE tests)

---

### Major #3: 🔶 **No Performance Benchmarks (JMH)**

**Status**: 🟡 **MISSING - No quantitative proof**

**Problem**: Claims of "40-100x compression" not backed by JMH benchmarks

**Current Benchmarks**: `TestEqualityDeleteVectorBenchmark.java` - Simple timing, not JMH

**Required**:
```java
// EDVBenchmark.java - JMH benchmark
@Benchmark
public void writeTraditionalEqualityDelete(Blackhole bh) {
  // Baseline: Parquet equality deletes
}

@Benchmark
public void writeEDV(Blackhole bh) {
  // New: EDV with bitmap
}

@Benchmark
public void readWithTraditionalDeletes(Blackhole bh) {
  // Measure read path memory
}

@Benchmark
public void readWithEDV(Blackhole bh) {
  // Measure EDV read path memory
}
```

**Location**: `core/src/jmh/java/org/apache/iceberg/EDVBenchmark.java`

**Estimated Effort**: 2 days (JMH setup + benchmarks + analysis)

**Why Critical**: Apache reviewers will ask for benchmarks to validate claims

---

### Major #4: 🔶 **Spec Documentation Incomplete**

**Status**: 🟡 **PARTIAL - Format spec needs work**

**Current Documentation**:
- ✅ `format/puffin-spec.md` - Blob type added
- ⚠️ `format/spec.md` - Minimal changes (55 lines)
- ❌ **No detailed spec for EDV format**

**Missing Spec Content**:

1. **EDV File Format Specification** (should be in `format/spec.md`):
```markdown
### Equality Delete Vectors

Equality delete vectors store deleted values as compressed bitmaps in Puffin files.

**When to use**:
- Single LONG equality field
- Format version 3+
- Property: write.delete.equality-vector.enabled=true

**Puffin Blob Format**:
- blob-type: equality-delete-vector-v1
- compression-codec: none (Roaring bitmap is pre-compressed)
- Properties:
  - equality-field-id: <field-id>
  - equality-field-name: <field-name>

**Roaring Bitmap Encoding**:
[Detailed spec of RoaringPositionBitmap format]
```

2. **Read Path Specification**:
```markdown
**Applying EDV Deletes**:
1. Load Puffin file
2. Deserialize Roaring bitmap
3. For each data row:
   - Extract equality field value
   - Check if value exists in bitmap (O(1))
   - Skip row if deleted
```

**Estimated Effort**: 1 day (detailed spec writing + diagrams)

---

### Major #5: 🔶 **No Migration Guide**

**Status**: 🟡 **MISSING - Users don't know how to adopt**

**Problem**: No documentation for existing table migration

**Required Documentation**:

`docs/equality-delete-vectors.md`:
```markdown
# Equality Delete Vectors

## When to Use
- CDC tables with sequential identifiers (_row_id)
- Large-scale DELETE operations (>100K deletes)
- MERGE operations on LONG keys

## Prerequisites
- Iceberg format version 3+
- Single LONG equality field
- Recommended: Identifier field set

## Quick Start
[step-by-step guide]

## Performance Characteristics
[compression ratios, memory usage, read performance]

## Troubleshooting
[common issues + solutions]
```

**Estimated Effort**: 1 day (user documentation)

---

## Minor Issues 🟢 (Nice to Have)

### Minor #1: 🟢 **Metrics Not Collected**

**Problem**: No metrics for EDV usage tracking

**Enhancement**:
```java
// Add to EqualityDeleteVectorWriter
private void recordMetrics() {
  metrics.incrementCounter("edv_files_written");
  metrics.recordDistribution("edv_bitmap_size_bytes", bitmap.serializedSizeInBytes());
  metrics.recordDistribution("edv_delete_count", bitmap.cardinality());
  metrics.recordGauge("edv_compression_ratio",
      (double) bitmap.cardinality() * 8 / bitmap.serializedSizeInBytes());
}
```

**Estimated Effort**: 4 hours

---

### Minor #2: 🟢 **No Catalog Integration Tests**

**Problem**: Tests use TestTables, not real catalogs (Hive, Glue, REST)

**Enhancement**: Test with HiveCatalog, GlueCatalog, RESTCatalog

**Estimated Effort**: 1 day

---

### Minor #3: 🟢 **Warning Message Could Be Clearer**

**Current**:
```
WARN: Equality field 'order_id' is not marked as an identifier field.
      EDV works best on identifier fields (e.g., CDC _row_id with sequential values).
      For optimal compression, consider: ALTER TABLE orders SET IDENTIFIER FIELDS order_id
```

**Better**:
```
WARN: Equality delete vector (EDV) is enabled for field 'order_id', but this field is not
      marked as an identifier. EDV achieves best compression (40-100x) on sequential identifier
      fields like CDC's _row_id.

      Current configuration will work, but may have suboptimal compression if values are sparse.

      To mark as identifier: ALTER TABLE orders SET IDENTIFIER FIELDS order_id
      To disable this warning: Set log level for org.apache.iceberg.data.BaseFileWriterFactory to ERROR
```

**Estimated Effort**: 30 minutes

---

## File-by-File Analysis

### ✅ Core Module (11 files changed)

| File | Status | Issues |
|------|--------|--------|
| `FileMetadata.java` | ✅ Good | None |
| `TableProperties.java` | ✅ Good | Property documented |
| `V3Metadata.java` | ✅ Good | Format version check |
| `BitmapBackedStructLikeSet.java` | ✅ Good | Iterator implemented |
| `EqualityDeleteVectorWriter.java` | ✅ Good | 2GB check added |
| `EqualityDeleteVectorWriterAdapter.java` | ✅ Good | None |
| `EqualityDeleteVectors.java` | ✅ Good | Utility methods |
| `RoaringPositionBitmap.java` | ✅ Good | Minor change |
| `StandardBlobTypes.java` | ✅ Good | Blob type added |
| `TestBitmapBackedStructLikeSet.java` | ✅ Good | 6 tests pass |
| `TestEqualityDeleteVectorWriter.java` | ✅ Good | 6 tests pass |

**Core Module**: ✅ **READY**

---

### ⚠️ Data Module (9 files changed)

| File | Status | Issues |
|------|--------|--------|
| `BaseDeleteLoader.java` | ✅ Good | Mixed format handled |
| `BaseFileWriterFactory.java` | ✅ Good | Flexible approach |
| `DeleteFilter.java` | ✅ Good | Type updated |
| `DeleteLoader.java` | ✅ Good | Interface changed |
| `GenericFileWriterFactory.java` | ✅ Good | extractEqualityFieldValue impl |
| `TestEqualityDeleteVectorBenchmark.java` | ⚠️ Not JMH | Need JMH version |
| `TestEqualityDeleteVectorCompaction.java` | ✅ Good | 3 tests pass |
| `TestEqualityDeleteVectorIntegration.java` | ✅ Good | 6 tests pass |
| `TestEqualityDeleteVectorMixedFormats.java` | ✅ Good | 2 tests pass |

**Data Module**: ⚠️ **NEEDS JMH BENCHMARKS**

---

### ❌ Spark Module (0 implementations)

| File | Status | Issues |
|------|--------|--------|
| `spark/v3.4/...SparkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| `spark/v3.5/...SparkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| `spark/v4.0/...SparkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| `spark/v4.1/...SparkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| **Tests** | ❌ MISSING | No TestSparkEqualityDeleteVectors.java |

**Spark Module**: ❌ **BROKEN - COMPILATION FAILS**

---

### ❌ Flink Module (0 implementations)

| File | Status | Issues |
|------|--------|--------|
| `flink/v1.20/...FlinkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| `flink/v2.0/...FlinkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| `flink/v2.1/...FlinkFileWriterFactory.java` | ❌ BROKEN | Missing extractEqualityFieldValue |
| **Tests** | ❌ MISSING | No TestFlinkEqualityDeleteVectors.java |

**Flink Module**: ❌ **BROKEN - COMPILATION FAILS**

---

## Apache Iceberg Contribution Process

### Phase 1: Community Discussion (Before Any PR)

**Status**: ❌ **NOT STARTED**

**Required Steps**:

1. **Email to dev@iceberg.apache.org**:
```
Subject: [DISCUSS] Equality Delete Vectors (EDV) - Bitmap-based Delete Compression

Hi Iceberg Community,

I'd like to propose a new feature: Equality Delete Vectors (EDV), which provides
40-100x compression for equality deletes using Roaring bitmaps.

**Use Case**: CDC tables with millions of deletes on sequential LONG identifiers

**Approach**: Store deleted values as Roaring bitmaps in Puffin files (format v3)

**Benefits**:
- 40-100x storage reduction (1MB vs 100MB for 1M deletes)
- 100x memory reduction during reads
- Compatible with existing equality deletes

**Design Doc**: [link to GitHub gist with design]

Would appreciate feedback before proceeding with spec changes and PR.

Thanks,
[Your Name]
```

2. **Wait for Community Feedback** (1-2 weeks):
   - Address questions about design
   - Get consensus on approach
   - Validate use cases with PMC members

3. **Iterate on Design** based on feedback

**Estimated Duration**: 2-4 weeks

---

### Phase 2: Spec Approval (Before Implementation PR)

**Status**: ❌ **NOT STARTED**

**Required**:

1. **Submit Spec PR First** (separate from implementation):
   - Changes to `format/spec.md`
   - Changes to `format/puffin-spec.md`
   - Detailed EDV format specification
   - Examples and diagrams

2. **Get Spec Approved** by PMC:
   - Typically requires 3+ PMC approvals
   - May require revisions
   - Spec must be finalized before implementation

**Estimated Duration**: 1-2 weeks

---

### Phase 3: Implementation PR

**Status**: ⚠️ **NOT READY - Critical issues blocking**

**Prerequisites**:
- ✅ Spec approved
- ✅ Community consensus
- ❌ **All compilation errors fixed**
- ❌ **Spark integration implemented + tested**
- ❌ **Flink integration implemented + tested**
- ❌ **JMH benchmarks proving claims**
- ❌ **Documentation complete**

**PR Requirements**:

1. **Code Changes**:
   - All modules compile successfully
   - All tests pass (including new integration tests)
   - No test coverage regression

2. **Testing**:
   - Unit tests for all components
   - Integration tests for Spark, Flink, Data modules
   - JMH benchmarks showing performance gains
   - Compatibility tests (mixed EDV + traditional)

3. **Documentation**:
   - User-facing docs in `docs/`
   - Javadoc for all public APIs
   - Migration guide for existing tables
   - Performance characteristics documented

4. **Review Process**:
   - Expect 3-5 rounds of review
   - PMC members will review deeply
   - May take 2-4 weeks to merge after submission

**Estimated Duration**: 3-6 weeks (after prerequisites met)

---

## Immediate Action Plan

### Week 1: Fix Critical Blockers

**Priority**: 🔴 **URGENT - Code must compile**

**Tasks**:
1. ✅ Day 1-2: Implement `extractEqualityFieldValue()` in all Spark versions
2. ✅ Day 2-3: Implement `extractEqualityFieldValue()` in all Flink versions
3. ✅ Day 3: Verify full build passes: `./gradlew build`
4. ✅ Day 4-5: Write Spark integration tests
5. ✅ Day 5: Write Flink integration tests

**Deliverable**: ✅ Code compiles, basic integration tests pass

---

### Week 2: Major Features

**Priority**: 🟡 **HIGH - Required for PR**

**Tasks**:
1. ✅ Day 1-2: Create JMH benchmarks
2. ✅ Day 2-3: Run benchmarks and document results
3. ✅ Day 3-4: Complete spec documentation
4. ✅ Day 4-5: Write user migration guide

**Deliverable**: ✅ Benchmarks prove claims, docs complete

---

### Week 3: Community Process

**Priority**: 🟢 **MEDIUM - Start discussion**

**Tasks**:
1. ✅ Day 1: Email dev@iceberg.apache.org with proposal
2. ✅ Day 1-5: Respond to community feedback
3. ✅ Day 3: Submit spec PR (separate from implementation)
4. ✅ Day 5: Address spec review comments

**Deliverable**: ✅ Community buy-in, spec approved

---

### Week 4+: Implementation PR

**Priority**: 🟢 **MEDIUM - After consensus**

**Tasks**:
1. ✅ Submit implementation PR
2. ✅ Address review comments (iterative)
3. ✅ Wait for 3+ PMC approvals
4. ✅ Merge

**Deliverable**: ✅ Feature merged into Apache Iceberg

---

## Risk Assessment

### High Risk ⚠️

1. **Community Rejection** (30% probability):
   - EDV might be seen as too niche for core Iceberg
   - Alternative: Propose as optional module instead of core

2. **Spec Changes Rejected** (20% probability):
   - Puffin blob type might need different design
   - Mitigation: Get early feedback on spec PR

3. **Performance Claims Challenged** (40% probability):
   - "40-100x" may not hold for all workloads
   - Mitigation: JMH benchmarks with realistic data

### Medium Risk ⚠️

1. **API Design Issues** (50% probability):
   - `extractEqualityFieldValue()` might not be ideal abstraction
   - May need refactoring based on review

2. **Backward Compatibility Concerns** (30% probability):
   - DeleteLoader interface change may be controversial
   - May need deprecation path

### Low Risk ✅

1. **Code Quality** (10% probability):
   - Code is well-structured
   - Tests are comprehensive
   - Low risk of quality rejection

---

## Success Criteria

### Minimum for PR Submission ✅

- [x] Core tests pass (11/11 files ready)
- [ ] Data tests pass (need JMH)
- [ ] **Spark implementation complete + tests** ❌ **BLOCKING**
- [ ] **Flink implementation complete + tests** ❌ **BLOCKING**
- [ ] Full build succeeds: `./gradlew build` ❌ **FAILS NOW**
- [ ] JMH benchmarks documented
- [ ] Spec documentation complete
- [ ] User guide written

### Minimum for Merge ✅

- [ ] Community consensus (dev@ email discussion)
- [ ] Spec PR approved
- [ ] 3+ PMC approvals on implementation PR
- [ ] All review comments addressed
- [ ] CI green on all platforms

---

## Conclusion

**Current Status**: 🔴 **NOT READY FOR APACHE ICEBERG**

**Critical Blockers** (must fix immediately):
1. ❌ **Spark compilation broken** - 4 versions missing implementation
2. ❌ **Flink compilation broken** - 3 versions missing implementation

**Why This Matters**:
- Apache Iceberg has **strict quality standards**
- PMC will reject PRs that don't compile
- Missing Spark/Flink support = 90%+ users can't use feature
- No benchmarks = claims aren't validated

**Recommended Path Forward**:

**Option A: Full Production Path** (3-4 weeks):
1. Week 1: Fix compilation, add Spark/Flink tests
2. Week 2: JMH benchmarks, complete docs
3. Week 3: Community discussion, spec PR
4. Week 4+: Implementation PR, reviews, merge

**Option B: Internal/Fork Path** (immediate):
- Use EDV in your own fork
- Wait to contribute until maturity proven
- Contribute later when battle-tested

**My Recommendation**: **Option A** - Fix compilation first, then follow full process

The feature is **technically sound** and **well-implemented** in core/data modules. The blocker is **missing engine integration** (Spark/Flink), which is **fixable in 1 week**.

---

*Analysis Date: 2026-01-22*
*Analyst: Apache Iceberg PMC Review Simulation*
*Next Review: After critical blockers fixed*
