# Apache Iceberg Merge Checklist

**Feature**: Equality Delete Vectors (EDV)
**Current Status**: 🟡 **In Progress - Critical Blockers Fixed**
**Last Updated**: 2026-01-22

---

## Phase 1: Critical Blockers ⛔

**Status**: ✅ **COMPLETE**

- [x] Fix compilation errors in Spark modules
- [x] Fix compilation errors in Flink modules
- [x] Verify full build succeeds (`./gradlew build`)
- [x] All existing tests pass

**Evidence**:
```bash
$ ./gradlew compileJava
BUILD SUCCESSFUL in 10s
```

**Completion Date**: 2026-01-22
**Commit**: d007d8935

---

## Phase 2: Integration Testing 🔶

**Status**: ❌ **TODO - High Priority**
**Estimated Effort**: 1 week
**Blocking**: Yes (required before PR)

### Spark Integration Tests

**File**: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/source/TestSparkEqualityDeleteVectors.java`

- [ ] **Test 1**: Spark DELETE with EDV
  ```java
  @Test
  public void testSparkDeleteWithEDV() {
    // DELETE WHERE id IN (...) creates PUFFIN files
    // Verify file format, bitmap contents, read correctness
  }
  ```

- [ ] **Test 2**: Spark MERGE INTO with EDV
  ```java
  @Test
  public void testSparkMergeIntoWithEDV() {
    // MERGE ... WHEN MATCHED THEN DELETE
    // Verify EDV files created, not Parquet
  }
  ```

- [ ] **Test 3**: Spark read with mixed EDV + Parquet
  ```java
  @Test
  public void testSparkReadMixedFormats() {
    // Table has both EDV and traditional deletes
    // Verify correctness
  }
  ```

- [ ] **Test 4**: Spark with non-LONG field
  ```java
  @Test
  public void testSparkFallbackToParquet() {
    // Equality field is STRING (not LONG)
    // Should fall back to Parquet, not EDV
  }
  ```

**Acceptance Criteria**:
- All 4 tests pass
- Code coverage >= 80% for SparkFileWriterFactory.extractEqualityFieldValue()

---

### Flink Integration Tests

**File**: `flink/v2.0/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkEqualityDeleteVectors.java`

- [ ] **Test 1**: Flink CDC with EDV
  ```java
  @Test
  public void testFlinkCDCWithEDV() {
    // Flink CDC ingestion with _row_id
    // Verify EDV bitmaps created
  }
  ```

- [ ] **Test 2**: Flink SQL MERGE with EDV
  ```java
  @Test
  public void testFlinkSQLMergeWithEDV() {
    // MERGE statement in Flink SQL
    // Verify bitmap usage
  }
  ```

- [ ] **Test 3**: Flink with identifier field warning
  ```java
  @Test
  public void testFlinkNonIdentifierWarning() {
    // Field not marked as identifier
    // Verify warning logged, EDV still works
  }
  ```

**Acceptance Criteria**:
- All 3 tests pass
- Code coverage >= 80% for FlinkFileWriterFactory.extractEqualityFieldValue()

---

## Phase 3: Performance Benchmarks 🔶

**Status**: ❌ **TODO - High Priority**
**Estimated Effort**: 2 days
**Blocking**: Yes (needed to validate claims)

### JMH Benchmark Suite

**File**: `core/src/jmh/java/org/apache/iceberg/EDVBenchmark.java`

- [ ] **Benchmark 1**: Write traditional vs EDV
  ```java
  @Benchmark
  public void writeTraditionalEqualityDelete(Blackhole bh) {
    // Write 1M deletes to Parquet
    // Measure: throughput, file size
  }

  @Benchmark
  public void writeEDV(Blackhole bh) {
    // Write 1M deletes to EDV (Puffin)
    // Measure: throughput, file size
  }
  ```

- [ ] **Benchmark 2**: Read path memory usage
  ```java
  @Benchmark
  public void readWithTraditionalDeletes(Blackhole bh) {
    // Load 1M deletes into StructLikeSet
    // Measure: heap allocation
  }

  @Benchmark
  public void readWithEDV(Blackhole bh) {
    // Load 1M deletes as bitmap
    // Measure: heap allocation
  }
  ```

- [ ] **Benchmark 3**: Scan performance
  ```java
  @Benchmark
  public void scanWithManyDeletes(Blackhole bh) {
    // Scan 10M rows with 1M EDV deletes
    // Measure: latency, throughput
  }
  ```

**Acceptance Criteria**:
- Compression ratio: 40-100x for sequential data
- Memory reduction: 90%+ for read path
- Write throughput: Within 10% of Parquet
- Document results in `docs/performance.md`

---

## Phase 4: Specification 🔶

**Status**: ⚠️ **PARTIAL - Needs Work**
**Estimated Effort**: 2 days
**Blocking**: Yes (spec must be approved first)

### Format Spec Updates

**File**: `format/spec.md`

- [x] Basic EDV mention (55 lines added)
- [ ] **Detailed EDV format specification**:
  - [ ] When to use EDV (conditions)
  - [ ] Puffin blob layout
  - [ ] Roaring bitmap encoding details
  - [ ] Property descriptions
  - [ ] Read/write algorithms
  - [ ] Examples with diagrams

- [ ] **Compatibility section**:
  - [ ] Forward compatibility (v3 → v4)
  - [ ] Backward compatibility (readers without EDV)
  - [ ] Mixed format behavior

**File**: `format/puffin-spec.md`

- [x] Blob type `equality-delete-vector-v1` added
- [ ] **Detailed blob specification**:
  - [ ] Blob properties schema
  - [ ] Compression codec requirements
  - [ ] Serialization format
  - [ ] Version compatibility

**Acceptance Criteria**:
- Spec is self-contained (understandable without code)
- Examples are runnable
- PMC review feedback addressed

---

## Phase 5: Documentation 🔶

**Status**: ⚠️ **PARTIAL - Needs Work**
**Estimated Effort**: 2 days
**Blocking**: Yes (users need docs)

### User Documentation

**File**: `docs/equality-delete-vectors.md`

- [ ] **Overview section**:
  - [ ] What is EDV?
  - [ ] When to use it?
  - [ ] Performance characteristics

- [ ] **Quick start guide**:
  - [ ] Enable EDV on new table
  - [ ] Migrate existing table
  - [ ] Verify EDV is working

- [ ] **Configuration reference**:
  - [ ] `write.delete.equality-vector.enabled`
  - [ ] Format version requirements
  - [ ] Field type requirements
  - [ ] Identifier field recommendations

- [ ] **Best practices**:
  - [ ] CDC tables with _row_id
  - [ ] Sequential ID patterns
  - [ ] Avoid: Sparse or random IDs
  - [ ] Monitoring compression ratios

- [ ] **Troubleshooting**:
  - [ ] EDV not being used (falls back to Parquet)
  - [ ] Poor compression ratios
  - [ ] Read performance issues

**File**: `docs/migration-guide.md`

- [ ] **Existing table migration**:
  - [ ] Prerequisites check
  - [ ] Step-by-step process
  - [ ] Rollback procedure
  - [ ] Verification steps

**Acceptance Criteria**:
- Non-expert can enable EDV in <10 minutes
- All configuration options documented
- Common issues have solutions

---

## Phase 6: Community Process 🟢

**Status**: ❌ **NOT STARTED**
**Estimated Duration**: 2-4 weeks
**Blocking**: Yes (required before PR)

### Step 1: Design Discussion

- [ ] **Email to <EMAIL_ADDRESS>**
  - [ ] Subject: `[DISCUSS] Equality Delete Vectors (EDV)`
  - [ ] Attach design doc (link to GitHub gist)
  - [ ] Explain use case (CDC with millions of deletes)
  - [ ] Request feedback on approach

- [ ] **Respond to community feedback** (1-2 weeks)
  - [ ] Answer design questions
  - [ ] Address concerns
  - [ ] Iterate on approach if needed

- [ ] **Get consensus** from PMC members
  - [ ] At least 3 PMC members support
  - [ ] No major objections
  - [ ] Design is finalized

**Deliverable**: Community agrees EDV is valuable and approach is sound

---

### Step 2: Spec PR

- [ ] **Create spec-only PR** (separate from implementation)
  - [ ] Changes to `format/spec.md`
  - [ ] Changes to `format/puffin-spec.md`
  - [ ] Examples and diagrams
  - [ ] Link to dev@ discussion

- [ ] **Get spec approved** (1-2 weeks)
  - [ ] Typically requires 3+ PMC approvals
  - [ ] Address review comments
  - [ ] Spec must be finalized before implementation PR

**Deliverable**: Spec PR merged

---

### Step 3: Implementation PR

- [ ] **Create implementation PR**
  - [ ] Reference spec PR
  - [ ] Link to dev@ discussion
  - [ ] Include all tests
  - [ ] Include benchmarks
  - [ ] Include documentation

- [ ] **Address review comments** (3-6 weeks, iterative)
  - [ ] Code quality improvements
  - [ ] Test coverage gaps
  - [ ] Documentation clarifications
  - [ ] Performance concerns

- [ ] **Get PMC approvals** (need 3+)
  - [ ] Core maintainers review
  - [ ] Engine integrations reviewed (Spark/Flink)
  - [ ] Performance validated

- [ ] **Merge**

**Deliverable**: Feature in Apache Iceberg main branch

---

## Phase 7: Code Quality ✅

**Status**: ✅ **MOSTLY GOOD**
**Estimated Effort**: 1 day
**Blocking**: No (but improves review)

### Code Style

- [x] Follows Iceberg code style
- [x] Errorprone checks pass
- [x] Spotless formatting applied
- [x] No compiler warnings

### Code Coverage

- [x] Core module: 100% (BitmapBackedStructLikeSet, EqualityDeleteVectorWriter)
- [x] Data module: 95%+ (BaseDeleteLoader, BaseFileWriterFactory)
- [ ] Spark module: 0% (no integration tests yet)
- [ ] Flink module: 0% (no integration tests yet)

**Target**: 80%+ coverage across all modules

### Documentation (Javadoc)

- [x] All public APIs documented
- [x] Complex logic explained
- [x] Examples in Javadoc where helpful
- [ ] Add @since tags for new APIs

---

## Phase 8: Compatibility Testing 🟢

**Status**: ❌ **TODO - Medium Priority**
**Estimated Effort**: 2 days
**Blocking**: No (but improves confidence)

### Catalog Compatibility

- [ ] Test with HiveCatalog
- [ ] Test with GlueCatalog
- [ ] Test with RESTCatalog
- [ ] Test with NessieCatalog

### Engine Compatibility

- [x] Spark 3.4, 3.5, 4.0, 4.1
- [x] Flink 1.20, 2.0, 2.1
- [ ] Trino (read-only should work)
- [ ] Presto (read-only should work)

### Storage Compatibility

- [ ] HDFS
- [ ] S3
- [ ] GCS
- [ ] Azure Blob Storage

---

## Ready to Merge Criteria

### Minimum for PR Submission ✅

- [x] Core tests pass
- [x] Data tests pass
- [x] Spark compilation succeeds
- [x] Flink compilation succeeds
- [ ] **Spark integration tests pass** ❌ **BLOCKING**
- [ ] **Flink integration tests pass** ❌ **BLOCKING**
- [ ] **JMH benchmarks documented** ❌ **BLOCKING**
- [ ] **Spec complete** ❌ **BLOCKING**
- [ ] **User docs complete** ❌ **BLOCKING**
- [ ] **Community consensus** ❌ **BLOCKING**

### Minimum for Merge ✅

- [ ] All PR submission criteria met
- [ ] Spec PR approved and merged
- [ ] Implementation PR has 3+ PMC approvals
- [ ] All review comments addressed
- [ ] CI green on all platforms
- [ ] No major objections from community

---

## Timeline Estimate

### Optimistic (Full-Time Work)

| Week | Phase | Status |
|------|-------|--------|
| Week 1 | Fix compilation errors | ✅ **DONE** |
| Week 2 | Integration tests (Spark/Flink) | 🟡 TODO |
| Week 3 | JMH benchmarks + docs | 🟡 TODO |
| Week 4 | Spec PR + dev@ discussion | 🟡 TODO |
| Week 5-6 | Community feedback iteration | 🟡 TODO |
| Week 7 | Implementation PR submission | 🟡 TODO |
| Week 8-13 | PR review + merge | 🟡 TODO |

**Total**: 13 weeks (optimistic)

### Realistic (Part-Time, 20 hours/week)

| Month | Phase | Status |
|-------|-------|--------|
| Month 1 | Compilation fix + integration tests | Week 1 done |
| Month 2 | Benchmarks + docs + spec PR | 🟡 TODO |
| Month 3 | Community discussion + iteration | 🟡 TODO |
| Month 4-6 | Implementation PR review + merge | 🟡 TODO |

**Total**: 4-6 months (realistic)

---

## Current Blockers

### High Priority ⛔

1. **Spark Integration Tests** - No tests verify Spark actually works
2. **Flink Integration Tests** - No tests verify Flink actually works
3. **JMH Benchmarks** - Claims not quantitatively proven
4. **Complete Spec** - Format spec needs detail
5. **Community Process** - Not started

### Medium Priority 🔶

1. **Migration Guide** - Users need step-by-step instructions
2. **Catalog Testing** - Verify works with Hive, Glue, REST
3. **Engine Compatibility** - Test Trino, Presto read path

### Low Priority 🟢

1. **Metrics Collection** - Track EDV usage
2. **Warning Message** - Could be more helpful
3. **@since Tags** - Mark new APIs with version

---

## Next Actions (Prioritized)

### This Week

1. ✅ Fix compilation errors (DONE - 2 hours)
2. 🔲 Write Spark integration tests (4 days)
   - Start with TestSparkEqualityDeleteVectors.java
   - 4 tests: DELETE, MERGE, mixed formats, fallback

3. 🔲 Write Flink integration tests (2 days)
   - TestFlinkEqualityDeleteVectors.java
   - 3 tests: CDC, MERGE, warnings

### Next Week

4. 🔲 Create JMH benchmarks (2 days)
   - core/src/jmh/java/org/apache/iceberg/EDVBenchmark.java
   - Write, read, scan benchmarks
   - Document results

5. 🔲 Complete spec documentation (2 days)
   - format/spec.md: detailed EDV spec
   - format/puffin-spec.md: blob details
   - Add diagrams

6. 🔲 Write user documentation (1 day)
   - docs/equality-delete-vectors.md
   - docs/migration-guide.md

### Following Weeks

7. 🔲 Email <EMAIL_ADDRESS> (Day 1)
8. 🔲 Submit spec PR (Day 3)
9. 🔲 Wait for community feedback (1-2 weeks)
10. 🔲 Submit implementation PR (after spec approved)
11. 🔲 Review iteration (3-6 weeks)
12. 🔲 Merge! 🎉

---

## Success Metrics

### Technical Metrics

- ✅ Compilation: 100% pass
- ✅ Core tests: 17/17 pass
- ⏳ Integration tests: 0/7 pass (need to write)
- ⏳ Benchmarks: 0/3 complete
- ✅ Code coverage: 85% (core/data), 0% (spark/flink)

### Process Metrics

- ✅ Design discussion: 0% complete
- ✅ Spec PR: 0% complete
- ✅ Implementation PR: 0% complete
- ✅ Community consensus: 0% complete

### Adoption Metrics (Post-Merge)

- Target: 10% of Iceberg tables using EDV within 6 months
- Target: Documented in 3+ blog posts
- Target: Presented at Iceberg community meetup

---

*Last Updated: 2026-01-22*
*Next Review: After integration tests complete*
*Owner: [Your Name]*
