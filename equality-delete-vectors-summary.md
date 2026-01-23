# Equality Delete Vectors (EDV) - Implementation Summary

## Executive Summary

Equality Delete Vectors (EDV) provide **40-100x storage reduction** for equality deletes on LONG primary key columns by storing deleted values as Roaring bitmaps instead of full rows.

**Status**: Design complete, ready for implementation
**Target**: Iceberg format v3+ tables with LONG primary keys

---

## Key Constraints (Simplified MVP)

To ensure correctness and avoid edge cases, EDV supports **only**:

| Constraint | Requirement | Rationale |
|-----------|-------------|-----------|
| **Type** | `LONG` only | Avoids INT/LONG mixing, covers most PKs |
| **Values** | Non-negative (`>= 0`) | Leverages existing bitmap validation |
| **Nullability** | Non-null only | Bitmaps cannot represent NULL |
| **Cardinality** | Single field | No composite keys (future enhancement) |

**Automatic fallback**: Violating any constraint uses traditional equality deletes

---

## Benefits

### Storage Savings

| Scenario | Traditional (Parquet) | EDV (Bitmap) | Improvement |
|----------|----------------------|--------------|-------------|
| 1M deletes, wide schema (20 cols) | ~60-100 MB | ~1 MB | **60-100x** |
| 1M deletes, narrow schema (3 cols) | ~40 MB | ~1 MB | **40x** |
| 100K deletes, typical schema | ~5-10 MB | ~100 KB | **50-100x** |

### Memory Savings

Same ratio as storage - loading deletes into memory requires **40-100x less RAM**.

### Performance Benefits

- **Faster writes**: No Parquet encoding overhead
- **Faster reads**: Direct bitmap deserialization vs Parquet scanning
- **Better caching**: More delete files fit in memory cache

---

## Target Use Cases

### ✅ Perfect Fit

```sql
-- Auto-increment primary keys
CREATE TABLE orders (
  order_id BIGINT,      -- Sequential, always >= 0
  customer_id BIGINT,
  amount DECIMAL(10,2)
);

-- Bulk deletes by PK
DELETE FROM orders WHERE order_id IN (
  SELECT order_id FROM cancelled_orders
);
```

**Common patterns**:
- User management: `user_id BIGINT`
- Order processing: `order_id BIGINT`
- Event tracking: `event_id BIGINT`
- Transaction logs: `transaction_id BIGINT`

### ❌ Not Suitable

```sql
-- Falls back to traditional format
DELETE FROM accounts WHERE account_id = -100;        -- Negative
DELETE FROM users WHERE user_id IS NULL;             -- NULL
DELETE FROM events WHERE event_id = 5 AND type = 'x'; -- Composite
DELETE FROM products WHERE sku = 'ABC-123';          -- String type
DELETE FROM legacy WHERE id = 5;                     -- INT type (not LONG)
```

---

## Implementation Checklist

### Phase 1: Core Infrastructure

- [ ] **New Puffin blob type**: `equality-delete-vector-v1` spec
- [ ] **Writer class**: `EqualityDeleteVectorWriter`
  - [ ] Value extraction from rows
  - [ ] Validation (non-negative, non-null)
  - [ ] Bitmap building with `RoaringPositionBitmap`
  - [ ] Puffin blob serialization
- [ ] **Reader support**: Modify `BaseDeleteLoader`
  - [ ] Detect EDV files (format = PUFFIN, blob type)
  - [ ] Deserialize bitmap
  - [ ] Wrap in `StructLikeSet` interface
- [ ] **Auto-detection**: Modify `FileWriterFactory`
  - [ ] Check format version >= 3
  - [ ] Check field type = LONG
  - [ ] Check single field
  - [ ] Route to EDV or traditional writer

### Phase 2: Metadata & Optimization

- [ ] **Add table property**: `write.delete.equality-vector.enabled`
- [ ] **Blob metadata fields**:
  - [ ] `equality-field-id`
  - [ ] `cardinality`
  - [ ] `value-min` (for scan filtering)
  - [ ] `value-max` (for scan filtering)
  - [ ] `partition-spec-id` (for global deletes)
- [ ] **Sparse value heuristic**: Check value range, fall back if too sparse
- [ ] **Commit metrics**: Track EDV usage in snapshot summary

### Phase 3: Testing

- [ ] **Unit tests**:
  - [ ] Bitmap serialization/deserialization
  - [ ] Value validation (negative, null)
  - [ ] Auto-detection logic
  - [ ] Fallback scenarios
- [ ] **Integration tests**:
  - [ ] End-to-end delete with EDV
  - [ ] Mixed format reads (EDV + traditional)
  - [ ] Sequence number semantics
  - [ ] Partition scoping (partitioned + global)
- [ ] **Performance tests**:
  - [ ] Storage comparison
  - [ ] Memory usage
  - [ ] Read/write throughput
  - [ ] Scan filtering with min/max

---

## Critical Issues to Handle

### 🟡 Must Implement

| Issue | Description | Solution |
|-------|-------------|----------|
| **Validation** | Enforce non-negative, non-null | Validate in writer, throw clear error |
| **Sequence semantics** | Equality deletes use `<` not `<=` | Enforce in reader: `data_seq < delete_seq` |
| **Partition scope** | Support partitioned + global deletes | Store `partition-spec-id` in blob metadata |
| **File metrics** | Enable scan filtering | Store `value-min`, `value-max` in metadata |
| **Sparse values** | Prevent memory explosion | Check range/cardinality ratio before EDV |

### 🟢 Design Considerations

| Issue | Description | Approach |
|-------|-------------|----------|
| **Error handling** | What if negative value encountered? | **Option A**: Throw exception (fail fast)<br>**Option B**: Fall back to traditional format |
| **Schema evolution** | Field dropped after EDV written | Document that EDVs become invalid<br>Compaction can clean up orphaned files |
| **Type promotion** | INT upgraded to LONG | Only support LONG, user must upgrade schema first |

---

## Sequence Number Semantics (Critical)

### Position Deletes (DVs)
```java
// Can delete rows from SAME commit
if (dataFile.dataSequenceNumber() <= deleteFile.dataSequenceNumber()) {
  applyPositionDelete();
}
```

### Equality Deletes (EDVs)
```java
// CANNOT delete rows from same commit (strictly less than)
if (dataFile.dataSequenceNumber() < deleteFile.dataSequenceNumber()) {
  applyEqualityDelete();
}
```

**Why different?**
- Position deletes know exact file paths, can target specific commits
- Equality deletes match by value, must only apply to older data
- Prevents deleting rows that were just inserted in same transaction

**Must enforce this correctly in EDV reader implementation!**

---

## Partition Scoping

Equality deletes have two modes:

### 1. Partitioned Deletes
```sql
-- Table partitioned by date
CREATE TABLE events (id BIGINT, event_type STRING, date DATE)
PARTITIONED BY (date);

-- Delete only from specific partition
DELETE FROM events WHERE id = 100 AND date = '2024-01-01';
```

**EDV metadata**:
```java
properties.put("partition-spec-id", "1");
// Applies only to matching partition
```

### 2. Global Deletes (Unpartitioned Spec)
```sql
-- Delete across ALL partitions
DELETE FROM events WHERE id = 100;
```

**EDV metadata**:
```java
properties.put("partition-spec-id", "-1"); // or special marker
// Applies to all partitions
```

**Implementation**: Store partition spec ID in blob metadata, reader checks scope when applying.

---

## Sparse Value Handling

### Problem

Sparse values create large bitmaps:

```sql
-- Only 3 deletes, but 2 billion range!
DELETE FROM t WHERE id IN (1, 1000000000, 2000000000);
```

**Roaring bitmap behavior**:
- Allocates array of bitmaps for each "key" (high 32 bits)
- Sparse values → many mostly-empty bitmaps
- Could use **more** memory than Parquet in pathological cases

### Heuristic

Check sparsity before choosing EDV:

```java
long min = Collections.min(deleteValues);
long max = Collections.max(deleteValues);
long range = max - min;
long cardinality = deleteValues.size();

// If very sparse (range >> cardinality), use traditional format
if (range > cardinality * 1000) {
  log.info("Values too sparse for EDV, using traditional format");
  return traditionalEqualityDeleteWriter();
}
```

**Threshold**: If range > 1000x cardinality, fall back

**Example**:
- 1000 deletes in range [0, 1000] → ratio = 1 ✅ Use EDV
- 1000 deletes in range [0, 10000000] → ratio = 10000 ❌ Fall back

---

## Configuration

### Table Properties

```sql
-- Enable EDV
ALTER TABLE orders SET TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);
```

### When EDV is Used

```
User DELETE query
    ↓
Check table property: write.delete.equality-vector.enabled = true?
    ↓ YES
Check format version >= 3?
    ↓ YES
Check single equality field?
    ↓ YES
Check field type = LONG?
    ↓ YES
Check all values >= 0?
    ↓ YES
Check no NULL values?
    ↓ YES
Check not too sparse?
    ↓ YES
✅ Use EDV (bitmap)

Any NO ↓
❌ Use traditional equality delete (Parquet/Avro/ORC)
```

---

## Metrics & Observability

### Snapshot Summary

```json
{
  "total-equality-deletes": "15000",
  "equality-delete-vectors": "12",      // # of EDV files
  "equality-delete-files": "3",         // # of traditional files
  "edv-size-bytes": "1500000",          // 1.5 MB
  "edv-storage-saved-bytes": "58500000" // 58.5 MB saved vs Parquet
}
```

### Logs

```
INFO: Equality delete on field 'order_id' (LONG), 15000 deletes
INFO: All values non-negative, non-null, not sparse
INFO: Using Equality Delete Vector (bitmap format)
INFO: EDV file written: /metadata/00012-edv.puffin (1.5 MB)
INFO: Storage reduction: 98.3% (1.5 MB vs 88.5 MB traditional)
```

---

## Future Enhancements (Out of Scope for MVP)

### Phase 2 Possibilities

1. **Support INT type**
   - Simpler than LONG (fits in 32 bits)
   - Requires separate blob type or type metadata

2. **Support negative values**
   - Offset by `Long.MIN_VALUE` during storage
   - Transparent to user, adds complexity

3. **Support NULL values**
   - Store boolean flag in blob metadata: `contains-null: true`
   - Special handling in reader

4. **Composite LONG keys**
   - Multiple LONG fields: `(user_id, session_id)`
   - Multi-dimensional bitmaps (complex)

5. **Auto-compaction**
   - Maintenance operation to convert traditional → EDV
   - Reclaim storage from existing tables

6. **Adaptive threshold**
   - ML-based decision: when to use EDV vs traditional
   - Based on schema width, delete cardinality, value distribution

---

## Compatibility Matrix

| Component | Version | EDV Support | Behavior |
|-----------|---------|-------------|----------|
| **Table format** | v1, v2 | ❌ No | Falls back to traditional |
| **Table format** | v3+ | ✅ Yes | Can use EDV if enabled |
| **Reader (old)** | Pre-EDV | ⚠️ Ignores | Unknown blob type, skips EDV files |
| **Reader (new)** | Post-EDV | ✅ Reads | Supports both EDV + traditional |
| **Writer (old)** | Pre-EDV | ❌ No | Writes traditional only |
| **Writer (new)** | Post-EDV | ✅ Yes | Writes EDV if enabled + constraints met |

**Migration**: No data migration required. New deletes use EDV, old deletes remain in traditional format. Readers handle both transparently.

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Wrong sequence semantics** | 🔴 HIGH | Thorough testing, follow spec exactly (`<` not `<=`) |
| **Negative values crash** | 🟡 MEDIUM | Validate at write time, clear error message |
| **Sparse value memory** | 🟡 MEDIUM | Heuristic check before EDV, fall back if needed |
| **Missing file metrics** | 🟢 LOW | Add min/max to metadata, enables filtering |
| **Schema evolution** | 🟢 LOW | Document behavior, compaction cleans up |

---

## Success Criteria

### Correctness
- ✅ All deletes applied correctly (no false positives/negatives)
- ✅ Sequence number semantics enforced
- ✅ Partition scoping works (partitioned + global)
- ✅ Mixed format reads (EDV + traditional) correct

### Performance
- ✅ Storage reduction: 40-100x for typical workloads
- ✅ Memory reduction: 40-100x when loading deletes
- ✅ Write throughput: >= traditional (no regression)
- ✅ Read throughput: >= traditional (no regression)

### Usability
- ✅ Single property to enable
- ✅ Automatic fallback when constraints violated
- ✅ Clear error messages for invalid values
- ✅ Observable metrics in snapshot summary

---

## References

- **Design doc**: `equality-delete-vectors-design.md`
- **Deletion Vectors**: `core/src/main/java/org/apache/iceberg/deletes/DVFileWriter.java`
- **Roaring bitmap**: `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java`
- **Puffin spec**: `format/puffin-spec.md`
- **Iceberg spec**: `format/spec.md` (lines 865-873 - equality delete semantics)
