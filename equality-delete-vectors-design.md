# Equality Delete Vectors Design Document

## Overview

This proposal introduces **Equality Delete Vectors (EDV)** - a bitmap-based storage format for equality deletes on single int/long columns, similar to how Deletion Vectors optimize position deletes.

## Motivation

### Current Problem

When deleting rows by a single int/long primary key, Iceberg currently stores **entire rows** in equality delete files:

```sql
DELETE FROM orders WHERE order_id IN (100, 500, 1000);
```

**Today**: Stores 3 complete rows in Parquet/Avro/ORC (~120-300 bytes)
**Problem**: Wasteful when we only need to track integer values

### Opportunity

For single int/long equality fields, we only need to store the **values themselves** - which are just integers that fit perfectly in bitmaps.

**Storage Comparison** (1M deletes on wide schema):
- Current (Parquet): ~40-100 MB
- With EDV (Bitmap): ~1 MB
- **Savings: 40-100x reduction**

---

## Goals

1. **Reduce storage** for equality deletes on long primary keys
2. **Reduce memory** consumption when loading deletes
3. **Reuse existing infrastructure** from Deletion Vectors
4. **Simple enablement** via single table property
5. **Backward compatible** - old readers ignore, new readers support both formats

## Constraints (MVP)

To ensure correctness and simplicity, EDV **only supports**:

- ✅ **LONG type only** (not INT, not other types)
- ✅ **Non-negative values** (>= 0, no negative numbers)
- ✅ **Non-null values** (NULL deletes fall back to traditional format)

**Rationale**: This covers the most common use case (auto-increment/sequential primary keys) while avoiding complex edge cases. Future versions can expand support if needed.

---

## Design

### High-Level Approach

Store equality delete values in Roaring bitmaps within Puffin files, mirroring the Deletion Vector design.

### Key Differences from Deletion Vectors

| Aspect | Deletion Vectors (DV) | Equality Delete Vectors (EDV) |
|--------|----------------------|-------------------------------|
| **Values Stored** | Row positions (0, 1, 2, ...) | Primary key values (100, 500, 1000, ...) |
| **Scope** | Single data file | Multiple data files (global) |
| **Blob Property** | `referenced-data-file` | `equality-field-id` |
| **Puffin Type** | `deletion-vector-v1` | `equality-delete-vector-v1` (new) |

### Architecture

```
┌─────────────────────────────────────────┐
│  Equality Delete Writer                 │
│  ┌───────────────────────────────────┐  │
│  │ User deletes: [100, 500, 1000]    │  │
│  └───────────────────────────────────┘  │
│               ↓                          │
│  ┌───────────────────────────────────┐  │
│  │ RoaringPositionBitmap (reused)    │  │
│  │ - set(100)                        │  │
│  │ - set(500)                        │  │
│  │ - set(1000)                       │  │
│  └───────────────────────────────────┘  │
│               ↓                          │
│  ┌───────────────────────────────────┐  │
│  │ Serialize to Puffin blob          │  │
│  │ Type: equality-delete-vector-v1   │  │
│  │ Properties:                       │  │
│  │   - equality-field-id: 1          │  │
│  │   - cardinality: 3                │  │
│  └───────────────────────────────────┘  │
└─────────────────────────────────────────┘
```

---

## Implementation

### 1. New Puffin Blob Type

Add to `puffin-spec.md`:

```markdown
#### `equality-delete-vector-v1` blob type

A serialized bitmap of deleted values for a single int/long equality field.
The bitmap stores the actual column values (not row positions).

Serialization format:
- Same as deletion-vector-v1 (length + magic + Roaring bitmap + CRC-32)
- Uses RoaringPositionBitmap to store 64-bit values

The blob's `properties` must:
- Include `equality-field-id`, the field ID of the equality column
- Include `cardinality`, the number of deleted values in the bitmap
- Omit `compression-codec`; not compressed
```

### 2. Configuration

Add to `TableProperties.java`:

```java
/**
 * Controls whether to use bitmap-based equality delete vectors for long columns.
 *
 * Only applies when:
 * - Table format version >= 3 (Puffin support required)
 * - Single equality field (not composite keys)
 * - Field type is LONG (not INT or other types)
 * - All delete values are non-negative (>= 0)
 * - No NULL values in deletes
 *
 * Falls back to traditional equality deletes if any constraint is violated.
 */
public static final String EQUALITY_DELETE_VECTOR_ENABLED =
    "write.delete.equality-vector.enabled";
public static final boolean EQUALITY_DELETE_VECTOR_ENABLED_DEFAULT = false;
```

### 3. Writer Implementation

```java
public class EqualityDeleteVectorWriter<T> implements FileWriter<T, DeleteWriteResult> {
  private final RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
  private final int equalityFieldId;
  private final OutputFileFactory fileFactory;

  @Override
  public void write(T row) {
    // Extract the long value from the row
    long value = extractValue(row, equalityFieldId);

    // Validate constraints
    if (value < 0) {
      throw new IllegalArgumentException(
          "EDV only supports non-negative values, got: " + value +
          ". Use traditional equality deletes for negative values.");
    }

    // Check for NULL (implementation-specific)
    if (isNull(row, equalityFieldId)) {
      throw new IllegalArgumentException(
          "EDV does not support NULL values. " +
          "Use traditional equality deletes for NULL.");
    }

    bitmap.set(value);
  }

  @Override
  public void close() {
    // Write Puffin file with bitmap blob
    PuffinWriter writer = Puffin.write(fileFactory.newOutputFile()).build();
    writer.write(toBlob());
    writer.close();

    // Create DeleteFile metadata
    this.deleteFile = FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldId)
        .withFormat(FileFormat.PUFFIN)
        .withRecordCount(bitmap.cardinality())
        // ... other metadata
        .build();
  }

  private Blob toBlob() {
    return new Blob(
        "equality-delete-vector-v1",
        ImmutableList.of(equalityFieldId),
        -1, // snapshot ID inherited
        -1, // sequence number inherited
        bitmap.serialize(),
        null, // uncompressed
        ImmutableMap.of(
            "equality-field-id", String.valueOf(equalityFieldId),
            "cardinality", String.valueOf(bitmap.cardinality())));
  }
}
```

### 4. Auto-Detection in FileWriterFactory

```java
@Override
public EqualityDeleteWriter<T> newEqualityDeleteWriter(
    EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {

  boolean edvEnabled = table.properties().getAsBoolean(
      EQUALITY_DELETE_VECTOR_ENABLED,
      EQUALITY_DELETE_VECTOR_ENABLED_DEFAULT);

  // Check if we can use EDV
  boolean canUseEDV = edvEnabled &&
      table.formatVersion() >= 3 &&
      equalityFieldIds.length == 1 &&
      isLongType(schema.findType(equalityFieldIds[0]));

  private static boolean isLongType(Type type) {
    return type.typeId() == Type.TypeID.LONG;
  }

  if (canUseEDV) {
    return new EqualityDeleteVectorWriter<>(
        fileFactory, equalityFieldIds[0], spec, partition);
  } else {
    return new EqualityDeleteWriter<>(/* traditional writer */);
  }
}
```

### 5. Reader Implementation

Modify `BaseDeleteLoader.java`:

```java
private StructLikeSet loadEqualityDeletes(DeleteFile deleteFile) {
  if (deleteFile.format() == FileFormat.PUFFIN) {
    // Load EDV bitmap
    RoaringPositionBitmap bitmap = loadEDVBitmap(deleteFile);
    return new BitmapBackedStructLikeSet(bitmap, equalityFieldId, schema);
  } else {
    // Load traditional Parquet/Avro/ORC equality deletes
    return loadTraditionalEqualityDeletes(deleteFile);
  }
}

private RoaringPositionBitmap loadEDVBitmap(DeleteFile deleteFile) {
  byte[] bytes = readPuffinBlob(deleteFile);
  return BitmapPositionDeleteIndex.deserialize(ByteBuffer.wrap(bytes));
}
```

### 6. Reusable Components

✅ **RoaringPositionBitmap** - stores 64-bit values (reuse as-is)
✅ **BitmapPositionDeleteIndex** - serialization format (adapt for EDV)
✅ **Puffin infrastructure** - file writing (reuse as-is)
✅ **BaseDVFileWriter pattern** - writer structure (adapt for EDV)

**New Components Needed**:
- `EqualityDeleteVectorWriter` class
- `BitmapBackedStructLikeSet` wrapper for bitmap-based lookups
- Auto-detection logic in `FileWriterFactory`
- Puffin blob type spec addition

---

## Usage Examples

### Enable for a table

```sql
CREATE TABLE orders (
  order_id BIGINT,
  customer_id INT,
  amount DECIMAL(10,2)
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);
```

### Delete operations

```sql
-- Uses EDV (bitmap) - single long column, non-negative values
DELETE FROM orders WHERE order_id IN (100, 200, 300);

-- Uses EDV (bitmap) - single long column
DELETE FROM users WHERE user_id = 12345;

-- Falls back to traditional - INT type not supported
DELETE FROM orders WHERE customer_id = 5;

-- Falls back to traditional - composite key
DELETE FROM orders WHERE order_id = 5 AND customer_id = 10;

-- Falls back to traditional - string column
DELETE FROM products WHERE name IN ('A', 'B');

-- Falls back to traditional - negative values
DELETE FROM transactions WHERE transaction_id = -100;

-- Falls back to traditional - NULL values
DELETE FROM users WHERE user_id IS NULL;
```

### Enable for existing table

```sql
ALTER TABLE orders SET TBLPROPERTIES (
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);
```

---

## Compatibility

### Format Version

**Requires**: Iceberg format v3 (for Puffin file support)
**Fallback**: Automatically uses traditional equality deletes in v2 tables

### Reader Compatibility

| Reader Version | Behavior |
|----------------|----------|
| **Pre-EDV** | Ignores EDV files (unknown Puffin blob type) |
| **Post-EDV** | Reads both EDV and traditional equality deletes |

### Writer Compatibility

**New writers** (with EDV support):
- When `enabled=true`: Use EDV for eligible deletes
- When `enabled=false`: Use traditional format

**Old writers** (without EDV support):
- Continue writing traditional equality deletes
- Work alongside EDV-enabled writers (both formats coexist)

### Migration

**No migration needed!**
- Existing equality delete files remain unchanged
- New deletes use EDV if enabled
- Readers handle both formats transparently
- Compaction can consolidate old deletes into EDVs over time

---

## Testing Strategy

### Unit Tests
1. Bitmap serialization/deserialization
2. Value extraction for int/long columns
3. Auto-detection logic (various scenarios)
4. Puffin blob creation

### Integration Tests
1. End-to-end delete with EDV enabled
2. Read deletes with mixed formats (traditional + EDV)
3. Fallback to traditional format
4. Cross-version compatibility

### Performance Tests
1. Storage comparison (EDV vs traditional)
2. Memory usage during delete loading
3. Write throughput
4. Read performance with EDV filtering

---

## Metrics & Observability

### Commit Summary Additions
```
total-equality-deletes: 15000
equality-delete-vectors: 12        // EDV files
equality-delete-files: 3           // Traditional files
edv-size-bytes: 1500000           // 1.5 MB
```

### Logs
```
INFO: Writing equality deletes on field 'order_id' (BIGINT), 15000 deletes
INFO: Using Equality Delete Vector (bitmap format)
INFO: EDV file: /metadata/00012-edv.puffin (1.5 MB vs 60 MB traditional)
```

---

## Benefits Summary

| Metric | Current | With EDV | Improvement |
|--------|---------|----------|-------------|
| **Storage** (1M deletes, wide schema) | 40-100 MB | ~1 MB | **40-100x** |
| **Memory** (loading deletes) | ~40-100 MB | ~1 MB | **40-100x** |
| **Write Speed** | Baseline | Faster | Parquet encoding avoided |
| **Read Speed** | Baseline | Faster | Direct bitmap deserialization |
| **Cache Efficiency** | Low | High | More delete files fit in cache |

---

## Future Enhancements

### Phase 2 Possibilities (not in scope)

1. **Composite int/long keys**: Support multiple int/long fields using multi-dimensional bitmaps
2. **Auto-enable**: Default to `true` in future versions after proving stability
3. **Compaction**: Auto-convert traditional deletes to EDVs during maintenance
4. **Statistics**: Publish EDV usage metrics in table metadata

---

## Data Correctness Considerations

### Constraints Enforced

1. **LONG type only**
   - ✅ Simplifies implementation
   - ✅ Avoids INT vs LONG type mixing
   - ✅ Covers most common use case (auto-increment PKs)

2. **Non-negative values only**
   - ✅ Leverages RoaringPositionBitmap's existing validation
   - ✅ No offset transformation needed
   - ✅ Common for primary keys (user_id, order_id, etc.)

3. **Non-null values only**
   - ✅ Bitmaps cannot represent NULL
   - ✅ Falls back gracefully to traditional format
   - ✅ Preserves correctness

### Runtime Validation

The writer validates each delete value:
```java
// Reject negative values
if (value < 0) {
  throw new IllegalArgumentException("EDV requires non-negative values");
}

// Reject NULL values
if (value == null) {
  throw new IllegalArgumentException("EDV does not support NULL");
}
```

**Alternative approach**: Detect constraint violations early and fall back to traditional format instead of throwing exceptions.

### Sequence Number Semantics

From Iceberg spec: Equality deletes apply when `data_seq < delete_seq` (strictly less than)

EDV must enforce this correctly:
```java
// In reader
if (dataFile.dataSequenceNumber() < deleteFile.dataSequenceNumber()) {
  applyEqualityDelete(bitmap, row);
}
```

**Different from position deletes** which use `<=` (can delete same commit).

### Partition Scoping

Equality deletes can be:
- **Partitioned**: Apply only to matching partition
- **Global** (unpartitioned spec): Apply to all partitions

EDV must store partition scope in blob metadata:
```java
properties.put("partition-spec-id", String.valueOf(specId));
// Use -1 for unpartitioned/global deletes
```

### Sparse Value Handling

**Problem**: Very sparse values create large bitmaps

Example:
```sql
DELETE FROM t WHERE id IN (1, 1000000000, 2000000000);
```

**Heuristic**: Check value range before choosing EDV
```java
long min = Collections.min(values);
long max = Collections.max(values);
long range = max - min;
long cardinality = values.size();

// If very sparse (range >> cardinality), fall back
if (range > cardinality * 1000) {
  return false; // Use traditional format
}
```

### File Metrics for Scan Optimization

Store min/max values in blob metadata to enable file skipping:
```java
properties.put("value-min", String.valueOf(minDeleteValue));
properties.put("value-max", String.valueOf(maxDeleteValue));
```

Allows scan planning to skip EDV files that can't match query predicates.

---

## Summary

**What**: Bitmap-based storage for equality deletes on LONG columns with non-negative, non-null values

**Why**: 40-100x storage/memory reduction for common use case (auto-increment primary keys)

**How**: Reuse Deletion Vector infrastructure, new Puffin blob type

**Enablement**: Single property `write.delete.equality-vector.enabled`

**Constraints**: LONG type only, non-negative (>= 0), non-null values

**Compatibility**: Backward compatible, opt-in feature, format v3+

**Fallback**: Automatically uses traditional equality deletes when constraints violated
