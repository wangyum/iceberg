# Equality Delete Vectors (EDV) Design Document

## Table of Contents

1. [Overview](#overview)
2. [Motivation](#motivation)
3. [Key Concepts](#key-concepts)
4. [Architecture](#architecture)
5. [Design Details](#design-details)
6. [API Changes](#api-changes)
7. [Format Version Requirements](#format-version-requirements)
8. [Migration Guide](#migration-guide)
9. [Limitations and Future Work](#limitations-and-future-work)
10. [Testing Strategy](#testing-strategy)

---

## Overview

Equality Delete Vectors (EDV) provide a bitmap-based storage format for equality deletes in Apache Iceberg V3+ tables. This feature extends the existing Deletion Vector (DV) infrastructure to support equality-based row deletion alongside position-based deletion.

### What is an Equality Delete Vector?

An Equality Delete Vector is a compact bitmap that stores the values of an equality field that should be deleted. Unlike position deletes that mark specific row positions within a single data file, equality deletes apply to all data files in the table based on field value matching.

**Example:**
```
// Traditional Equality Delete
Row{ id: 100, name: "Alice" } → Delete file contains: { id: 100 }
Row{ id: 100, name: "Bob" }   → Matched by equality delete, deleted
Row{ id: 101, name: "Carol" } → Not matched, kept

// Equality Delete Vector
Bitmap: [0, 1, 0, 1, ...] where index 100 is set
// All rows where id=100 are automatically marked for deletion
```

---

## Motivation

### Problem Statement

Before EDVs, equality deletes in Iceberg used traditional row-based formats (Parquet, Avro, ORC):

1. **Storage Overhead**: Each equality delete record stores full row data, including all fields
2. **Read Amplification**: Reading equality deletes requires parsing entire delete files
3. **Write Complexity**: Engines must convert row records to delete file format
4. **Inconsistency**: Position deletes could use DVs but equality deletes always used row-based formats

### Solution

EDVs extend the DV infrastructure to support equality-based deletion using the same Puffin + Roaring Bitmap format:

1. **Compact Storage**: Bitmaps store only the deleted values, not full row data
2. **Fast Operations**: Bitmap operations (OR, AND, NOT) are highly optimized
3. **Unified Infrastructure**: Position and equality deletes share the same writer/reader code
4. **Automatic Selection**: EDVs are automatically used for eligible equality deletes in V3+ tables

---

## Key Concepts

### Position Deletion Vectors (DV)

| Property | Value |
|----------|-------|
| **Scope** | File-scoped (applies to ONE data file) |
| **Key** | Data file path |
| **Content** | Row positions to delete (e.g., row 100, row 500) |
| **Merging** | Must merge with previous DVs during writes |
| **Referenced Data File** | Required |
| **Use Case** | Deleting scattered rows within a single file |

### Equality Deletion Vectors (EDV)

| Property | Value |
|----------|-------|
| **Scope** | Table-scoped (applies to ALL data files) |
| **Key** | Equality field ID |
| **Content** | Field values to delete (e.g., id=100, id=200) |
| **Merging** | No merge needed (apply by value, not position) |
| **Referenced Data File** | NULL (required) |
| **Use Case** | Deleting rows by field value across the entire table |

### Comparison Matrix

| Aspect | Position DV | Equality DV |
|--------|-------------|-------------|
| **Bitmap Index** | Row position in file | Field value (LONG only) |
| **File Scope** | Single data file | All data files |
| **Referenced Data File** | Set (required) | Null (required) |
| **Merge Required** | Yes | No |
| **Format** | Puffin + DV blob | Puffin + EDV blob |
| **Blob Type** | `DV_V1` | `EDV_V1` |

### Value Constraints

EDVs have specific constraints on supported values:

| Constraint | Description |
|------------|-------------|
| **Value Type** | LONG only |
| **Value Range** | 0 to ~9.2 quintillion (Roaring bitmap max) |
| **Negative Values** | Not supported (use traditional deletes) |
| **Multiple Fields** | Each field gets its own EDV file |
| **Null Values** | Not supported (nulls handled separately) |

---

## Architecture

### Class Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DELETES PACKAGE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │                     BitmapDeleteWriter                             │ │
│  │                                                                    │ │
│  │  Unified writer for both position and equality deletion vectors    │ │
│  │  - deletePosition(path, pos, spec, partition)                     │ │
│  │  - deleteEquality(fieldId, value, spec, partition)                 │ │
│  │  - close() → writes Puffin file, returns DeleteFiles               │ │
│  │                                                                    │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ┌──────────────────────────┐  ┌─────────────────────────────────────┐ │
│  │     DeleteKey            │  │      BitmapAccumulator              │ │
│  │  (interface)             │  │  (interface)                        │ │
│  │                          │  │                                      │ │
│  │  - keyId()               │  │  - add(long value)                  │ │
│  │  - toDeleteFile(...)     │  │  - toBlob()                         │ │
│  │                          │  │  - cardinality()                    │ │
│  └──────────────────────────┘  │  - merge(...)                       │ │
│           ▲                    │  - referencedDataFiles()             │ │
│           │                    └──────────────────────────────────────┘ │
│  ┌────────┴────────┐                    ▲                               │
│  │                 │                    │                               │
│  ▼                 ▼                    ▼                               │
│ ┌──────────┐  ┌──────────────┐  ┌───────────────────┐                  │
│ │Position  │  │Equality      │  │ PositionAccumulator                  │
│ │DeleteKey │  │DeleteKey     │  │   - wraps BitmapPositionDeleteIndex  │
│ └──────────┘  └──────────────┘  │   - supports merge with previous     │
│                                  └───────────────────┘                  │
│                                  ┌───────────────────┐                  │
│                                  │ EqualityAccumulator                  │
│                                  │   - wraps RoaringPositionBitmap      │
│                                  │   - no merge (standalone)            │
│                                  └───────────────────┘                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### BitmapDeleteWriter

The central writer class that handles both position and equality deletes:

**Key Methods:**
```java
public class BitmapDeleteWriter implements Closeable {
    
    // Constructor for position deletes with previous DV loading
    public BitmapDeleteWriter(
        OutputFileFactory fileFactory, 
        Function<String, PositionDeleteIndex> loadPreviousDeletes)
    
    // Constructor for equality deletes (no previous loading)
    public BitmapDeleteWriter(OutputFileFactory fileFactory)
    
    // Record a position delete
    public void deletePosition(
        String path, long position, 
        PartitionSpec spec, StructLike partition)
    
    // Record an equality delete
    public void deleteEquality(
        int equalityFieldId, long value, 
        PartitionSpec spec, StructLike partition)
    
    // Get result after close
    public DeleteWriteResult result()
}
```

#### DeleteKey Interface

Abstraction for different delete key types:

```java
public interface DeleteKey {
    // Unique identifier for this delete (used as map key)
    String keyId();
    
    // Create DeleteFile metadata from written blob
    DeleteFile toDeleteFile(
        String puffinPath, long puffinSize, 
        BlobMetadata blobMetadata, long cardinality);
}
```

**Implementations:**

| Class | Key Format | Purpose |
|-------|------------|---------|
| `PositionDeleteKey` | Data file path | Position DV per file |
| `EqualityDeleteKey` | `"field:" + fieldId` | EDV per equality field |

#### Accumulator Interface

Manages the in-memory bitmap and blob creation:

```java
interface BitmapAccumulator {
    void add(long value);           // Add value to bitmap
    
    Blob toBlob();                  // Create Puffin blob
    
    long cardinality();             // Number of deleted values
    
    // Position-only methods (noop for equality)
    void merge(
        Function<String, PositionDeleteIndex> loadPreviousDeletes,
        List<DeleteFile> rewrittenDeleteFiles);
    
    Iterable<CharSequence> referencedDataFiles();
}
```

---

## Design Details

### Puffin Blob Format

#### Position DV Blob (DV_V1)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Position DV Blob                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Properties:                                                     │
│  - referenced-data-file: "/path/to/data.parquet"                │
│  - cardinality: 12345                                           │
│                                                                 │
│  Data:                                                           │
│  - Serialized BitmapPositionDeleteIndex                         │
│  - Contains positions to delete from the referenced file        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Equality DV Blob (EDV_V1)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Equality DV Blob                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Properties:                                                     │
│  - equality-field-id: 100                                       │
│  - cardinality: 5000                                            │
│  - value-min: 0                                                 │
│  - value-max: 99999                                             │
│                                                                 │
│  Data:                                                           │
│  - Serialized RoaringPositionBitmap                             │
│  - Contains deleted field values (not positions)                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### File Layout in Puffin

A single Puffin file can contain multiple blobs:

```
puffin file
├── blob 1: Position DV for data-file-1.parquet (DV_V1)
│   └── positions: [100, 500, 1000, ...]
├── blob 2: Position DV for data-file-2.parquet (DV_V1)
│   └── positions: [50, 200, 350, ...]
├── blob 3: Equality DV for field 100 (EDV_V1)
│   └── values: [1, 2, 3, 100, 500, ...]
└── blob 4: Equality DV for field 200 (EDV_V1)
    └── values: [1000, 2000, 3000, ...]
```

Each blob gets its own `DeleteFile` entry in the manifest with:
- Same Puffin file path
- Different content offset/size (blob location)
- Different referenced data file (null for EDV)

### Write Flow

```
1. Create BitmapDeleteWriter
   ├─ Position DV: BitmapDeleteWriter(fileFactory, loadPreviousDeletes)
   └─ Equality DV: BitmapDeleteWriter(fileFactory)

2. Record deletes
   ├─ Position: writer.deletePosition(path, position, spec, partition)
   └─ Equality: writer.deleteEquality(fieldId, value, spec, partition)

3. Close writer
   ├─ For each unique key:
   │   ├─ Position: merge with previous DVs (if any)
   │   ├─ Equality: no merge
   │   └─ Write bitmap to Puffin blob
   └─ Create DeleteFile metadata for each blob

4. Get result
   └─ writer.result()
       └─ List<DeleteFile> (one per blob)
```

### Read Flow

```
1. Read manifests to get DeleteFiles
   ├─ Position DVs: filter by data file path
   └─ Equality DVs: collect all EDVs

2. For each data file:
   ├─ Load position DVs (apply to specific positions)
   ├─ Load all EDVs (apply by value matching)
   └─ Combine: (row is deleted) = (position in DV) OR (value in any EDV)

3. During row read:
   ├─ Check if current position is deleted by position DV
   ├─ Check if current row's field values are in any EDV
   └─ Skip row if either condition is true
```

---

## API Changes

### New Classes

| Class | Package | Purpose |
|-------|---------|---------|
| `BitmapDeleteWriter` | `org.apache.iceberg.deletes` | Unified DV/EDV writer |
| `DeleteKey` | `org.apache.iceberg.deletes` | Key abstraction interface |
| `PositionDeleteKey` | `org.apache.iceberg.deletes` | Position DV key |
| `EqualityDeleteKey` | `org.apache.iceberg.deletes` | Equality DV key |
| `BitmapAccumulator` | `org.apache.iceberg.deletes` | Accumulator interface |
| `PositionAccumulator` | `org.apache.iceberg.deletes` | Position accumulator |
| `EqualityAccumulator` | `org.apache.iceberg.deletes` | Equality accumulator |
| `EqualityDeleteVectors` | `org.apache.iceberg.deletes` | EDV read utilities |

### Modified Classes

| Class | Changes |
|-------|---------|
| `DeleteFile` | Added `contentOffset()` and `contentSizeInBytes()` with extensive documentation |
| `ContentFileUtil` | Added `isPositionDV()`, `isEqualityDV()`, `isFileScoped()`, `dvTypeDescription()` |
| `MergingSnapshotProducer` | Added format version validation for DVs/EDVs |
| `ManifestFilterManager` | Added dangling DV detection (Position DVs only) |

### Deprecated/Removed

| Class | Status | Replacement |
|-------|--------|-------------|
| `BaseDVFileWriter` | Removed | Use `BitmapDeleteWriter` |
| `BaseEDVFileWriter` | Removed | Use `BitmapDeleteWriter` |
| `EqualityDeleteVectorWriter` | Removed | Use `BitmapDeleteWriter` |
| `DVFileWriter` | Removed | Use `BitmapDeleteWriter` |

### Factory Methods

```java
// Spark
SparkFileWriterFactory factory = ...;

// Position DV writer (existing)
factory.newPositionDeleteWriter(...)

// Equality DV writer (new)
factory.newEqualityDeleteVectorWriter(fileFactory)

// Flink
FlinkFileWriterFactory factory = ...;
factory.newEqualityDeleteVectorWriter(fileFactory)

// Generic (data module)
GenericFileWriterFactory factory = ...;
BitmapDeleteWriter writer = factory.newEqualityDeleteVectorWriter(fileFactory);
```

---

## Format Version Requirements

### Version Compatibility Matrix

| Table Version | Position Deletes | Equality Deletes |
|---------------|------------------|------------------|
| V1 | Not supported | Not supported |
| V2 | Row-based (Parquet/Avro/ORC) only | Row-based only |
| V3 | DV required | **EDV automatic** (for LONG fields) |
| V4 | DV required | **EDV automatic** (for LONG fields) |

### Validation Rules

```java
// V1: No deletes allowed
if (version == 1) {
    throw "Deletes not supported in V1";
}

// V2: No DV/EDV allowed
if (version == 2) {
    Preconditions.checkArgument(!isPositionDV(file), 
        "Position DVs require V3+");
    Preconditions.checkArgument(!isEqualityDV(file), 
        "Equality DVs require V3+");
}

// V3/V4: Position must use DV
if (file.content() == POSITION_DELETES && version >= 3) {
    Preconditions.checkArgument(isPositionDV(file),
        "Position deletes must use DV in V3+");
}
```

### Automatic EDV Selection

EDVs are automatically used when:
1. Table format version is V3 or higher
2. Equality delete field is of type LONG
3. Value to delete is non-negative
4. Table property `write.delete.mode` is not set to `copy-on-write` (if applicable)

Fallback to traditional deletes when:
- Table format is V2
- Equality field is not LONG
- Value is negative
- Manual override via `FileWriterFactory`

---

## Migration Guide

### Upgrading from V2 to V3

**Before (V2 - Traditional Deletes):**
```
// Write equality deletes as Parquet
table.newRowDelta()
    .addDeletes(deleteFile)  // deleteFile.format() == PARQUET
    .commit()
```

**After (V3 - Equality DVs):**
```
// EDVs are written automatically for LONG equality fields
table.newRowDelta()
    .addDeletes(deleteFile)  // deleteFile.format() == PUFFIN
    .commit()

// Or explicitly write using the new API
BitmapDeleteWriter writer = factory.newEqualityDeleteVectorWriter(fileFactory);
writer.deleteEquality(fieldId, value, spec, partition);
writer.close();
DeleteFile deleteFile = writer.result().deleteFiles().get(0);
```

### Handling Negative Values

EDVs do not support negative values. For negative values, use traditional deletes:

```java
// Using traditional delete for negative values
DataWriter<Record> writer = factory.newEqualityDeleteWriter(...);
for (Record row : records) {
    Long value = row.getField(equalityFieldId);
    if (value != null && value < 0) {
        // Use traditional delete format
        writer.write(row);
    }
}
```

### Checking Existing Delete Files

```java
// Check if a delete file is a position DV
if (ContentFileUtil.isPositionDV(deleteFile)) {
    // Handle position DV
    String dataFile = deleteFile.referencedDataFile();
}

// Check if a delete file is an equality DV
if (ContentFileUtil.isEqualityDV(deleteFile)) {
    // Handle equality DV
    assert deleteFile.referencedDataFile() == null;  // Always null
}

// Get human-readable description
String type = ContentFileUtil.dvTypeDescription(deleteFile);
// Returns: "Position DV", "Equality DV", "Unknown DV type", or "Not a DV"
```

---

## Limitations and Future Work

### Current Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **Single LONG Field** | EDVs only support single LONG equality fields | Use traditional deletes for multi-field or non-LONG |
| **Non-Negative Values** | Cannot delete negative LONG values | Use traditional deletes |
| **No Null Support** | Cannot represent null in EDV bitmap | Nulls handled by separate null-value tracking |
| **No Merge for EDVs** | EDVs cannot be merged with previous EDVs | Multiple EDVs for same field accumulate |

### Known Issues

1. **Schema Evolution**: If an equality field is renamed or removed, existing EDVs may become invalid
2. **Compaction Complexity**: Merging multiple EDVs requires bitmap OR operations
3. **Memory Usage**: Large EDVs may require significant memory during read

### Future Enhancements

1. **Multi-Field EDVs**: Support composite keys (e.g., (id, version))
2. **Type Expansion**: Support INTEGER, STRING, and other types
3. **Null Bitmap**: Add explicit null tracking in EDVs
4. **Bloom Filter Optimization**: Use bloom filters for sparse EDVs
5. **Compression**: Apply additional compression for EDV blobs

---

## Testing Strategy

### Unit Tests

| Test File | Coverage |
|-----------|----------|
| `TestEqualityDeleteVectorEdgeCases` | Boundary values, error conditions |
| `TestEqualityDeleteVectorSchemaEvolution` | Schema changes, field type evolution |
| `TestEqualityDeleteVectorMixedFormats` | EDV + traditional delete mixing |
| `TestDeletionVectorsMixedScenarios` | Position + equality DV interaction |

### Integration Tests

| Test File | Coverage |
|-----------|----------|
| `TestSparkEqualityDeleteVectors` | Spark write/read cycles |
| `TestFlinkEqualityDeleteVectors` | Flink streaming writes |
| `TestEqualityDeleteVectorIntegration` | End-to-end table operations |

### JMH Benchmarks

| Benchmark | Metric |
|-----------|--------|
| `EqualityDeleteVectorBenchmark.writeEDV` | EDV write throughput |
| `EqualityDeleteVectorBenchmark.readEDV` | EDV read latency |
| `EqualityDeleteVectorBenchmark.applyEDV` | EDV apply to data scan |

---

## Appendix

### Glossary

| Term | Definition |
|------|------------|
| **DV (Deletion Vector)** | Bitmap-based index marking rows for deletion |
| **EDV (Equality Deletion Vector)** | DV variant for equality-based row deletion |
| **Position Delete** | Marks specific row positions in a data file |
| **Equality Delete** | Marks rows by field value matching |
| **Puffin** | Iceberg's bitmap file format |
| **Roaring Bitmap** | Compressed bitmap format used in DVs |

### References

- [Iceberg Specification: Deletion Vectors](https://iceberg.apache.org/spec/)
- [Puffin Format](https://github.com/apache/iceberg/blob/main/puffin.md)
- [Roaring Bitmap](https://roaringbitmap.org/)
- [Iceberg Format Version 3](https://iceberg.apache.org/spec/format-version-3/)

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01 | Apache Iceberg Community | Initial design document |

---

*This document is part of the Apache Iceberg project and is licensed under the Apache License 2.0.*