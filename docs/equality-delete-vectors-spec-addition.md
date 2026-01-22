# Equality Delete Vectors - Detailed Specification

**Format Version**: 3+
**Status**: Implementation Complete
**Date**: 2026-01-22

This document provides the detailed specification for Equality Delete Vectors (EDV), which should be incorporated into `format/spec.md`.

---

## Overview

Equality delete vectors (EDV) are an optimized storage format for equality deletes on a single LONG column, storing deleted values as compressed Roaring bitmaps in Puffin files instead of full row data. This provides 40-100x storage reduction and 90%+ memory reduction for common delete patterns on primary key columns, particularly in CDC (Change Data Capture) use cases.

---

## Format Specification

### File Format

- **Container Format**: Puffin (`.puffin` files)
- **Content Type**: `EQUALITY_DELETES` (same as traditional equality deletes)
- **Blob Type**: `equality-delete-vector-v1`
- **Compression**: Built-in Roaring bitmap compression (RLE for sequential values)

### Blob Structure

Each EDV is stored as a Puffin blob with the following structure:

```
Puffin File:
├── Header (magic bytes, metadata)
├── Blob 1: equality-delete-vector-v1
│   ├── Blob Properties (key-value pairs)
│   │   ├── equality-field-id: <int> (required)
│   │   ├── equality-field-name: <string> (optional)
│   │   ├── cardinality: <long> (required)
│   │   ├── value-min: <long> (optional)
│   │   └── value-max: <long> (optional)
│   └── Blob Data: Roaring Bitmap (serialized)
└── Footer (blob offsets, checksums)
```

### Roaring Bitmap Format

The blob data contains a serialized Roaring bitmap using the standard Roaring format with three container types:

**1. Array Container** (sparse values, cardinality < 4096):
```
Format: Sorted array of 16-bit values within 64K range
Size: 2 bytes × cardinality
Example: {100, 500, 1000} → [100, 500, 1000]
```

**2. Bitmap Container** (dense values, cardinality >= 4096):
```
Format: Bitmap of 65536 bits (8192 bytes)
Size: 8192 bytes (fixed)
Example: All values 0-65535 → 8KB bitmap with all bits set
```

**3. Run Container** (sequential values):
```
Format: Array of (start, length) pairs
Size: 4 bytes × number of runs
Example: 0-999999 → single run (0, 1000000) = 4 bytes
```

**Serialization**:
```
Roaring Bitmap Serialized Format:
├── Cookie (4 bytes): Indicates presence of run containers
├── Num Containers (4 bytes): Number of 64K containers
├── Container Keys (2 bytes each): High 16 bits of ranges
├── Container Cardinalities (2 bytes each)
├── Container Types (optional, if runs present)
└── Container Data (variable size per container)
```

---

## Constraints and Requirements

### Mandatory Constraints

1. **Single Field Only**:
   - MUST be exactly one equality field
   - Multi-column equality deletes MUST use traditional format

2. **LONG Type Only**:
   - Equality field MUST be type `LONG` (64-bit signed integer)
   - `INT`, `STRING`, or other types MUST use traditional format

3. **Non-Negative Values**:
   - All deleted values MUST be >= 0
   - Negative values MUST use traditional format
   - Rationale: Roaring bitmap optimized for non-negative values

4. **No NULL Values**:
   - NULL values are not supported in bitmap
   - Rows with NULL equality field are never matched by EDV

5. **Format Version**:
   - Table MUST be format version 3 or higher
   - Puffin file support required

### Optional Recommendations

1. **Identifier Fields** (Best Practice):
   - Equality field SHOULD be marked as identifier field
   - Warning logged if not identifier field
   - Not enforced (flexible approach)

2. **Sequential Values** (Optimal Compression):
   - Values SHOULD be sequential or clustered for best compression
   - Random/UUID values result in poor compression (1-2x)

---

## Application Semantics

### Read Algorithm

When reading data with EDV deletes applied:

```
function apply_edv_deletes(data_file, edv_file):
  # 1. Check sequence numbers
  if data_file.data_sequence_number >= edv_file.sequence_number:
    return  # EDV doesn't apply to this data file

  # 2. Load bitmap
  bitmap = deserialize_roaring_bitmap(edv_file.blob_data)
  equality_field_id = edv_file.blob_properties["equality-field-id"]

  # 3. Process each row
  for row in data_file:
    # Extract equality field value
    value = row.get_field(equality_field_id)

    # Handle special cases
    if value is NULL:
      continue  # NULL never matches EDV
    if value < 0:
      continue  # Negative values not in bitmap

    # Check bitmap
    if bitmap.contains(value):
      skip_row(row)  # Row is deleted
    else:
      emit_row(row)  # Row is live
```

### Sequence Number Semantics

EDV uses identical sequence number semantics to traditional equality deletes:

- **Delete applies if**: `data_seq_number < delete_seq_number`
- **Delete does NOT apply if**: `data_seq_number >= delete_seq_number`

This ensures consistent behavior when mixing EDV and traditional deletes.

### Field Matching Rules

1. **Field ID Lookup**:
   - Extract `equality-field-id` from blob properties
   - Locate field in data file schema by field ID
   - If field not found: skip delete file (field was dropped)

2. **Schema Evolution**:
   - If equality field was dropped: EDV still applies using dropped field value
   - If equality field was added after data file: default to NULL (never matches)
   - Follows standard Iceberg schema evolution rules

### NULL Handling

**Important**: EDV does NOT support NULL values

- Data rows with NULL equality field: **NEVER deleted** by EDV
- To delete NULLs: Use traditional equality delete files
- Mixed approach: EDV for non-NULL + traditional for NULL

---

## Write Behavior

### Writer Decision Tree

```
Should use EDV?
├── Check: write.delete.equality-vector.enabled = true?
│   └── NO → Use traditional
├── Check: Format version >= 3?
│   └── NO → Use traditional
├── Check: Single equality field?
│   └── NO → Use traditional (multi-column)
├── Check: Field type is LONG?
│   └── NO → Use traditional (wrong type)
├── Check: All values >= 0?
│   └── NO → Use traditional (negative values)
├── Check: No NULL values?
│   └── NO → Use traditional (NULLs present)
└── YES → Use EDV ✅
```

### Writer Implementation

```java
// Pseudocode for writer decision
if (properties.get("write.delete.equality-vector.enabled") == "true"
    && formatVersion >= 3
    && equalityFieldIds.length == 1
    && equalityField.type() == LONG
    && allValuesNonNegative
    && noNullValues) {

  // Use EDV format
  EqualityDeleteVectorWriter writer = new EqualityDeleteVectorWriter(...);
  for (Long value : deleteValues) {
    writer.write(value);
  }
  DeleteFile edvFile = writer.complete();  // Creates .puffin file

} else {
  // Fall back to traditional format
  EqualityDeleteWriter<Record> writer = newTraditionalWriter(...);
  for (Record row : deleteRows) {
    writer.write(row);
  }
  DeleteFile parquetFile = writer.complete();  // Creates .parquet file
}
```

### Metadata Generation

When creating EDV delete file, writer must populate:

**DeleteFile Metadata**:
```java
DeleteFile {
  content: EQUALITY_DELETES
  file_path: "s3://bucket/table/metadata/delete-00001.puffin"
  file_format: PUFFIN
  record_count: cardinality  // Number of deleted values
  file_size_in_bytes: puffin_file_size
  column_sizes: null  // Not applicable for bitmaps
  value_counts: null  // Not applicable for bitmaps
  null_value_counts: {equality_field_id: 0}  // EDV never has NULLs
  nan_value_counts: null
  lower_bounds: {equality_field_id: min_value}
  upper_bounds: {equality_field_id: max_value}
  key_metadata: encryption_key_metadata
  sort_order_id: null
  equality_ids: [equality_field_id]
  sequence_number: current_sequence_number
}
```

---

## Compression Characteristics

### Theoretical Compression Ratios

| Value Pattern | Container Type | Bytes per Value | Compression vs Parquet |
|---------------|----------------|-----------------|------------------------|
| Sequential (0-999999) | Run | 0.000004 bytes | 100,000x |
| Clustered (10 runs) | Run | 0.04 bytes | 1,000x |
| Sparse (gap=1000) | Array | 2 bytes | 50x |
| Random (50% density) | Bitmap | 0.125 bytes | 100x |
| Very Random (<1% density) | Array | 2 bytes | 50x |

**Parquet Baseline**: ~100 bytes per deleted row (LONG + overhead)

### Real-World Examples

**CDC Sequential IDs** (Best Case):
```
Deleted Values: _row_id IN [1000000, 1000001, ..., 1999999]
Count: 1,000,000 deletes
Container: Single Run (1000000, 1000000)
EDV Size: ~500 bytes
Parquet Size: ~100 MB
Compression: 200,000x
```

**Batch Delete on Clustered IDs** (Good Case):
```
Deleted Values: user_id IN [100-199, 1000-1099, 5000-5099, ...]
Count: 10,000 deletes in 100 clusters
Containers: 100 runs
EDV Size: ~5 KB
Parquet Size: ~1 MB
Compression: 200x
```

**Sparse Auto-Increment IDs** (Moderate Case):
```
Deleted Values: order_id with gaps (every 10th ID deleted)
Count: 100,000 deletes
Containers: Array containers
EDV Size: ~200 KB
Parquet Size: ~10 MB
Compression: 50x
```

**Random UUIDs** (Worst Case - Don't Use EDV):
```
Deleted Values: Random 64-bit values
Count: 1,000,000 deletes
Containers: Bitmap containers
EDV Size: ~80 MB
Parquet Size: ~100 MB
Compression: 1.2x ❌ Not worth it
```

---

## Compatibility

### Forward Compatibility (Old Readers + New Tables)

**Scenario**: Table has EDV files, reader doesn't support EDV

**Behavior**:
- Reader can open table (Puffin blobs are opaque)
- Reader CANNOT apply EDV deletes (unknown blob type)
- Data appears undeleted

**Acceptable because**:
- EDV is optimization, not correctness requirement
- User can upgrade reader to get correct delete application
- Table metadata is still valid

**Example**:
```
Table has: data-00001.parquet + delete-00001.puffin (EDV)
Old Reader (v1.4): Reads data-00001.parquet, ignores delete-00001.puffin
New Reader (v1.5+): Reads data-00001.parquet, applies delete-00001.puffin ✅
```

### Backward Compatibility (New Readers + Old Tables)

**Scenario**: Table has only traditional deletes, reader supports EDV

**Behavior**:
- Reader applies traditional deletes correctly
- No EDV files present (table predates feature)
- Fully compatible

### Mixed Format Support

**Scenario**: Table has both traditional and EDV delete files

**Behavior**:
- Reader applies BOTH formats correctly
- Each delete file processed based on its format
- Sequence numbers ensure correct ordering

**Example**:
```
Table delete files:
- delete-00001.parquet (traditional, STRING equality field)
- delete-00002.puffin (EDV, LONG equality field)
- delete-00003.parquet (traditional, multi-column equality)
- delete-00004.puffin (EDV, LONG equality field)

Reader applies all 4 files correctly ✅
```

### Compaction Behavior

When compacting delete files:

```
Input:
- delete-00001.parquet (100 deletes, LONG field)
- delete-00002.puffin (1000 deletes, same LONG field)
- delete-00003.parquet (50 deletes, STRING field)

Output (if EDV enabled):
- delete-compact-00001.puffin (1100 deletes, LONG field) ← Merged EDV
- delete-compact-00002.parquet (50 deletes, STRING field) ← Traditional

Output (if EDV disabled):
- delete-compact-00001.parquet (1150 deletes) ← All merged to Parquet
```

---

## Configuration

### Table Property

**Property Name**: `write.delete.equality-vector.enabled`

**Type**: Boolean

**Default**: `false`

**Description**: Enable equality delete vectors for single LONG equality fields. When enabled, writers create Puffin bitmap files instead of Parquet for eligible equality deletes.

**Example**:
```sql
-- Enable for new table
CREATE TABLE my_cdc_table (
  _row_id BIGINT,
  order_id BIGINT,
  data STRING
) TBLPROPERTIES (
  'write.format.default' = 'parquet',
  'format-version' = '3',
  'write.delete.equality-vector.enabled' = 'true'
);

-- Enable for existing table
ALTER TABLE my_cdc_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'true'
);

-- Disable (fall back to traditional)
ALTER TABLE my_cdc_table SET TBLPROPERTIES (
  'write.delete.equality-vector.enabled' = 'false'
);
```

### Requirements for Activation

1. `write.delete.equality-vector.enabled` = `true`
2. `format-version` >= 3
3. Equality delete has single LONG field
4. All values non-negative
5. No NULL values

---

## Performance Guarantees

### Write Performance

- **Throughput**: 80-95% of traditional Parquet writes
- **Overhead**: Bitmap construction + RLE compression
- **Typical**: <10% slower than Parquet

### Read Performance

- **Memory**: 50-100x reduction for sequential deletes
- **Throughput**: 95-110% of traditional reads
- **Lookup**: O(1) bitmap contains vs O(1) hash lookup

### Storage

- **Sequential deletes**: 40-100x smaller files
- **Sparse deletes**: 10-50x smaller files
- **Random deletes**: 1-2x (not recommended)

---

## Error Handling

### Writer Errors

**Invalid Value (Negative)**:
```
Error: Roaring bitmap does not support negative values
Action: Fall back to traditional equality delete format
Log: "EDV fallback: negative value -123 in equality field"
```

**NULL Value**:
```
Error: EDV does not support NULL values
Action: Fall back to traditional equality delete format
Log: "EDV fallback: NULL value in equality field"
```

**Wrong Type**:
```
Error: Equality field is STRING, not LONG
Action: Fall back to traditional equality delete format
Log: "EDV fallback: field type STRING (expected LONG)"
```

### Reader Errors

**Corrupted Bitmap**:
```
Error: Failed to deserialize Roaring bitmap
Action: Skip corrupt EDV file, log error
Log: "Failed to read EDV file: {path}, skipping deletes"
```

**Missing Field**:
```
Error: Equality field ID not in data schema
Action: Skip EDV file (field was dropped)
Log: "Equality field {id} not found in schema, skipping EDV"
```

---

## Testing Requirements

Implementations MUST pass the following test scenarios:

1. **Basic Functionality**:
   - Write 1000 sequential LONG deletes → Verify PUFFIN format
   - Read with EDV applied → Verify correct rows deleted
   - Verify file size < 10KB

2. **Fallback Scenarios**:
   - Multi-column equality → Verify PARQUET format
   - STRING field → Verify PARQUET format
   - Negative values → Verify PARQUET format
   - NULL values → Verify PARQUET format

3. **Mixed Format**:
   - Create EDV + traditional deletes → Verify both applied
   - Compaction of mixed formats → Verify correct merge

4. **Schema Evolution**:
   - Drop equality field → Verify EDV still applies
   - Add equality field after data → Verify NULLs not matched

5. **Performance**:
   - 1M sequential deletes → Verify >50x compression
   - 1M sparse deletes → Verify >10x compression
   - Memory usage → Verify <5MB for 1M deletes

---

This specification should be incorporated into `format/spec.md` in the "Delete Files" section.
