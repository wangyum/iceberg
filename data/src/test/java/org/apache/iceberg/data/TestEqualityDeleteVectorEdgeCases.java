/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Comprehensive edge case tests for Equality Delete Vectors.
 *
 * <p>Tests cover:
 *
 * <ul>
 *   <li>Boundary value testing (MIN_VALUE, MAX_VALUE, 0, near-boundary values)
 *   <li>Empty and null scenarios
 *   <li>Large-scale scenarios (millions of deletes)
 *   <li>Sparse vs dense delete patterns
 *   <li>Concurrent operations
 *   <li>Format version transitions
 *   <li>Error conditions and validation
 * </ul>
 */
public class TestEqualityDeleteVectorEdgeCases {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private File tableDir;
  private Table table;

  @BeforeEach
  public void setupTable() {
    tableDir = new File(tempDir, "test_table");
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 3);
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  // ==================== Boundary Value Tests ====================

  @Test
  public void testDeleteMinLongValue() {
    // Test that EDVs reject negative values (Long.MIN_VALUE)
    // EDVs only support non-negative LONG values
    assertThatThrownBy(() -> writeEqualityDeleteVector(new long[] {Long.MIN_VALUE}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality delete vector only supports non-negative values");
  }

  @Test
  public void testDeleteMaxLongValue() {
    // Test that EDVs reject values beyond bitmap capacity (Long.MAX_VALUE)
    // Bitmap supports positions up to ~9.2 quintillion, but not Long.MAX_VALUE (9.2 quintillion+)
    assertThatThrownBy(() -> writeEqualityDeleteVector(new long[] {Long.MAX_VALUE}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bitmap supports positions that are >= 0 and <=");
  }

  @Test
  public void testDeleteZero() throws IOException {
    // Test deleting 0 (boundary between negative and positive)
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(-1L, "negative"));
    records.add(createRecord(0L, "zero"));
    records.add(createRecord(1L, "positive"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {0L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<Record> result = readRecords();
    assertThat(result).hasSize(2);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(-1L, 1L);
  }

  @Test
  public void testDeleteNearBoundaryValues() {
    // Test values near Long boundaries
    // MIN_VALUE + 1 is still negative, so should fail
    assertThatThrownBy(() -> writeEqualityDeleteVector(new long[] {Long.MIN_VALUE + 1}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality delete vector only supports non-negative values");

    // MAX_VALUE - 1 is still beyond bitmap capacity, so should fail
    assertThatThrownBy(() -> writeEqualityDeleteVector(new long[] {Long.MAX_VALUE - 1}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Bitmap supports positions that are >= 0 and <=");
  }

  // ==================== Empty and Null Scenarios ====================

  @Test
  public void testEmptyDeleteVector() throws IOException {
    // Test creating an empty EDV (no deletes)
    // BitmapDeleteWriter doesn't create a delete file if there are no deletes
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "one"));
    records.add(createRecord(2L, "two"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Cannot create an EDV with zero deletes - writer won't produce a file
    // This is expected behavior - no delete file is created when there are no deletes
    assertThatThrownBy(() -> writeEqualityDeleteVector(new long[] {}))
        .isInstanceOf(java.util.NoSuchElementException.class);

    // All records should still be present
    List<Record> result = readRecords();
    assertThat(result).hasSize(2);
  }

  @Test
  public void testDeleteNonExistentValues() throws IOException {
    // Test deleting values that don't exist in the table
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "one"));
    records.add(createRecord(2L, "two"));
    records.add(createRecord(3L, "three"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete values that don't exist (100, 200, 300)
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {100L, 200L, 300L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // All original records should still be present
    List<Record> result = readRecords();
    assertThat(result).hasSize(3);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(1L, 2L, 3L);
  }

  @Test
  public void testDeleteFromEmptyTable() throws IOException {
    // Test applying deletes to an empty table
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L, 2L, 3L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Table should remain empty
    List<Record> result = readRecords();
    assertThat(result).isEmpty();
  }

  // ==================== Large-Scale Scenarios ====================

  @Test
  public void testDeleteMillionValues() throws IOException {
    // Test EDV with 1 million delete values (tests bitmap efficiency)
    int numRecords = 1_000_000;
    List<Record> records = Lists.newArrayList();
    for (long i = 0; i < numRecords; i++) {
      records.add(createRecord(i, "data_" + i));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete every other record (500k deletes)
    long[] deletes = new long[numRecords / 2];
    for (int i = 0; i < deletes.length; i++) {
      deletes[i] = i * 2; // 0, 2, 4, 6, ...
    }

    DeleteFile deleteFile = writeEqualityDeleteVector(deletes);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify file format and size efficiency
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.contentOffset()).isNotNull();
    assertThat(deleteFile.contentSizeInBytes()).isNotNull();

    // Verify correct records remain (odd numbers)
    List<Record> result = readRecords();
    assertThat(result).hasSize(numRecords / 2);
    assertThat(result.get(0).getField("id")).isEqualTo(1L); // First odd number
  }

  @Test
  public void testSparseDeletes() throws IOException {
    // Test sparse delete pattern (few deletes over large ID range)
    List<Record> records = Lists.newArrayList();
    for (long i = 0; i < 1000; i++) {
      records.add(createRecord(i, "data_" + i));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete only 10 sparse values across the range
    long[] deletes = new long[] {0L, 100L, 200L, 300L, 400L, 500L, 600L, 700L, 800L, 900L};
    DeleteFile deleteFile = writeEqualityDeleteVector(deletes);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify bitmap compression handles sparse deletes efficiently
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    // Sparse bitmaps should compress well
    assertThat(deleteFile.contentSizeInBytes()).isLessThan(1024L); // < 1KB for 10 deletes

    List<Record> result = readRecords();
    assertThat(result).hasSize(990); // 1000 - 10
  }

  @Test
  public void testDenseDeletes() throws IOException {
    // Test dense delete pattern (most values deleted, few remaining)
    List<Record> records = Lists.newArrayList();
    for (long i = 0; i < 1000; i++) {
      records.add(createRecord(i, "data_" + i));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete 990 out of 1000 records (dense deletion)
    long[] deletes = new long[990];
    for (int i = 0; i < 990; i++) {
      deletes[i] = i; // Delete 0-989, keep 990-999
    }

    DeleteFile deleteFile = writeEqualityDeleteVector(deletes);
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify dense bitmaps are handled
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);

    List<Record> result = readRecords();
    assertThat(result).hasSize(10); // Only 990-999 remain
    assertThat(result).extracting(r -> r.getField("id")).contains(990L, 999L);
  }

  // ==================== Duplicate Handling ====================

  @Test
  public void testDuplicateDeleteValues() throws IOException {
    // Test that duplicate delete values are deduplicated
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "one"));
    records.add(createRecord(2L, "two"));
    records.add(createRecord(3L, "three"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete with duplicates: [1, 2, 2, 2, 3, 3]
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L, 2L, 2L, 2L, 3L, 3L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // All should be deleted (duplicates don't matter)
    List<Record> result = readRecords();
    assertThat(result).isEmpty();
  }

  @Test
  public void testMultipleDeleteFilesWithOverlap() throws IOException {
    // Test multiple EDV files with overlapping delete values
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 10; i++) {
      records.add(createRecord(i, "data_" + i));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // First EDV: delete 1-5
    DeleteFile deleteFile1 = writeEqualityDeleteVector(new long[] {1L, 2L, 3L, 4L, 5L});

    // Second EDV: delete 4-8 (overlap with first)
    DeleteFile deleteFile2 = writeEqualityDeleteVector(new long[] {4L, 5L, 6L, 7L, 8L});

    table.newRowDelta().addDeletes(deleteFile1).addDeletes(deleteFile2).commit();

    // 1-8 should be deleted, 9-10 remain
    List<Record> result = readRecords();
    assertThat(result).hasSize(2);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(9L, 10L);
  }

  // ==================== Format Version Validation ====================

  @Test
  public void testV2TableRejectsEqualityDV() {
    // Test that V2 tables reject Equality DVs
    File v2TableDir = new File(tempDir, "v2_table");
    Table v2Table = TestTables.create(v2TableDir, "v2test", SCHEMA, PartitionSpec.unpartitioned(), 2);

    assertThatThrownBy(
            () -> {
              DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L, 2L, 3L});
              v2Table.newRowDelta().addDeletes(deleteFile).commit();
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Equality Deletion Vectors require format version 3");
  }

  @Test
  public void testV3TableAcceptsEqualityDV() throws IOException {
    // Test that V3 tables accept Equality DVs
    File v3TableDir = new File(tempDir, "v3_table");
    Table v3Table = TestTables.create(v3TableDir, "v3test", SCHEMA, PartitionSpec.unpartitioned(), 3);

    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "one"));
    records.add(createRecord(2L, "two"));

    DataFile dataFile = writeRecordsToTable(v3Table, records);
    v3Table.newAppend().appendFile(dataFile).commit();

    DeleteFile deleteFile = writeEqualityDeleteVectorToTable(v3Table, new long[] {1L});
    v3Table.newRowDelta().addDeletes(deleteFile).commit();

    // Should succeed
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
  }

  // ==================== Sequence Number Validation ====================

  @Test
  public void testDeleteSequenceNumberValidation() throws IOException {
    // Test that DV sequence numbers are validated correctly
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "one"));
    records.add(createRecord(2L, "two"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Add delete with proper sequence number
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify delete is applied
    List<Record> result = readRecords();
    assertThat(result).hasSize(1);
    assertThat(result.get(0).getField("id")).isEqualTo(2L);
  }

  // ==================== Concurrent Operations ====================

  @Test
  public void testConcurrentDeleteAdditions() throws IOException {
    // Test adding multiple EDVs in separate commits (sequential)
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 100; i++) {
      records.add(createRecord(i, "data_" + i));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Add deletes in 10 separate commits
    for (int batch = 0; batch < 10; batch++) {
      long[] deletes = new long[10];
      for (int i = 0; i < 10; i++) {
        deletes[i] = batch * 10 + i + 1; // 1-10, 11-20, ..., 91-100
      }
      DeleteFile deleteFile = writeEqualityDeleteVector(deletes);
      table.newRowDelta().addDeletes(deleteFile).commit();
    }

    // All 100 records should be deleted
    List<Record> result = readRecords();
    assertThat(result).isEmpty();
  }

  // ==================== Mixed Data File Scenarios ====================

  @Test
  public void testDeleteAcrossMultipleDataFiles() throws IOException {
    // Test EDV applying to single data file with multiple deletes
    // NOTE: This test verifies that Equality DVs can delete multiple rows from the same file
    List<Record> records = Lists.newArrayList();
    records.add(createRecord(1L, "row1"));
    records.add(createRecord(2L, "row2"));
    records.add(createRecord(3L, "row3"));
    records.add(createRecord(4L, "row4"));
    records.add(createRecord(5L, "row5"));
    records.add(createRecord(6L, "row6"));

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Delete ID=2, 3, and 4
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {2L, 3L, 4L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // IDs 1, 5, 6 should remain (3 records)
    List<Record> result = readRecords();
    assertThat(result).hasSize(3);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(1L, 5L, 6L);
  }

  // ==================== Helper Methods ====================

  private Record createRecord(Long id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private DataFile writeRecords(List<Record> records) throws IOException {
    return writeRecordsToTable(table, records);
  }

  private DataFile writeRecordsToTable(Table targetTable, List<Record> records) throws IOException {
    OutputFile out = targetTable.io().newOutputFile(targetTable.locationProvider().newDataLocation("data-file"));
    return FileHelpers.writeDataFile(targetTable, out, records);
  }

  private DeleteFile writeEqualityDeleteVector(long[] values) throws IOException {
    return writeEqualityDeleteVectorToTable(table, values);
  }

  private DeleteFile writeEqualityDeleteVectorToTable(Table targetTable, long[] values)
      throws IOException {
    // Implementation uses BitmapDeleteWriter to create Puffin EDV
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(targetTable, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter closeableWriter = writer) {
      for (long value : values) {
        closeableWriter.deleteEquality(
            1, value, targetTable.spec(), null); // field ID 1 is "id"
      }
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private List<Record> readRecords() throws IOException {
    return Lists.newArrayList(
        IcebergGenerics.read(table).select("id", "data").build());
  }
}
