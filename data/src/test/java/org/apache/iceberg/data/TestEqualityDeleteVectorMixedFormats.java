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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests to verify that EDV files can coexist with traditional Parquet equality delete files. This
 * is important for scenarios like compaction where both formats may be present.
 */
public class TestEqualityDeleteVectorMixedFormats {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private File tableDir;
  private Table table;
  private List<Record> records;

  @BeforeEach
  public void setupTable() {
    tableDir = new File(tempDir, "test_table");
    table =
        TestTables.create(
            tableDir,
            "test",
            SCHEMA,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            3 /* format version */);

    // Create test records
    records = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(SCHEMA);
    for (long i = 1; i <= 10; i++) {
      records.add(record.copy("id", i, "data", "data" + i));
    }
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testMixedEDVAndTraditionalDeletes() throws IOException {
    // Write data file
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // Verify all records are readable
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);

    // Write EDV delete file (enabled)
    table.updateProperties().set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true").commit();
    DeleteFile edvDeleteFile = writeEqualityDeleteVector(new long[] {2L, 4L, 6L});

    // Write traditional Parquet delete file (disabled)
    table
        .updateProperties()
        .set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "false")
        .commit();
    DeleteFile parquetDeleteFile = writeTraditionalEqualityDelete(new long[] {1L, 3L, 5L});

    // Commit both delete files together
    table.newRowDelta().addDeletes(edvDeleteFile).addDeletes(parquetDeleteFile).commit();

    // Verify both sets of deletes are applied
    // Should remain: 7, 8, 9, 10 (deleted: 1, 2, 3, 4, 5, 6)
    assertThat(readRecordIds()).containsExactlyInAnyOrder(7L, 8L, 9L, 10L);
  }

  @Test
  public void testCompactionWithMixedFormats() throws IOException {
    // Simulate a compaction scenario where old traditional deletes exist
    // and new EDV deletes are being added

    // Write data
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // Add traditional delete file first (from old data)
    table
        .updateProperties()
        .set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "false")
        .commit();
    DeleteFile oldDeleteFile = writeTraditionalEqualityDelete(new long[] {1L, 2L});
    table.newRowDelta().addDeletes(oldDeleteFile).commit();

    // Verify initial state
    assertThat(readRecordIds()).containsExactlyInAnyOrder(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);

    // Now enable EDV and add more deletes (simulating new MERGE operations)
    table.updateProperties().set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true").commit();
    DeleteFile newDeleteFile = writeEqualityDeleteVector(new long[] {3L, 4L});
    table.newRowDelta().addDeletes(newDeleteFile).commit();

    // Verify both old and new deletes are applied
    assertThat(readRecordIds()).containsExactlyInAnyOrder(5L, 6L, 7L, 8L, 9L, 10L);
  }

  private DataFile writeDataFile(List<Record> recordsToWrite) throws IOException {
    OutputFile output =
        org.apache.iceberg.Files.localOutput(
            new File(tableDir, "data-" + System.nanoTime() + ".parquet"));
    return FileHelpers.writeDataFile(table, output, recordsToWrite);
  }

  private DeleteFile writeEqualityDeleteVector(long[] idsToDelete) throws IOException {
    List<Record> deleteRecords = Lists.newArrayList();
    GenericRecord deleteRecord = GenericRecord.create(table.schema().select("id"));

    for (long id : idsToDelete) {
      deleteRecords.add(deleteRecord.copy("id", id));
    }

    OutputFile output =
        org.apache.iceberg.Files.localOutput(
            new File(tableDir, "delete-edv-" + System.nanoTime() + ".puffin"));

    Schema deleteSchema = table.schema().select("id");
    return FileHelpers.writeDeleteFile(table, output, null, deleteRecords, deleteSchema);
  }

  private DeleteFile writeTraditionalEqualityDelete(long[] idsToDelete) throws IOException {
    List<Record> deleteRecords = Lists.newArrayList();
    GenericRecord deleteRecord = GenericRecord.create(table.schema().select("id"));

    for (long id : idsToDelete) {
      deleteRecords.add(deleteRecord.copy("id", id));
    }

    OutputFile output =
        org.apache.iceberg.Files.localOutput(
            new File(tableDir, "delete-parquet-" + System.nanoTime() + ".parquet"));

    Schema deleteSchema = table.schema().select("id");
    return FileHelpers.writeDeleteFile(table, output, null, deleteRecords, deleteSchema);
  }

  private List<Long> readRecordIds() throws IOException {
    List<Long> ids = Lists.newArrayList();
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select("id").build()) {
      for (Record record : reader) {
        ids.add((Long) record.getField("id"));
      }
    }
    return ids;
  }
}
