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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for Equality Delete Vector (EDV) feature.
 *
 * <p>Tests the full end-to-end flow of writing data, writing equality deletes as bitmaps, and
 * reading with deletes applied.
 */
public class TestEqualityDeleteVectorIntegration {

  // Schema with LONG id field for EDV support
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private Table table;
  private List<Record> records;

  @BeforeEach
  public void setupTable() throws IOException {
    // Create table with format version 3 (required for Puffin support)
    this.table =
        TestTables.create(
            tableDir,
            "test_edv",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(
                TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true",
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

    // Create test records
    this.records = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(SCHEMA);
    records.add(record.copy("id", 1L, "data", "a"));
    records.add(record.copy("id", 2L, "data", "b"));
    records.add(record.copy("id", 3L, "data", "c"));
    records.add(record.copy("id", 4L, "data", "d"));
    records.add(record.copy("id", 5L, "data", "e"));
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testEqualityDeleteVectorWriteAndRead() throws IOException {
    // Write data file
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // Verify all records are readable
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);

    // Write equality delete vector for ids 2 and 4
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {2L, 4L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify deletes are applied
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 3L, 5L);
  }

  @Test
  public void testEqualityDeleteVectorFormat() throws IOException {
    // Write data file
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // Write equality delete vector
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L, 3L, 5L});

    // Verify delete file format is PUFFIN
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);

    // Verify equality field IDs
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1); // field id for "id"

    // Verify record count matches deleted values
    assertThat(deleteFile.recordCount()).isEqualTo(3);

    // Commit and verify deletes work
    table.newRowDelta().addDeletes(deleteFile).commit();
    assertThat(readRecordIds()).containsExactlyInAnyOrder(2L, 4L);
  }

  @Test
  public void testEqualityDeleteVectorCompression() throws IOException {
    // Create a larger dataset with sequential IDs (good for run-length encoding)
    List<Record> largeRecords = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(SCHEMA);
    for (long i = 0; i < 1000; i++) {
      largeRecords.add(record.copy("id", i, "data", "data" + i));
    }

    // Write data file
    DataFile dataFile = writeDataFile(largeRecords);
    table.newAppend().appendFile(dataFile).commit();

    // Delete sequential range (should compress well with RLE)
    long[] deletedIds = new long[500];
    for (int i = 0; i < 500; i++) {
      deletedIds[i] = i; // Delete first 500 records
    }

    DeleteFile deleteFile = writeEqualityDeleteVector(deletedIds);

    // Verify file is small (less than 10KB for 500 sequential deletes)
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(10_000);

    // Verify deletes work
    table.newRowDelta().addDeletes(deleteFile).commit();
    List<Long> remainingIds = readRecordIds();
    assertThat(remainingIds).hasSize(500);
    assertThat(remainingIds).allMatch(id -> id >= 500 && id < 1000);
  }

  @Test
  public void testEqualityDeleteVectorWithSparseValues() throws IOException {
    // Create dataset with sparse IDs
    List<Record> sparseRecords = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(SCHEMA);
    long[] sparseIds = {1L, 1000L, 1_000_000L, 1_000_000_000L};
    for (long id : sparseIds) {
      sparseRecords.add(record.copy("id", id, "data", "data" + id));
    }

    // Write data file
    DataFile dataFile = writeDataFile(sparseRecords);
    table.newAppend().appendFile(dataFile).commit();

    // Delete some sparse values
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1000L, 1_000_000_000L});

    // Verify file is still small despite sparse values (bitmap compression)
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(1_000);

    // Verify deletes work
    table.newRowDelta().addDeletes(deleteFile).commit();
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 1_000_000L);
  }

  @Test
  public void testEqualityDeleteVectorWithMultipleFiles() throws IOException {
    // Write first data file
    DataFile dataFile1 = writeDataFile(records);
    table.newAppend().appendFile(dataFile1).commit();

    // Write second data file with different IDs
    List<Record> moreRecords = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(SCHEMA);
    moreRecords.add(record.copy("id", 10L, "data", "j"));
    moreRecords.add(record.copy("id", 11L, "data", "k"));
    moreRecords.add(record.copy("id", 12L, "data", "l"));

    DataFile dataFile2 = writeDataFile(moreRecords);
    table.newAppend().appendFile(dataFile2).commit();

    // Verify all records
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 10L, 11L, 12L);

    // Delete values from both files
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {2L, 4L, 10L, 12L});
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Verify deletes applied across both files
    assertThat(readRecordIds()).containsExactlyInAnyOrder(1L, 3L, 5L, 11L);
  }

  @Test
  public void testEqualityDeleteVectorAutoDetection() throws IOException {
    // This test verifies that BaseFileWriterFactory automatically uses EDV
    // when conditions are met (format v3, LONG field, single equality field)

    // Write data file
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // Use GenericFileWriterFactory which should auto-detect and use EDV
    DeleteFile deleteFile = writeEqualityDeleteVector(new long[] {1L, 5L});

    // Verify it created a PUFFIN file (EDV)
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);

    table.newRowDelta().addDeletes(deleteFile).commit();
    assertThat(readRecordIds()).containsExactlyInAnyOrder(2L, 3L, 4L);
  }

  private DataFile writeDataFile(List<Record> recordsToWrite) throws IOException {
    OutputFile output =
        Files.localOutput(new File(tableDir, "data-" + System.nanoTime() + ".parquet"));
    return FileHelpers.writeDataFile(table, output, Row.of(), recordsToWrite);
  }

  private DeleteFile writeEqualityDeleteVector(long[] idsToDelete) throws IOException {
    // Create delete records with the IDs to delete
    List<Record> deleteRecords = Lists.newArrayList();
    GenericRecord deleteRecord = GenericRecord.create(table.schema().select("id"));

    for (long id : idsToDelete) {
      deleteRecords.add(deleteRecord.copy("id", id));
    }

    // Write equality delete file using FileHelpers
    // GenericFileWriterFactory will auto-detect and use EDV format
    OutputFile output =
        Files.localOutput(new File(tableDir, "delete-" + System.nanoTime() + ".puffin"));

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
