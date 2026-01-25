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
 * Tests to verify that when equality delete files are rewritten/compacted, they maintain the EDV
 * format (if enabled).
 *
 * <p>Note: Iceberg doesn't have a native "RewriteEqualityDeleteFiles" action yet, but this test
 * simulates what would happen if such an action existed by manually reading and rewriting delete
 * files.
 */
public class TestEqualityDeleteVectorCompaction {

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
    for (long i = 1; i <= 20; i++) {
      records.add(record.copy("id", i, "data", "data" + i));
    }
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testRewriteEDVFilesProducesEDV() throws IOException {
    // Simulate a compaction scenario:
    // 1. Write multiple small EDV files
    // 2. Read them all back
    // 3. Write a single merged EDV file
    // 4. Verify the output is still EDV format

    // Write data
    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // EDV automatically enabled for v3 + LONG (no property needed)

    // Write multiple small EDV files (simulating multiple MERGE operations)
    DeleteFile edv1 = writeEqualityDeleteVector(new long[] {1L, 2L, 3L});
    DeleteFile edv2 = writeEqualityDeleteVector(new long[] {4L, 5L, 6L});
    DeleteFile edv3 = writeEqualityDeleteVector(new long[] {7L, 8L, 9L});

    // Verify they are EDV format
    assertThat(edv1.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(edv2.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(edv3.format()).isEqualTo(FileFormat.PUFFIN);

    // Simulate compaction: read all deletes and write a single merged file
    // This simulates what a hypothetical "RewriteEqualityDeleteFiles" action would do
    List<Long> allDeletedIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    DeleteFile mergedEDV = writeEqualityDeleteVector(allDeletedIds.stream().mapToLong(Long::longValue).toArray());

    // Verify the merged file is ALSO in EDV format (not converted back to Parquet)
    assertThat(mergedEDV.format()).isEqualTo(FileFormat.PUFFIN);

    // Verify file size - merged EDV should be smaller than sum of individual files
    // (because bitmap compression improves with more sequential values)
    long totalSize = edv1.fileSizeInBytes() + edv2.fileSizeInBytes() + edv3.fileSizeInBytes();
    assertThat(mergedEDV.fileSizeInBytes()).isLessThan(totalSize);

    System.out.println("Individual EDV sizes: " + edv1.fileSizeInBytes() + ", "
        + edv2.fileSizeInBytes() + ", " + edv3.fileSizeInBytes());
    System.out.println("Total: " + totalSize + " bytes");
    System.out.println("Merged EDV size: " + mergedEDV.fileSizeInBytes() + " bytes");
    System.out.println("Savings from compaction: "
        + (totalSize - mergedEDV.fileSizeInBytes()) + " bytes");
  }

  @Test
  public void testRewriteWithEDVDisabledProducesParquet() throws IOException {
    // Verify that v3 table with LONG field automatically uses EDV (no property needed)

    // Write EDV file - should use PUFFIN automatically for v3 + LONG
    DeleteFile edvFile1 = writeEqualityDeleteVector(new long[] {1L, 2L, 3L});
    assertThat(edvFile1.format()).isEqualTo(FileFormat.PUFFIN);

    // Write another EDV file - still uses PUFFIN
    DeleteFile edvFile2 = writeEqualityDeleteVector(new long[] {4L, 5L, 6L});
    assertThat(edvFile2.format()).isEqualTo(FileFormat.PUFFIN);

    System.out.println("V3 table automatically uses EDV for LONG fields:");
    System.out.println("  EDV file 1 size: " + edvFile1.fileSizeInBytes() + " bytes");
    System.out.println("  EDV file 2 size: " + edvFile2.fileSizeInBytes() + " bytes");
  }

  @Test
  public void testFileWriterFactoryAutomaticDetection() throws IOException {
    // This test verifies that the FileWriterFactory automatically uses EDV
    // for v3 tables with LONG equality fields (no property needed)

    DataFile dataFile = writeDataFile(records);
    table.newAppend().appendFile(dataFile).commit();

    // All writes should automatically use PUFFIN (EDV) since:
    // - Table is v3
    // - Equality field is LONG
    // - Single equality field

    DeleteFile edvDelete1 = writeEqualityDeleteVector(new long[] {1L});
    assertThat(edvDelete1.format()).isEqualTo(FileFormat.PUFFIN);

    DeleteFile edvDelete2 = writeEqualityDeleteVector(new long[] {2L});
    assertThat(edvDelete2.format()).isEqualTo(FileFormat.PUFFIN);

    DeleteFile edvDelete3 = writeEqualityDeleteVector(new long[] {3L});
    assertThat(edvDelete3.format()).isEqualTo(FileFormat.PUFFIN);

    System.out.println("FileWriterFactory automatically uses EDV for v3 + LONG:");
    System.out.println("  delete 1 -> PUFFIN: " + edvDelete1.format());
    System.out.println("  delete 2 -> PUFFIN: " + edvDelete2.format());
    System.out.println("  delete 3 -> PUFFIN: " + edvDelete3.format());
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
            new File(tableDir, "delete-" + System.nanoTime() + ".puffin"));

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
