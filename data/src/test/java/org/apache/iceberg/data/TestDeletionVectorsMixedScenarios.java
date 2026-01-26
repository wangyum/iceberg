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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests mixing different deletion mechanisms:
 *
 * <ul>
 *   <li>Position DVs + Equality DVs
 *   <li>Position DVs + Traditional equality deletes
 *   <li>Equality DVs + Traditional position deletes
 *   <li>All three types together
 *   <li>Multiple partitions with different delete types
 * </ul>
 */
public class TestDeletionVectorsMixedScenarios {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "category", Types.StringType.get()));

  @TempDir private File tempDir;

  private File tableDir;
  private Table table;

  @BeforeEach
  public void setupTable() {
    tableDir = new File(tempDir, "mixed_deletes_table");
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 3);
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  // ==================== Position DV + Equality DV ====================

  @Test
  public void testPositionDVAndEqualityDVTogether() throws IOException {
    // Test combining Position DVs and Equality DVs in same table
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 20; i++) {
      records.add(createRecord(i, "data_" + i, "category_A"));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Position DV: Delete positions 0, 2, 4 (rows with id=1, 3, 5)
    DeleteFile positionDV = writePositionDV(dataFile.location(), new long[] {0L, 2L, 4L});

    // Equality DV: Delete id=10, id=15, id=20
    DeleteFile equalityDV = writeEqualityDV(new long[] {10L, 15L, 20L});

    table.newRowDelta().addDeletes(positionDV).addDeletes(equalityDV).commit();

    // Total deleted: positions 0,2,4 (id=1,3,5) + id=10,15,20 = 6 rows
    // Remaining: 20 - 6 = 14 rows
    List<Record> result = readRecords();
    assertThat(result).hasSize(14);

    // Verify correct IDs remain
    List<Long> remainingIds = Lists.newArrayList(2L, 4L, 6L, 7L, 8L, 9L, 11L, 12L, 13L, 14L, 16L, 17L, 18L, 19L);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrderElementsOf(remainingIds);
  }

  @Test
  public void testMultiplePositionDVsAndEqualityDVs() throws IOException {
    // Test multiple files of each type
    List<Record> records1 = createRecords(1L, 10L, "file1");
    List<Record> records2 = createRecords(11L, 20L, "file2");
    List<Record> records3 = createRecords(21L, 30L, "file3");

    DataFile dataFile1 = writeRecords(records1);
    DataFile dataFile2 = writeRecords(records2);
    DataFile dataFile3 = writeRecords(records3);

    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).appendFile(dataFile3).commit();

    // Position DVs for each file
    DeleteFile posDV1 = writePositionDV(dataFile1.location(), new long[] {0L, 1L}); // id=1,2
    DeleteFile posDV2 = writePositionDV(dataFile2.location(), new long[] {0L, 1L}); // id=11,12
    DeleteFile posDV3 = writePositionDV(dataFile3.location(), new long[] {0L, 1L}); // id=21,22

    // Equality DVs
    DeleteFile eqDV1 = writeEqualityDV(new long[] {5L, 15L, 25L});
    DeleteFile eqDV2 = writeEqualityDV(new long[] {10L, 20L, 30L});

    table
        .newRowDelta()
        .addDeletes(posDV1)
        .addDeletes(posDV2)
        .addDeletes(posDV3)
        .addDeletes(eqDV1)
        .addDeletes(eqDV2)
        .commit();

    // Total: 30 records
    // Position DVs delete: 6 records (2 per file)
    // Equality DVs delete: 6 records (5,10,15,20,25,30)
    // Remaining: 30 - 12 = 18
    List<Record> result = readRecords();
    assertThat(result).hasSize(18);
  }

  // ==================== Position DV + Traditional Equality Deletes ====================

  @Test
  public void testPositionDVWithTraditionalEqualityDeletes() throws IOException {
    // Combine Position DVs with Parquet-based equality deletes
    List<Record> records = createRecords(1L, 30L, "main");
    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Position DV: Delete first 5 positions
    DeleteFile positionDV = writePositionDV(dataFile.location(), new long[] {0L, 1L, 2L, 3L, 4L});

    // Traditional equality delete (Parquet format)
    List<Record> equalityDeletes = Lists.newArrayList();
    equalityDeletes.add(createRecord(10L, null, null));
    equalityDeletes.add(createRecord(20L, null, null));
    equalityDeletes.add(createRecord(30L, null, null));

    Schema deleteSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    OutputFile out = table.io().newOutputFile(table.locationProvider().newDataLocation("delete-file"));
    DeleteFile traditionalEqDelete =
        FileHelpers.writeDeleteFile(table, out, equalityDeletes, deleteSchema);

    table.newRowDelta().addDeletes(positionDV).addDeletes(traditionalEqDelete).commit();

    // Position DV deleted: id=1,2,3,4,5 (5 rows)
    // Equality deleted: id=10,20,30 (3 rows)
    // Remaining: 30 - 8 = 22
    List<Record> result = readRecords();
    assertThat(result).hasSize(22);

    assertThat(traditionalEqDelete.format()).isEqualTo(FileFormat.PARQUET); // Traditional format
  }

  // ==================== Equality DV + Traditional Position Deletes ====================

  @Test
  public void testEqualityDVWithTraditionalPositionDeletes() throws IOException {
    // This scenario can only occur in special cases (mixed format versions or migrations)
    // In pure V3, position deletes are always DVs

    List<Record> records = createRecords(1L, 20L, "test");
    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Equality DV
    DeleteFile equalityDV = writeEqualityDV(new long[] {1L, 5L, 10L, 15L, 20L});

    // Traditional position delete (Parquet) - simulate V2 migration scenario
    List<PositionDelete<Record>> positionDeletes = Lists.newArrayList();
    PositionDelete<Record> posDelete = PositionDelete.create();
    positionDeletes.add(posDelete.set(dataFile.location(), 1L, null)); // position 1 (id=2)
    positionDeletes.add(posDelete.set(dataFile.location(), 3L, null)); // position 3 (id=4)

    DeleteFile traditionalPosDelete =
        writeTraditionalPositionDeletes(dataFile.location(), positionDeletes);

    table.newRowDelta().addDeletes(equalityDV).addDeletes(traditionalPosDelete).commit();

    // Equality DV deleted: id=1,5,10,15,20 (5 rows)
    // Position deleted: positions 1,3 (id=2,4) (2 rows)
    // Remaining: 20 - 7 = 13
    List<Record> result = readRecords();
    assertThat(result).hasSize(13);
  }

  // ==================== All Three Types Together ====================

  @Test
  public void testPositionDVEqualityDVAndTraditionalDeletes() throws IOException {
    // Test all three delete mechanisms together
    List<Record> records = createRecords(1L, 50L, "comprehensive");
    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // 1. Position DV: Delete positions 0-4 (id=1-5)
    DeleteFile positionDV = writePositionDV(dataFile.location(), new long[] {0L, 1L, 2L, 3L, 4L});

    // 2. Equality DV: Delete id=10,20,30,40,50
    DeleteFile equalityDV = writeEqualityDV(new long[] {10L, 20L, 30L, 40L, 50L});

    // 3. Traditional equality delete: Delete id=15,25,35,45
    List<Record> traditionalEqDeletes = Lists.newArrayList();
    for (long id : new long[] {15L, 25L, 35L, 45L}) {
      traditionalEqDeletes.add(createRecord(id, null, null));
    }
    Schema deleteSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    OutputFile out2 = table.io().newOutputFile(table.locationProvider().newDataLocation("delete-file"));
    DeleteFile traditionalEqDelete =
        FileHelpers.writeDeleteFile(table, out2, traditionalEqDeletes, deleteSchema);

    table
        .newRowDelta()
        .addDeletes(positionDV)
        .addDeletes(equalityDV)
        .addDeletes(traditionalEqDelete)
        .commit();

    // Total deleted:
    // - Position DV: 5 (id=1-5)
    // - Equality DV: 5 (id=10,20,30,40,50)
    // - Traditional: 4 (id=15,25,35,45)
    // Total: 14 deleted, 36 remaining
    List<Record> result = readRecords();
    assertThat(result).hasSize(36);
  }

  // ==================== Partitioned Table Scenarios ====================

  @Test
  public void testMixedDeletesAcrossPartitions() throws IOException {
    // Create partitioned table
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    File partitionedTableDir = new File(tempDir, "partitioned_mixed_table");
    Table partitionedTable = TestTables.create(partitionedTableDir, "partitioned", SCHEMA, spec, 3);

    try {
      // Write data to different partitions
      List<Record> categoryA = createRecordsWithCategory(1L, 10L, "A");
      List<Record> categoryB = createRecordsWithCategory(11L, 20L, "B");
      List<Record> categoryC = createRecordsWithCategory(21L, 30L, "C");

      DataFile fileA = writeRecordsToTable(partitionedTable, categoryA);
      DataFile fileB = writeRecordsToTable(partitionedTable, categoryB);
      DataFile fileC = writeRecordsToTable(partitionedTable, categoryC);

      partitionedTable.newAppend().appendFile(fileA).appendFile(fileB).appendFile(fileC).commit();

      // Different delete types for different partitions
      // Partition A: Position DV
      DeleteFile posDV_A = writePositionDVToTable(partitionedTable, fileA.location(), new long[] {0L, 1L});

      // Partition B: Equality DV
      DeleteFile eqDV_B = writeEqualityDVToTable(partitionedTable, new long[] {11L, 12L});

      // Partition C: Traditional equality delete
      List<Record> traditionalDeletes = Lists.newArrayList();
      traditionalDeletes.add(createRecord(21L, null, "C"));
      traditionalDeletes.add(createRecord(22L, null, "C"));
      Schema deleteSchema =
          new Schema(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(3, "category", Types.StringType.get()));
      OutputFile out3 = partitionedTable.io().newOutputFile(partitionedTable.locationProvider().newDataLocation("delete-file"));
      DeleteFile traditionalDelete =
          FileHelpers.writeDeleteFile(partitionedTable, out3, traditionalDeletes, deleteSchema);

      partitionedTable
          .newRowDelta()
          .addDeletes(posDV_A)
          .addDeletes(eqDV_B)
          .addDeletes(traditionalDelete)
          .commit();

      // Each partition loses 2 records: 30 - 6 = 24
      List<Record> result =
          Lists.newArrayList(IcebergGenerics.read(partitionedTable).select("id", "category").build());
      assertThat(result).hasSize(24);

    } finally {
      // Cleanup handled by TestTables.clearTables() in @AfterEach
    }
  }

  // ==================== Sequential Delete Operations ====================

  @Test
  public void testSequentialMixedDeleteOperations() throws IOException {
    // Test adding different delete types in sequence
    List<Record> records = createRecords(1L, 100L, "sequential");
    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Step 1: Add Position DV
    DeleteFile posDV1 = writePositionDV(dataFile.location(), new long[] {0L, 1L, 2L, 3L, 4L});
    table.newRowDelta().addDeletes(posDV1).commit();

    List<Record> afterStep1 = readRecords();
    assertThat(afterStep1).hasSize(95); // 100 - 5

    // Step 2: Add Equality DV
    DeleteFile eqDV1 = writeEqualityDV(new long[] {10L, 20L, 30L});
    table.newRowDelta().addDeletes(eqDV1).commit();

    List<Record> afterStep2 = readRecords();
    assertThat(afterStep2).hasSize(92); // 95 - 3

    // Step 3: Add another Position DV
    DeleteFile posDV2 = writePositionDV(dataFile.location(), new long[] {10L, 11L});
    table.newRowDelta().addDeletes(posDV2).commit();

    List<Record> afterStep3 = readRecords();
    assertThat(afterStep3).hasSize(90); // 92 - 2

    // Step 4: Add traditional equality delete
    List<Record> traditionalDeletes = Lists.newArrayList();
    traditionalDeletes.add(createRecord(50L, null, null));
    Schema deleteSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    OutputFile out4 = table.io().newOutputFile(table.locationProvider().newDataLocation("delete-file"));
    DeleteFile traditionalDelete =
        FileHelpers.writeDeleteFile(table, out4, traditionalDeletes, deleteSchema);
    table.newRowDelta().addDeletes(traditionalDelete).commit();

    List<Record> finalResult = readRecords();
    assertThat(finalResult).hasSize(89); // 90 - 1
  }

  // ==================== Overlapping Deletes ====================

  @Test
  public void testOverlappingPositionAndEqualityDeletes() throws IOException {
    // Test when Position DV and Equality DV delete the same rows
    List<Record> records = createRecords(1L, 20L, "overlap");
    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Position DV deletes positions 0-4 (id=1-5)
    DeleteFile posDV = writePositionDV(dataFile.location(), new long[] {0L, 1L, 2L, 3L, 4L});

    // Equality DV deletes id=3,4,5,6,7 (overlap with position deletes on id=3,4,5)
    DeleteFile eqDV = writeEqualityDV(new long[] {3L, 4L, 5L, 6L, 7L});

    table.newRowDelta().addDeletes(posDV).addDeletes(eqDV).commit();

    // Deleted by position: id=1,2,3,4,5 (5 rows)
    // Deleted by equality: id=3,4,5,6,7 (5 rows, but 3,4,5 already deleted)
    // Total unique deletes: id=1,2,3,4,5,6,7 (7 rows)
    // Remaining: 20 - 7 = 13
    List<Record> result = readRecords();
    assertThat(result).hasSize(13);
  }

  // ==================== Helper Methods ====================

  private Record createRecord(Long id, String data, String category) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("category", category);
    return record;
  }

  private List<Record> createRecords(long startId, long endId, String dataPrefix) {
    List<Record> records = Lists.newArrayList();
    for (long i = startId; i <= endId; i++) {
      records.add(createRecord(i, dataPrefix + "_" + i, null));
    }
    return records;
  }

  private List<Record> createRecordsWithCategory(long startId, long endId, String category) {
    List<Record> records = Lists.newArrayList();
    for (long i = startId; i <= endId; i++) {
      records.add(createRecord(i, "data_" + i, category));
    }
    return records;
  }

  private DataFile writeRecords(List<Record> records) throws IOException {
    return writeRecordsToTable(table, records);
  }

  private DataFile writeRecordsToTable(Table targetTable, List<Record> records) throws IOException {
    OutputFile out = targetTable.io().newOutputFile(targetTable.locationProvider().newDataLocation("data-file"));
    return FileHelpers.writeDataFile(targetTable, out, records);
  }

  private DeleteFile writePositionDV(String dataFilePath, long[] positions) throws IOException {
    return writePositionDVToTable(table, dataFilePath, positions);
  }

  private DeleteFile writePositionDVToTable(Table targetTable, String dataFilePath, long[] positions)
      throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(targetTable, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory, path -> null);
    try (BitmapDeleteWriter closeableWriter = writer) {
      for (long position : positions) {
        closeableWriter.deletePosition(dataFilePath, position, targetTable.spec(), null);
      }
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private DeleteFile writeEqualityDV(long[] values) throws IOException {
    return writeEqualityDVToTable(table, values);
  }

  private DeleteFile writeEqualityDVToTable(Table targetTable, long[] values) throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(targetTable, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter closeableWriter = writer) {
      for (long value : values) {
        closeableWriter.deleteEquality(1, value, targetTable.spec(), null); // field ID 1 is "id"
      }
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private DeleteFile writeTraditionalPositionDeletes(
      String dataFilePath, List<PositionDelete<Record>> deletes) throws IOException {
    GenericFileWriterFactory writerFactory = GenericFileWriterFactory.builderFor(table).build();

    OutputFile outputFile = table.io().newOutputFile(table.locationProvider().newDataLocation("delete-file"));

    org.apache.iceberg.deletes.PositionDeleteWriter<Record> writer =
        writerFactory.newPositionDeleteWriter(
            EncryptedFiles.encryptedOutput(outputFile, (byte[]) null),
            table.spec(),
            null);

    try (org.apache.iceberg.deletes.PositionDeleteWriter<Record> closeableWriter = writer) {
      for (PositionDelete<Record> delete : deletes) {
        closeableWriter.write(delete);
      }
    }

    return writer.toDeleteFile();
  }

  private List<Record> readRecords() throws IOException {
    return Lists.newArrayList(IcebergGenerics.read(table).select("id", "data", "category").build());
  }
}
