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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for schema evolution with Equality Delete Vectors.
 *
 * <p>Verifies that:
 *
 * <ul>
 *   <li>Type narrowing (LONG → INT) is blocked to protect EDV compatibility
 *   <li>Type widening (INT → LONG) is allowed and automatically uses EDVs
 *   <li>Existing EDVs remain valid after schema changes
 *   <li>Mixed delete file formats (traditional + EDV) work correctly
 * </ul>
 */
public class TestEqualityDeleteVectorSchemaEvolution {

  private static final Schema LONG_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final Schema INT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private File tableDir;

  @BeforeEach
  public void setup() {
    tableDir = new File(tempDir, "test_table");
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  // ==================== Type Narrowing Tests (LONG → INT) ====================

  @Test
  public void testLongToIntSchemaChangeBlocked() {
    // LONG → INT type narrowing should be blocked by schema evolution
    Table table =
        TestTables.create(tableDir, "test", LONG_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    assertThatThrownBy(
            () -> {
              table.updateSchema().updateColumn("id", Types.IntegerType.get()).commit();
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type: id: long -> int");
  }

  @Test
  public void testLongToIntBlockedEvenWithExistingEDVs() throws IOException {
    // Even with existing EDVs, LONG → INT should be blocked
    Table table = TestTables.create(tableDir, "test", LONG_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data
    List<Record> records = createLongRecords(1L, 10L);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    // Write EDV (automatically created for LONG equality field in V3)
    DeleteFile edv = writeEqualityDV(table, new long[] {1L, 2L, 3L});
    table.newRowDelta().addDeletes(edv).commit();

    // Verify EDV was created
    assertThat(edv.format()).isEqualTo(FileFormat.PUFFIN);

    // Attempt schema change: LONG → INT should still be blocked
    assertThatThrownBy(
            () -> {
              table.updateSchema().updateColumn("id", Types.IntegerType.get()).commit();
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change column type: id: long -> int");

    // Verify EDV still works after failed schema change attempt
    List<Record> result = readRecords(table);
    assertThat(result).hasSize(7); // 10 - 3 deleted = 7
    assertThat(result)
        .extracting(r -> r.getField("id"))
        .doesNotContain(1L, 2L, 3L)
        .contains(4L, 5L, 6L, 7L, 8L, 9L, 10L);
  }

  @Test
  public void testLongToIntBlockedPreservesTableState() throws IOException {
    // Verify that failed schema change doesn't corrupt table
    Table table = TestTables.create(tableDir, "test", LONG_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data and EDV
    List<Record> records = createLongRecords(1L, 5L);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    DeleteFile edv = writeEqualityDV(table, new long[] {1L});
    table.newRowDelta().addDeletes(edv).commit();

    long snapshotIdBefore = table.currentSnapshot().snapshotId();
    Schema schemaBefore = table.schema();

    // Attempt schema change (should fail)
    try {
      table.updateSchema().updateColumn("id", Types.IntegerType.get()).commit();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    // Verify table state unchanged
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotIdBefore);
    assertThat(table.schema()).isEqualTo(schemaBefore);
    assertThat(table.schema().findField("id").type()).isEqualTo(Types.LongType.get());

    // Verify reads still work
    List<Record> result = readRecords(table);
    assertThat(result).hasSize(4); // 5 - 1 deleted
  }

  // ==================== Type Widening Tests (INT → LONG) ====================

  @Test
  public void testIntToLongSchemaChangeAllowed() {
    // INT → LONG type widening should be allowed
    Table table = TestTables.create(tableDir, "test", INT_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Schema change should succeed
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

    // Verify schema changed
    assertThat(table.schema().findField("id").type()).isEqualTo(Types.LongType.get());
  }

  @Test
  public void testIntToLongAutomaticallyUsesEDVs() throws IOException {
    // After INT → LONG, new equality deletes should use EDVs automatically
    Table table = TestTables.create(tableDir, "test", INT_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data with INT type
    List<Record> records = createIntRecords(1, 10);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    // Write traditional equality delete (INT field, no EDV in V3 for INT)
    DeleteFile traditionalDelete = writeTraditionalEqualityDelete(table, new int[] {1, 2});
    table.newRowDelta().addDeletes(traditionalDelete).commit();
    assertThat(traditionalDelete.format()).isEqualTo(FileFormat.PARQUET);

    // Schema change: INT → LONG
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

    // New equality deletes should use EDV (LONG field in V3)
    DeleteFile edv = writeEqualityDV(table, new long[] {3L, 4L});
    table.newRowDelta().addDeletes(edv).commit();
    assertThat(edv.format()).isEqualTo(FileFormat.PUFFIN);

    // Verify both delete types work together
    List<Record> result = readRecords(table);
    assertThat(result).hasSize(6); // 10 - 4 deleted (1,2,3,4)
    assertThat(result)
        .extracting(r -> r.getField("id"))
        .doesNotContain(1L, 2L, 3L, 4L)
        .contains(5L, 6L, 7L, 8L, 9L, 10L);
  }

  @Test
  public void testIntToLongPreservesTraditionalDeletes() throws IOException {
    // Traditional equality deletes should still work after INT → LONG
    Table table = TestTables.create(tableDir, "test", INT_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data and traditional deletes with INT type
    List<Record> records = createIntRecords(1, 20);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    DeleteFile traditionalDelete1 = writeTraditionalEqualityDelete(table, new int[] {5, 10, 15});
    table.newRowDelta().addDeletes(traditionalDelete1).commit();

    // Verify traditional deletes work before schema change
    List<Record> resultBefore = readRecords(table);
    assertThat(resultBefore).hasSize(17); // 20 - 3

    // Schema change: INT → LONG
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

    // Verify traditional deletes still work after schema change
    List<Record> resultAfter = readRecords(table);
    assertThat(resultAfter).hasSize(17); // Still 20 - 3
    assertThat(resultAfter).extracting(r -> r.getField("id")).doesNotContain(5L, 10L, 15L);
  }

  @Test
  public void testIntToLongMixedDeleteFileFormats() throws IOException {
    // Test that after INT → LONG schema change, new deletes use EDVs
    Table table = TestTables.create(tableDir, "test", INT_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data
    List<Record> records = createIntRecords(1, 100);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    // Schema change: INT → LONG
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

    // After schema change: EDV deletes (LONG type)
    DeleteFile edv1 = writeEqualityDV(table, new long[] {10L, 20L, 30L});
    DeleteFile edv2 = writeEqualityDV(table, new long[] {40L, 50L, 60L});
    table.newRowDelta().addDeletes(edv1).addDeletes(edv2).commit();

    assertThat(edv1.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(edv2.format()).isEqualTo(FileFormat.PUFFIN);

    // Verify EDV deletes work: 6 deletes
    List<Record> result = readRecords(table);
    assertThat(result).hasSize(94); // 100 - 6
    assertThat(result)
        .extracting(r -> r.getField("id"))
        .doesNotContain(10L, 20L, 30L, 40L, 50L, 60L);
  }

  // ==================== No-Op Schema Changes ====================

  @Test
  public void testLongToLongNoOpPreservesEDVs() throws IOException {
    // Changing LONG → LONG (no-op) should preserve EDV functionality
    Table table = TestTables.create(tableDir, "test", LONG_SCHEMA, org.apache.iceberg.PartitionSpec.unpartitioned(), 3);

    // Write data and EDV
    List<Record> records = createLongRecords(1L, 10L);
    DataFile dataFile = writeRecords(table, records);
    table.newAppend().appendFile(dataFile).commit();

    DeleteFile edv = writeEqualityDV(table, new long[] {1L, 5L, 10L});
    table.newRowDelta().addDeletes(edv).commit();

    // No-op schema change (LONG → LONG)
    table.updateSchema().updateColumn("id", Types.LongType.get()).commit();

    // Verify EDVs still work
    List<Record> result = readRecords(table);
    assertThat(result).hasSize(7); // 10 - 3
    assertThat(result).extracting(r -> r.getField("id")).doesNotContain(1L, 5L, 10L);
  }

  // ==================== Helper Methods ====================

  private List<Record> createLongRecords(long start, long end) {
    List<Record> records = Lists.newArrayList();
    for (long i = start; i <= end; i++) {
      Record record = GenericRecord.create(LONG_SCHEMA);
      record.setField("id", i);
      record.setField("data", "data_" + i);
      records.add(record);
    }
    return records;
  }

  private List<Record> createIntRecords(int start, int end) {
    List<Record> records = Lists.newArrayList();
    for (int i = start; i <= end; i++) {
      Record record = GenericRecord.create(INT_SCHEMA);
      record.setField("id", i);
      record.setField("data", "data_" + i);
      records.add(record);
    }
    return records;
  }

  private DataFile writeRecords(Table table, List<Record> records) throws IOException {
    OutputFile out =
        table.io().newOutputFile(table.locationProvider().newDataLocation("data-file"));
    return FileHelpers.writeDataFile(table, out, records);
  }

  private DeleteFile writeEqualityDV(Table table, long[] values) throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter closeableWriter = writer) {
      for (long value : values) {
        closeableWriter.deleteEquality(1, value, table.spec(), null); // field ID 1 is "id"
      }
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private DeleteFile writeTraditionalEqualityDelete(Table table, int[] values) throws IOException {
    // Write traditional Parquet-based equality delete
    List<Record> deletes = Lists.newArrayList();
    Schema deleteSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    for (int value : values) {
      Record deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("id", value);
      deletes.add(deleteRecord);
    }

    int[] equalityFieldIds = new int[] {1};
    GenericFileWriterFactory writerFactory =
        GenericFileWriterFactory.builderFor(table)
            .equalityDeleteRowSchema(deleteSchema)
            .equalityFieldIds(equalityFieldIds)
            .build();

    OutputFile out =
        table.io().newOutputFile(table.locationProvider().newDataLocation("delete-file"));

    EqualityDeleteWriter<Record> writer =
        writerFactory.newEqualityDeleteWriter(
            EncryptedFiles.encryptedOutput(out, (byte[]) null), table.spec(), null);

    try (EqualityDeleteWriter<Record> closeableWriter = writer) {
      closeableWriter.write(deletes);
    }

    return writer.toDeleteFile();
  }

  private List<Record> readRecords(Table table) throws IOException {
    return Lists.newArrayList(IcebergGenerics.read(table).select("id", "data").build());
  }
}
