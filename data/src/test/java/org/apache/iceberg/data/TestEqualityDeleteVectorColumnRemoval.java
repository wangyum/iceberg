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
 * Tests behavior of Equality Delete Vectors when columns are removed from the schema.
 *
 * <p>This test verifies what happens when:
 *
 * <ul>
 *   <li>EDVs exist that reference a specific column's field ID
 *   <li>That column is subsequently removed from the schema
 *   <li>Scans are performed that need to apply those EDVs
 * </ul>
 */
public class TestEqualityDeleteVectorColumnRemoval {

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
    tableDir = new File(tempDir, "column_removal_test");
    table = TestTables.create(tableDir, "test", SCHEMA, PartitionSpec.unpartitioned(), 3);
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testRemoveColumnWithExistingEDV() throws IOException {
    // Write data
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 10; i++) {
      records.add(createRecord(i, "data_" + i, "category_A"));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Create EDV that deletes rows with id=5 and id=7
    DeleteFile edv = writeEqualityDV(new long[] {5L, 7L});
    table.newRowDelta().addDeletes(edv).commit();

    // Verify EDV works before schema change
    List<Record> resultBefore = readRecords();
    assertThat(resultBefore).hasSize(8); // 10 - 2 = 8

    // Remove the 'category' column (field ID 3) - NOT the equality field
    table.updateSchema().deleteColumn("category").commit();

    // EDV should still work (it references field ID 1, not 3)
    List<Record> resultAfterNonEqColumn = readRecords();
    assertThat(resultAfterNonEqColumn).hasSize(8); // Still 8 records

    // Now test removing the actual equality field that EDV depends on
    // Question: What happens when we remove the column that the EDV references?
    table.updateSchema().deleteColumn("id").commit();

    // Let's see what actually happens
    try {
      List<Record> resultAfterRemoval = readRecords();
      System.out.println("SUCCESS: Read " + resultAfterRemoval.size() + " records after removing equality column");
      System.out.println("Records: " + resultAfterRemoval);
      // If we get here, Iceberg handles the missing column gracefully
      assertThat(resultAfterRemoval).hasSize(10); // All 10 records, EDV ignored?
    } catch (Exception e) {
      System.out.println("FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
      throw e;
    }
  }

  @Test
  public void testTraditionalEqualityDeleteAfterColumnRemoval() throws IOException {
    // For comparison: test traditional (Parquet) equality deletes with column removal
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 10; i++) {
      records.add(createRecord(i, "data_" + i, "category_A"));
    }

    DataFile dataFile = writeRecords(records);
    table.newAppend().appendFile(dataFile).commit();

    // Create traditional equality delete (Parquet format)
    DeleteFile traditionalDelete = writeTraditionalEqualityDelete(new long[] {3L, 6L});
    table.newRowDelta().addDeletes(traditionalDelete).commit();

    // Verify it works before schema change
    List<Record> resultBefore = readRecords();
    assertThat(resultBefore).hasSize(8); // 10 - 2 = 8

    // Remove the equality field
    table.updateSchema().deleteColumn("id").commit();

    // Let's see what actually happens with traditional deletes
    try {
      List<Record> resultAfterRemoval = readRecords();
      System.out.println("TRADITIONAL DELETE SUCCESS: Read " + resultAfterRemoval.size() +
          " records after removing equality column");
      // If we get here, behavior is different from EDV
      assertThat(resultAfterRemoval).isNotNull();
    } catch (NullPointerException e) {
      System.out.println("TRADITIONAL DELETE FAILED: NullPointerException - " + e.getMessage());
      throw e;
    } catch (Exception e) {
      System.out.println("TRADITIONAL DELETE FAILED: " + e.getClass().getSimpleName() + " - " + e.getMessage());
      throw e;
    }
  }

  // ==================== Helper Methods ====================

  private Record createRecord(long id, String data, String category) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("category", category);
    return record;
  }

  private DataFile writeRecords(List<Record> records) throws IOException {
    OutputFile out =
        table
            .io()
            .newOutputFile(table.locationProvider().newDataLocation("data-" + System.nanoTime()));
    return FileHelpers.writeDataFile(table, out, records);
  }

  private DeleteFile writeEqualityDV(long[] idsToDelete) throws IOException {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter closeableWriter = writer) {
      for (long id : idsToDelete) {
        closeableWriter.deleteEquality(1, id, table.spec(), null); // field ID 1 = "id" column
      }
    }

    return Iterables.getOnlyElement(writer.result().deleteFiles());
  }

  private DeleteFile writeTraditionalEqualityDelete(long[] idsToDelete) throws IOException {
    // Write traditional Parquet-based equality delete
    List<Record> deletes = Lists.newArrayList();
    Schema deleteSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    for (long id : idsToDelete) {
      Record deleteRecord = GenericRecord.create(deleteSchema);
      deleteRecord.setField("id", id);
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

  private List<Record> readRecords() {
    return Lists.newArrayList(
        IcebergGenerics.read(table).select("id", "data").build().iterator());
  }
}
