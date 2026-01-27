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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for Equality Delete Vectors (EDV) with Spark SQL.
 *
 * <p>This test creates ACTUAL equality deletes using the programmatic API and verifies Spark SQL
 * can read tables with EDV correctly.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteVectorSparkIntegration extends SparkRowLevelOperationsTestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSparkReadWithEqualityDeleteVector() throws NoSuchTableException, IOException {
    // Format version must be >= 3 for PUFFIN support
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");

    // Write data using Spark SQL first
    StringBuilder insertData = new StringBuilder();
    for (long i = 1; i <= 10; i++) {
      insertData.append(String.format("{ \"id\": %d, \"data\": \"data_%d\" }\n", i, i));
    }
    append(tableName, insertData.toString());

    createBranchIfNeeded();

    // Verify data before EDV
    List<Object[]> countResult = sql("SELECT COUNT(*) FROM %s", selectTarget());
    assertThat(countResult.get(0)[0]).isEqualTo(10L);

    // Get table for programmatic API
    Table table = validationCatalog.loadTable(tableIdent);

    // Create EQUALITY DELETE using BitmapDeleteWriter (creates EDV!)
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter closeableWriter = writer) {
      // Delete rows where id=2, id=4, id=6 (EQUALITY deletes!)
      closeableWriter.deleteEquality(1, 2L, table.spec(), null); // field ID 1 = "id" column
      closeableWriter.deleteEquality(1, 4L, table.spec(), null);
      closeableWriter.deleteEquality(1, 6L, table.spec(), null);
    }

    DeleteFile edvFile = Iterables.getOnlyElement(writer.result().deleteFiles());

    // Apply the EDV
    table.newRowDelta().addDeletes(edvFile).commit();

    // Verify the delete file is ACTUAL equality delete
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    assertThat(deleteFiles).hasSize(1);
    DeleteFile actualEDV = deleteFiles.get(0);

    System.out.println("\n=== ACTUAL EQUALITY DELETE VECTOR ===");
    System.out.println("Format: " + actualEDV.format());
    System.out.println("Content: " + actualEDV.content());
    System.out.println("Equality field IDs: " + actualEDV.equalityFieldIds());
    System.out.println("Record count: " + actualEDV.recordCount());
    System.out.println("File size: " + actualEDV.fileSizeInBytes() + " bytes");
    System.out.println("======================================");

    // Verify it's actually an equality delete
    assertThat(actualEDV.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(actualEDV.content().toString()).isEqualTo("EQUALITY_DELETES");
    assertThat(actualEDV.equalityFieldIds()).containsExactly(1);
    assertThat(actualEDV.recordCount()).isEqualTo(3);

    // THIS IS THE KEY TEST: Verify Spark SQL can read the table with EDV correctly
    assertEquals(
        "Should have 7 rows after equality deletes",
        ImmutableList.of(
            row(1L, "data_1"),
            row(3L, "data_3"),
            row(5L, "data_5"),
            row(7L, "data_7"),
            row(8L, "data_8"),
            row(9L, "data_9"),
            row(10L, "data_10")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testSparkReadWithMultipleEDVFiles() throws NoSuchTableException, IOException {
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");
    Table table = validationCatalog.loadTable(tableIdent);

    // Write data
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 20; i++) {
      Record record = GenericRecord.create(SCHEMA);
      record.setField("id", i);
      record.setField("data", "data_" + i);
      records.add(record);
    }

    OutputFile dataOut =
        table.io().newOutputFile(table.locationProvider().newDataLocation("data-file"));
    DataFile dataFile = FileHelpers.writeDataFile(table, dataOut, records);
    table.newAppend().appendFile(dataFile).commit();

    createBranchIfNeeded();

    // Create first EDV file
    OutputFileFactory fileFactory1 =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    BitmapDeleteWriter writer1 = new BitmapDeleteWriter(fileFactory1);
    try (BitmapDeleteWriter w = writer1) {
      w.deleteEquality(1, 2L, table.spec(), null);
      w.deleteEquality(1, 4L, table.spec(), null);
    }
    table.newRowDelta().addDeletes(Iterables.getOnlyElement(writer1.result().deleteFiles())).commit();

    // Create second EDV file
    OutputFileFactory fileFactory2 =
        OutputFileFactory.builderFor(table, 1, 2).format(FileFormat.PUFFIN).build();
    BitmapDeleteWriter writer2 = new BitmapDeleteWriter(fileFactory2);
    try (BitmapDeleteWriter w = writer2) {
      w.deleteEquality(1, 6L, table.spec(), null);
      w.deleteEquality(1, 8L, table.spec(), null);
      w.deleteEquality(1, 10L, table.spec(), null);
    }
    table.newRowDelta().addDeletes(Iterables.getOnlyElement(writer2.result().deleteFiles())).commit();

    // Verify Spark can read with multiple EDV files
    List<Object[]> result = sql("SELECT * FROM %s WHERE id <= 10 ORDER BY id", selectTarget());
    assertThat(result).hasSize(5);
    assertThat(result.get(0)[0]).isEqualTo(1L);
    assertThat(result.get(1)[0]).isEqualTo(3L);
    assertThat(result.get(2)[0]).isEqualTo(5L);
    assertThat(result.get(3)[0]).isEqualTo(7L);
    assertThat(result.get(4)[0]).isEqualTo(9L);

    System.out.println("\n=== Spark SQL successfully read table with multiple EDV files ===");
  }

  @TestTemplate
  public void testEDVCompressionBenefits() throws NoSuchTableException, IOException {
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");
    Table table = validationCatalog.loadTable(tableIdent);

    // Write 1000 rows
    List<Record> records = Lists.newArrayList();
    for (long i = 1; i <= 1000; i++) {
      Record record = GenericRecord.create(SCHEMA);
      record.setField("id", i);
      record.setField("data", "data_" + i);
      records.add(record);
    }

    OutputFile dataOut =
        table.io().newOutputFile(table.locationProvider().newDataLocation("data-file"));
    DataFile dataFile = FileHelpers.writeDataFile(table, dataOut, records);
    table.newAppend().appendFile(dataFile).commit();

    createBranchIfNeeded();

    // Delete 100 scattered rows using EDV
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter w = writer) {
      for (long i = 0; i < 100; i++) {
        w.deleteEquality(1, i * 10 + 2, table.spec(), null); // 2, 12, 22, 32, ..., 992
      }
    }

    DeleteFile edvFile = Iterables.getOnlyElement(writer.result().deleteFiles());
    table.newRowDelta().addDeletes(edvFile).commit();

    System.out.println("\n=== EDV COMPRESSION BENEFITS ===");
    System.out.println("Deleted 100 rows from 1000 total rows");
    System.out.println("EDV file size: " + edvFile.fileSizeInBytes() + " bytes");
    System.out.println("Bytes per delete: " + (edvFile.fileSizeInBytes() / 100.0) + " bytes");
    System.out.println("================================");

    // EDV file should be very small due to bitmap compression
    assertThat(edvFile.fileSizeInBytes())
        .as("EDV should use < 50 bytes per delete due to bitmap compression")
        .isLessThan(5000);

    // Verify Spark reads correctly
    long count = (long) sql("SELECT COUNT(*) FROM %s", selectTarget()).get(0)[0];
    assertThat(count).isEqualTo(900);
  }
}
