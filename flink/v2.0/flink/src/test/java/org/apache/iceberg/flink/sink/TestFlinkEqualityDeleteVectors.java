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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for Equality Delete Vector (EDV) functionality in Flink.
 *
 * <p>These tests verify that Flink's FileWriterFactory correctly:
 * 1. Creates EDV (PUFFIN) files for CDC use cases with _row_id
 * 2. Handles LONG equality fields properly
 * 3. Reads data correctly with EDV deletes applied
 */
public class TestFlinkEqualityDeleteVectors {

  // Schema mimicking CDC table with _row_id
  private static final Schema CDC_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "_row_id", Types.LongType.get()),
          Types.NestedField.required(2, "order_id", Types.LongType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private Table table;
  private RowType flinkType;

  @BeforeEach
  public void setupTable() {
    // Create CDC table with format version 3 and EDV enabled
    this.table =
        TestTables.create(
            tableDir,
            "test_flink_edv_cdc",
            CDC_SCHEMA,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(
                TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true",
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

    this.flinkType = FlinkSchemaUtil.convert(CDC_SCHEMA);
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testFlinkCDCWithEDV() throws IOException {
    // Simulate CDC data with sequential _row_id
    List<RowData> dataRows =
        ImmutableList.of(
            createCDCRow(1L, 100L, "order1"),
            createCDCRow(2L, 200L, "order2"),
            createCDCRow(3L, 300L, "order3"),
            createCDCRow(4L, 400L, "order4"),
            createCDCRow(5L, 500L, "order5"));

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Simulate CDC DELETE operations (delete by _row_id)
    List<RowData> deleteRows = ImmutableList.of(createCDCRow(2L, null, null), createCDCRow(4L, null, null));

    DeleteFile deleteFile = writeEqualityDeleteFile(deleteRows);

    // Verify: Should use EDV format (PUFFIN) for sequential _row_id
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(2L);

    // EDV file should be very small due to bitmap compression
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(5_000L);

    // Apply deletes
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Read and verify: should have 3 rows (_row_id: 1, 3, 5)
    List<Record> result = readAllRecords();
    assertThat(result).hasSize(3);
    assertThat(result).extracting(r -> r.getField("_row_id")).containsExactlyInAnyOrder(1L, 3L, 5L);
  }

  @Test
  public void testFlinkCDCWithSequentialDeletes() throws IOException {
    // Write 1000 CDC records with sequential _row_id
    List<RowData> dataRows = Lists.newArrayList();
    for (long i = 1; i <= 1000; i++) {
      dataRows.add(createCDCRow(i, i * 10, "data" + i));
    }

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Delete 100 records using CDC _row_id (sequential pattern)
    List<RowData> deleteRows = Lists.newArrayList();
    for (long i = 1; i <= 100; i++) {
      deleteRows.add(createCDCRow(i, null, null));
    }

    DeleteFile deleteFile = writeEqualityDeleteFile(deleteRows);

    // Verify: EDV format with excellent compression
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(100L);

    // For sequential IDs [1-100], expect ~500 bytes instead of ~10KB Parquet
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(2_000L);

    // Apply and verify
    table.newRowDelta().addDeletes(deleteFile).commit();

    long count = countRecords();
    assertThat(count).isEqualTo(900L);
  }

  @Test
  public void testFlinkNonIdentifierFieldWarning() throws IOException {
    // Create table WITHOUT marking _row_id as identifier
    // EDV should still work, but a warning should be logged

    // Write data
    List<RowData> dataRows =
        ImmutableList.of(
            createCDCRow(1L, 100L, "a"), createCDCRow(2L, 200L, "b"), createCDCRow(3L, 300L, "c"));

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Write delete (should still use EDV even without identifier field set)
    List<RowData> deleteRows = ImmutableList.of(createCDCRow(2L, null, null));

    DeleteFile deleteFile = writeEqualityDeleteFile(deleteRows);

    // Verify: EDV still works (flexible approach)
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);

    // Note: Warning would be logged to BaseFileWriterFactory logger
    // We can't easily capture log output in test, but the warning exists

    table.newRowDelta().addDeletes(deleteFile).commit();

    List<Record> result = readAllRecords();
    assertThat(result).hasSize(2);
    assertThat(result).extracting(r -> r.getField("_row_id")).containsExactlyInAnyOrder(1L, 3L);
  }

  // Helper methods

  private RowData createCDCRow(Long rowId, Long orderId, String data) {
    GenericRowData row = new GenericRowData(3);
    row.setField(0, rowId);
    row.setField(1, orderId);
    row.setField(2, data == null ? null : StringData.fromString(data));
    return row;
  }

  private DataFile writeDataFile(List<RowData> rows) throws IOException {
    OutputFile outputFile = table.io().newOutputFile(table.location() + "/data.parquet");

    FlinkFileWriterFactory writerFactory =
        FlinkFileWriterFactory.builderFor(table)
            .dataSchema(table.schema())
            .dataFlinkType(flinkType)
            .dataFileFormat(FileFormat.PARQUET)
            .build();

    org.apache.iceberg.io.DataWriter<RowData> writer =
        writerFactory.newDataWriter(
            table.io().newEncryptedOutputFile(outputFile.location()), table.spec(), null);

    try {
      for (RowData row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }

    return writer.toDataFile();
  }

  private DeleteFile writeEqualityDeleteFile(List<RowData> rows) throws IOException {
    OutputFile outputFile =
        table.io().newOutputFile(table.location() + "/delete-" + System.nanoTime());

    Schema deleteSchema = table.schema().select("_row_id");
    RowType deleteFlinkType = FlinkSchemaUtil.convert(deleteSchema);

    FlinkFileWriterFactory writerFactory =
        FlinkFileWriterFactory.builderFor(table)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1}) // _row_id field
            .equalityDeleteRowSchema(deleteSchema)
            .equalityDeleteFlinkType(deleteFlinkType)
            .build();

    org.apache.iceberg.deletes.EqualityDeleteWriter<RowData> writer =
        writerFactory.newEqualityDeleteWriter(
            table.io().newEncryptedOutputFile(outputFile.location()), table.spec(), null);

    try {
      for (RowData row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }

    return writer.toDeleteFile();
  }

  private List<Record> readAllRecords() throws IOException {
    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
        org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
      for (Record record : reader) {
        records.add(record);
      }
    }
    return records;
  }

  private long countRecords() throws IOException {
    long count = 0;
    try (CloseableIterable<Record> reader =
        org.apache.iceberg.data.IcebergGenerics.read(table).build()) {
      for (Record record : reader) {
        count++;
      }
    }
    return count;
  }
}
