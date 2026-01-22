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
package org.apache.iceberg.spark.source;

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
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for Equality Delete Vector (EDV) functionality in Spark.
 *
 * <p>These tests verify that Spark's FileWriterFactory correctly:
 * 1. Creates EDV (PUFFIN) files when conditions are met
 * 2. Falls back to traditional Parquet when EDV cannot be used
 * 3. Reads data correctly with EDV deletes applied
 * 4. Handles mixed EDV and traditional delete files
 */
public class TestSparkEqualityDeleteVectors {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private static final Schema SCHEMA_WITH_STRING_ID =
      new Schema(
          Types.NestedField.required(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private Table table;
  private StructType sparkType;

  @BeforeEach
  public void setupTable() {
    // Create table with format version 3 (required for Puffin support)
    this.table =
        TestTables.create(
            tableDir,
            "test_spark_edv",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(
                TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true",
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

    this.sparkType = SparkSchemaUtil.convert(SCHEMA);
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testSparkWritesEDVForLongField() throws IOException {
    // Write data using Spark InternalRow
    List<InternalRow> dataRows =
        ImmutableList.of(
            createRow(1L, "a"),
            createRow(2L, "b"),
            createRow(3L, "c"),
            createRow(4L, "d"),
            createRow(5L, "e"));

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Write equality deletes (should use EDV format)
    List<InternalRow> deleteRows = ImmutableList.of(createRow(2L, null), createRow(4L, null));

    DeleteFile deleteFile = writeEqualityDeleteFile(deleteRows);

    // Verify: Delete file should be PUFFIN format (EDV), not PARQUET
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(2L);

    // Apply deletes and verify correctness
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Read and verify: should have 3 rows (1, 3, 5)
    List<Record> result = readAllRecords();
    assertThat(result).hasSize(3);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(1L, 3L, 5L);
  }

  @Test
  public void testSparkFallbackToParquetForStringField() throws IOException {
    // Create table with STRING id (not supported by EDV)
    Table stringTable =
        TestTables.create(
            new File(tableDir, "string_table"),
            "test_string_id",
            SCHEMA_WITH_STRING_ID,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(
                TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true",
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

    StructType stringSparkType = SparkSchemaUtil.convert(SCHEMA_WITH_STRING_ID);

    // Write data
    List<InternalRow> dataRows =
        ImmutableList.of(
            createStringRow("a", "data1"),
            createStringRow("b", "data2"),
            createStringRow("c", "data3"));

    DataFile dataFile = writeDataFile(stringTable, stringSparkType, dataRows);
    stringTable.newAppend().appendFile(dataFile).commit();

    // Write equality deletes - should fall back to PARQUET (STRING field, not LONG)
    List<InternalRow> deleteRows = ImmutableList.of(createStringRow("b", null));

    DeleteFile deleteFile = writeEqualityDeleteFile(stringTable, stringSparkType, deleteRows);

    // Verify: Should fall back to PARQUET (EDV only supports LONG)
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PARQUET);

    TestTables.clearTables();
  }

  @Test
  public void testSparkReadWithMixedEDVAndParquetDeletes() throws IOException {
    // Write data
    List<InternalRow> dataRows =
        ImmutableList.of(
            createRow(1L, "a"),
            createRow(2L, "b"),
            createRow(3L, "c"),
            createRow(4L, "d"),
            createRow(5L, "e"),
            createRow(6L, "f"));

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Write EDV delete file (deletes 2, 4)
    List<InternalRow> edvDeleteRows = ImmutableList.of(createRow(2L, null), createRow(4L, null));
    DeleteFile edvDeleteFile = writeEqualityDeleteFile(edvDeleteRows);
    assertThat(edvDeleteFile.format()).isEqualTo(FileFormat.PUFFIN); // EDV format

    // Disable EDV temporarily to force Parquet
    table
        .updateProperties()
        .set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "false")
        .commit();

    // Write traditional Parquet delete file (deletes 1, 5)
    List<InternalRow> parquetDeleteRows =
        ImmutableList.of(createRow(1L, null), createRow(5L, null));
    DeleteFile parquetDeleteFile = writeEqualityDeleteFile(parquetDeleteRows);
    assertThat(parquetDeleteFile.format()).isEqualTo(FileFormat.PARQUET); // Traditional format

    // Apply both delete files
    table.newRowDelta().addDeletes(edvDeleteFile).addDeletes(parquetDeleteFile).commit();

    // Read and verify: should have 2 rows (3, 6) - both EDV and Parquet deletes applied
    List<Record> result = readAllRecords();
    assertThat(result).hasSize(2);
    assertThat(result).extracting(r -> r.getField("id")).containsExactlyInAnyOrder(3L, 6L);
  }

  @Test
  public void testSparkEDVWithLargeDeleteSet() throws IOException {
    // Write 10,000 records
    List<InternalRow> dataRows = Lists.newArrayList();
    for (long i = 0; i < 10000; i++) {
      dataRows.add(createRow(i, "data" + i));
    }

    DataFile dataFile = writeDataFile(dataRows);
    table.newAppend().appendFile(dataFile).commit();

    // Delete 1,000 records using EDV
    List<InternalRow> deleteRows = Lists.newArrayList();
    for (long i = 0; i < 1000; i++) {
      deleteRows.add(createRow(i, null));
    }

    DeleteFile deleteFile = writeEqualityDeleteFile(deleteRows);

    // Verify: EDV format and compression
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(1000L);

    // EDV file should be much smaller than Parquet would be
    // For sequential IDs, expect ~1KB instead of ~100KB
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(10_000L);

    // Apply deletes and verify correctness
    table.newRowDelta().addDeletes(deleteFile).commit();

    // Read and verify: should have 9,000 rows
    long count = countRecords();
    assertThat(count).isEqualTo(9000L);
  }

  // Helper methods

  private InternalRow createRow(Long id, String data) {
    GenericInternalRow row = new GenericInternalRow(2);
    row.update(0, id);
    row.update(1, data == null ? null : UTF8String.fromString(data));
    return row;
  }

  private InternalRow createStringRow(String id, String data) {
    GenericInternalRow row = new GenericInternalRow(2);
    row.update(0, id == null ? null : UTF8String.fromString(id));
    row.update(1, data == null ? null : UTF8String.fromString(data));
    return row;
  }

  private DataFile writeDataFile(List<InternalRow> rows) throws IOException {
    return writeDataFile(table, sparkType, rows);
  }

  private DataFile writeDataFile(Table targetTable, StructType type, List<InternalRow> rows)
      throws IOException {
    OutputFile outputFile = targetTable.io().newOutputFile(targetTable.location() + "/data.parquet");

    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(targetTable)
            .dataSchema(targetTable.schema())
            .dataSparkType(type)
            .dataFileFormat(FileFormat.PARQUET)
            .build();

    org.apache.iceberg.io.DataWriter<InternalRow> writer =
        writerFactory.newDataWriter(
            targetTable.io().newEncryptedOutputFile(outputFile.location()),
            targetTable.spec(),
            null);

    try {
      for (InternalRow row : rows) {
        writer.write(row);
      }
    } finally {
      writer.close();
    }

    return writer.toDataFile();
  }

  private DeleteFile writeEqualityDeleteFile(List<InternalRow> rows) throws IOException {
    return writeEqualityDeleteFile(table, sparkType, rows);
  }

  private DeleteFile writeEqualityDeleteFile(
      Table targetTable, StructType type, List<InternalRow> rows) throws IOException {
    OutputFile outputFile =
        targetTable.io().newOutputFile(targetTable.location() + "/delete-" + System.nanoTime());

    Schema deleteSchema = targetTable.schema().select("id");
    StructType deleteSparkType = SparkSchemaUtil.convert(deleteSchema);

    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(targetTable)
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(new int[] {1}) // id field
            .equalityDeleteRowSchema(deleteSchema)
            .equalityDeleteSparkType(deleteSparkType)
            .build();

    org.apache.iceberg.deletes.EqualityDeleteWriter<InternalRow> writer =
        writerFactory.newEqualityDeleteWriter(
            targetTable.io().newEncryptedOutputFile(outputFile.location()),
            targetTable.spec(),
            null);

    try {
      for (InternalRow row : rows) {
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
