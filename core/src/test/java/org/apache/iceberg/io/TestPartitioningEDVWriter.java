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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.function.BiFunction;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.EqualityDelete;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link PartitioningEDVWriter}.
 *
 * <p>Validates the partitioning wrapper functionality.
 */
public class TestPartitioningEDVWriter extends org.apache.iceberg.TestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  @TempDir private File tempDir;
  private OutputFileFactory fileFactory;

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    super.setupTable();
    this.fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
  }

  @Test
  public void testBasicWrite() throws IOException {
    // Simple row type for testing
    BiFunction<TestRow, Integer, Long> extractor = (row, fieldId) -> row.getValue(fieldId);

    PartitioningEDVWriter<TestRow> writer =
        new PartitioningEDVWriter<>(fileFactory, extractor);

    // Write some deletes
    writer.write(EqualityDelete.create(new TestRow(1, 100L), 1), SPEC, null);
    writer.write(EqualityDelete.create(new TestRow(2, 200L), 1), SPEC, null);
    writer.write(EqualityDelete.create(new TestRow(3, 300L), 1), SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile deleteFile = result.deleteFiles().get(0);
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(3);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);
  }

  @Test
  public void testNullValueRejected() {
    BiFunction<TestRow, Integer, Long> extractor = (row, fieldId) -> row.getValue(fieldId);

    PartitioningEDVWriter<TestRow> writer =
        new PartitioningEDVWriter<>(fileFactory, extractor);

    // NULL value should be rejected
    assertThatThrownBy(
            () -> writer.write(EqualityDelete.create(new TestRow(1, null), 1), SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NULL");
  }

  @Test
  public void testValueExtraction() throws IOException {
    // Extractor that adds 1000 to the value
    BiFunction<TestRow, Integer, Long> extractor = (row, fieldId) -> {
      Long value = row.getValue(fieldId);
      return value != null ? value + 1000 : null;
    };

    PartitioningEDVWriter<TestRow> writer =
        new PartitioningEDVWriter<>(fileFactory, extractor);

    writer.write(EqualityDelete.create(new TestRow(1, 100L), 1), SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    // Should have written value 1100 (100 + 1000)
    DeleteFile deleteFile = result.deleteFiles().get(0);
    assertThat(deleteFile.recordCount()).isEqualTo(1);
  }

  @Test
  public void testMultipleFieldIds() throws IOException {
    BiFunction<TestRow, Integer, Long> extractor = (row, fieldId) -> row.getValue(fieldId);

    PartitioningEDVWriter<TestRow> writer =
        new PartitioningEDVWriter<>(fileFactory, extractor);

    // Write deletes for different equality fields
    writer.write(EqualityDelete.create(new TestRow(1, 100L), 1), SPEC, null);
    writer.write(EqualityDelete.create(new TestRow(2, 200L), 2), SPEC, null);
    writer.write(EqualityDelete.create(new TestRow(3, 300L), 1), SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    // Should have two DeleteFiles (one per equality field)
    assertThat(result.deleteFiles()).hasSize(2);
  }

  /** Simple test row class. */
  private static class TestRow {
    private final int id;
    private final Long value;

    TestRow(int id, Long value) {
      this.id = id;
      this.value = value;
    }

    Long getValue(int fieldId) {
      return value;
    }
  }
}
