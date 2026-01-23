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
package org.apache.iceberg.deletes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link BaseEDVFileWriter}.
 *
 * <p>Tests the core writer functionality following the DV pattern.
 */
public class TestBaseEDVFileWriter extends org.apache.iceberg.TestBase {

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
  public void testSingleEqualityField() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Write some deleted values for field 1 (id)
    writer.delete(100L, 1, SPEC, null);
    writer.delete(200L, 1, SPEC, null);
    writer.delete(300L, 1, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile deleteFile = result.deleteFiles().get(0);
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFile.recordCount()).isEqualTo(3);
    assertThat(deleteFile.equalityFieldIds()).containsExactly(1);
  }

  @Test
  public void testMultipleEqualityFields() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Write deleted values for field 1 (id)
    writer.delete(100L, 1, SPEC, null);
    writer.delete(200L, 1, SPEC, null);

    // Write deleted values for field 2 (different equality field)
    writer.delete(10L, 2, SPEC, null);
    writer.delete(20L, 2, SPEC, null);
    writer.delete(30L, 2, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(2);

    // Should have one DeleteFile per equality field
    int field1Count = 0;
    int field2Count = 0;
    for (DeleteFile deleteFile : result.deleteFiles()) {
      assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);
      assertThat(deleteFile.equalityFieldIds()).hasSize(1);
      int fieldId = deleteFile.equalityFieldIds().get(0);
      if (fieldId == 1) {
        field1Count++;
        assertThat(deleteFile.recordCount()).isEqualTo(2);
      } else if (fieldId == 2) {
        field2Count++;
        assertThat(deleteFile.recordCount()).isEqualTo(3);
      }
    }

    assertThat(field1Count).isEqualTo(1);
    assertThat(field2Count).isEqualTo(1);
  }

  @Test
  public void testEmptyWriter() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Close without writing any deletes
    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).isEmpty();
  }

  @Test
  public void testNegativeValueRejected() {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    assertThatThrownBy(() -> writer.delete(-1L, 1, SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testDuplicateValues() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Write the same value multiple times
    writer.delete(100L, 1, SPEC, null);
    writer.delete(100L, 1, SPEC, null);
    writer.delete(100L, 1, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile deleteFile = result.deleteFiles().get(0);
    // Bitmap should deduplicate, so cardinality = 1
    assertThat(deleteFile.recordCount()).isEqualTo(1);
  }

  @Test
  public void testSequentialValues() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Write sequential values (should compress well with RLE)
    for (long i = 0; i < 1000; i++) {
      writer.delete(i, 1, SPEC, null);
    }

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile deleteFile = result.deleteFiles().get(0);
    assertThat(deleteFile.recordCount()).isEqualTo(1000);
    // File size should be small due to RLE compression
    assertThat(deleteFile.fileSizeInBytes()).isLessThan(1000);
  }

  @Test
  public void testSparseValues() throws IOException {
    EDVFileWriter writer = new BaseEDVFileWriter(fileFactory);

    // Write sparse values
    writer.delete(10L, 1, SPEC, null);
    writer.delete(1000L, 1, SPEC, null);
    writer.delete(10000L, 1, SPEC, null);
    writer.delete(100000L, 1, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(1);

    DeleteFile deleteFile = result.deleteFiles().get(0);
    assertThat(deleteFile.recordCount()).isEqualTo(4);
  }
}
