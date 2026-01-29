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

import java.io.IOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EqualityDVWriter}.
 *
 * <p>Validates the unified writer handles both position and equality deletes correctly.
 */
public class TestEqualityDVWriter extends org.apache.iceberg.TestBase {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private OutputFileFactory fileFactory;

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    super.setupTable();
    this.fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
  }

  @Test
  public void testPositionDeletes() throws IOException {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory, path -> null);

    // Write some position deletes
    writer.deletePosition("file1.parquet", 100L, SPEC, null);
    writer.deletePosition("file1.parquet", 200L, SPEC, null);
    writer.deletePosition("file2.parquet", 50L, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(2);

    // Check file1 deletes
    DeleteFile file1Delete =
        result.deleteFiles().stream()
            .filter(df -> df.referencedDataFile().equals("file1.parquet"))
            .findFirst()
            .orElseThrow();
    assertThat(file1Delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(file1Delete.recordCount()).isEqualTo(2);
    assertThat(file1Delete.referencedDataFile()).isEqualTo("file1.parquet");

    // Check file2 deletes
    DeleteFile file2Delete =
        result.deleteFiles().stream()
            .filter(df -> df.referencedDataFile().equals("file2.parquet"))
            .findFirst()
            .orElseThrow();
    assertThat(file2Delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(file2Delete.recordCount()).isEqualTo(1);
    assertThat(file2Delete.referencedDataFile()).isEqualTo("file2.parquet");
  }

  @Test
  public void testEqualityDeletes() throws IOException {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);

    // Write some equality deletes
    writer.delete(1, 100L, SPEC, null);
    writer.delete(1, 200L, SPEC, null);
    writer.delete(2, 50L, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(2);

    // Check field 1 deletes
    DeleteFile field1Delete =
        result.deleteFiles().stream()
            .filter(df -> df.equalityFieldIds().contains(1))
            .findFirst()
            .orElseThrow();
    assertThat(field1Delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(field1Delete.recordCount()).isEqualTo(2);
    assertThat(field1Delete.equalityFieldIds()).containsExactly(1);

    // Check field 2 deletes
    DeleteFile field2Delete =
        result.deleteFiles().stream()
            .filter(df -> df.equalityFieldIds().contains(2))
            .findFirst()
            .orElseThrow();
    assertThat(field2Delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(field2Delete.recordCount()).isEqualTo(1);
    assertThat(field2Delete.equalityFieldIds()).containsExactly(2);
  }

  @Test
  public void testMixedDeleteTypes() throws IOException {
    // This test shows the writer can handle both types in same instance
    // (though in practice, you'd use separate instances)
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory, path -> null);

    // Write position deletes
    writer.deletePosition("file1.parquet", 100L, SPEC, null);

    // Write equality deletes
    writer.delete(1, 200L, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(2);

    // Has one position delete file
    assertThat(result.deleteFiles().stream().filter(df -> df.referencedDataFile() != null).count())
        .isEqualTo(1);

    // Has one equality delete file
    assertThat(
            result.deleteFiles().stream()
                .filter(df -> df.equalityFieldIds() != null && !df.equalityFieldIds().isEmpty())
                .count())
        .isEqualTo(1);
  }

  @Test
  public void testEqualityDeleteNegativeValueRejected() {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);

    assertThatThrownBy(() -> writer.delete(1, -1L, SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testEmptyWriter() throws IOException {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);

    // Close without writing any deletes
    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).isEmpty();
  }
}
