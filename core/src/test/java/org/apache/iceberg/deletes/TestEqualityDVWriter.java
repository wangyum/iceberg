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
  public void testEqualityDeletes() throws IOException {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);

    writer.delete(1, 100L, SPEC, null);
    writer.delete(1, 200L, SPEC, null);
    writer.delete(2, 50L, SPEC, null);

    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).hasSize(2);

    DeleteFile field1Delete =
        result.deleteFiles().stream()
            .filter(df -> df.equalityFieldIds().contains(1))
            .findFirst()
            .orElseThrow();
    assertThat(field1Delete.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(field1Delete.recordCount()).isEqualTo(2);
    assertThat(field1Delete.equalityFieldIds()).containsExactly(1);

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
  public void testNegativeValueRejected() {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);

    assertThatThrownBy(() -> writer.delete(1, -1L, SPEC, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testEmptyWriter() throws IOException {
    EqualityDVWriter writer = new EqualityDVWriter(fileFactory);
    writer.close();

    DeleteWriteResult result = writer.result();
    assertThat(result.deleteFiles()).isEmpty();
  }
}
