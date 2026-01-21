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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEqualityDeleteVectorWriter {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final int EQUALITY_FIELD_ID = 1;

  @TempDir private File tempDir;

  private PartitionSpec spec;

  @BeforeEach
  public void before() {
    spec = PartitionSpec.unpartitioned();
  }

  @Test
  public void testWriteValidValues() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    // Write valid deletes
    writer.write(record(100L, "a"));
    writer.write(record(200L, "b"));
    writer.write(record(300L, "c"));
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    assertThat(result).isNotNull();
    assertThat(result.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(result.recordCount()).isEqualTo(3);
    assertThat(result.equalityFieldIds()).containsExactly(EQUALITY_FIELD_ID);

    // Verify Puffin blob was written correctly
    verifyPuffinBlob(result, 100L, 300L, 3);
  }

  @Test
  public void testRejectNegativeValues() {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    assertThatThrownBy(() -> writer.write(record(-100L, "test")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  public void testRejectNullValues() {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    assertThatThrownBy(() -> writer.write(record(null, "test")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NULL");
  }

  @Test
  public void testMinMaxTracking() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    writer.write(record(500L, "a"));
    writer.write(record(100L, "b"));
    writer.write(record(1000L, "c"));
    writer.write(record(250L, "d"));
    writer.close();

    DeleteFile result = writer.toDeleteFile();

    // Verify min/max in blob metadata
    verifyPuffinBlob(result, 100L, 1000L, 4);
  }

  @Test
  public void testEmptyBitmap() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    assertThat(result).isNull(); // No deletes written
  }

  @Test
  public void testLargeValues() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    // Test with large LONG values
    // Use values that are large but don't cause excessive memory allocation
    // Roaring bitmap uses the upper 4 bytes as key, so keep keys reasonable
    long largeValue1 = 1_000_000_000_000L; // 1 trillion
    long largeValue2 = 999_999_999_999L; // ~1 trillion
    writer.write(record(largeValue1, "a"));
    writer.write(record(largeValue2, "b"));
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    assertThat(result).isNotNull();
    assertThat(result.recordCount()).isEqualTo(2);
  }

  @Test
  public void testDuplicateValues() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    // Write duplicate values (should be idempotent)
    writer.write(record(100L, "a"));
    writer.write(record(100L, "b")); // Duplicate
    writer.write(record(200L, "c"));
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    // Bitmap should deduplicate
    assertThat(result.recordCount()).isEqualTo(2); // Only 2 unique values
  }

  @Test
  public void testSequentialValues() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    // Write sequential values (should compress well with run-length encoding)
    for (long i = 0; i < 1000; i++) {
      writer.write(record(i, "data" + i));
    }
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    assertThat(result.recordCount()).isEqualTo(1000);

    // File should be small due to run-length encoding
    assertThat(result.fileSizeInBytes()).isLessThan(10000); // Less than 10KB for 1000 values
  }

  @Test
  public void testSparseValues() throws IOException {
    EqualityDeleteVectorWriter<Record> writer = createWriter();

    // Write sparse values
    writer.write(record(1L, "a"));
    writer.write(record(1000000L, "b"));
    writer.write(record(2000000L, "c"));
    writer.close();

    DeleteFile result = writer.toDeleteFile();
    assertThat(result.recordCount()).isEqualTo(3);
    // Should still be small due to bitmap compression
  }

  private EqualityDeleteVectorWriter<Record> createWriter() {
    File outputFile = new File(tempDir, "delete-vector-" + System.nanoTime() + ".puffin");
    EncryptedOutputFile encryptedOutput = encryptedOutput(outputFile);

    return new EqualityDeleteVectorWriter<>(
        encryptedOutput,
        spec,
        null, // unpartitioned
        null, // no encryption
        EQUALITY_FIELD_ID,
        (record, fieldId) -> {
          if (fieldId == EQUALITY_FIELD_ID) {
            return record.get(0, Long.class); // Get first field (id)
          }
          throw new IllegalArgumentException("Unknown field ID: " + fieldId);
        });
  }

  private Record record(Long id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private EncryptedOutputFile encryptedOutput(File file) {
    OutputFile output = org.apache.iceberg.Files.localOutput(file);
    return org.apache.iceberg.encryption.EncryptedFiles.encryptedOutput(output, (ByteBuffer) null);
  }

  private void verifyPuffinBlob(
      DeleteFile deleteFile, long expectedMin, long expectedMax, int expectedCardinality)
      throws IOException {
    File file = new File(deleteFile.path().toString());
    assertThat(file).exists();

    try (PuffinReader reader = Puffin.read(org.apache.iceberg.Files.localInput(file)).build()) {
      List<org.apache.iceberg.puffin.BlobMetadata> blobs = reader.fileMetadata().blobs();
      assertThat(blobs).hasSize(1);

      org.apache.iceberg.puffin.BlobMetadata blob = blobs.get(0);
      assertThat(blob.type()).isEqualTo(StandardBlobTypes.EDV_V1);
      assertThat(blob.inputFields()).containsExactly(EQUALITY_FIELD_ID);

      // Verify blob properties
      assertThat(blob.properties())
          .containsEntry("equality-field-id", String.valueOf(EQUALITY_FIELD_ID))
          .containsEntry("cardinality", String.valueOf(expectedCardinality))
          .containsEntry("value-min", String.valueOf(expectedMin))
          .containsEntry("value-max", String.valueOf(expectedMax));

      // Verify we can deserialize the bitmap
      Iterable<org.apache.iceberg.util.Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer>>
          blobData = reader.readAll(java.util.Collections.singletonList(blob));
      org.apache.iceberg.util.Pair<org.apache.iceberg.puffin.BlobMetadata, ByteBuffer> blobPair =
          org.apache.iceberg.relocated.com.google.common.collect.Iterables.getOnlyElement(blobData);
      ByteBuffer data = blobPair.second();
      assertThat(data).isNotNull();
      assertThat(data.remaining()).isGreaterThan(0);
    }
  }
}
