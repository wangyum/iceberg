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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * A writer for equality delete vectors that stores deleted LONG values as Roaring bitmaps in Puffin
 * files.
 *
 * <p>Constraints:
 *
 * <ul>
 *   <li>Only supports LONG type (not INT or other types)
 *   <li>Only supports non-negative values (>= 0)
 *   <li>Does not support NULL values
 * </ul>
 *
 * <p>Violating any constraint will throw {@link IllegalArgumentException}.
 */
public class EqualityDeleteVectorWriter<T> implements FileWriter<T, DeleteWriteResult> {

  private static final String EQUALITY_FIELD_ID_KEY = "equality-field-id";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String VALUE_MIN_KEY = "value-min";
  private static final String VALUE_MAX_KEY = "value-max";

  private final EncryptedOutputFile outputFile;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final int equalityFieldId;
  private final ValueExtractor<T> valueExtractor;

  private final RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
  private long minValue = Long.MAX_VALUE;
  private long maxValue = Long.MIN_VALUE;
  private DeleteFile deleteFile = null;

  /**
   * Functional interface to extract LONG value from a row.
   *
   * @param <T> the row type
   */
  @FunctionalInterface
  public interface ValueExtractor<T> {
    /**
     * Extracts the LONG value from the given row at the specified field.
     *
     * @param row the row to extract from
     * @param fieldId the field ID to extract
     * @return the extracted value, or null if the field is NULL
     */
    Long extract(T row, int fieldId);
  }

  public EqualityDeleteVectorWriter(
      EncryptedOutputFile outputFile,
      PartitionSpec spec,
      StructLike partition,
      EncryptionKeyMetadata keyMetadata,
      int equalityFieldId,
      ValueExtractor<T> valueExtractor) {
    this.outputFile = outputFile;
    this.spec = spec;
    this.partition = StructLikeUtil.copy(partition);
    this.keyMetadata = keyMetadata != null ? keyMetadata.buffer() : null;
    this.equalityFieldId = equalityFieldId;
    this.valueExtractor = valueExtractor;
  }

  @Override
  public void write(T row) {
    // Extract the LONG value from the row
    Long value = valueExtractor.extract(row, equalityFieldId);

    // Validate: NULL not supported
    if (value == null) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Equality delete vector does not support NULL values for field %d. "
                  + "Use traditional equality deletes for NULL.",
              equalityFieldId));
    }

    // Validate: only non-negative values supported
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Equality delete vector only supports non-negative values, got %d for field %d. "
                  + "Use traditional equality deletes for negative values.",
              value,
              equalityFieldId));
    }

    // Add to bitmap
    bitmap.set(value);

    // Track min/max for scan filtering
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  @Override
  public long length() {
    // Estimate based on bitmap serialized size
    return bitmap.serializedSizeInBytes();
  }

  @Override
  public void close() throws IOException {
    if (deleteFile == null) {
      // Check if bitmap is empty
      if (bitmap.isEmpty()) {
        // No deletes written, create empty result
        this.deleteFile = createEmptyDeleteFile();
        return;
      }

      // Check for possible size overflow before optimization/serialization
      if (bitmap.serializedSizeInBytes() > Integer.MAX_VALUE) {
        throw new IllegalStateException(
            "Equality delete vector is too large: " + bitmap.serializedSizeInBytes() + " bytes");
      }

      // Optimize bitmap with run-length encoding
      bitmap.runLengthEncode();

      // Write Puffin file with equality delete vector blob
      try (PuffinWriter writer =
          Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build()) {
        BlobMetadata blobMetadata = writer.write(toBlob());
        writer.finish();

        // Create delete file metadata
        this.deleteFile = createDeleteFile(writer.location(), writer.fileSize(), blobMetadata);
      }
    }
  }

  private Blob toBlob() {
    // Serialize the bitmap to a ByteBuffer
    long sizeInBytes = bitmap.serializedSizeInBytes();

    // Validate bitmap size is within ByteBuffer limits (2GB max)
    // This is extremely unlikely in practice (would require billions of sparse deletes)
    // but we validate for safety
    if (sizeInBytes > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          String.format(
              java.util.Locale.ROOT,
              "Bitmap size %d bytes exceeds maximum ByteBuffer size %d. "
                  + "Consider using traditional equality deletes for extremely large delete sets.",
              sizeInBytes,
              Integer.MAX_VALUE));
    }

    int size = (int) sizeInBytes;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    bitmap.serialize(buffer);
    buffer.flip();

    return new Blob(
        StandardBlobTypes.EDV_V1,
        ImmutableList.of(equalityFieldId),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        buffer,
        null /* uncompressed */,
        ImmutableMap.of(
            EQUALITY_FIELD_ID_KEY,
            String.valueOf(equalityFieldId),
            CARDINALITY_KEY,
            String.valueOf(bitmap.cardinality()),
            VALUE_MIN_KEY,
            String.valueOf(minValue),
            VALUE_MAX_KEY,
            String.valueOf(maxValue)));
  }

  private DeleteFile createDeleteFile(String path, long size, BlobMetadata blobMetadata) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldId)
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(partition)
        .withEncryptionKeyMetadata(EncryptionUtil.setFileLength(keyMetadata, size))
        .withFileSizeInBytes(size)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(bitmap.cardinality())
        .build();
  }

  private DeleteFile createEmptyDeleteFile() {
    // Return a marker for no deletes (implementation-specific)
    return null;
  }

  public DeleteFile toDeleteFile() {
    // If close() hasn't been called yet, deleteFile will be null (not an empty bitmap case)
    // But we need to distinguish between: not closed yet vs. closed with empty bitmap
    // For now, we allow null to be returned (empty bitmap case)
    return deleteFile;
  }

  @Override
  public DeleteWriteResult result() {
    DeleteFile file = toDeleteFile();
    if (file == null) {
      // No deletes written
      return new DeleteWriteResult(java.util.Collections.emptyList());
    }
    return new DeleteWriteResult(file);
  }
}
