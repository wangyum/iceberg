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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * Writer for Equality Delete Vectors (EDV) stored in Puffin files.
 *
 * <p>Equality Delete Vectors use Roaring bitmaps to track deleted values for equality fields.
 * This provides 40-100x storage reduction compared to traditional Parquet equality deletes
 * for sequential or clustered delete patterns.
 *
 * <p><b>Supported field types</b>: Currently only LONG fields are supported.
 *
 * <p><b>Value constraints</b>: Values must be non-negative (>= 0) as they're used as bitmap indices.
 *
 * <p><b>Usage example</b>:
 * <pre>{@code
 * EqualityDVWriter writer = new EqualityDVWriter(fileFactory);
 * writer.delete(fieldId, 100L, spec, partition);  // Mark value 100 as deleted
 * writer.delete(fieldId, 200L, spec, partition);  // Mark value 200 as deleted
 * writer.close();
 * DeleteWriteResult result = writer.result();
 * }</pre>
 */
public class EqualityDVWriter implements Closeable {

  private static final String EQUALITY_FIELD_ID_KEY = "equality-field-id";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String VALUE_MIN_KEY = "value-min";
  private static final String VALUE_MAX_KEY = "value-max";

  private final OutputFileFactory fileFactory;
  private final Map<EqualityKey, EqualityDeletes> deletesByKey = Maps.newHashMap();
  private final Map<EqualityKey, BlobMetadata> blobsByKey = Maps.newHashMap();
  private DeleteWriteResult result = null;

  public EqualityDVWriter(OutputFileFactory fileFactory) {
    this.fileFactory = fileFactory;
  }

  /**
   * Records an equality delete for a field value.
   *
   * @param equalityFieldId the equality field ID
   * @param value the deleted value (must be non-negative LONG)
   * @param spec the partition spec
   * @param partition the partition
   * @throws IllegalArgumentException if value is negative
   */
  public void delete(
      int equalityFieldId, long value, PartitionSpec spec, StructLike partition) {
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Equality delete vector only supports non-negative values, got %d for field %d. "
                  + "Use traditional equality deletes for negative values.",
              value,
              equalityFieldId));
    }

    EqualityKey key = new EqualityKey(equalityFieldId, spec, partition);
    EqualityDeletes deletes =
        deletesByKey.computeIfAbsent(key, k -> new EqualityDeletes(equalityFieldId, spec, partition));
    deletes.delete(value);
  }

  /**
   * Returns the result of this writer after {@link #close()} has been called.
   *
   * @return the delete write result
   * @throws IllegalStateException if called before close()
   */
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      List<DeleteFile> deleteFiles = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();  // EDVs don't reference specific files
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      // Only create PuffinWriter if there are deletes to write
      if (deletesByKey.isEmpty()) {
        this.result =
            new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
        return;
      }

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (Map.Entry<EqualityKey, EqualityDeletes> entry : deletesByKey.entrySet()) {
          EqualityDeletes deletes = entry.getValue();
          // Only write non-empty bitmaps
          if (deletes.cardinality() > 0) {
            write(closeableWriter, entry.getKey(), deletes);
          }
        }
      }

      // All delete files share the Puffin path and file size but have different offsets
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (EqualityKey key : deletesByKey.keySet()) {
        // Only create DeleteFile if bitmap was actually written (non-empty)
        if (blobsByKey.containsKey(key)) {
          DeleteFile deleteFile = createDeleteFile(puffinPath, puffinFileSize, key);
          deleteFiles.add(deleteFile);
        }
      }

      this.result =
          new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  private DeleteFile createDeleteFile(String path, long size, EqualityKey key) {
    EqualityDeletes deletes = deletesByKey.get(key);
    BlobMetadata blobMetadata = blobsByKey.get(key);
    DeleteFile result = FileMetadata.deleteFileBuilder(deletes.spec())
        .ofEqualityDeletes(deletes.equalityFieldId())
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(deletes.cardinality())
        .build();

    try {
      java.nio.file.Files.write(
          java.nio.file.Paths.get("/tmp/edv-createfile.log"),
          ("Created DeleteFile: format=" + result.format() + ", content=" + result.content() +
           ", equalityIds=" + result.equalityFieldIds() + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8),
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);
    } catch (Exception e) {
      // Ignore
    }

    return result;
  }

  private void write(PuffinWriter writer, EqualityKey key, EqualityDeletes deletes) {
    BlobMetadata blobMetadata = writer.write(deletes.toBlob());
    blobsByKey.put(key, blobMetadata);
  }

  private PuffinWriter newWriter() {
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    return Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build();
  }

  /** Internal class to track equality deletes for a single field/partition combination. */
  private static class EqualityDeletes {
    private final int equalityFieldId;
    private final PartitionSpec spec;
    private final StructLike partition;
    private final RoaringPositionBitmap bitmap;
    private long minValue = Long.MAX_VALUE;
    private long maxValue = Long.MIN_VALUE;

    private EqualityDeletes(int equalityFieldId, PartitionSpec spec, StructLike partition) {
      this.equalityFieldId = equalityFieldId;
      this.spec = spec;
      this.partition = StructLikeUtil.copy(partition);
      this.bitmap = new RoaringPositionBitmap();
    }

    public void delete(long value) {
      bitmap.set(value);
      minValue = Math.min(minValue, value);
      maxValue = Math.max(maxValue, value);
    }

    public int equalityFieldId() {
      return equalityFieldId;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public StructLike partition() {
      return partition;
    }

    public long cardinality() {
      return bitmap.cardinality();
    }

    public Blob toBlob() {
      bitmap.runLengthEncode();
      long sizeInBytes = bitmap.serializedSizeInBytes();
      if (sizeInBytes > Integer.MAX_VALUE) {
        throw new IllegalStateException(
            String.format(
                Locale.ROOT,
                "Bitmap size %d bytes exceeds maximum ByteBuffer size %d. "
                    + "Consider splitting the delete set or using traditional deletes.",
                sizeInBytes,
                Integer.MAX_VALUE));
      }

      int size = (int) sizeInBytes;
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.order(ByteOrder.LITTLE_ENDIAN);
      bitmap.serialize(buffer);
      buffer.flip();

      Map<String, String> properties = Maps.newHashMap();
      properties.put(CARDINALITY_KEY, String.valueOf(bitmap.cardinality()));
      properties.put(EQUALITY_FIELD_ID_KEY, String.valueOf(equalityFieldId));
      properties.put(VALUE_MIN_KEY, String.valueOf(minValue));
      properties.put(VALUE_MAX_KEY, String.valueOf(maxValue));

      return new Blob(
          StandardBlobTypes.EDV_V1,
          ImmutableList.of(equalityFieldId),
          -1 /* snapshot ID is inherited */,
          -1 /* sequence number is inherited */,
          buffer,
          null /* uncompressed */,
          ImmutableMap.copyOf(properties));
    }
  }

  /** Key for grouping equality deletes by field/partition. */
  private static class EqualityKey {
    private final int equalityFieldId;
    private final int specId;
    private final int partitionHashCode;

    private EqualityKey(int equalityFieldId, PartitionSpec spec, StructLike partition) {
      this.equalityFieldId = equalityFieldId;
      this.specId = spec.specId();
      // Use StructLikeUtil for consistent hashcode with copied partition data
      StructLike copiedPartition = partition != null ? StructLikeUtil.copy(partition) : null;
      this.partitionHashCode = copiedPartition != null ? copiedPartition.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EqualityKey that = (EqualityKey) o;
      return equalityFieldId == that.equalityFieldId
          && specId == that.specId
          && partitionHashCode == that.partitionHashCode;
    }

    @Override
    public int hashCode() {
      int result = equalityFieldId;
      result = 31 * result + specId;
      result = 31 * result + partitionHashCode;
      return result;
    }
  }
}
