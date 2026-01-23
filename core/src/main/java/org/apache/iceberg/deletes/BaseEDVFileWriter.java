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
 * Base writer for equality delete vector (EDV) files.
 *
 * <p>This class mirrors {@link BaseDVFileWriter} to maintain consistency in Iceberg's delete file
 * patterns. Like position deletion vectors, equality delete vectors:
 *
 * <ul>
 *   <li>Accumulate deletes in memory using bitmaps
 *   <li>Write all deletes to a single Puffin file on close
 *   <li>Support multiple equality fields in one writer
 *   <li>Create one DeleteFile per equality field
 * </ul>
 *
 * <p>Key differences from BaseDVFileWriter:
 *
 * <ul>
 *   <li>Organizes by equality field ID (vs data file path)
 *   <li>Stores column values (vs row positions)
 *   <li>Not file-scoped (global/partitioned deletes)
 * </ul>
 */
public class BaseEDVFileWriter implements EDVFileWriter {

  private static final String EQUALITY_FIELD_ID_KEY = "equality-field-id";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String VALUE_MIN_KEY = "value-min";
  private static final String VALUE_MAX_KEY = "value-max";

  private final OutputFileFactory fileFactory;
  private final Map<Integer, EqualityDeletes> deletesByFieldId = Maps.newHashMap();
  private final Map<Integer, BlobMetadata> blobsByFieldId = Maps.newHashMap();
  private DeleteWriteResult result = null;

  public BaseEDVFileWriter(OutputFileFactory fileFactory) {
    this.fileFactory = fileFactory;
  }

  @Override
  public void delete(long value, int equalityFieldId, PartitionSpec spec, StructLike partition) {
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

    // Get or create deletes accumulator for this equality field
    EqualityDeletes deletes =
        deletesByFieldId.computeIfAbsent(
            equalityFieldId, key -> new EqualityDeletes(equalityFieldId, spec, partition));

    // Add value to bitmap
    RoaringPositionBitmap bitmap = deletes.bitmap();
    bitmap.set(value);

    // Track min/max for scan filtering
    deletes.updateBounds(value);
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      List<DeleteFile> edvs = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      // Only create PuffinWriter if there are deletes to write
      if (deletesByFieldId.isEmpty()) {
        this.result = new DeleteWriteResult(edvs, referencedDataFiles, rewrittenDeleteFiles);
        return;
      }

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (EqualityDeletes deletes : deletesByFieldId.values()) {
          // Optimize bitmap with run-length encoding before writing
          deletes.bitmap().runLengthEncode();

          // Write blob to Puffin
          write(closeableWriter, deletes);
        }
      }

      // EDVs share the Puffin path and file size but have different offsets
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (Integer fieldId : deletesByFieldId.keySet()) {
        DeleteFile edv = createEDV(puffinPath, puffinFileSize, fieldId);
        edvs.add(edv);
      }

      this.result = new DeleteWriteResult(edvs, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  private DeleteFile createEDV(String path, long size, int equalityFieldId) {
    EqualityDeletes deletes = deletesByFieldId.get(equalityFieldId);
    BlobMetadata blobMetadata = blobsByFieldId.get(equalityFieldId);
    return FileMetadata.deleteFileBuilder(deletes.spec())
        .ofEqualityDeletes(equalityFieldId)
        .withFormat(FileFormat.PUFFIN)
        .withPath(path)
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(deletes.bitmap().cardinality())
        .build();
  }

  private void write(PuffinWriter writer, EqualityDeletes deletes) {
    int equalityFieldId = deletes.equalityFieldId();
    RoaringPositionBitmap bitmap = deletes.bitmap();
    BlobMetadata blobMetadata = writer.write(toBlob(bitmap, deletes));
    blobsByFieldId.put(equalityFieldId, blobMetadata);
  }

  private PuffinWriter newWriter() {
    EncryptedOutputFile outputFile = fileFactory.newOutputFile();
    return Puffin.write(outputFile).createdBy(IcebergBuild.fullVersion()).build();
  }

  private Blob toBlob(RoaringPositionBitmap bitmap, EqualityDeletes deletes) {
    // Serialize the bitmap to a ByteBuffer
    long sizeInBytes = bitmap.serializedSizeInBytes();

    // Validate bitmap size is within ByteBuffer limits (2GB max)
    if (sizeInBytes > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
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
        ImmutableList.of(deletes.equalityFieldId()),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        buffer,
        null /* uncompressed */,
        ImmutableMap.of(
            EQUALITY_FIELD_ID_KEY,
            String.valueOf(deletes.equalityFieldId()),
            CARDINALITY_KEY,
            String.valueOf(bitmap.cardinality()),
            VALUE_MIN_KEY,
            String.valueOf(deletes.minValue()),
            VALUE_MAX_KEY,
            String.valueOf(deletes.maxValue())));
  }

  /**
   * Internal class to track deletes for a single equality field.
   *
   * <p>Mirrors {@link BaseDVFileWriter.Deletes} structure.
   */
  private static class EqualityDeletes {
    private final int equalityFieldId;
    private final RoaringPositionBitmap bitmap;
    private final PartitionSpec spec;
    private final StructLike partition;
    private long minValue = Long.MAX_VALUE;
    private long maxValue = Long.MIN_VALUE;

    private EqualityDeletes(int equalityFieldId, PartitionSpec spec, StructLike partition) {
      this.equalityFieldId = equalityFieldId;
      this.bitmap = new RoaringPositionBitmap();
      this.spec = spec;
      this.partition = StructLikeUtil.copy(partition);
    }

    public int equalityFieldId() {
      return equalityFieldId;
    }

    public RoaringPositionBitmap bitmap() {
      return bitmap;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public StructLike partition() {
      return partition;
    }

    public long minValue() {
      return minValue;
    }

    public long maxValue() {
      return maxValue;
    }

    public void updateBounds(long value) {
      minValue = Math.min(minValue, value);
      maxValue = Math.max(maxValue, value);
    }
  }
}
