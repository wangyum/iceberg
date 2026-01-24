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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Unified bitmap-based delete writer for both position and equality deletes.
 *
 * <p>This writer consolidates the infrastructure for {@code BaseDVFileWriter} and {@code
 * BaseEDVFileWriter} by using a {@link DeleteKey} abstraction to handle different delete types
 * with the same core logic.
 *
 * <p>Supported delete types:
 *
 * <ul>
 *   <li><b>Position deletes</b>: Use {@link PositionDeleteKey} keyed by data file path
 *   <li><b>Equality deletes</b>: Use {@link EqualityDeleteKey} keyed by equality field ID
 * </ul>
 *
 * <p>The writer:
 *
 * <ol>
 *   <li>Accumulates deletes in memory using bitmaps
 *   <li>Writes all bitmaps to a single Puffin file on close
 *   <li>Creates appropriate {@link DeleteFile} metadata based on the key type
 * </ol>
 *
 * <p><b>Design Benefits:</b>
 *
 * <ul>
 *   <li>Single implementation for all bitmap-based deletes (~50% less code)
 *   <li>Easy to extend to new delete types (INT, composite keys)
 *   <li>Shared infrastructure reduces duplication
 *   <li>Consistent behavior across delete types
 * </ul>
 *
 * <p><b>Usage Example (Position Deletes):</b>
 *
 * <pre>{@code
 * BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory, loadPreviousDeletes);
 * writer.deletePosition("file.parquet", 100L, spec, partition);
 * writer.deletePosition("file.parquet", 200L, spec, partition);
 * writer.close();
 * DeleteWriteResult result = writer.result();
 * }</pre>
 *
 * <p><b>Usage Example (Equality Deletes):</b>
 *
 * <pre>{@code
 * BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
 * writer.deleteEquality(1, 100L, spec, partition);  // field 1, value 100
 * writer.deleteEquality(1, 200L, spec, partition);  // field 1, value 200
 * writer.close();
 * DeleteWriteResult result = writer.result();
 * }</pre>
 */
public class BitmapDeleteWriter implements Closeable {

  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String EQUALITY_FIELD_ID_KEY = "equality-field-id";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String VALUE_MIN_KEY = "value-min";
  private static final String VALUE_MAX_KEY = "value-max";

  private final OutputFileFactory fileFactory;
  private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
  private final Map<String, BitmapDeletes> deletesByKey = Maps.newHashMap();
  private final Map<String, BlobMetadata> blobsByKey = Maps.newHashMap();
  private DeleteWriteResult result = null;

  /**
   * Creates a bitmap delete writer for position deletes (with previous delete loading).
   *
   * @param fileFactory factory for creating output files
   * @param loadPreviousDeletes function to load previous position deletes for merging
   */
  public BitmapDeleteWriter(
      OutputFileFactory fileFactory, Function<String, PositionDeleteIndex> loadPreviousDeletes) {
    this.fileFactory = fileFactory;
    this.loadPreviousDeletes = loadPreviousDeletes;
  }

  /**
   * Creates a bitmap delete writer for equality deletes (no previous delete loading).
   *
   * @param fileFactory factory for creating output files
   */
  public BitmapDeleteWriter(OutputFileFactory fileFactory) {
    this(fileFactory, path -> null);
  }

  /**
   * Records a position delete in a data file.
   *
   * <p>For position deletes, the writer uses {@link BitmapPositionDeleteIndex} to support merging
   * with previous deletes.
   *
   * @param path the data file path
   * @param position the position in the data file
   * @param spec the partition spec
   * @param partition the partition
   */
  public void deletePosition(String path, long position, PartitionSpec spec, StructLike partition) {
    PositionDeleteKey key = new PositionDeleteKey(path, spec, partition);
    BitmapDeletes deletes =
        deletesByKey.computeIfAbsent(
            key.keyId(), k -> new BitmapDeletes(key, new BitmapPositionDeleteIndex()));
    deletes.positionIndex().delete(position);
  }

  /**
   * Records an equality delete for a field value.
   *
   * <p>For equality deletes, the writer uses {@link RoaringPositionBitmap} directly for optimal
   * performance.
   *
   * @param equalityFieldId the equality field ID
   * @param value the deleted value (must be non-negative LONG)
   * @param spec the partition spec
   * @param partition the partition
   * @throws IllegalArgumentException if value is negative
   */
  public void deleteEquality(
      int equalityFieldId, long value, PartitionSpec spec, StructLike partition) {
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

    EqualityDeleteKey key = new EqualityDeleteKey(equalityFieldId, spec, partition);
    BitmapDeletes deletes =
        deletesByKey.computeIfAbsent(
            key.keyId(), k -> new BitmapDeletes(key, new RoaringPositionBitmap()));
    deletes.bitmap().set(value);

    // Track min/max for scan filtering
    key.updateBounds(value);
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
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      // Only create PuffinWriter if there are deletes to write
      if (deletesByKey.isEmpty()) {
        this.result =
            new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
        return;
      }

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (BitmapDeletes deletes : deletesByKey.values()) {
          DeleteKey key = deletes.key();

          // For position deletes: merge with previous deletes
          if (key instanceof PositionDeleteKey) {
            PositionDeleteKey posKey = (PositionDeleteKey) key;
            PositionDeleteIndex previousDeletes = loadPreviousDeletes.apply(posKey.dataFilePath());
            if (previousDeletes != null) {
              deletes.positionIndex().merge(previousDeletes);
              for (DeleteFile previousDeleteFile : previousDeletes.deleteFiles()) {
                // only DVs and file-scoped deletes can be discarded from the table state
                if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                  rewrittenDeleteFiles.add(previousDeleteFile);
                }
              }
            }
            referencedDataFiles.add(posKey.dataFilePath());
          }

          // Optimize bitmap with run-length encoding before writing
          // (For equality deletes only - position deletes don't expose RLE)
          if (key instanceof EqualityDeleteKey) {
            deletes.bitmap().runLengthEncode();
          }

          // Write blob to Puffin
          write(closeableWriter, deletes);
        }
      }

      // All delete files share the Puffin path and file size but have different offsets
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (BitmapDeletes deletes : deletesByKey.values()) {
        DeleteFile deleteFile = createDeleteFile(puffinPath, puffinFileSize, deletes);
        deleteFiles.add(deleteFile);
      }

      this.result =
          new DeleteWriteResult(deleteFiles, referencedDataFiles, rewrittenDeleteFiles);
    }
  }

  private DeleteFile createDeleteFile(
      String puffinPath, long puffinSize, BitmapDeletes deletes) {
    DeleteKey key = deletes.key();
    BlobMetadata blobMetadata = blobsByKey.get(key.keyId());

    // Get cardinality based on type
    long cardinality;
    if (key instanceof PositionDeleteKey) {
      cardinality = deletes.positionIndex().cardinality();
    } else {
      cardinality = deletes.bitmap().cardinality();
    }

    return key.toDeleteFile(
        puffinPath, puffinSize, blobMetadata, cardinality, key.spec(), key.partition());
  }

  private void write(PuffinWriter writer, BitmapDeletes deletes) {
    DeleteKey key = deletes.key();
    BlobMetadata blobMetadata = writer.write(toBlob(deletes));
    blobsByKey.put(key.keyId(), blobMetadata);
  }

  private PuffinWriter newWriter() {
    return Puffin.write(fileFactory.newOutputFile()).createdBy(IcebergBuild.fullVersion()).build();
  }

  private Blob toBlob(BitmapDeletes deletes) {
    DeleteKey key = deletes.key();
    ByteBuffer buffer;
    long cardinality;

    // Serialize based on delete type
    if (key instanceof PositionDeleteKey) {
      // Position deletes: serialize PositionDeleteIndex
      PositionDeleteIndex index = deletes.positionIndex();
      buffer = index.serialize();
      cardinality = index.cardinality();
    } else if (key instanceof EqualityDeleteKey) {
      // Equality deletes: serialize RoaringPositionBitmap
      RoaringPositionBitmap bitmap = deletes.bitmap();

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
      buffer = ByteBuffer.allocate(size);
      buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      bitmap.serialize(buffer);
      buffer.flip();
      cardinality = bitmap.cardinality();
    } else {
      throw new IllegalStateException("Unknown delete key type: " + key.getClass());
    }

    // Build blob metadata based on key type
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CARDINALITY_KEY, String.valueOf(cardinality));

    List<Integer> inputFields;
    if (key instanceof PositionDeleteKey) {
      PositionDeleteKey posKey = (PositionDeleteKey) key;
      properties.put(REFERENCED_DATA_FILE_KEY, posKey.dataFilePath());
      inputFields = ImmutableList.of();
    } else if (key instanceof EqualityDeleteKey) {
      EqualityDeleteKey eqKey = (EqualityDeleteKey) key;
      properties.put(EQUALITY_FIELD_ID_KEY, String.valueOf(eqKey.equalityFieldId()));
      properties.put(VALUE_MIN_KEY, String.valueOf(eqKey.minValue()));
      properties.put(VALUE_MAX_KEY, String.valueOf(eqKey.maxValue()));
      inputFields = ImmutableList.of(eqKey.equalityFieldId());
    } else {
      throw new IllegalStateException("Unknown delete key type: " + key.getClass());
    }

    return new Blob(
        key.blobType(),
        inputFields,
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        buffer,
        null /* uncompressed */,
        ImmutableMap.copyOf(properties));
  }

  /**
   * Internal class to track deletes and their bitmap for a single key.
   *
   * <p>Works for both position deletes (using PositionDeleteIndex) and equality deletes (using
   * RoaringPositionBitmap directly).
   */
  private static class BitmapDeletes {
    private final DeleteKey key;
    private final Object bitmapOrIndex; // PositionDeleteIndex or RoaringPositionBitmap

    private BitmapDeletes(DeleteKey key, Object bitmapOrIndex) {
      this.key = key;
      this.bitmapOrIndex = bitmapOrIndex;
    }

    public DeleteKey key() {
      return key;
    }

    /**
     * Returns the bitmap for this delete set.
     *
     * <p>Only valid for equality deletes. For position deletes, use {@link #positionIndex()}.
     */
    public RoaringPositionBitmap bitmap() {
      Preconditions.checkState(
          bitmapOrIndex instanceof RoaringPositionBitmap,
          "Bitmap only available for equality deletes, use positionIndex() for position deletes");
      return (RoaringPositionBitmap) bitmapOrIndex;
    }

    /**
     * Returns the PositionDeleteIndex for position deletes.
     *
     * <p>Only valid for position deletes.
     */
    public PositionDeleteIndex positionIndex() {
      Preconditions.checkState(
          bitmapOrIndex instanceof PositionDeleteIndex,
          "Position index only available for position deletes");
      return (PositionDeleteIndex) bitmapOrIndex;
    }
  }
}
