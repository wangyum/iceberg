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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Unified bitmap-based delete writer for both position and equality deletes.
 *
 * <p>This writer consolidates the infrastructure for position deletion vectors (DVs) and
 * equality delete vectors (EDVs) by using a {@link DeleteKey} abstraction to handle different
 * delete types with the same core logic.
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
 *   <li>Accumulates deletes in memory using Roaring bitmaps
 *   <li>Writes all bitmaps to a single PUFFIN file on close
 *   <li>Creates appropriate {@link DeleteFile} metadata based on the key type
 * </ol>
 *
 * <p><b>Design Benefits:</b>
 *
 * <ul>
 *   <li>Single implementation for all bitmap-based deletes (~50% less code vs separate writers)
 *   <li>Shared PUFFIN serialization infrastructure
 *   <li>Consistent behavior across delete types
 *   <li>Easy to test and maintain
 * </ul>
 *
 * <p><b>EQUALITY DELETE CONSTRAINTS:</b>
 *
 * <p>Only LONG fields with non-negative values are supported for equality deletes.
 * This covers approximately 80% of real-world use cases:
 * <ul>
 *   <li>Auto-increment IDs (user_id, order_id, etc.)</li>
 *   <li>Timestamps (epoch milliseconds)</li>
 *   <li>Counters and metrics</li>
 * </ul>
 *
 * <p>For other types, {@code deleteEquality()} will throw {@link IllegalArgumentException}
 * with guidance to use traditional equality deletes instead.
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
 * // Configure table for equality delete strategy
 * table.updateProperties()
 *   .set(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName())
 *   .set(TableProperties.DELETE_STRATEGY, TableProperties.DELETE_STRATEGY_EQUALITY)
 *   .commit();
 *
 * // Write equality deletes
 * BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
 * writer.deleteEquality(1, 100L, spec, partition);  // field ID 1, value 100
 * writer.deleteEquality(1, 200L, spec, partition);  // field ID 1, value 200
 * writer.close();
 *
 * // Commit to table
 * table.newRowDelta()
 *   .addDeletes(writer.result().deleteFiles().get(0))
 *   .commit();
 * }</pre>
 *
 * <p><b>Why EDVs Avoid Compaction Conflicts:</b>
 *
 * <p>Equality delete vectors store <b>logical values</b> (user IDs) instead of <b>physical
 * positions</b> (row offsets). This means they remain valid after file compaction, enabling:
 * <ul>
 *   <li>Concurrent compaction and MERGE INTO operations</li>
 *   <li>No serialization conflicts</li>
 *   <li>Better throughput for high-update CDC tables</li>
 * </ul>
 */
public class BitmapDeleteWriter implements Closeable {

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
            key.keyId(),
            k -> new BitmapDeletes(key, new PositionAccumulator(path, new BitmapPositionDeleteIndex())));
    deletes.accumulator().add(position);
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
    EqualityDeleteKey key = new EqualityDeleteKey(equalityFieldId, spec, partition);
    BitmapDeletes deletes =
        deletesByKey.computeIfAbsent(
            key.keyId(),
            k -> new BitmapDeletes(key, new EqualityAccumulator(equalityFieldId, new RoaringPositionBitmap())));
    deletes.accumulator().add(value);
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
          BitmapAccumulator accumulator = deletes.accumulator();

          // Merge logic only for position deletes
          if (accumulator instanceof PositionBitmapAccumulator) {
            PositionBitmapAccumulator positionAccumulator = (PositionBitmapAccumulator) accumulator;
            positionAccumulator.merge(loadPreviousDeletes, rewrittenDeleteFiles);
            Iterables.addAll(referencedDataFiles, positionAccumulator.referencedDataFiles());
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

    return key.toDeleteFile(
        puffinPath, puffinSize, blobMetadata, deletes.accumulator().cardinality());
  }

  private void write(PuffinWriter writer, BitmapDeletes deletes) {
    DeleteKey key = deletes.key();
    BlobMetadata blobMetadata = writer.write(deletes.accumulator().toBlob());
    blobsByKey.put(key.keyId(), blobMetadata);
  }

  private PuffinWriter newWriter() {
    return Puffin.write(fileFactory.newOutputFile()).createdBy(IcebergBuild.fullVersion()).build();
  }

  /**
   * Internal class to track deletes and their bitmap for a single key.
   *
   * <p>Works for both position deletes (using PositionDeleteIndex) and equality deletes (using
   * RoaringPositionBitmap directly).
   */
  private static class BitmapDeletes {
    private final DeleteKey key;
    private final BitmapAccumulator accumulator;

    private BitmapDeletes(DeleteKey key, BitmapAccumulator accumulator) {
      this.key = key;
      this.accumulator = accumulator;
    }

    public DeleteKey key() {
      return key;
    }

    public BitmapAccumulator accumulator() {
      return accumulator;
    }
  }
}
