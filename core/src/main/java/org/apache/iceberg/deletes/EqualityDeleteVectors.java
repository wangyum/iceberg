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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.Pair;

/**
 * Utility methods for working with Equality Delete Vectors (EDVs).
 *
 * <p>Provides centralized reading logic for EDV PUFFIN files, including:
 * <ul>
 *   <li>Low-level bitmap reading for advanced use cases</li>
 *   <li>High-level Set conversion for delete filtering</li>
 * </ul>
 */
public class EqualityDeleteVectors {

  private EqualityDeleteVectors() {}

  /**
   * Reads an equality delete vector from a PUFFIN file and returns a Set for delete filtering.
   *
   * <p>This is the preferred method for reading EDVs, as it returns a ready-to-use Set
   * implementation backed by the compressed bitmap for memory efficiency.
   *
   * <p><b>Usage Example:</b>
   * <pre>{@code
   * Set<StructLike> deletedValues = EqualityDeleteVectors.readEqualityDeleteSet(
   *     inputFile, equalityFieldId, tableSchema);
   *
   * // Use in delete filtering
   * if (deletedValues.contains(row)) {
   *   // Skip deleted row
   * }
   * }</pre>
   *
   * @param inputFile the PUFFIN file containing the EDV
   * @param equalityFieldId the field ID being deleted
   * @param schema the table schema for creating StructLike wrappers
   * @return an efficient Set implementation backed by the bitmap
   */
  public static Set<StructLike> readEqualityDeleteSet(
      InputFile inputFile,
      int equalityFieldId,
      Schema schema) {

    // Read the underlying bitmap
    RoaringPositionBitmap bitmap = readEqualityDeleteVectorBitmap(inputFile);

    // Wrap in efficient set implementation
    return new BitmapBackedStructLikeSet(bitmap, equalityFieldId, schema);
  }

  /**
   * Reads an equality delete vector from a Puffin file and returns the deserialized bitmap.
   *
   * <p>This is a low-level method for advanced use cases. Most callers should use
   * {@link #readEqualityDeleteSet(InputFile, int, Schema)} instead.
   *
   * @param inputFile the EDV Puffin file to read
   * @return the deserialized {@link RoaringPositionBitmap}
   */
  public static RoaringPositionBitmap readEqualityDeleteVectorBitmap(InputFile inputFile) {
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      // Find the equality-delete-vector-v1 blob
      BlobMetadata edvBlob = null;
      for (BlobMetadata blobMetadata : reader.fileMetadata().blobs()) {
        if (StandardBlobTypes.EDV_V1.equals(blobMetadata.type())) {
          edvBlob = blobMetadata;
          break;
        }
      }

      if (edvBlob == null) {
        throw new IllegalArgumentException(
            String.format(
                "No %s blob found in Puffin file %s",
                StandardBlobTypes.EDV_V1, inputFile.location()));
      }

      // Read the blob data
      Iterable<Pair<BlobMetadata, ByteBuffer>> blobData =
          reader.readAll(Collections.singletonList(edvBlob));
      Pair<BlobMetadata, ByteBuffer> blobPair = Iterables.getOnlyElement(blobData);
      ByteBuffer data = blobPair.second();

      // Set byte order to little-endian (required for Roaring bitmap deserialization)
      data.order(java.nio.ByteOrder.LITTLE_ENDIAN);

      // Deserialize the Roaring bitmap - deserialize is a static method that returns a new bitmap
      RoaringPositionBitmap bitmap = RoaringPositionBitmap.deserialize(data);

      return bitmap;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read equality delete vector", e);
    }
  }
}
