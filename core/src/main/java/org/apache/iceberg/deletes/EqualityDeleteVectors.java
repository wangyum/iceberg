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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.Pair;

/** Utility methods for working with Equality Delete Vectors. */
public class EqualityDeleteVectors {

  private EqualityDeleteVectors() {}

  /**
   * Reads an equality delete vector from a Puffin file and returns the deserialized bitmap.
   *
   * @param inputFile the EDV Puffin file to read
   * @return the deserialized {@link RoaringPositionBitmap}
   */
  /**
   * Reads an equality delete vector bitmap from a Puffin file.
   *
   * @param inputFile the Puffin file containing the EDV blob
   * @return the deserialized Roaring bitmap
   * @deprecated Use {@link #readEqualityDeleteVectorBitmap(InputFile, long)} with contentOffset
   */
  @Deprecated
  public static RoaringPositionBitmap readEqualityDeleteVectorBitmap(InputFile inputFile) {
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      BlobMetadata edvBlob = reader.fileMetadata().blobs().stream()
          .filter(b -> StandardBlobTypes.EDV_V1.equals(b.type()))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              String.format("No %s blob found in Puffin file %s",
                  StandardBlobTypes.EDV_V1, inputFile.location())));

      ByteBuffer data = Iterables.getOnlyElement(
          reader.readAll(Collections.singletonList(edvBlob))).second();
      data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      return RoaringPositionBitmap.deserialize(data);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read equality delete vector", e);
    }
  }

  /**
   * Reads an equality delete vector bitmap from a Puffin file at a specific offset.
   *
   * @param inputFile the Puffin file containing the EDV blob
   * @param contentOffset the offset of the blob within the Puffin file
   * @return the deserialized Roaring bitmap
   */
  public static RoaringPositionBitmap readEqualityDeleteVectorBitmap(
      InputFile inputFile, long contentOffset) {
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      BlobMetadata edvBlob = reader.fileMetadata().blobs().stream()
          .filter(b -> StandardBlobTypes.EDV_V1.equals(b.type()))
          .filter(b -> b.offset() == contentOffset)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException(
              String.format(java.util.Locale.ROOT, "No %s blob found at offset %d in Puffin file %s",
                  StandardBlobTypes.EDV_V1, contentOffset, inputFile.location())));

      ByteBuffer data = Iterables.getOnlyElement(
          reader.readAll(Collections.singletonList(edvBlob))).second();
      data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      return RoaringPositionBitmap.deserialize(data);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read equality delete vector", e);
    }
  }
}
