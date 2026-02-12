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
package org.apache.iceberg;

import java.util.List;

/**
 * Interface for delete files listed in a table delete manifest.
 *
 * <p>Delete files mark rows for deletion. Two types exist:
 * <ul>
 *   <li><b>Position deletes</b>: Mark specific (file_path, row_position) pairs. V3+ uses deletion vectors (Puffin/Roaring bitmaps).
 *   <li><b>Equality deletes</b>: Mark rows by field values (e.g., id=100). V3+ can use equality delete vectors for single LONG fields.
 * </ul>
 *
 * @see FileContent#POSITION_DELETES
 * @see FileContent#EQUALITY_DELETES
 */
public interface DeleteFile extends ContentFile<DeleteFile> {
  /**
   * @return List of recommended split locations, if applicable, null otherwise. When available,
   *     this information is used for planning scan tasks whose boundaries are determined by these
   *     offsets. The returned list must be sorted in ascending order.
   */
  @Override
  default List<Long> splitOffsets() {
    return null;
  }

  /**
   * Returns the path to the data file this delete file applies to.
   *
   * <p>Position deletes (including position DVs in V3+) reference a specific data file. Equality deletes apply to all files and return null.
   *
   * @return the referenced data file path for position deletes, null for equality deletes
   */
  default String referencedDataFile() {
    return null;
  }

  /**
   * Returns the offset in the file where the deletion vector blob starts.
   *
   * <p>This method applies to Deletion Vectors only (PUFFIN format files):
   *
   * <p><b>Deletion Vectors (Position DVs and Equality DVs):</b>
   *
   * <ul>
   *   <li>MUST return the byte offset where the Roaring bitmap blob starts in the Puffin file
   *   <li>Used with {@link #contentSizeInBytes()} to enable direct blob access without reading the
   *       entire Puffin file
   *   <li>Points to the serialized Roaring bitmap data within the Puffin blob
   * </ul>
   *
   * <p><b>Traditional Delete Files (Parquet/Avro/ORC):</b>
   *
   * <ul>
   *   <li>MUST return null (not applicable to row-based delete files)
   * </ul>
   *
   * <p><b>Example Usage:</b>
   *
   * <pre>{@code
   * DeleteFile dv = ...;  // Position DV or Equality DV
   * InputFile puffinFile = io.newInputFile(dv.location());
   * long offset = dv.contentOffset();        // e.g., 512 bytes into the Puffin file
   * int size = dv.contentSizeInBytes();      // e.g., 1024 bytes
   * byte[] bitmapData = readBytes(puffinFile, offset, size);
   * RoaringBitmap bitmap = RoaringBitmap.deserialize(bitmapData);
   * }</pre>
   *
   * @return the byte offset of the deletion vector blob for DVs, null for traditional delete files
   * @see #contentSizeInBytes()
   * @see #referencedDataFile()
   */
  default Long contentOffset() {
    return null;
  }

  /**
   * Returns the size in bytes of the deletion vector blob.
   *
   * <p>For deletion vectors (V3+ PUFFIN files), returns the size of the Roaring bitmap blob. For traditional delete files, returns null.
   *
   * @return the size in bytes of the deletion vector blob, or null for traditional delete files
   * @see #contentOffset()
   */
  default Long contentSizeInBytes() {
    return null;
  }
}
