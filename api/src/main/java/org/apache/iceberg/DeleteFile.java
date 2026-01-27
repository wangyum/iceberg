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
 * <p>Delete files mark rows for deletion in Iceberg tables. There are two types of delete files,
 * each with different storage formats and use cases:
 *
 * <h3>1. Position Delete Files</h3>
 *
 * <p>Position delete files mark specific row positions for deletion in data files. They contain
 * (file_path, row_position) pairs identifying rows to delete.
 *
 * <p><b>Storage Formats:</b>
 *
 * <ul>
 *   <li><b>V2 Tables</b>: Parquet/Avro/ORC files containing row-based position delete records
 *   <li><b>V3+ Tables</b>: Deletion Vectors (Puffin files with Roaring bitmap blobs) - REQUIRED
 * </ul>
 *
 * <p><b>Key Characteristics:</b>
 *
 * <ul>
 *   <li><b>File-Scoped</b>: Each position delete file references ONE specific data file via {@link
 *       #referencedDataFile()}
 *   <li><b>Targeted</b>: Efficient for deleting scattered rows within a single data file
 *   <li><b>Compaction-Sensitive</b>: When the referenced data file is rewritten, the position
 *       delete file must be removed or merged
 * </ul>
 *
 * <h3>2. Equality Delete Files</h3>
 *
 * <p>Equality delete files mark rows for deletion based on equality field values. They contain
 * records with specific field values (e.g., id=100, id=200) that match rows to delete across ALL
 * data files.
 *
 * <p><b>Storage Formats:</b>
 *
 * <ul>
 *   <li><b>Traditional</b>: Parquet/Avro/ORC files containing row-based equality delete records
 *   <li><b>V3+ Equality Delete Vectors (EDV)</b>: Puffin files with Roaring bitmap blobs -
 *       Automatic for single LONG equality fields
 * </ul>
 *
 * <p><b>Key Characteristics:</b>
 *
 * <ul>
 *   <li><b>Standalone</b>: Apply to ALL data files in the table, {@link #referencedDataFile()}
 *       MUST be null
 *   <li><b>Cross-File</b>: Delete matching rows across the entire table, not just one file
 *   <li><b>Compaction-Independent</b>: Remain valid even when data files are rewritten (match on
 *       values, not positions)
 * </ul>
 *
 * <h3>Deletion Vectors (Format V3+)</h3>
 *
 * <p>Both position and equality deletes can use Deletion Vectors in V3+ tables:
 *
 * <ul>
 *   <li><b>Position DVs</b>: File-scoped, {@link #referencedDataFile()} required, {@link
 *       #contentOffset()} and {@link #contentSizeInBytes()} required
 *   <li><b>Equality DVs</b>: Standalone, {@link #referencedDataFile()} must be null, {@link
 *       #contentOffset()} and {@link #contentSizeInBytes()} required
 * </ul>
 *
 * <p>Deletion Vectors use Roaring bitmaps stored in Puffin files for efficient storage and fast
 * read performance.
 *
 * @see FileContent#POSITION_DELETES
 * @see FileContent#EQUALITY_DELETES
 * @see FileFormat#PUFFIN
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
   * Returns the physical encoding of this delete file.
   *
   * <p>The encoding specifies HOW deletes are physically stored, separate from WHAT type of
   * deletes they are (position vs equality). This is an optimization detail that affects storage
   * format and efficiency but not semantics.
   *
   * <p><b>Supported Encodings:</b>
   *
   * <ul>
   *   <li><b>{@link DeleteEncoding#FULL_ROW}</b>: Traditional row-based storage in
   *       Parquet/Avro/ORC format (default)
   *   <li><b>{@link DeleteEncoding#DELETION_VECTOR}</b>: Compressed bitmap storage in PUFFIN
   *       format (for sparse deletes)
   * </ul>
   *
   * <p><b>Default Value:</b> If this method returns null (for delete files written before this
   * field was added), readers should infer the encoding from the file format: PUFFIN format
   * implies DELETION_VECTOR, while Parquet/Avro/ORC implies FULL_ROW.
   *
   * @return the delete encoding, or null if not explicitly set (backward compatibility)
   * @see DeleteEncoding
   */
  default DeleteEncoding encoding() {
    // Default implementation for backward compatibility
    // Infer encoding from format if not explicitly set
    if (format() == FileFormat.PUFFIN) {
      return DeleteEncoding.DELETION_VECTOR;
    }
    return DeleteEncoding.FULL_ROW;
  }

  /**
   * Returns the path to the data file this delete file applies to.
   *
   * <p>This method returns different values depending on the delete file type:
   *
   * <p><b>Position Delete Files (Position DVs):</b>
   *
   * <ul>
   *   <li><b>Position DVs (V3+)</b>: MUST return the referenced data file path (required)
   *   <li><b>Traditional Position Deletes (V2)</b>: MAY return the data file path if file-scoped,
   *       or null if the delete file applies to multiple data files
   * </ul>
   *
   * <p>Position delete files mark specific row positions (e.g., row 100, row 500) in a data file.
   * When stored as Position DVs (V3+ tables), the referenced data file MUST be set because the
   * bitmap positions are meaningless without knowing which file they apply to.
   *
   * <p><b>Equality Delete Files (Equality DVs):</b>
   *
   * <ul>
   *   <li>MUST return null (required)
   * </ul>
   *
   * <p>Equality delete files mark rows based on field values (e.g., id=100, id=200) that apply to
   * ALL data files in the table. They are standalone and not tied to any specific data file, so
   * this method MUST return null.
   *
   * <p><b>Key Architectural Difference:</b>
   *
   * <ul>
   *   <li><b>Position DVs</b>: File-scoped (referencedDataFile != null)
   *   <li><b>Equality DVs</b>: Standalone (referencedDataFile == null)
   * </ul>
   *
   * @return the referenced data file path for Position DVs, null for Equality DVs and
   *     multi-file-scoped traditional position deletes
   * @see #contentOffset()
   * @see #contentSizeInBytes()
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
   * <p>This method applies to Deletion Vectors only (PUFFIN format files):
   *
   * <p><b>Deletion Vectors (Position DVs and Equality DVs):</b>
   *
   * <ul>
   *   <li>MUST return the size in bytes of the Roaring bitmap blob in the Puffin file
   *   <li>Used with {@link #contentOffset()} to enable direct blob access
   *   <li>Indicates the exact number of bytes to read from the Puffin file
   * </ul>
   *
   * <p><b>Traditional Delete Files (Parquet/Avro/ORC):</b>
   *
   * <ul>
   *   <li>MUST return null (not applicable to row-based delete files)
   * </ul>
   *
   * <p>The content size enables efficient reading by allowing readers to:
   *
   * <ol>
   *   <li>Seek directly to {@link #contentOffset()} in the Puffin file
   *   <li>Read exactly {@code contentSizeInBytes} bytes
   *   <li>Deserialize the bitmap without parsing the entire Puffin file
   * </ol>
   *
   * @return the size in bytes of the deletion vector blob for DVs, null for traditional delete
   *     files
   * @see #contentOffset()
   * @see #referencedDataFile()
   */
  default Long contentSizeInBytes() {
    return null;
  }
}
