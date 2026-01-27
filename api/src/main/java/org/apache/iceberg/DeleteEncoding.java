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

import java.util.Locale;

/**
 * Enum representing the physical encoding of delete files.
 *
 * <p>This specifies HOW deletes are physically stored, separate from WHAT type of deletes they
 * are (position vs equality).
 *
 * <p>The encoding is an optimization detail - readers and writers may choose different encodings
 * based on data characteristics and table properties, but all encodings for the same delete type
 * are semantically equivalent.
 */
public enum DeleteEncoding {
  /**
   * Full row storage encoding (default).
   *
   * <p>For position deletes: stores (file_path, row_position) tuples in Parquet/Avro/ORC format.
   *
   * <p>For equality deletes: stores full row data matching equality fields in Parquet/Avro/ORC
   * format.
   *
   * <p>This is the traditional storage format and is always supported.
   */
  FULL_ROW(0),

  /**
   * Deletion Vector encoding using compressed bitmaps.
   *
   * <p>For position deletes: stores a Roaring bitmap of deleted row positions in PUFFIN format.
   *
   * <p>For equality deletes: stores a Roaring bitmap of deleted field values in PUFFIN format.
   * Currently requires a single primitive equality field (LONG type).
   *
   * <p>This encoding is more space-efficient for sparse deletes but has limitations on supported
   * types.
   *
   * <p>Requirements:
   *
   * <ul>
   *   <li>File format must be PUFFIN
   *   <li>For equality deletes: single primitive field only (currently LONG)
   *   <li>For position deletes: must reference a single data file
   * </ul>
   */
  DELETION_VECTOR(1),

  /**
   * Hash-based encoding for composite keys (reserved for future use).
   *
   * <p>This encoding is not yet implemented. It is reserved for future support of equality deletes
   * on composite keys using hash-based storage.
   */
  HASH_MAP(2);

  private final int id;

  DeleteEncoding(int id) {
    this.id = id;
  }

  /**
   * Returns the numeric ID for this encoding.
   *
   * <p>IDs are used for serialization in manifest files.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the DeleteEncoding for the given numeric ID.
   *
   * @param id the numeric ID
   * @return the corresponding DeleteEncoding
   * @throws IllegalArgumentException if the ID is unknown
   */
  public static DeleteEncoding fromId(int id) {
    switch (id) {
      case 0:
        return FULL_ROW;
      case 1:
        return DELETION_VECTOR;
      case 2:
        return HASH_MAP;
      default:
        throw new IllegalArgumentException("Unknown delete encoding id: " + id);
    }
  }

  /**
   * Validates that this encoding is compatible with the given file metadata.
   *
   * @param format the file format
   * @param content the file content type
   * @param equalityFieldIds the equality field IDs (for equality deletes)
   * @param referencedDataFile the referenced data file (for position delete vectors)
   * @throws IllegalArgumentException if the encoding is incompatible with the metadata
   */
  public void validate(
      FileFormat format,
      FileContent content,
      java.util.List<Integer> equalityFieldIds,
      CharSequence referencedDataFile) {

    switch (this) {
      case FULL_ROW:
        // FULL_ROW encoding requires traditional formats
        if (format != FileFormat.PARQUET
            && format != FileFormat.AVRO
            && format != FileFormat.ORC) {
          throw new IllegalArgumentException(
              String.format(
                  Locale.ROOT,
                  "FULL_ROW encoding requires PARQUET, AVRO, or ORC format, got: %s",
                  format));
        }
        break;

      case DELETION_VECTOR:
        // DELETION_VECTOR encoding requires PUFFIN format
        if (format != FileFormat.PUFFIN) {
          throw new IllegalArgumentException(
              String.format(
                  Locale.ROOT,
                  "DELETION_VECTOR encoding requires PUFFIN format, got: %s",
                  format));
        }

        // For equality deletes, currently only single primitive field supported
        if (content == FileContent.EQUALITY_DELETES) {
          if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
            throw new IllegalArgumentException(
                "DELETION_VECTOR encoding for equality deletes requires equality field IDs");
          }
          if (equalityFieldIds.size() != 1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "DELETION_VECTOR encoding currently supports single equality field only, got: %d fields",
                    equalityFieldIds.size()));
          }
        }

        // For position deletes, must have referenced data file
        if (content == FileContent.POSITION_DELETES && referencedDataFile == null) {
          throw new IllegalArgumentException(
              "DELETION_VECTOR encoding for position deletes requires referenced data file");
        }
        break;

      case HASH_MAP:
        throw new UnsupportedOperationException(
            "HASH_MAP encoding is reserved for future use and not yet implemented");

      default:
        throw new IllegalStateException("Unknown encoding: " + this);
    }
  }
}
