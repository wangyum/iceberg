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
package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.RewriteTablePathUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

/**
 * Utility methods for working with content files (data files and delete files).
 *
 * <p>This class provides helper methods for:
 *
 * <ul>
 *   <li>Identifying delete file types (Position DVs vs Equality DVs)
 *   <li>Extracting referenced data file information
 *   <li>Copying files with specific statistics settings
 *   <li>Replacing file paths for table migration
 * </ul>
 *
 * <h3>Deletion Vector Type Detection</h3>
 *
 * <p>The following methods help identify different types of deletion vectors:
 *
 * <ul>
 *   <li>{@link #isPositionDV(DeleteFile)} - Checks for Position Deletion Vectors (file-scoped,
 *       PUFFIN format)
 *   <li>{@link #isEqualityDV(DeleteFile)} - Checks for Equality Deletion Vectors (standalone,
 *       PUFFIN format)
 *   <li>{@link #isDVType(DeleteFile, FileContent)} - Convenience method to check if a file matches
 *       a specific DV type
 *   <li>{@link #dvTypeDescription(DeleteFile)} - Returns a human-readable description of the DV
 *       type
 *   <li>{@link #isDV(DeleteFile)} - Deprecated generic check for any deletion vector
 * </ul>
 *
 * <p><b>Key Architectural Distinction:</b>
 *
 * <ul>
 *   <li><b>Position DVs</b>: File-scoped, {@link DeleteFile#referencedDataFile()} != null, marks
 *       row positions in ONE specific data file
 *   <li><b>Equality DVs</b>: Standalone, {@link DeleteFile#referencedDataFile()} == null, marks
 *       equality field values across ALL data files
 * </ul>
 *
 * <p><b>Example Usage:</b>
 *
 * <pre>{@code
 * DeleteFile deleteFile = ...;
 *
 * if (ContentFileUtil.isPositionDV(deleteFile)) {
 *   // Handle Position DV: requires referencedDataFile
 *   String dataFile = deleteFile.referencedDataFile();  // Not null
 *   long offset = deleteFile.contentOffset();
 *   int size = deleteFile.contentSizeInBytes();
 *   // Read bitmap from Puffin file and apply to specific data file
 *
 * } else if (ContentFileUtil.isEqualityDV(deleteFile)) {
 *   // Handle Equality DV: applies to all data files
 *   assert deleteFile.referencedDataFile() == null;  // Always null
 *   long offset = deleteFile.contentOffset();
 *   int size = deleteFile.contentSizeInBytes();
 *   // Read bitmap from Puffin file and apply to all data files
 * }
 * }</pre>
 *
 * @see DeleteFile
 * @see FileContent
 * @see FileFormat
 */
public class ContentFileUtil {
  private ContentFileUtil() {}

  private static final int PATH_ID = MetadataColumns.DELETE_FILE_PATH.fieldId();
  private static final Type PATH_TYPE = MetadataColumns.DELETE_FILE_PATH.type();

  /**
   * Copies the {@link ContentFile} with the specific stat settings.
   *
   * @param file a generic data file to copy.
   * @param withStats whether to keep any stats
   * @param requestedColumnIds column ids for which to keep stats. If <code>null</code> then every
   *     column stat is kept.
   * @return The copied file
   */
  public static <F extends ContentFile<K>, K> K copy(
      F file, boolean withStats, Set<Integer> requestedColumnIds) {
    if (withStats) {
      return requestedColumnIds != null ? file.copyWithStats(requestedColumnIds) : file.copy();
    } else {
      return file.copyWithoutStats();
    }
  }

  public static CharSequence referencedDataFile(DeleteFile deleteFile) {
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      return null;
    }

    if (deleteFile.referencedDataFile() != null) {
      return deleteFile.referencedDataFile();
    }

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(PATH_ID) : null;
    if (lowerPathBound == null) {
      return null;
    }

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(PATH_ID) : null;
    if (upperPathBound == null) {
      return null;
    }

    if (lowerPathBound.equals(upperPathBound)) {
      return Conversions.fromByteBuffer(PATH_TYPE, lowerPathBound);
    } else {
      return null;
    }
  }

  /**
   * Replace file_path reference for a delete file manifest entry, if file_path field's lower_bound
   * and upper_bound metrics are equal. Else clear file_path lower and upper bounds.
   *
   * @param deleteFile delete file whose entry will be replaced
   * @param sourcePrefix source prefix which will be replaced
   * @param targetPrefix target prefix which will replace it
   * @return metrics for the new delete file entry
   */
  public static Metrics replacePathBounds(
      DeleteFile deleteFile, String sourcePrefix, String targetPrefix) {
    Preconditions.checkArgument(
        deleteFile.content() == FileContent.POSITION_DELETES,
        "Only position delete files supported");

    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(PATH_ID) : null;
    if (lowerPathBound == null) {
      return metricsWithoutPathBounds(deleteFile);
    }

    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(PATH_ID) : null;
    if (upperPathBound == null) {
      return metricsWithoutPathBounds(deleteFile);
    }

    if (lowerPathBound.equals(upperPathBound)) {
      CharBuffer path = Conversions.fromByteBuffer(PATH_TYPE, lowerPathBound);
      CharBuffer newPath =
          CharBuffer.wrap(
              RewriteTablePathUtil.newPath(path.toString(), sourcePrefix, targetPrefix));
      ByteBuffer newBytes = Conversions.toByteBuffer(PATH_TYPE, newPath);
      return metricsWithPathBounds(deleteFile, newBytes);
    } else {
      // The file_path's lower_bound and upper_bound are only used for filtering data files when
      // both values match
      // (file-scoped position delete). Hence do not rewrite but set null if they do not match.
      return metricsWithoutPathBounds(deleteFile);
    }
  }

  public static String referencedDataFileLocation(DeleteFile deleteFile) {
    CharSequence location = referencedDataFile(deleteFile);
    return location != null ? location.toString() : null;
  }

  public static boolean isFileScoped(DeleteFile deleteFile) {
    return referencedDataFile(deleteFile) != null;
  }

  /**
   * Checks if a delete file is any type of deletion vector (Position or Equality).
   *
   * @param deleteFile the delete file to check
   * @return true if the file is in PUFFIN format (either Position DV or Equality DV)
   * @deprecated Use {@link #isPositionDV(DeleteFile)} or {@link #isEqualityDV(DeleteFile)} for
   *     clarity. This method will be removed in a future release.
   */
  @Deprecated
  public static boolean isDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN;
  }

  /**
   * Checks if a delete file is a Position Deletion Vector.
   *
   * <p>Position DVs are file-scoped deletion vectors that mark specific row positions for deletion
   * in a referenced data file. They are stored in PUFFIN format and must have a referencedDataFile
   * set.
   *
   * @param deleteFile the delete file to check
   * @return true if the file is a Position DV (PUFFIN format + POSITION_DELETES content)
   */
  public static boolean isPositionDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN
        && deleteFile.content() == FileContent.POSITION_DELETES;
  }

  /**
   * Checks if a delete file is an Equality Deletion Vector.
   *
   * <p>Equality DVs are standalone deletion vectors that mark equality field values for deletion
   * across all data files. They are stored in PUFFIN format and must NOT have a referencedDataFile
   * (they're not tied to a specific data file).
   *
   * @param deleteFile the delete file to check
   * @return true if the file is an Equality DV (PUFFIN format + EQUALITY_DELETES content)
   */
  public static boolean isEqualityDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN
        && deleteFile.content() == FileContent.EQUALITY_DELETES;
  }

  public static boolean containsSingleDV(Iterable<DeleteFile> deleteFiles) {
    return Iterables.size(deleteFiles) == 1 && Iterables.all(deleteFiles, ContentFileUtil::isDV);
  }

  /**
   * Returns a human-readable description of the DV type.
   *
   * <p>This method provides a quick way to get a string description of a delete file's type,
   * useful for logging, debugging, and error messages.
   *
   * @param deleteFile the delete file to describe
   * @return a description string: "Position DV", "Equality DV", "Unknown DV type", or "Not a DV"
   */
  public static String dvTypeDescription(DeleteFile deleteFile) {
    if (isPositionDV(deleteFile)) {
      return "Position DV";
    }
    if (isEqualityDV(deleteFile)) {
      return "Equality DV";
    }
    if (deleteFile.format() == FileFormat.PUFFIN) {
      return "Unknown DV type";
    }
    return "Not a DV";
  }

  /**
   * Checks if a delete file matches the expected DV type.
   *
   * <p>This is a convenience method that combines format and content checks in a single call,
   * useful when you need to validate that a file is both a DV and of a specific type.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * // Validate that a file is specifically a Position DV
   * if (ContentFileUtil.isDVType(deleteFile, FileContent.POSITION_DELETES)) {
   *   // Handle position DV
   * }
   *
   * // Validate that a file is specifically an Equality DV
   * if (ContentFileUtil.isDVType(deleteFile, FileContent.EQUALITY_DELETES)) {
   *   // Handle equality DV
   * }
   * }</pre>
   *
   * @param deleteFile the delete file to check
   * @param expectedContent the expected content type (POSITION_DELETES or EQUALITY_DELETES)
   * @return true if the file is in PUFFIN format AND has the expected content type
   */
  public static boolean isDVType(DeleteFile deleteFile, FileContent expectedContent) {
    return deleteFile.format() == FileFormat.PUFFIN
        && deleteFile.content() == expectedContent;
  }

  public static String dvDesc(DeleteFile deleteFile) {
    return String.format(
        "DV{location=%s, offset=%s, length=%s, referencedDataFile=%s}",
        deleteFile.location(),
        deleteFile.contentOffset(),
        deleteFile.contentSizeInBytes(),
        deleteFile.referencedDataFile());
  }

  private static Metrics metricsWithoutPathBounds(DeleteFile file) {
    Map<Integer, ByteBuffer> lowerBounds =
        file.lowerBounds() == null ? null : Maps.newHashMap(file.lowerBounds());
    Map<Integer, ByteBuffer> upperBounds =
        file.upperBounds() == null ? null : Maps.newHashMap(file.upperBounds());
    if (lowerBounds != null) {
      lowerBounds.remove(PATH_ID);
    }
    if (upperBounds != null) {
      upperBounds.remove(PATH_ID);
    }

    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        lowerBounds == null ? null : Collections.unmodifiableMap(lowerBounds),
        upperBounds == null ? null : Collections.unmodifiableMap(upperBounds));
  }

  private static Metrics metricsWithPathBounds(DeleteFile file, ByteBuffer bound) {
    Map<Integer, ByteBuffer> lowerBounds =
        file.lowerBounds() == null ? null : Maps.newHashMap(file.lowerBounds());
    Map<Integer, ByteBuffer> upperBounds =
        file.upperBounds() == null ? null : Maps.newHashMap(file.upperBounds());
    if (lowerBounds != null) {
      lowerBounds.put(PATH_ID, bound);
    }
    if (upperBounds != null) {
      upperBounds.put(PATH_ID, bound);
    }

    return new Metrics(
        file.recordCount(),
        file.columnSizes(),
        file.valueCounts(),
        file.nullValueCounts(),
        file.nanValueCounts(),
        lowerBounds == null ? null : Collections.unmodifiableMap(lowerBounds),
        upperBounds == null ? null : Collections.unmodifiableMap(upperBounds));
  }
}
