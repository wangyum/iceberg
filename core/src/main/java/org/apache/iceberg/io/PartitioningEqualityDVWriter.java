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
package org.apache.iceberg.io;

import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.EqualityDVWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * PartitioningEqualityDVWriter accumulates equality deletes across partitions and writes out
 * equality deletion vector files in PUFFIN format with Roaring bitmaps.
 *
 * <p>This writer is used for UPDATE operations in merge-on-read mode when equality delete vectors
 * are enabled.
 *
 * @param <T> the row type (e.g., InternalRow for Spark)
 */
public class PartitioningEqualityDVWriter<T> implements PartitioningWriter<T, DeleteWriteResult> {

  private final EqualityDVWriter writer;
  private final int equalityFieldId;
  private final FieldValueExtractor<T> valueExtractor;
  private DeleteWriteResult result;

  public PartitioningEqualityDVWriter(
      OutputFileFactory fileFactory,
      int equalityFieldId,
      FieldValueExtractor<T> valueExtractor) {
    this.writer = new EqualityDVWriter(fileFactory);
    this.equalityFieldId = equalityFieldId;
    this.valueExtractor = valueExtractor;
  }

  @Override
  public void write(T row, PartitionSpec spec, StructLike partition) {
    Long value = valueExtractor.extract(row, equalityFieldId);
    if (value != null) {
      writer.delete(equalityFieldId, value, spec, partition);
    }
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      writer.close();
      this.result = writer.result();
    }
  }

  /**
   * Interface for extracting LONG field values from rows.
   *
   * @param <T> the row type
   */
  public interface FieldValueExtractor<T> {
    /**
     * Extracts the value of the specified field from the row.
     *
     * @param row the row to extract from
     * @param fieldId the field ID to extract
     * @return the extracted Long value, or null if the field is null
     */
    Long extract(T row, int fieldId);
  }
}
