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
import java.util.Locale;
import java.util.function.BiFunction;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.BaseEDVFileWriter;
import org.apache.iceberg.deletes.EDVFileWriter;
import org.apache.iceberg.deletes.EqualityDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Partitioning writer for equality delete vectors (EDVs).
 *
 * <p>This class mirrors {@link PartitioningDVWriter} to maintain consistency in Iceberg's delete
 * file patterns. It wraps {@link BaseEDVFileWriter} and provides:
 *
 * <ul>
 *   <li>Value extraction from engine-specific row types
 *   <li>Validation of non-NULL, non-negative values
 *   <li>Delegation to core EDV writer
 * </ul>
 *
 * @param <T> the row data type (engine-specific, e.g., InternalRow for Spark)
 */
public class PartitioningEDVWriter<T>
    implements PartitioningWriter<EqualityDelete<T>, DeleteWriteResult> {

  private final EDVFileWriter writer;
  private final BiFunction<T, Integer, Long> valueExtractor;
  private DeleteWriteResult result;

  /**
   * Creates a new partitioning EDV writer.
   *
   * @param fileFactory factory for creating output files
   * @param valueExtractor function to extract LONG values from rows
   */
  public PartitioningEDVWriter(
      OutputFileFactory fileFactory, BiFunction<T, Integer, Long> valueExtractor) {
    this.writer = new BaseEDVFileWriter(fileFactory);
    this.valueExtractor = valueExtractor;
  }

  @Override
  public void write(EqualityDelete<T> row, PartitionSpec spec, StructLike partition) {
    T data = row.data();
    int equalityFieldId = row.equalityFieldId();

    // Extract the LONG value from the row
    Long value = valueExtractor.apply(data, equalityFieldId);

    // Validate: NULL not supported
    if (value == null) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Equality delete vector does not support NULL values for field %d. "
                  + "Use traditional equality deletes for NULL.",
              equalityFieldId));
    }

    // Delegate to base writer (which validates non-negative)
    writer.delete(value, equalityFieldId, spec, partition);
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
}
