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
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.deletes.EqualityDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * PartitioningEqualityDeleteWriter accumulates equality deletes across partitions and writes them
 * as deletion vectors (bitmap format in PUFFIN files).
 *
 * <p>This writer is used when Spark SQL DELETE operations are configured to use equality deletes
 * instead of position deletes via the {@code write.delete.strategy=equality} table property.
 *
 * <p>Delete files are automatically written in PUFFIN format with bitmap encoding for optimal
 * storage efficiency.
 */
public class PartitioningEqualityDeleteWriter<T>
    implements PartitioningWriter<EqualityDelete<T>, DeleteWriteResult> {

  private final BitmapDeleteWriter bitmapWriter;
  private final int equalityFieldId;
  private DeleteWriteResult result;

  /**
   * Creates a new equality delete writer.
   *
   * @param fileFactory file factory for creating delete files (will use PUFFIN format)
   * @param equalityFieldId the equality field ID
   */
  public PartitioningEqualityDeleteWriter(OutputFileFactory fileFactory, int equalityFieldId) {
    this.bitmapWriter = new BitmapDeleteWriter(fileFactory);
    this.equalityFieldId = equalityFieldId;
  }

  @Override
  public void write(EqualityDelete<T> row, PartitionSpec spec, StructLike partition) {
    long value = row.value();
    bitmapWriter.deleteEquality(equalityFieldId, value, spec, partition);
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(result != null, "Cannot get result from unclosed writer");
    return result;
  }

  @Override
  public void close() throws IOException {
    if (result == null) {
      bitmapWriter.close();
      this.result = bitmapWriter.result();
    }
  }
}
