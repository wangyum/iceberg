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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;

/**
 * A writer for equality delete vector (EDV) files.
 *
 * <p>EDV files store deleted values as bitmaps in Puffin format, similar to how {@link
 * DVFileWriter} stores position deletes.
 *
 * <p>This interface mirrors {@link DVFileWriter} to maintain consistency in Iceberg's delete file
 * patterns.
 */
public interface EDVFileWriter extends Closeable {
  /**
   * Records a deleted value for an equality field.
   *
   * <p>The value is accumulated in memory and written to a Puffin file when {@link #close()} is
   * called.
   *
   * @param value the deleted value (must be non-negative LONG)
   * @param equalityFieldId the field ID of the equality delete column
   * @param spec the partition spec for the delete
   * @param partition the partition for the delete
   * @throws IllegalArgumentException if value is negative or NULL
   */
  void delete(long value, int equalityFieldId, PartitionSpec spec, StructLike partition);

  /**
   * Returns the result of this writer after {@link #close()} has been called.
   *
   * @return the delete write result containing the written delete files
   * @throws IllegalStateException if called before close()
   */
  DeleteWriteResult result();
}
