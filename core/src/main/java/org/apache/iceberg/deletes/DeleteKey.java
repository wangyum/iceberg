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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.puffin.BlobMetadata;

/**
 * Abstraction for different types of bitmap-based delete keys.
 *
 * <p>This allows {@link BitmapDeleteWriter} to handle both position deletes (keyed by data file
 * path) and equality deletes (keyed by equality field ID) with the same infrastructure.
 *
 * <p>Implementations:
 *
 * <ul>
 *   <li>{@link PositionDeleteKey} - For position deletion vectors (existing)
 *   <li>{@link EqualityDeleteKey} - For equality delete vectors (new)
 * </ul>
 */
public interface DeleteKey {
  /**
   * Returns a unique string identifier for this delete key.
   *
   * <p>For position deletes: the data file path For equality deletes: "field:" + field ID
   *
   * @return unique key identifier
   */
  String keyId();

  /**
   * Returns the blob type for this delete key.
   *
   * @return blob type (deletion-vector-v1 or equality-delete-vector-v1)
   */
  String blobType();

  /**
   * Creates a DeleteFile metadata entry from the written blob.
   *
   * @param puffinPath the Puffin file path
   * @param puffinSize the Puffin file size
   * @param blobMetadata the blob metadata
   * @param cardinality the number of deleted values in the bitmap
   * @param spec the partition spec
   * @param partition the partition
   * @return DeleteFile metadata
   */
  DeleteFile toDeleteFile(
      String puffinPath,
      long puffinSize,
      BlobMetadata blobMetadata,
      long cardinality,
      PartitionSpec spec,
      StructLike partition);

  /**
   * Returns the partition spec for this delete.
   *
   * @return partition spec
   */
  PartitionSpec spec();

  /**
   * Returns the partition for this delete.
   *
   * @return partition
   */
  StructLike partition();
}
