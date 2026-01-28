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

import org.apache.iceberg.puffin.Blob;

/**
 * Base interface for bitmap-based delete accumulators.
 *
 * <p>Accumulates delete values in memory and serializes them to PUFFIN blobs.
 * Used by {@link BitmapDeleteWriter} for both position and equality deletes.
 *
 * <p>Implementations:
 * <ul>
 *   <li>{@link PositionAccumulator} - For position deletes (via {@link PositionBitmapAccumulator})</li>
 *   <li>{@link EqualityAccumulator} - For equality deletes</li>
 * </ul>
 */
interface BitmapAccumulator {

  /**
   * Adds a value to this accumulator.
   *
   * <p>For position deletes, this is a row position.
   * For equality deletes, this is a LONG field value.
   *
   * @param value the value to add (must be non-negative)
   */
  void add(long value);

  /**
   * Serializes this accumulator to a PUFFIN blob.
   *
   * @return the serialized blob
   */
  Blob toBlob();

  /**
   * Returns the number of values in this accumulator.
   *
   * @return the cardinality
   */
  long cardinality();
}
