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

import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;

/**
 * Bitmap accumulator for position deletes.
 *
 * <p>Extends {@link BitmapAccumulator} with position-specific operations:
 * <ul>
 *   <li>Merging with previous delete files</li>
 *   <li>Tracking referenced data files</li>
 * </ul>
 *
 * <p>This interface separates position delete concerns from equality delete concerns,
 * making the abstraction clearer and eliminating unnecessary no-op methods.
 */
interface PositionBitmapAccumulator extends BitmapAccumulator {

  /**
   * Merges this accumulator with previous delete files for the same data file.
   *
   * <p>This is necessary for position deletes to consolidate multiple delete files
   * pointing to the same data file.
   *
   * @param loadPreviousDeletes function to load previous position deletes
   * @param rewrittenDeleteFiles list to collect delete files that were rewritten
   */
  void merge(
      Function<String, PositionDeleteIndex> loadPreviousDeletes,
      List<DeleteFile> rewrittenDeleteFiles);

  /**
   * Returns the data file paths referenced by this accumulator.
   *
   * <p>For position deletes, this is the set of data files that have deleted positions.
   *
   * @return iterable of data file paths
   */
  Iterable<CharSequence> referencedDataFiles();
}
