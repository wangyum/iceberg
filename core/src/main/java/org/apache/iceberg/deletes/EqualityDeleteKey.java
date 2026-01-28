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

import java.util.Objects;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * Delete key for equality delete vectors (EDVs).
 *
 * <p>Keyed by equality field ID - each equality field gets its own bitmap of deleted values.
 *
 * <p><b>DESIGN CONSTRAINTS:</b>
 *
 * <h3>LONG-Only Support</h3>
 * EDVs currently support only single LONG fields. This covers approximately 80% of real-world
 * use cases:
 * <ul>
 *   <li>Auto-increment IDs (user_id, order_id, customer_id)</li>
 *   <li>Timestamps (epoch milliseconds)</li>
 *   <li>Counters and sequence numbers</li>
 * </ul>
 *
 * <p>This constraint keeps implementation simple and maintainable. For non-LONG equality deletes,
 * use traditional equality delete files (full-row Parquet files). The performance difference is
 * minimal for:
 * <ul>
 *   <li>Small delete sets (< 1000 rows)</li>
 *   <li>Non-numeric keys (UUIDs, strings)</li>
 *   <li>Composite keys</li>
 * </ul>
 *
 * <h3>Non-Negative Values Only</h3>
 * EDVs store values in Roaring bitmaps, which require non-negative integers. Negative values
 * are rejected at write time with a clear error message pointing users to traditional deletes.
 *
 * <h3>Core Benefit: Compaction Conflict Avoidance</h3>
 * EDVs remain valid after file compaction because they reference <b>logical values</b> (user IDs)
 * not <b>physical positions</b> (row offsets). This enables:
 * <ul>
 *   <li>Concurrent compaction + MERGE INTO operations</li>
 *   <li>No serialization conflicts between operations</li>
 *   <li>Better throughput for high-update CDC tables</li>
 *   <li>Simplified operational workflows</li>
 * </ul>
 *
 * <h3>When to Use EDVs</h3>
 * <table border="1">
 *   <tr><th>Scenario</th><th>Recommendation</th></tr>
 *   <tr><td>High-throughput CDC table with LONG primary key</td><td>✅ Use EDV</td></tr>
 *   <tr><td>Continuous MERGE INTO + background compaction</td><td>✅ Use EDV</td></tr>
 *   <tr><td>Frequent deletes on LONG equality field</td><td>✅ Use EDV</td></tr>
 *   <tr><td>String/UUID primary key</td><td>❌ Use traditional equality deletes</td></tr>
 *   <tr><td>Composite key (multiple columns)</td><td>❌ Use traditional equality deletes</td></tr>
 *   <tr><td>Negative ID values</td><td>❌ Use traditional equality deletes</td></tr>
 *   <tr><td>Small infrequent deletes</td><td>❌ Use traditional equality deletes</td></tr>
 * </table>
 *
 * <p><b>Configuration:</b>
 * <pre>{@code
 * table.updateProperties()
 *   .set(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName())
 *   .set(TableProperties.DELETE_STRATEGY, TableProperties.DELETE_STRATEGY_EQUALITY)
 *   .commit();
 * }</pre>
 */
public class EqualityDeleteKey implements DeleteKey {
  private final int equalityFieldId;
  private final PartitionSpec spec;
  private final StructLike partition;

  public EqualityDeleteKey(int equalityFieldId, PartitionSpec spec, StructLike partition) {
    this.equalityFieldId = equalityFieldId;
    this.spec = spec;
    this.partition = StructLikeUtil.copy(partition);
  }

  public int equalityFieldId() {
    return equalityFieldId;
  }

  @Override
  public String keyId() {
    return "field:" + equalityFieldId;
  }

  @Override
  public DeleteFile toDeleteFile(
      String puffinPath, long puffinSize, BlobMetadata blobMetadata, long cardinality) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes(equalityFieldId)
        .withFormat(FileFormat.PUFFIN)
        .withPath(puffinPath)
        .withPartition(partition)
        .withFileSizeInBytes(puffinSize)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(cardinality)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EqualityDeleteKey)) {
      return false;
    }
    EqualityDeleteKey that = (EqualityDeleteKey) o;
    return equalityFieldId == that.equalityFieldId;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(equalityFieldId);
  }
}
