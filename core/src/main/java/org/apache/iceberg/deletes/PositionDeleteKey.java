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
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.util.StructLikeUtil;

/**
 * Delete key for position deletion vectors.
 *
 * <p>Keyed by data file path - each data file gets its own bitmap of deleted positions.
 */
public class PositionDeleteKey implements DeleteKey {
  private final String dataFilePath;
  private final PartitionSpec spec;
  private final StructLike partition;

  public PositionDeleteKey(String dataFilePath, PartitionSpec spec, StructLike partition) {
    this.dataFilePath = dataFilePath;
    this.spec = spec;
    this.partition = StructLikeUtil.copy(partition);
  }

  public String dataFilePath() {
    return dataFilePath;
  }

  @Override
  public String keyId() {
    return dataFilePath;
  }

  @Override
  public String blobType() {
    return StandardBlobTypes.DV_V1;
  }

  @Override
  public DeleteFile toDeleteFile(
      String puffinPath,
      long puffinSize,
      BlobMetadata blobMetadata,
      long cardinality,
      PartitionSpec spec,
      StructLike partition) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(puffinPath)
        .withPartition(partition)
        .withFileSizeInBytes(puffinSize)
        .withReferencedDataFile(dataFilePath)
        .withContentOffset(blobMetadata.offset())
        .withContentSizeInBytes(blobMetadata.length())
        .withRecordCount(cardinality)
        .build();
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public StructLike partition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PositionDeleteKey)) {
      return false;
    }
    PositionDeleteKey that = (PositionDeleteKey) o;
    return Objects.equals(dataFilePath, that.dataFilePath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataFilePath);
  }
}
