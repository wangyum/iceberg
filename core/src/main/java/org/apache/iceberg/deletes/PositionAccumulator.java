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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ContentFileUtil;

class PositionAccumulator implements BitmapAccumulator {
  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final String dataFilePath;
  private final PositionDeleteIndex positionIndex;

  PositionAccumulator(String dataFilePath, PositionDeleteIndex positionIndex) {
    this.dataFilePath = dataFilePath;
    this.positionIndex = positionIndex;
  }

  @Override
  public void add(long value) {
    positionIndex.delete(value);
  }

  @Override
  public void merge(
      Function<String, PositionDeleteIndex> loadPreviousDeletes,
      List<DeleteFile> rewrittenDeleteFiles) {
    PositionDeleteIndex previousDeletes = loadPreviousDeletes.apply(dataFilePath);
    if (previousDeletes != null) {
      positionIndex.merge(previousDeletes);
      for (DeleteFile previousDeleteFile : previousDeletes.deleteFiles()) {
        // only DVs and file-scoped deletes can be discarded from the table state
        if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
          rewrittenDeleteFiles.add(previousDeleteFile);
        }
      }
    }
  }

  @Override
  public Iterable<CharSequence> referencedDataFiles() {
    return Collections.singleton(dataFilePath);
  }

  @Override
  public Blob toBlob() {
    ByteBuffer buffer = positionIndex.serialize();
    long cardinality = positionIndex.cardinality();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CARDINALITY_KEY, String.valueOf(cardinality));
    properties.put(REFERENCED_DATA_FILE_KEY, dataFilePath);

    return new Blob(
        StandardBlobTypes.DV_V1,
        ImmutableList.of(),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        buffer,
        null /* uncompressed */,
        ImmutableMap.copyOf(properties));
  }

  @Override
  public long cardinality() {
    return positionIndex.cardinality();
  }

  public PositionDeleteIndex positionIndex() {
    return positionIndex;
  }
}
