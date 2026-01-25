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

class EqualityAccumulator implements BitmapAccumulator {
  private static final String EQUALITY_FIELD_ID_KEY = "equality-field-id";
  private static final String CARDINALITY_KEY = "cardinality";
  private static final String VALUE_MIN_KEY = "value-min";
  private static final String VALUE_MAX_KEY = "value-max";

  private final int equalityFieldId;
  private final RoaringPositionBitmap bitmap;
  private long minValue = Long.MAX_VALUE;
  private long maxValue = Long.MIN_VALUE;

  EqualityAccumulator(int equalityFieldId, RoaringPositionBitmap bitmap) {
    this.equalityFieldId = equalityFieldId;
    this.bitmap = bitmap;
  }

  @Override
  public void add(long value) {
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ROOT,
              "Equality delete vector only supports non-negative values, got %d for field %d. "
                  + "Use traditional equality deletes for negative values.",
              value,
              equalityFieldId));
    }
    bitmap.set(value);
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  @Override
  public void merge(
      Function<String, PositionDeleteIndex> loadPreviousDeletes,
      List<DeleteFile> rewrittenDeleteFiles) {
    // No-op for equality deletes
  }

  @Override
  public Iterable<CharSequence> referencedDataFiles() {
    return Collections.emptyList();
  }

  @Override
  public Blob toBlob() {
    bitmap.runLengthEncode();
    long sizeInBytes = bitmap.serializedSizeInBytes();
    if (sizeInBytes > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Bitmap size %d bytes exceeds maximum ByteBuffer size %d. "
                  + "Consider splitting the delete set or using traditional deletes.",
              sizeInBytes,
              Integer.MAX_VALUE));
    }

    int size = (int) sizeInBytes;
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    bitmap.serialize(buffer);
    buffer.flip();
    long cardinality = bitmap.cardinality();

    Map<String, String> properties = Maps.newHashMap();
    properties.put(CARDINALITY_KEY, String.valueOf(cardinality));
    properties.put(EQUALITY_FIELD_ID_KEY, String.valueOf(equalityFieldId));
    properties.put(VALUE_MIN_KEY, String.valueOf(minValue));
    properties.put(VALUE_MAX_KEY, String.valueOf(maxValue));

    return new Blob(
        StandardBlobTypes.EDV_V1,
        ImmutableList.of(equalityFieldId),
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        buffer,
        null /* uncompressed */,
        ImmutableMap.copyOf(properties));
  }

  @Override
  public long cardinality() {
    return bitmap.cardinality();
  }

  public RoaringPositionBitmap bitmap() {
    return bitmap;
  }
}
