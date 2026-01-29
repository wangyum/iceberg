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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestBitmapBackedStructLikeSet {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final int EQUALITY_FIELD_ID = 1;

  @Test
  public void testContains() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    bitmap.set(200);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThat(set.contains(record(100L, "a"))).isTrue();
    assertThat(set.contains(record(200L, "b"))).isTrue();
    assertThat(set.contains(record(300L, "c"))).isFalse();
    assertThat(set.contains(record(null, "d"))).isFalse();
    assertThat(set.contains(record(-100L, "e"))).isFalse();
  }

  @Test
  public void testSizeAndEmpty() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    bitmap.set(200);
    bitmap.set(300);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThat(set.size()).isEqualTo(3);
    assertThat(set.isEmpty()).isFalse();

    RoaringPositionBitmap emptyBitmap = new RoaringPositionBitmap();
    BitmapBackedStructLikeSet emptySet =
        new BitmapBackedStructLikeSet(emptyBitmap, EQUALITY_FIELD_ID, SCHEMA);
    assertThat(emptySet.isEmpty()).isTrue();
  }

  private Record record(Long id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }
}
