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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
  public void testContainsPositiveValues() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    bitmap.set(200);
    bitmap.set(300);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThat(set.contains(record(100L, "a"))).isTrue();
    assertThat(set.contains(record(200L, "b"))).isTrue();
    assertThat(set.contains(record(300L, "c"))).isTrue();
    assertThat(set.contains(record(400L, "d"))).isFalse();
  }

  @Test
  public void testContainsNullValue() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    // NULL values should not be in the bitmap (EDV doesn't support NULLs)
    assertThat(set.contains(record(null, "test"))).isFalse();
  }

  @Test
  public void testContainsNegativeValue() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    // Negative values should not be in the bitmap (EDV doesn't support negatives)
    assertThat(set.contains(record(-100L, "test"))).isFalse();
  }

  @Test
  public void testSize() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    bitmap.set(200);
    bitmap.set(300);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThat(set.size()).isEqualTo(3);
  }

  @Test
  public void testIsEmpty() {
    RoaringPositionBitmap emptyBitmap = new RoaringPositionBitmap();
    BitmapBackedStructLikeSet emptySet =
        new BitmapBackedStructLikeSet(emptyBitmap, EQUALITY_FIELD_ID, SCHEMA);
    assertThat(emptySet.isEmpty()).isTrue();

    RoaringPositionBitmap nonEmptyBitmap = new RoaringPositionBitmap();
    nonEmptyBitmap.set(100);
    BitmapBackedStructLikeSet nonEmptySet =
        new BitmapBackedStructLikeSet(nonEmptyBitmap, EQUALITY_FIELD_ID, SCHEMA);
    assertThat(nonEmptySet.isEmpty()).isFalse();
  }

  @Test
  public void testLargeCardinality() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    for (long i = 0; i < 10000; i++) {
      bitmap.set(i);
    }

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThat(set.size()).isEqualTo(10000);
    assertThat(set.contains(record(5000L, "test"))).isTrue();
    assertThat(set.contains(record(20000L, "test"))).isFalse();
  }

  @Test
  public void testIteratorUnsupported() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);

    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThatThrownBy(() -> set.iterator())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Iteration is not supported");
  }

  @Test
  public void testAddUnsupported() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThatThrownBy(() -> set.add(record(100L, "test")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("read-only");
  }

  @Test
  public void testRemoveUnsupported() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    assertThatThrownBy(() -> set.remove(record(100L, "test")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("read-only");
  }

  @Test
  public void testInvalidFieldId() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    // Non-existent field ID
    assertThatThrownBy(() -> new BitmapBackedStructLikeSet(bitmap, 999, SCHEMA))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found in schema");
  }

  @Test
  public void testNonLongFieldType() {
    Schema stringSchema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();

    // Field is not LONG type
    assertThatThrownBy(() -> new BitmapBackedStructLikeSet(bitmap, 1, stringSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be LONG type");
  }

  @Test
  public void testContainsNonStructLike() {
    RoaringPositionBitmap bitmap = new RoaringPositionBitmap();
    bitmap.set(100);
    BitmapBackedStructLikeSet set =
        new BitmapBackedStructLikeSet(bitmap, EQUALITY_FIELD_ID, SCHEMA);

    // Non-StructLike object should return false
    assertThat(set.contains("not a struct")).isFalse();
    assertThat(set.contains(123)).isFalse();
    assertThat(set.contains(null)).isFalse();
  }

  private Record record(Long id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }
}
