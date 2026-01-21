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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Test;

/** Test to verify StructLike matching in StructLikeSet. */
public class TestStructLikeMatching {

  private static final Schema SINGLE_LONG_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Test
  public void testSimpleStructLikeMatching() {
    // Create a set with a single LONG field
    StructLikeSet set = StructLikeSet.create(SINGLE_LONG_SCHEMA.asStruct());

    // Add value 42 using a simple StructLike
    StructLike record1 =
        new StructLike() {
          @Override
          public int size() {
            return 1;
          }

          @Override
          public <T> T get(int pos, Class<T> javaClass) {
            if (pos == 0) {
              return javaClass.cast(42L);
            }
            throw new IllegalArgumentException("Invalid position");
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
          }
        };

    set.add(record1);

    // Create another StructLike with the same value
    StructLike record2 =
        new StructLike() {
          @Override
          public int size() {
            return 1;
          }

          @Override
          public <T> T get(int pos, Class<T> javaClass) {
            if (pos == 0) {
              return javaClass.cast(42L);
            }
            throw new IllegalArgumentException("Invalid position");
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
          }
        };

    // Should match
    assertThat(set.contains(record2)).isTrue();

    // Create a StructLike with different value
    StructLike record3 =
        new StructLike() {
          @Override
          public int size() {
            return 1;
          }

          @Override
          public <T> T get(int pos, Class<T> javaClass) {
            if (pos == 0) {
              return javaClass.cast(99L);
            }
            throw new IllegalArgumentException("Invalid position");
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
          }
        };

    // Should not match
    assertThat(set.contains(record3)).isFalse();
  }

  @Test
  public void testGenericRecordMatching() {
    // Create a set
    StructLikeSet set = StructLikeSet.create(SINGLE_LONG_SCHEMA.asStruct());

    // Add using GenericRecord
    GenericRecord record1 = GenericRecord.create(SINGLE_LONG_SCHEMA.asStruct());
    record1.set(0, 42L);
    set.add(record1.copy());

    // Check with another GenericRecord
    GenericRecord record2 = GenericRecord.create(SINGLE_LONG_SCHEMA.asStruct());
    record2.set(0, 42L);

    assertThat(set.contains(record2)).isTrue();
  }

  @Test
  public void testWrappedRecordMatching() {
    // Create a set
    StructLikeSet set = StructLikeSet.create(SINGLE_LONG_SCHEMA.asStruct());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(SINGLE_LONG_SCHEMA.asStruct());

    // Add using wrapped GenericRecord
    GenericRecord record1 = GenericRecord.create(SINGLE_LONG_SCHEMA.asStruct());
    record1.set(0, 42L);
    set.add(wrapper.copyFor(record1.copy()));

    // Check with simple StructLike
    StructLike simple =
        new StructLike() {
          @Override
          public int size() {
            return 1;
          }

          @Override
          public <T> T get(int pos, Class<T> javaClass) {
            if (pos == 0) {
              return javaClass.cast(42L);
            }
            throw new IllegalArgumentException("Invalid position");
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException();
          }
        };

    assertThat(set.contains(simple)).isTrue();
  }
}
