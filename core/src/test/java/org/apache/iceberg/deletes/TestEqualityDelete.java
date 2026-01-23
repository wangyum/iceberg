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

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link EqualityDelete}.
 *
 * <p>Mirrors {@code PositionDelete} test pattern.
 */
public class TestEqualityDelete {

  @Test
  public void testCreate() {
    String data = "test-data";
    int fieldId = 1;

    EqualityDelete<String> delete = EqualityDelete.create(data, fieldId);

    assertThat(delete).isNotNull();
    assertThat(delete.data()).isEqualTo(data);
    assertThat(delete.equalityFieldId()).isEqualTo(fieldId);
  }

  @Test
  public void testCreateWithNullData() {
    assertThatThrownBy(() -> EqualityDelete.create(null, 1))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("cannot be null");
  }

  @Test
  public void testSet() {
    EqualityDelete<String> delete = EqualityDelete.create("initial", 1);

    delete.set("updated", 2);

    assertThat(delete.data()).isEqualTo("updated");
    assertThat(delete.equalityFieldId()).isEqualTo(2);
  }

  @Test
  public void testSetReturnsSelf() {
    EqualityDelete<String> delete = EqualityDelete.create("initial", 1);

    EqualityDelete<String> result = delete.set("updated", 2);

    assertThat(result).isSameAs(delete);
  }

  @Test
  public void testGenericType() {
    // Test with Integer data
    EqualityDelete<Integer> intDelete = EqualityDelete.create(42, 1);
    assertThat(intDelete.data()).isEqualTo(42);

    // Test with custom object
    TestRow row = new TestRow(100L, "test");
    EqualityDelete<TestRow> rowDelete = EqualityDelete.create(row, 2);
    assertThat(rowDelete.data()).isEqualTo(row);
    assertThat(rowDelete.data().id).isEqualTo(100L);
  }

  /** Simple test class for generic type testing. */
  private static class TestRow {
    final Long id;
    final String value;

    TestRow(Long id, String value) {
      this.id = id;
      this.value = value;
    }
  }
}
