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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteEncoding;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDeleteWriterFactory {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final Schema INT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  @Test
  public void testAutoModeWithSingleLongField() {
    Table table = mockTable(new HashMap<>());
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    List<Integer> equalityFieldIds = Collections.singletonList(1);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.DELETION_VECTOR);
  }

  @Test
  public void testAutoModeWithMultipleFields() {
    Table table = mockTable(new HashMap<>());
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    List<Integer> equalityFieldIds = List.of(1, 2);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.FULL_ROW);
  }

  @Test
  public void testAutoModeWithNonLongField() {
    Table table = mockTable(new HashMap<>());
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    List<Integer> equalityFieldIds = Collections.singletonList(1);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, INT_SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.FULL_ROW);
  }

  @Test
  public void testHintFullRow() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.DELETE_ENCODING_HINT, "full-row");

    Table table = mockTable(properties);
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    List<Integer> equalityFieldIds = Collections.singletonList(1);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.FULL_ROW);
  }

  @Test
  public void testHintDeletionVector() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.DELETE_ENCODING_HINT, "deletion-vector");

    Table table = mockTable(properties);
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    List<Integer> equalityFieldIds = Collections.singletonList(1);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.DELETION_VECTOR);
  }

  @Test
  public void testHintDeletionVectorFallbackToFullRow() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.DELETE_ENCODING_HINT, "deletion-vector");

    Table table = mockTable(properties);
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    // Multiple fields - not applicable for deletion vector
    List<Integer> equalityFieldIds = List.of(1, 2);
    DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, SCHEMA);

    assertThat(encoding).isEqualTo(DeleteEncoding.FULL_ROW);
  }

  @Test
  public void testPositionDeleteEncodingDefault() {
    Table table = mockTable(new HashMap<>());
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    DeleteEncoding encoding = factory.selectPositionDeleteEncoding();

    assertThat(encoding).isEqualTo(DeleteEncoding.DELETION_VECTOR);
  }

  @Test
  public void testPositionDeleteEncodingFullRowHint() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.DELETE_ENCODING_HINT, "full-row");

    Table table = mockTable(properties);
    DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);

    DeleteEncoding encoding = factory.selectPositionDeleteEncoding();

    assertThat(encoding).isEqualTo(DeleteEncoding.FULL_ROW);
  }

  private Table mockTable(Map<String, String> properties) {
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(properties);
    return table;
  }
}
