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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Represents an equality delete row for use with equality delete vectors.
 *
 * <p>This class mirrors {@link PositionDelete} to maintain consistency in Iceberg's delete
 * patterns.
 *
 * <p>Unlike position deletes which reference (path, position), equality deletes reference a data
 * row and the equality field ID to extract the delete value from.
 *
 * @param <T> the row data type
 */
public class EqualityDelete<T> {
  private T data;
  private int equalityFieldId;

  private EqualityDelete(T data, int equalityFieldId) {
    this.data = data;
    this.equalityFieldId = equalityFieldId;
  }

  /**
   * Creates an equality delete for the given row and field.
   *
   * @param data the row data containing the delete value
   * @param equalityFieldId the field ID to extract from the row
   * @param <T> the row data type
   * @return a new EqualityDelete instance
   */
  public static <T> EqualityDelete<T> create(T data, int equalityFieldId) {
    Preconditions.checkNotNull(data, "Row data cannot be null");
    return new EqualityDelete<>(data, equalityFieldId);
  }

  /**
   * Sets the row data for this delete.
   *
   * @param newData the new row data
   * @return this instance for method chaining
   */
  public EqualityDelete<T> set(T newData, int newEqualityFieldId) {
    this.data = newData;
    this.equalityFieldId = newEqualityFieldId;
    return this;
  }

  /**
   * Returns the row data containing the delete value.
   *
   * @return the row data
   */
  public T data() {
    return data;
  }

  /**
   * Returns the equality field ID to extract from the row.
   *
   * @return the equality field ID
   */
  public int equalityFieldId() {
    return equalityFieldId;
  }
}
