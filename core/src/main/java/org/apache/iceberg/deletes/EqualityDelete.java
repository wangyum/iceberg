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

/**
 * Represents an equality delete - a delete identified by a field value rather than file position.
 *
 * <p>Currently supports only single LONG field deletes for use with bitmap-based equality delete
 * vectors.
 *
 * @param <T> the type of data stored in the row (not used for equality deletes, kept for API
 *     consistency)
 */
public class EqualityDelete<T> {
  private long value;

  private EqualityDelete() {}

  public static <T> EqualityDelete<T> create() {
    return new EqualityDelete<>();
  }

  public EqualityDelete<T> set(long value) {
    this.value = value;
    return this;
  }

  public long value() {
    return value;
  }
}
