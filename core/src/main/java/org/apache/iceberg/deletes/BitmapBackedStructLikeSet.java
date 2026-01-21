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

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.roaringbitmap.IntIterator;

/**
 * A {@link Set} implementation backed by a {@link RoaringPositionBitmap} for efficient equality
 * delete lookups on a single LONG field.
 *
 * <p>This class provides O(1) lookup performance for checking if a row should be deleted based on a
 * single LONG equality field value. It wraps a Roaring bitmap that stores deleted LONG values.
 *
 * <p>This is optimized for equality delete vectors where deleted values are stored as bitmaps
 * instead of full row data.
 */
public class BitmapBackedStructLikeSet extends AbstractSet<StructLike> implements Set<StructLike> {

  private final RoaringPositionBitmap bitmap;
  private final int equalityFieldId;
  private final int fieldIndex;

  private final Schema schema;

  /**
   * Creates a new bitmap-backed set for equality deletes.
   *
   * @param bitmap the Roaring bitmap containing deleted values
   * @param equalityFieldId the field ID of the LONG equality column
   * @param schema the schema to extract field values from
   */
  public BitmapBackedStructLikeSet(
      RoaringPositionBitmap bitmap, int equalityFieldId, Schema schema) {
    this.bitmap = Preconditions.checkNotNull(bitmap, "bitmap cannot be null");
    this.equalityFieldId = equalityFieldId;
    this.schema = Preconditions.checkNotNull(schema, "schema cannot be null");

    // Find the field index for the equality field ID
    Types.NestedField field = schema.findField(equalityFieldId);
    Preconditions.checkArgument(
        field != null, "Equality field %s not found in schema", equalityFieldId);
    Preconditions.checkArgument(
        field.type().typeId() == Type.TypeID.LONG,
        "Equality field %s must be LONG type, got %s",
        equalityFieldId,
        field.type());

    // Find the field index by iterating through columns
    int index = -1;
    for (int i = 0; i < schema.columns().size(); i++) {
      if (schema.columns().get(i).fieldId() == equalityFieldId) {
        index = i;
        break;
      }
    }
    Preconditions.checkState(index >= 0, "Field index not found for field %s", equalityFieldId);
    this.fieldIndex = index;
  }

  @Override
  public Iterator<StructLike> iterator() {
    return new BitmapIterator();
  }

  private class BitmapIterator implements Iterator<StructLike> {
    private final Iterator<Long> delegate;

    BitmapIterator() {
      // RoaringPositionBitmap doesn't expose a Long iterator directly,
      // so we use forEach to build a list or implement a custom iterator.
      // For simplicity and since iterator() is rarely used in hot paths (mostly tests/tools),
      // we can collect to a list. For true laziness, we'd need to extend RoaringPositionBitmap.
      //
      // However, RoaringBitmap DOES expose an IntIterator.
      // Let's assume we can get an iterator from the underlying bitmap.
      // Since RoaringPositionBitmap wraps RoaringBitmap, we can iterate it.
      this.delegate = new RoaringPositionBitmapIterator(bitmap);
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public StructLike next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Long value = delegate.next();
      // Create a fresh GenericRecord for each iteration
      // This is necessary because iterator() returns distinct objects
      GenericRecord iterRecord = GenericRecord.create(schema.asStruct());
      iterRecord.set(fieldIndex, value);
      return iterRecord;
    }
  }

  /**
   * Adapts RoaringPositionBitmap iteration to Iterator<Long>.
   * Note: This implementation assumes the standard RoaringPositionBitmap structure.
   */
  private static class RoaringPositionBitmapIterator implements Iterator<Long> {
    // This is a simplified iterator that collects all values.
    // In a production environment, this should be lazy to avoid memory overhead.
    // Given the previous limitation, even a list-based iterator is better than throwing exception,
    // as it allows tests and tools to work.
    private final Iterator<Long> iterator;

    RoaringPositionBitmapIterator(RoaringPositionBitmap bitmap) {
      // Use a list to collect values for iteration
      // This is not ideal for huge bitmaps but enables functionality
      // A better approach would be adding a lazy iterator to RoaringPositionBitmap class
      java.util.List<Long> values = new java.util.ArrayList<>();
      bitmap.forEach(values::add);
      this.iterator = values.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Long next() {
      return iterator.next();
    }
  }

  @Override
  public int size() {
    // Return the cardinality of the bitmap
    long cardinality = bitmap.cardinality();
    // Cap at Integer.MAX_VALUE for Set interface
    return cardinality > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) cardinality;
  }

  @Override
  public boolean isEmpty() {
    return bitmap.isEmpty();
  }

  @Override
  public boolean contains(Object obj) {
    if (!(obj instanceof StructLike)) {
      return false;
    }

    StructLike struct = (StructLike) obj;

    // Extract the LONG value from the struct at the equality field index
    Object value = struct.get(fieldIndex, Long.class);

    // NULL values are not in the bitmap (EDV doesn't support NULLs)
    if (value == null) {
      return false;
    }

    long longValue = (Long) value;

    // Negative values are not in the bitmap (EDV doesn't support negatives)
    if (longValue < 0) {
      return false;
    }

    // Check if the value is in the bitmap
    return bitmap.contains(longValue);
  }

  @Override
  public boolean add(StructLike struct) {
    throw new UnsupportedOperationException(
        "Add is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  @Override
  public boolean remove(Object obj) {
    throw new UnsupportedOperationException(
        "Remove is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  @Override
  public boolean addAll(Collection<? extends StructLike> c) {
    throw new UnsupportedOperationException(
        "AddAll is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "RemoveAll is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(
        "RetainAll is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(
        "Clear is not supported for BitmapBackedStructLikeSet. This is a read-only view.");
  }

  /**
   * Returns the underlying bitmap (for testing).
   *
   * @return the Roaring bitmap
   */
  RoaringPositionBitmap bitmap() {
    return bitmap;
  }

  /**
   * Returns the equality field ID (for testing).
   *
   * @return the field ID
   */
  int equalityFieldId() {
    return equalityFieldId;
  }
}
