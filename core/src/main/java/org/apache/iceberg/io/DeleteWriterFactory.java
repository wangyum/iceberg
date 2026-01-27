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

import java.util.List;
import org.apache.iceberg.DeleteEncoding;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

/**
 * Factory for creating delete writers with automatic encoding selection.
 *
 * <p>This factory automatically chooses the most appropriate delete encoding based on:
 *
 * <ul>
 *   <li>Table property hints ({@link TableProperties#DELETE_ENCODING_HINT})
 *   <li>Data characteristics (field types, cardinality)
 *   <li>Delete type (position vs equality)
 * </ul>
 *
 * <p><b>Encoding Selection Logic:</b>
 *
 * <ol>
 *   <li>If table hint is "full-row", use FULL_ROW encoding
 *   <li>If table hint is "deletion-vector", use DELETION_VECTOR (if applicable)
 *   <li>If table hint is "auto" (default):
 *       <ul>
 *         <li>For equality deletes with single LONG field → DELETION_VECTOR
 *         <li>Otherwise → FULL_ROW
 *       </ul>
 * </ol>
 *
 * <p><b>Usage Example:</b>
 *
 * <pre>{@code
 * Table table = ...;
 * DeleteWriterFactory factory = DeleteWriterFactory.forTable(table);
 *
 * // Factory automatically selects encoding based on schema
 * List<Integer> equalityFieldIds = Collections.singletonList(idFieldId);
 * DeleteEncoding encoding = factory.selectEqualityDeleteEncoding(equalityFieldIds, schema);
 * }</pre>
 */
public class DeleteWriterFactory {
  private final Table table;

  private DeleteWriterFactory(Table table) {
    this.table = Preconditions.checkNotNull(table, "table cannot be null");
  }

  /**
   * Creates a new DeleteWriterFactory for the given table.
   *
   * @param table the table
   * @return a new factory instance
   */
  public static DeleteWriterFactory forTable(Table table) {
    return new DeleteWriterFactory(table);
  }

  /**
   * Selects the appropriate encoding for equality deletes.
   *
   * @param equalityFieldIds the equality field IDs
   * @param deleteSchema the schema of delete records
   * @return the selected encoding
   */
  public DeleteEncoding selectEqualityDeleteEncoding(
      List<Integer> equalityFieldIds, Schema deleteSchema) {
    Preconditions.checkNotNull(equalityFieldIds, "equalityFieldIds cannot be null");
    Preconditions.checkArgument(
        !equalityFieldIds.isEmpty(), "equalityFieldIds cannot be empty");
    Preconditions.checkNotNull(deleteSchema, "deleteSchema cannot be null");

    // Check table hint
    String hint = table.properties().getOrDefault(
        TableProperties.DELETE_ENCODING_HINT,
        TableProperties.DELETE_ENCODING_HINT_DEFAULT);

    if ("full-row".equalsIgnoreCase(hint)) {
      return DeleteEncoding.FULL_ROW;
    }

    if ("deletion-vector".equalsIgnoreCase(hint)) {
      // Verify deletion vector is applicable
      if (isDeletionVectorApplicable(equalityFieldIds, deleteSchema)) {
        return DeleteEncoding.DELETION_VECTOR;
      } else {
        // Fall back to full-row if not applicable
        return DeleteEncoding.FULL_ROW;
      }
    }

    // Default "auto" mode - automatically detect
    if (isDeletionVectorApplicable(equalityFieldIds, deleteSchema)) {
      return DeleteEncoding.DELETION_VECTOR;
    }

    return DeleteEncoding.FULL_ROW;
  }

  /**
   * Selects the appropriate encoding for position deletes.
   *
   * @return the selected encoding (currently always DELETION_VECTOR for V3+ tables)
   */
  public DeleteEncoding selectPositionDeleteEncoding() {
    // Position deletes in V3+ tables use deletion vectors by default
    // Check table hint
    String hint = table.properties().getOrDefault(
        TableProperties.DELETE_ENCODING_HINT,
        TableProperties.DELETE_ENCODING_HINT_DEFAULT);

    if ("full-row".equalsIgnoreCase(hint)) {
      return DeleteEncoding.FULL_ROW;
    }

    // Default to deletion vector for position deletes
    return DeleteEncoding.DELETION_VECTOR;
  }

  /**
   * Checks if deletion vector encoding is applicable for the given equality delete schema.
   *
   * <p>Deletion vectors are applicable when:
   *
   * <ul>
   *   <li>Single equality field
   *   <li>Field type is LONG
   * </ul>
   *
   * @param equalityFieldIds the equality field IDs
   * @param deleteSchema the schema of delete records
   * @return true if deletion vector encoding is applicable
   */
  private boolean isDeletionVectorApplicable(
      List<Integer> equalityFieldIds, Schema deleteSchema) {
    // Currently only support single field
    if (equalityFieldIds.size() != 1) {
      return false;
    }

    int fieldId = equalityFieldIds.get(0);
    Type fieldType = deleteSchema.findType(fieldId);

    // Field must exist and be LONG type
    return fieldType != null && fieldType.typeId() == Type.TypeID.LONG;
  }
}
