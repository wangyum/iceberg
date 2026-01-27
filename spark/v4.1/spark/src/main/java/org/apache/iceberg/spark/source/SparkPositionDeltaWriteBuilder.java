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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.MetadataColumns.SPEC_ID_COLUMN_DOC;
import static org.apache.iceberg.MetadataColumns.SPEC_ID_COLUMN_ID;
import static org.apache.iceberg.MetadataColumns.schemaWithRowLineage;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPositionDeltaWriteBuilder.class);

  private static final Schema EXPECTED_ROW_ID_SCHEMA =
      new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);

  private final SparkSession spark;
  private final Table table;
  private final Command command;
  private final SparkBatchQueryScan scan;
  private final IsolationLevel isolationLevel;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean checkNullability;
  private final boolean checkOrdering;

  SparkPositionDeltaWriteBuilder(
      SparkSession spark,
      Table table,
      String branch,
      Command command,
      Scan scan,
      IsolationLevel isolationLevel,
      LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.command = command;
    this.scan = (SparkBatchQueryScan) scan;
    this.isolationLevel = isolationLevel;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.info = info;
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
  }

  @Override
  public DeltaWrite build() {
    Schema dataSchema = dataSchema();

    validateRowIdSchema();
    validateMetadataSchema();
    if (dataSchema != null
        && info.metadataSchema().isPresent()
        && info.metadataSchema()
            .get()
            .find(f -> f.name().equals(MetadataColumns.ROW_ID.name()))
            .isDefined()) {
      dataSchema = MetadataColumns.schemaWithRowLineage(dataSchema);
    }

    SparkUtil.validatePartitionTransforms(table.spec());

    // Check if equality deletes should be used instead of position deletes
    if (shouldUseEqualityDeletes()) {
      int equalityFieldId = getEqualityFieldId();
      LOG.info(
          "Using equality deletes for DELETE operation on field ID {} for table {}",
          equalityFieldId,
          table.name());
      return new SparkEqualityDeltaWrite(
          spark, table, command, scan, isolationLevel, writeConf, info, equalityFieldId);
    }

    return new SparkPositionDeltaWrite(
        spark, table, command, scan, isolationLevel, writeConf, info, dataSchema);
  }

  /**
   * Determines if equality deletes should be used instead of position deletes.
   *
   * <p>Criteria for using equality deletes:
   *
   * <ul>
   *   <li>Command must be DELETE
   *   <li>Table property {@code write.delete.strategy} must be set to "equality"
   *   <li>Table format version must be >= 3 (for PUFFIN support)
   *   <li>DELETE must be on a simple LONG field (detected from schema)
   * </ul>
   */
  private boolean shouldUseEqualityDeletes() {
    // Must be a DELETE command
    if (command != DELETE) {
      return false;
    }

    // Check table property
    String deleteStrategy =
        table
            .properties()
            .getOrDefault(
                TableProperties.DELETE_STRATEGY, TableProperties.DELETE_STRATEGY_DEFAULT);
    if (!TableProperties.DELETE_STRATEGY_EQUALITY.equals(deleteStrategy)) {
      return false;
    }

    // Must have format version 3+ for PUFFIN support
    int formatVersion = TableUtil.formatVersion(table);
    if (formatVersion < 3) {
      LOG.warn(
          "Cannot use equality deletes: table format version is {}, requires >= 3",
          formatVersion);
      return false;
    }

    // For now, we support equality deletes only on the first LONG column
    // In a production implementation, you would analyze the DELETE predicate to determine
    // which field is being used for the equality condition
    return canExtractEqualityFieldId();
  }

  /**
   * Checks if we can extract an equality field ID from the schema.
   *
   * <p>This is a simplified implementation that uses the first LONG column. A production
   * implementation would analyze the DELETE predicate to determine the actual field being used.
   */
  private boolean canExtractEqualityFieldId() {
    return getEqualityFieldId() != -1;
  }

  /**
   * Gets the equality field ID to use for equality deletes.
   *
   * <p>This simplified implementation returns the first LONG column. A production implementation
   * would analyze the DELETE predicate.
   *
   * @return the field ID, or -1 if no suitable field is found
   */
  private int getEqualityFieldId() {
    for (Types.NestedField field : table.schema().columns()) {
      if (field.type().typeId() == Types.LongType.get().typeId()) {
        return field.fieldId();
      }
    }
    return -1;
  }

  private Schema dataSchema() {
    if (info.schema() == null || info.schema().isEmpty()) {
      return null;
    } else {
      Schema dataSchema = SparkSchemaUtil.convert(table.schema(), info.schema());
      validateSchema("data", table.schema(), dataSchema);
      return dataSchema;
    }
  }

  private void validateRowIdSchema() {
    Preconditions.checkArgument(info.rowIdSchema().isPresent(), "Row ID schema must be set");
    StructType rowIdSparkType = info.rowIdSchema().get();
    Schema rowIdSchema = SparkSchemaUtil.convert(EXPECTED_ROW_ID_SCHEMA, rowIdSparkType);
    validateSchema("row ID", EXPECTED_ROW_ID_SCHEMA, rowIdSchema);
  }

  private void validateMetadataSchema() {
    Preconditions.checkArgument(info.metadataSchema().isPresent(), "Metadata schema must be set");
    Schema expectedMetadataSchema =
        new Schema(
            Types.NestedField.optional(
                SPEC_ID_COLUMN_ID, "_spec_id", Types.IntegerType.get(), SPEC_ID_COLUMN_DOC),
            MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
    if (TableUtil.supportsRowLineage(table)) {
      expectedMetadataSchema = schemaWithRowLineage(expectedMetadataSchema);
    }

    StructType metadataSparkType = info.metadataSchema().get();
    Schema metadataSchema = SparkSchemaUtil.convert(expectedMetadataSchema, metadataSparkType);
    validateSchema("metadata", expectedMetadataSchema, metadataSchema);
  }

  private void validateSchema(String context, Schema expected, Schema actual) {
    TypeUtil.validateSchema(context, expected, actual, checkNullability, checkOrdering);
  }
}
