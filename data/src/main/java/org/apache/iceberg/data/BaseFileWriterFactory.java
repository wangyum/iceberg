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

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.deletes.EqualityDeleteVectorWriterAdapter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A base writer factory to be extended by query engine integrations. */
public abstract class BaseFileWriterFactory<T> implements FileWriterFactory<T>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileWriterFactory.class);
  private final Table table;
  private final FileFormat dataFileFormat;
  private final Schema dataSchema;
  private final SortOrder dataSortOrder;
  private final FileFormat deleteFileFormat;
  private final int[] equalityFieldIds;
  private final Schema equalityDeleteRowSchema;
  private final SortOrder equalityDeleteSortOrder;
  private final Schema positionDeleteRowSchema;
  private final Map<String, String> writerProperties;

  protected BaseFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Map<String, String> writerProperties) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.dataSchema = dataSchema;
    this.dataSortOrder = dataSortOrder;
    this.deleteFileFormat = deleteFileFormat;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.equalityDeleteSortOrder = equalityDeleteSortOrder;
    this.writerProperties = writerProperties;
    this.positionDeleteRowSchema = null;
  }

  /**
   * @deprecated This constructor is deprecated as of version 1.11.0 and will be removed in 1.12.0.
   *     Position deletes that include row data are no longer supported. Use {@link
   *     #BaseFileWriterFactory(Table, FileFormat, Schema, SortOrder, FileFormat, int[], Schema,
   *     SortOrder, Map)} instead.
   */
  @Deprecated
  protected BaseFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema,
      Map<String, String> writerProperties) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.dataSchema = dataSchema;
    this.dataSortOrder = dataSortOrder;
    this.deleteFileFormat = deleteFileFormat;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.equalityDeleteSortOrder = equalityDeleteSortOrder;
    this.positionDeleteRowSchema = positionDeleteRowSchema;
    this.writerProperties = writerProperties;
  }

  @Deprecated
  protected BaseFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema) {
    this.table = table;
    this.dataFileFormat = dataFileFormat;
    this.dataSchema = dataSchema;
    this.dataSortOrder = dataSortOrder;
    this.deleteFileFormat = deleteFileFormat;
    this.equalityFieldIds = equalityFieldIds;
    this.equalityDeleteRowSchema = equalityDeleteRowSchema;
    this.equalityDeleteSortOrder = equalityDeleteSortOrder;
    this.positionDeleteRowSchema = positionDeleteRowSchema;
    this.writerProperties = ImmutableMap.of();
  }

  protected abstract void configureDataWrite(Avro.DataWriteBuilder builder);

  protected abstract void configureEqualityDelete(Avro.DeleteWriteBuilder builder);

  protected abstract void configurePositionDelete(Avro.DeleteWriteBuilder builder);

  protected abstract void configureDataWrite(Parquet.DataWriteBuilder builder);

  protected abstract void configureEqualityDelete(Parquet.DeleteWriteBuilder builder);

  protected abstract void configurePositionDelete(Parquet.DeleteWriteBuilder builder);

  protected abstract void configureDataWrite(ORC.DataWriteBuilder builder);

  protected abstract void configureEqualityDelete(ORC.DeleteWriteBuilder builder);

  protected abstract void configurePositionDelete(ORC.DeleteWriteBuilder builder);

  /**
   * Extracts the value of a field from a row for equality delete vector writing.
   *
   * <p>Subclasses should override this to enable equality delete vector support. The default
   * implementation throws UnsupportedOperationException to allow engines to opt-in to this feature.
   *
   * @param row the row to extract from
   * @param fieldId the field ID to extract
   * @return the extracted Long value, or null if the field is null
   * @throws UnsupportedOperationException if equality delete vectors are not supported by this
   *     engine
   */
  protected Long extractEqualityFieldValue(T row, int fieldId) {
    throw new UnsupportedOperationException(
        "Equality delete vectors are not supported. "
            + "Override extractEqualityFieldValue() to enable this feature.");
  }

  @Override
  public DataWriter<T> newDataWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table == null ? ImmutableMap.of() : table.properties();
    MetricsConfig metricsConfig =
        table == null ? MetricsConfig.getDefault() : MetricsConfig.forTable(table);

    try {
      switch (dataFileFormat) {
        case AVRO:
          Avro.DataWriteBuilder avroBuilder =
              Avro.writeData(file)
                  .schema(dataSchema)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(dataSortOrder)
                  .overwrite();

          configureDataWrite(avroBuilder);

          return avroBuilder.build();

        case PARQUET:
          Parquet.DataWriteBuilder parquetBuilder =
              Parquet.writeData(file)
                  .schema(dataSchema)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(dataSortOrder)
                  .overwrite();

          configureDataWrite(parquetBuilder);

          return parquetBuilder.build();

        case ORC:
          ORC.DataWriteBuilder orcBuilder =
              ORC.writeData(file)
                  .schema(dataSchema)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(dataSortOrder)
                  .overwrite();

          configureDataWrite(orcBuilder);

          return orcBuilder.build();

        default:
          throw new UnsupportedOperationException(
              "Unsupported data file format: " + dataFileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<T> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    Map<String, String> properties = table == null ? ImmutableMap.of() : table.properties();

    // Check if we should use Equality Delete Vector (EDV) format
    if (shouldUseEqualityDeleteVector(properties)) {
      return newEqualityDeleteVectorWriter(file, spec, partition);
    }

    // Traditional path: write to Parquet/Avro/ORC
    return newTraditionalEqualityDeleteWriter(file, spec, partition);
  }

  /**
   * Checks if the equality delete should be written as a bitmap-based delete vector.
   *
   * <p>EDV is used when all of the following are true: - Table property {@code
   * write.delete.equality-vector.enabled} is true - Format version >= 3 (Puffin support required) -
   * Single equality field (not composite keys) - Field type is LONG (not INT or other types)
   *
   * @param properties the table properties
   * @return true if EDV should be used, false otherwise
   */
  private boolean shouldUseEqualityDeleteVector(Map<String, String> properties) {
    // Check if EDV is enabled
    boolean edvEnabled =
        Boolean.parseBoolean(
            properties.getOrDefault(
                TableProperties.EQUALITY_DELETE_VECTOR_ENABLED,
                String.valueOf(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED_DEFAULT)));

    LOG.debug("EDV enabled check: {}", edvEnabled);
    if (!edvEnabled) {
      LOG.debug("EDV not enabled, using traditional path");
      return false;
    }

    // Check format version >= 3
    if (table != null) {
      int formatVersion =
          ((org.apache.iceberg.BaseTable) table).operations().current().formatVersion();
      LOG.debug("Format version: {}", formatVersion);
      if (formatVersion < 3) {
        LOG.debug("Format version < 3, using traditional path");
        return false;
      }
    } else {
      LOG.debug("Table is null, cannot check format version");
    }

    // Check single equality field
    LOG.debug(
        "Equality field IDs: {}",
        equalityFieldIds == null ? "null" : java.util.Arrays.toString(equalityFieldIds));
    if (equalityFieldIds == null || equalityFieldIds.length != 1) {
      LOG.debug("Not a single equality field, using traditional path");
      return false;
    }

    // Check field type is LONG
    int equalityFieldId = equalityFieldIds[0];
    Types.NestedField field = equalityDeleteRowSchema.findField(equalityFieldId);
    LOG.debug("Equality field: {}, type: {}", field, field == null ? "null" : field.type());
    if (field == null || field.type().typeId() != Type.TypeID.LONG) {
      LOG.debug("Field is not LONG type, using traditional path");
      return false;
    }

    // Warn if field is not an identifier field (best practice for CDC use cases)
    // Note: We don't block non-identifier fields - EDV can work on any LONG field
    // This maintains consistency with equality deletes (which can use any columns)
    if (table != null) {
      Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
      if (identifierFieldIds == null || !identifierFieldIds.contains(equalityFieldId)) {
        LOG.warn(
            "Equality field '{}' is not marked as an identifier field. "
                + "EDV works best on identifier fields (e.g., CDC _row_id with sequential values). "
                + "For optimal compression, consider: ALTER TABLE {} SET IDENTIFIER FIELDS {}",
            field.name(),
            table.name(),
            field.name());
      }
    }

    LOG.debug("All checks passed, using EDV!");
    return true;
  }

  /**
   * Creates a new Equality Delete Vector writer that stores deleted values as a Roaring bitmap.
   *
   * @param file the output file
   * @param spec the partition spec
   * @param partition the partition
   * @return the EDV writer
   */
  private EqualityDeleteWriter<T> newEqualityDeleteVectorWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    Preconditions.checkState(
        equalityFieldIds != null && equalityFieldIds.length == 1,
        "EDV requires exactly one equality field");

    int equalityFieldId = equalityFieldIds[0];

    // Use adapter to wrap EDV writer as EqualityDeleteWriter
    return EqualityDeleteVectorWriterAdapter.wrap(
        file,
        spec,
        partition,
        file.keyMetadata(),
        equalityFieldId,
        this::extractEqualityFieldValue);
  }

  /**
   * Creates a traditional equality delete writer (Parquet/Avro/ORC).
   *
   * @param file the output file
   * @param spec the partition spec
   * @param partition the partition
   * @return the traditional equality delete writer
   */
  private EqualityDeleteWriter<T> newTraditionalEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table == null ? ImmutableMap.of() : table.properties();
    MetricsConfig metricsConfig =
        table == null ? MetricsConfig.getDefault() : MetricsConfig.forTable(table);

    try {
      switch (deleteFileFormat) {
        case AVRO:
          Avro.DeleteWriteBuilder avroBuilder =
              Avro.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(equalityDeleteRowSchema)
                  .equalityFieldIds(equalityFieldIds)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(equalityDeleteSortOrder)
                  .overwrite();

          configureEqualityDelete(avroBuilder);

          return avroBuilder.buildEqualityWriter();

        case PARQUET:
          Parquet.DeleteWriteBuilder parquetBuilder =
              Parquet.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(equalityDeleteRowSchema)
                  .equalityFieldIds(equalityFieldIds)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(equalityDeleteSortOrder)
                  .overwrite();

          configureEqualityDelete(parquetBuilder);

          return parquetBuilder.buildEqualityWriter();

        case ORC:
          ORC.DeleteWriteBuilder orcBuilder =
              ORC.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(equalityDeleteRowSchema)
                  .equalityFieldIds(equalityFieldIds)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .withSortOrder(equalityDeleteSortOrder)
                  .overwrite();

          configureEqualityDelete(orcBuilder);

          return orcBuilder.buildEqualityWriter();

        default:
          throw new UnsupportedOperationException(
              "Unsupported format for equality deletes: " + deleteFileFormat);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new equality delete writer", e);
    }
  }

  @Override
  public PositionDeleteWriter<T> newPositionDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Map<String, String> properties = table == null ? ImmutableMap.of() : table.properties();
    MetricsConfig metricsConfig =
        table == null ? MetricsConfig.forPositionDelete() : MetricsConfig.forPositionDelete(table);

    try {
      switch (deleteFileFormat) {
        case AVRO:
          Avro.DeleteWriteBuilder avroBuilder =
              Avro.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(positionDeleteRowSchema)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .overwrite();

          configurePositionDelete(avroBuilder);

          return avroBuilder.buildPositionWriter();

        case PARQUET:
          Parquet.DeleteWriteBuilder parquetBuilder =
              Parquet.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(positionDeleteRowSchema)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .overwrite();

          configurePositionDelete(parquetBuilder);

          return parquetBuilder.buildPositionWriter();

        case ORC:
          ORC.DeleteWriteBuilder orcBuilder =
              ORC.writeDeletes(file)
                  .setAll(properties)
                  .setAll(writerProperties)
                  .metricsConfig(metricsConfig)
                  .rowSchema(positionDeleteRowSchema)
                  .withSpec(spec)
                  .withPartition(partition)
                  .withKeyMetadata(keyMetadata)
                  .overwrite();

          configurePositionDelete(orcBuilder);

          return orcBuilder.buildPositionWriter();

        default:
          throw new UnsupportedOperationException(
              "Unsupported format for position deletes: " + deleteFileFormat);
      }

    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create new position delete writer", e);
    }
  }

  protected Schema dataSchema() {
    return dataSchema;
  }

  protected Schema equalityDeleteRowSchema() {
    return equalityDeleteRowSchema;
  }

  /**
   * @deprecated This method is deprecated as of version 1.11.0 and will be removed in 1.12.0.
   *     Position deletes that include row data are no longer supported.
   */
  @Deprecated
  protected Schema positionDeleteRowSchema() {
    return positionDeleteRowSchema;
  }
}
