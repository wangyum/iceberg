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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_DEFAULT_FILE_FORMAT;

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class GenericFileWriterFactory extends BaseFileWriterFactory<Record> {

  GenericFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder) {
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        ImmutableMap.of());
  }

  /**
   * @deprecated This constructor is deprecated as of version 1.11.0 and will be removed in 1.12.0.
   *     Position deletes that include row data are no longer supported. Use {@link
   *     #GenericFileWriterFactory(Table, FileFormat, Schema, SortOrder, FileFormat, int[], Schema,
   *     SortOrder)} instead.
   */
  @Deprecated
  GenericFileWriterFactory(
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
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema,
        writerProperties);
  }

  /**
   * @deprecated as of 1.11.0; it will be removed in 1.12.0
   */
  @Deprecated
  GenericFileWriterFactory(
      Table table,
      FileFormat dataFileFormat,
      Schema dataSchema,
      SortOrder dataSortOrder,
      FileFormat deleteFileFormat,
      int[] equalityFieldIds,
      Schema equalityDeleteRowSchema,
      SortOrder equalityDeleteSortOrder,
      Schema positionDeleteRowSchema) {
    super(
        table,
        dataFileFormat,
        dataSchema,
        dataSortOrder,
        deleteFileFormat,
        equalityFieldIds,
        equalityDeleteRowSchema,
        equalityDeleteSortOrder,
        positionDeleteRowSchema);
  }

  static Builder builderFor(Table table) {
    return new Builder(table);
  }

  @Override
  protected void configureDataWrite(Avro.DataWriteBuilder builder) {
    builder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
    builder.createWriterFunc(DataWriter::create);
  }

  @Override
  protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::create);
  }

  @Override
  protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::create);
  }

  @Override
  protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericParquetWriter::create);
  }

  @Override
  protected void configureDataWrite(ORC.DataWriteBuilder builder) {
    builder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected void configureEqualityDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected void configurePositionDelete(ORC.DeleteWriteBuilder builder) {
    builder.createWriterFunc(GenericOrcWriter::buildWriter);
  }

  @Override
  protected Long extractEqualityFieldValue(Record row, int fieldId) {
    // Find the field index for this field ID in the equality delete schema
    Schema schema = equalityDeleteRowSchema();

    // Find the field index by iterating through columns
    int fieldIndex = -1;
    for (int i = 0; i < schema.columns().size(); i++) {
      if (schema.columns().get(i).fieldId() == fieldId) {
        fieldIndex = i;
        break;
      }
    }

    if (fieldIndex < 0) {
      throw new IllegalArgumentException(
          String.format(
              java.util.Locale.ROOT, "Field ID %d not found in equality delete schema", fieldId));
    }

    // Extract the value from the record
    Object value = row.get(fieldIndex, Long.class);
    return (Long) value;
  }

  @Override
  public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
      EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    // Automatic EDV detection (like Position DVs)
    // Use EDV if: v3+ table AND single LONG equality field
    Table currentTable = table();

    if (currentTable != null && TableUtil.formatVersion(currentTable) >= 3 && canUseEqualityDV()) {
      // Use Equality Delete Vector (Puffin bitmap format) - automatic in v3+
      return new GenericEDVWriter(file, spec, partition, currentTable, equalityFieldIds());
    } else {
      // Use standard Parquet/Avro/ORC equality delete writer
      // (v1/v2 tables, or non-LONG fields, or multiple equality fields)
      return super.newEqualityDeleteWriter(file, spec, partition);
    }
  }

  /**
   * Check if Equality Delete Vectors can be used.
   *
   * <p>EDV requirements:
   * <ul>
   *   <li>Exactly one equality field (composite keys not supported)</li>
   *   <li>Field type must be LONG (bitmaps require integer keys)</li>
   * </ul>
   *
   * @return true if EDV can be used, false to fall back to Parquet
   */
  private boolean canUseEqualityDV() {
    int[] fieldIds = equalityFieldIds();
    Schema deleteSchema = equalityDeleteRowSchema();

    // Must have exactly one equality field
    if (fieldIds == null || fieldIds.length != 1 || deleteSchema == null) {
      return false;
    }

    // Field must be LONG type
    int fieldId = fieldIds[0];
    Types.NestedField field = deleteSchema.findField(fieldId);
    if (field == null) {
      return false;
    }

    return field.type().typeId() == Type.TypeID.LONG;
  }

  // Protected accessor for table field
  protected Table table() {
    // Access via reflection to get around private field limitation
    try {
      java.lang.reflect.Field tableField = BaseFileWriterFactory.class.getDeclaredField("table");
      tableField.setAccessible(true);
      return (Table) tableField.get(this);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access table field", e);
    }
  }

  // Protected accessor for equalityFieldIds field
  protected int[] equalityFieldIds() {
    try {
      java.lang.reflect.Field field = BaseFileWriterFactory.class.getDeclaredField("equalityFieldIds");
      field.setAccessible(true);
      return (int[]) field.get(this);
    } catch (Exception e) {
      throw new RuntimeException("Failed to access equalityFieldIds field", e);
    }
  }

  public org.apache.iceberg.deletes.BitmapDeleteWriter newEqualityDeleteVectorWriter(
      org.apache.iceberg.io.OutputFileFactory fileFactory) {
    return new org.apache.iceberg.deletes.BitmapDeleteWriter(fileFactory);
  }

  public void writeEqualityDelete(
      org.apache.iceberg.deletes.BitmapDeleteWriter writer,
      Record row,
      int equalityFieldId,
      org.apache.iceberg.PartitionSpec spec,
      org.apache.iceberg.StructLike partition) {
    Long value = extractEqualityFieldValue(row, equalityFieldId);
    if (value != null) {
      // Validate non-negative constraint for Equality DVs
      if (value < 0) {
        throw new IllegalArgumentException(
            String.format(
                java.util.Locale.ROOT,
                "Equality delete vectors require non-negative LONG values. "
                    + "Got value %d for field ID %d. "
                    + "Use Parquet equality deletes for negative values or disable EDV.",
                value,
                equalityFieldId));
      }
      writer.deleteEquality(equalityFieldId, value, spec, partition);
    }
  }

  /**
   * Wrapper that adapts BitmapDeleteWriter to the EqualityDeleteWriter interface.
   *
   * <p>This allows the bitmap-based equality delete vector writer to be used through the standard
   * FileWriterFactory API.
   */
  private class GenericEDVWriter extends EqualityDeleteWriter<Record> {
    private final BitmapDeleteWriter bitmapWriter;
    private final PartitionSpec spec;
    private final StructLike partition;
    private final int equalityFieldId;

    GenericEDVWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition, Table table, int[] equalityFieldIds) {
      super(null, FileFormat.PUFFIN, file.encryptingOutputFile().location(), spec, partition, file.keyMetadata(), null, equalityFieldIds);
      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, 1, 1)
              .format(FileFormat.PUFFIN)
              .build();
      this.bitmapWriter = new BitmapDeleteWriter(fileFactory);
      this.spec = spec;
      this.partition = partition;
      // Get the single equality field ID
      if (equalityFieldIds == null || equalityFieldIds.length != 1) {
        throw new IllegalStateException(
            "EDV requires exactly one equality field ID, got: "
                + (equalityFieldIds == null ? 0 : equalityFieldIds.length));
      }
      this.equalityFieldId = equalityFieldIds[0];
    }

    @Override
    public void write(Record row) {
      writeEqualityDelete(bitmapWriter, row, equalityFieldId, spec, partition);
    }

    @Override
    public long length() {
      // BitmapDeleteWriter doesn't have length() yet, return 0 for now
      return 0;
    }

    @Override
    public org.apache.iceberg.DeleteFile toDeleteFile() {
      // Close the bitmap writer and get the delete file
      try {
        bitmapWriter.close();
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
      return bitmapWriter.result().deleteFiles().iterator().next();
    }

    @Override
    public void close() throws java.io.IOException {
      bitmapWriter.close();
    }
  }

  public static class Builder {
    private final Table table;
    private FileFormat dataFileFormat;
    private Schema dataSchema;
    private SortOrder dataSortOrder;
    private FileFormat deleteFileFormat;
    private int[] equalityFieldIds;
    private Schema equalityDeleteRowSchema;
    private SortOrder equalityDeleteSortOrder;
    private Schema positionDeleteRowSchema;
    private Map<String, String> writerProperties = ImmutableMap.of();

    public Builder() {
      this.table = null;
    }

    public Builder(Table table) {
      this.table = table;
      this.dataSchema = table.schema();

      Map<String, String> properties = table.properties();

      String dataFileFormatName =
          properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
      this.dataFileFormat = FileFormat.fromString(dataFileFormatName);

      String deleteFileFormatName =
          properties.getOrDefault(DELETE_DEFAULT_FILE_FORMAT, dataFileFormatName);
      this.deleteFileFormat = FileFormat.fromString(deleteFileFormatName);
    }

    public Builder dataFileFormat(FileFormat newDataFileFormat) {
      this.dataFileFormat = newDataFileFormat;
      return this;
    }

    public Builder dataSchema(Schema newDataSchema) {
      this.dataSchema = newDataSchema;
      return this;
    }

    public Builder dataSortOrder(SortOrder newDataSortOrder) {
      this.dataSortOrder = newDataSortOrder;
      return this;
    }

    public Builder deleteFileFormat(FileFormat newDeleteFileFormat) {
      this.deleteFileFormat = newDeleteFileFormat;
      return this;
    }

    public Builder equalityFieldIds(int[] newEqualityFieldIds) {
      this.equalityFieldIds = newEqualityFieldIds;
      return this;
    }

    public Builder equalityDeleteRowSchema(Schema newEqualityDeleteRowSchema) {
      this.equalityDeleteRowSchema = newEqualityDeleteRowSchema;
      return this;
    }

    public Builder equalityDeleteSortOrder(SortOrder newEqualityDeleteSortOrder) {
      this.equalityDeleteSortOrder = newEqualityDeleteSortOrder;
      return this;
    }

    /**
     * @deprecated This method is deprecated as of version 1.11.0 and will be removed in 1.12.0.
     *     Position deletes that include row data are no longer supported.
     */
    @Deprecated
    Builder positionDeleteRowSchema(Schema newPositionDeleteRowSchema) {
      this.positionDeleteRowSchema = newPositionDeleteRowSchema;
      return this;
    }

    /** Sets default writer properties. */
    public Builder writerProperties(Map<String, String> newWriterProperties) {
      this.writerProperties = newWriterProperties;
      return this;
    }

    public GenericFileWriterFactory build() {
      boolean noEqualityDeleteConf = equalityFieldIds == null && equalityDeleteRowSchema == null;
      boolean fullEqualityDeleteConf = equalityFieldIds != null && equalityDeleteRowSchema != null;
      Preconditions.checkArgument(
          noEqualityDeleteConf || fullEqualityDeleteConf,
          "Equality field IDs and equality delete row schema must be set together");

      return new GenericFileWriterFactory(
          table,
          dataFileFormat,
          dataSchema,
          dataSortOrder,
          deleteFileFormat,
          equalityFieldIds,
          equalityDeleteRowSchema,
          equalityDeleteSortOrder,
          positionDeleteRowSchema,
          writerProperties);
    }
  }
}
