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

import static org.apache.iceberg.IsolationLevel.SERIALIZABLE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.EqualityDelete;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitioningEqualityDeleteWriter;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.SparkWriteRequirements;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements equality delete support for Spark SQL DELETE operations.
 *
 * <p>This class is used when the table property {@code write.delete.strategy=equality} is set and
 * the DELETE operation meets the criteria for equality deletes (simple predicate on LONG field).
 */
class SparkEqualityDeltaWrite implements DeltaWrite, RequiresDistributionAndOrdering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkEqualityDeltaWrite.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final Command command;
  private final SparkBatchQueryScan scan;
  private final IsolationLevel isolationLevel;
  private final String applicationId;
  private final boolean wapEnabled;
  private final String wapId;
  private final String branch;
  private final Map<String, String> extraSnapshotMetadata;
  private final SparkWriteRequirements writeRequirements;
  private final int equalityFieldId;
  private final String queryId;

  private boolean cleanupOnAbort = false;

  SparkEqualityDeltaWrite(
      SparkSession spark,
      Table table,
      Command command,
      SparkBatchQueryScan scan,
      IsolationLevel isolationLevel,
      SparkWriteConf writeConf,
      LogicalWriteInfo info,
      int equalityFieldId) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.command = command;
    this.scan = scan;
    this.isolationLevel = isolationLevel;
    this.applicationId = spark.sparkContext().applicationId();
    this.wapEnabled = writeConf.wapEnabled();
    this.wapId = writeConf.wapId();
    this.branch = writeConf.branch();
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
    this.writeRequirements = writeConf.positionDeltaRequirements(command);
    this.equalityFieldId = equalityFieldId;
    this.queryId = info.queryId();
  }

  @Override
  public Distribution requiredDistribution() {
    Distribution distribution = writeRequirements.distribution();
    LOG.debug("Requesting {} as write distribution for table {}", distribution, table.name());
    return distribution;
  }

  @Override
  public boolean distributionStrictlyRequired() {
    return false;
  }

  @Override
  public SortOrder[] requiredOrdering() {
    SortOrder[] ordering = writeRequirements.ordering();
    LOG.debug("Requesting {} as write ordering for table {}", ordering, table.name());
    return ordering;
  }

  @Override
  public long advisoryPartitionSizeInBytes() {
    long size = writeRequirements.advisoryPartitionSize();
    LOG.debug("Requesting {} bytes advisory partition size for table {}", size, table.name());
    return size;
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new EqualityDeltaBatchWrite();
  }

  private class EqualityDeltaBatchWrite implements DeltaBatchWrite {

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      return new EqualityDeleteWriteFactory(
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table)),
          equalityFieldId,
          queryId);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      RowDelta rowDelta = table.newRowDelta();

      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      int addedDeleteFilesCount = 0;

      for (WriterCommitMessage message : messages) {
        EqualityDeleteTaskCommit taskCommit = (EqualityDeleteTaskCommit) message;

        for (DeleteFile deleteFile : taskCommit.deleteFiles()) {
          rowDelta.addDeletes(deleteFile);
          addedDeleteFilesCount += 1;
        }

        referencedDataFiles.addAll(Arrays.asList(taskCommit.referencedDataFiles()));
      }

      if (scan != null) {
        Expression conflictDetectionFilter = conflictDetectionFilter(scan);
        rowDelta.conflictDetectionFilter(conflictDetectionFilter);

        rowDelta.validateDataFilesExist(referencedDataFiles);

        if (scan.snapshotId() != null) {
          rowDelta.validateFromSnapshot(scan.snapshotId());
        }

        if (isolationLevel == SERIALIZABLE) {
          rowDelta.validateNoConflictingDataFiles();
        }

        String commitMsg =
            String.format(
                Locale.ROOT,
                "equality delete with %d delete files (scanSnapshotId: %d, conflictDetectionFilter: %s, isolationLevel: %s)",
                addedDeleteFilesCount,
                scan.snapshotId(),
                conflictDetectionFilter,
                isolationLevel);
        commitOperation(rowDelta, commitMsg);

      } else {
        String commitMsg =
            String.format(
                Locale.ROOT,
                "equality delete with %d delete files (no validation required)",
                addedDeleteFilesCount);
        commitOperation(rowDelta, commitMsg);
      }
    }

    private Expression conflictDetectionFilter(SparkBatchQueryScan queryScan) {
      Expression filter = Expressions.alwaysTrue();

      for (Expression expr : queryScan.filterExpressions()) {
        filter = Expressions.and(filter, expr);
      }

      return filter;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      if (cleanupOnAbort) {
        SparkCleanupUtil.deleteFiles("job abort", table.io(), files(messages));
      } else {
        LOG.warn("Skipping cleanup of written files");
      }
    }

    private List<ContentFile<?>> files(WriterCommitMessage[] messages) {
      List<ContentFile<?>> files = Lists.newArrayList();

      for (WriterCommitMessage message : messages) {
        if (message != null) {
          EqualityDeleteTaskCommit taskCommit = (EqualityDeleteTaskCommit) message;
          files.addAll(Arrays.asList(taskCommit.deleteFiles()));
        }
      }

      return files;
    }

    private void commitOperation(SnapshotUpdate<?> operation, String description) {
      LOG.info("Committing {} to table {}", description, table);
      if (applicationId != null) {
        operation.set("spark.app.id", applicationId);
      }

      extraSnapshotMetadata.forEach(operation::set);

      CommitMetadata.commitProperties().forEach(operation::set);

      if (wapEnabled && wapId != null) {
        operation.set(SnapshotSummary.STAGED_WAP_ID_PROP, wapId);
        operation.stageOnly();
      }

      if (branch != null) {
        operation.toBranch(branch);
      }

      try {
        long start = System.currentTimeMillis();
        operation.commit();
        long duration = System.currentTimeMillis() - start;
        LOG.info("Committed in {} ms", duration);
      } catch (Exception e) {
        cleanupOnAbort = e instanceof CleanableFailure;
        throw e;
      }
    }
  }

  public static class EqualityDeleteTaskCommit implements WriterCommitMessage {
    private final DeleteFile[] deleteFiles;
    private final CharSequence[] referencedDataFiles;

    EqualityDeleteTaskCommit(DeleteWriteResult result) {
      this.deleteFiles = result.deleteFiles().toArray(new DeleteFile[0]);
      this.referencedDataFiles = result.referencedDataFiles().toArray(new CharSequence[0]);
    }

    DeleteFile[] deleteFiles() {
      return deleteFiles;
    }

    CharSequence[] referencedDataFiles() {
      return referencedDataFiles;
    }
  }

  private static class EqualityDeleteWriteFactory implements DeltaWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final int equalityFieldId;
    private final String queryId;

    EqualityDeleteWriteFactory(
        Broadcast<Table> tableBroadcast, int equalityFieldId, String queryId) {
      this.tableBroadcast = tableBroadcast;
      this.equalityFieldId = equalityFieldId;
      this.queryId = queryId;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();

      OutputFileFactory deleteFileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(FileFormat.PUFFIN)
              .operationId(queryId)
              .suffix("deletes")
              .build();

      return new EqualityDeleteOnlyDeltaWriter(table, deleteFileFactory, equalityFieldId);
    }
  }

  private static class EqualityDeleteOnlyDeltaWriter implements DeltaWriter<InternalRow> {
    private final PartitioningWriter<EqualityDelete<InternalRow>, DeleteWriteResult> delegate;
    private final EqualityDelete<InternalRow> equalityDelete;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specs;
    private final int specIdOrdinal;
    private final int partitionOrdinal;
    private final int fieldValueOrdinal;

    private boolean closed = false;

    EqualityDeleteOnlyDeltaWriter(Table table, OutputFileFactory deleteFileFactory, int equalityFieldId) {
      this.delegate = new PartitioningEqualityDeleteWriter<>(deleteFileFactory, equalityFieldId);
      this.equalityDelete = EqualityDelete.create();
      this.io = table.io();
      this.specs = table.specs();

      // The row ID schema contains: _spec_id, _partition, _file, _pos
      // For equality deletes, we need _spec_id, _partition, and the field value
      // The field value is passed in the "id" row which has the structure defined by Spark
      this.specIdOrdinal = 0;  // _spec_id is first
      this.partitionOrdinal = 1;  // _partition is second
      this.fieldValueOrdinal = 0;  // Assume field value is first in the id row
    }

    @Override
    public void delete(InternalRow metadata, InternalRow id) throws IOException {
      int specId = metadata.getInt(specIdOrdinal);
      PartitionSpec spec = specs.get(specId);

      // Extract the equality field value from the id row
      // For now, we assume the value is a LONG at position 0
      long value = id.getLong(fieldValueOrdinal);
      equalityDelete.set(value);

      // Get partition - for unpartitioned tables, pass null
      StructLike partition = null;
      if (!spec.isUnpartitioned()) {
        InternalRow partitionRow = metadata.getStruct(partitionOrdinal, spec.partitionType().fields().size());
        // Create a simple wrapper that just passes through the partition
        partition = new StructLike() {
          @Override
          public int size() {
            return spec.partitionType().fields().size();
          }

          @Override
          public <T> T get(int pos, Class<T> javaClass) {
            return javaClass.cast(partitionRow.get(pos, null));
          }

          @Override
          public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException("Partition is read-only");
          }
        };
      }

      delegate.write(equalityDelete, spec, partition);
    }

    @Override
    public void update(InternalRow metadata, InternalRow id, InternalRow row) {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement update");
    }

    @Override
    public void insert(InternalRow row) throws IOException {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " does not implement insert");
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();

      DeleteWriteResult result = delegate.result();
      return new EqualityDeleteTaskCommit(result);
    }

    @Override
    public void abort() throws IOException {
      close();

      DeleteWriteResult result = delegate.result();
      SparkCleanupUtil.deleteTaskFiles(io, result.deleteFiles());
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        delegate.close();
        this.closed = true;
      }
    }
  }
}
