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

import java.io.IOException;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileAppender;

/**
 * Adapter that wraps {@link EqualityDeleteVectorWriter} to make it compatible with {@link
 * EqualityDeleteWriter}.
 *
 * <p>This allows EDV writers to be used wherever EqualityDeleteWriter is expected, enabling
 * transparent use of bitmap-based equality deletes.
 */
public class EqualityDeleteVectorWriterAdapter<T> extends EqualityDeleteWriter<T> {

  private final EqualityDeleteVectorWriter<T> delegate;

  // Dummy appender that's never used
  private static final FileAppender<Object> DUMMY_APPENDER =
      new FileAppender<Object>() {
        @Override
        public void add(Object datum) {
          throw new UnsupportedOperationException("Dummy appender should never be called");
        }

        @Override
        public Metrics metrics() {
          throw new UnsupportedOperationException("Dummy appender should never be called");
        }

        @Override
        public long length() {
          return 0;
        }

        @Override
        public java.util.List<Long> splitOffsets() {
          return java.util.Collections.emptyList();
        }

        @Override
        public void close() throws IOException {
          // no-op
        }
      };

  @SuppressWarnings("unchecked")
  public EqualityDeleteVectorWriterAdapter(EqualityDeleteVectorWriter<T> delegate) {
    // Call super constructor with dummy appender - we won't use it
    super(
        (FileAppender<T>) DUMMY_APPENDER,
        FileFormat.PUFFIN,
        "",
        PartitionSpec.unpartitioned(),
        null,
        null,
        null);
    this.delegate = delegate;
  }

  public static <T> EqualityDeleteWriter<T> wrap(
      EncryptedOutputFile file,
      PartitionSpec spec,
      StructLike partition,
      EncryptionKeyMetadata keyMetadata,
      int equalityFieldId,
      EqualityDeleteVectorWriter.ValueExtractor<T> valueExtractor) {
    EqualityDeleteVectorWriter<T> writer =
        new EqualityDeleteVectorWriter<>(
            file, spec, partition, keyMetadata, equalityFieldId, valueExtractor);
    return new EqualityDeleteVectorWriterAdapter<>(writer);
  }

  @Override
  public void write(T row) {
    delegate.write(row);
  }

  @Override
  public long length() {
    return delegate.length();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public DeleteFile toDeleteFile() {
    return delegate.toDeleteFile();
  }

  @Override
  public DeleteWriteResult result() {
    return delegate.result();
  }
}
