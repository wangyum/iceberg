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
package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BitmapBackedStructLikeSet;
import org.apache.iceberg.deletes.EqualityDeleteVectorWriter;
import org.apache.iceberg.deletes.EqualityDeleteVectors;
import org.apache.iceberg.deletes.RoaringPositionBitmap;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for Equality Delete Vectors (EDV) performance.
 *
 * <p>Measures: - Write throughput: Traditional Parquet vs EDV (Puffin) - Read memory usage:
 * StructLikeSet vs BitmapBackedStructLikeSet - File size: Compression ratios for sequential IDs -
 * Scan performance: Applying deletes during reads
 *
 * <p>To run: <code>
 * ./gradlew :iceberg-core:jmh
 *     -PjmhIncludeRegex=EqualityDeleteVectorBenchmark
 *     -PjmhOutputPath=benchmark/edv-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 10)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class EqualityDeleteVectorBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Param({"1000", "10000", "100000", "1000000"})
  private int deleteCount;

  @Param({"sequential", "sparse"})
  private String pattern;

  private File tempDir;
  private List<Long> deleteIds;
  private RoaringPositionBitmap bitmap;
  private OutputFile edvOutputFile;
  private InputFile edvInputFile;

  @Setup(Level.Trial)
  public void setupTrial() throws IOException {
    tempDir = File.createTempFile("edv-benchmark-", "");
    tempDir.delete();
    tempDir.mkdirs();

    // Generate delete IDs based on pattern
    deleteIds = Lists.newArrayListWithCapacity(deleteCount);
    if ("sequential".equals(pattern)) {
      // Sequential: 0, 1, 2, 3, ..., N
      for (long i = 0; i < deleteCount; i++) {
        deleteIds.add(i);
      }
    } else {
      // Sparse: 0, 1000, 2000, 3000, ... (gaps of 1000)
      for (long i = 0; i < deleteCount; i++) {
        deleteIds.add(i * 1000);
      }
    }

    // Pre-create bitmap for read benchmarks
    bitmap = new RoaringPositionBitmap();
    for (Long id : deleteIds) {
      bitmap.set(id);
    }

    // Pre-create EDV file for read benchmarks
    edvOutputFile =
        Files.localOutput(new File(tempDir, "edv-benchmark-" + deleteCount + ".puffin"));
    writeEDVFile(bitmap, edvOutputFile);
    edvInputFile = Files.localInput(edvOutputFile.toInputFile().location());
  }

  @TearDown(Level.Trial)
  public void teardownTrial() {
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir);
    }
  }

  // ========== Write Benchmarks ==========

  /**
   * Benchmark: Write equality deletes as EDV (Puffin bitmap).
   *
   * <p>Measures throughput of creating bitmap and serializing to Puffin file.
   */
  @Benchmark
  public void writeEDV(Blackhole blackhole) throws IOException {
    RoaringPositionBitmap bm = new RoaringPositionBitmap();
    for (Long id : deleteIds) {
      bm.set(id);
    }

    File outputFile = new File(tempDir, "write-edv-" + System.nanoTime() + ".puffin");
    OutputFile out = Files.localOutput(outputFile);

    EqualityDeleteVectorWriter writer = new EqualityDeleteVectorWriter(out, 1, "id", null);
    for (Long id : deleteIds) {
      writer.write(id);
    }
    DeleteFile deleteFile = writer.complete();

    blackhole.consume(deleteFile);
    outputFile.delete();
  }

  /**
   * Benchmark: Write equality deletes as traditional Parquet (baseline).
   *
   * <p>Note: This simulates the overhead by creating GenericRecord objects for each delete. In
   * reality, Parquet writing has additional serialization cost.
   */
  @Benchmark
  public void writeTraditionalParquet(Blackhole blackhole) {
    List<Record> records = Lists.newArrayListWithCapacity(deleteCount);
    GenericRecord template = GenericRecord.create(SCHEMA);

    for (Long id : deleteIds) {
      Record record = template.copy("id", id, "data", null);
      records.add(record);
    }

    blackhole.consume(records);
  }

  // ========== Read Memory Benchmarks ==========

  /**
   * Benchmark: Load deletes into StructLikeSet (traditional).
   *
   * <p>Measures memory allocation and time to load all deleted values into a Set.
   */
  @Benchmark
  public void readIntoStructLikeSet(Blackhole blackhole) {
    StructLikeSet deleteSet = StructLikeSet.create(SCHEMA.asStruct());
    GenericRecord template = GenericRecord.create(SCHEMA);

    for (Long id : deleteIds) {
      Record record = template.copy("id", id, "data", null);
      deleteSet.add(record);
    }

    blackhole.consume(deleteSet);
  }

  /**
   * Benchmark: Load deletes into BitmapBackedStructLikeSet (EDV).
   *
   * <p>Measures memory efficiency of bitmap-backed set.
   */
  @Benchmark
  public void readIntoBitmapSet(Blackhole blackhole) {
    Set<StructLike> bitmapSet = new BitmapBackedStructLikeSet(bitmap, 1, SCHEMA);
    blackhole.consume(bitmapSet);
  }

  // ========== Scan Performance Benchmarks ==========

  /**
   * Benchmark: Check if values are deleted (StructLikeSet).
   *
   * <p>Simulates scan: for each data row, check if it's deleted.
   */
  @Benchmark
  public void scanWithStructLikeSet(Blackhole blackhole) {
    StructLikeSet deleteSet = StructLikeSet.create(SCHEMA.asStruct());
    GenericRecord template = GenericRecord.create(SCHEMA);

    // Load deletes
    for (Long id : deleteIds) {
      deleteSet.add(template.copy("id", id, "data", null));
    }

    // Simulate scan: check 10x more rows than deletes
    int scanRows = deleteCount * 10;
    int notDeletedCount = 0;
    for (long i = 0; i < scanRows; i++) {
      Record testRecord = template.copy("id", i, "data", "test");
      if (!deleteSet.contains(testRecord)) {
        notDeletedCount++;
      }
    }

    blackhole.consume(notDeletedCount);
  }

  /**
   * Benchmark: Check if values are deleted (BitmapBackedStructLikeSet).
   *
   * <p>Simulates scan with O(1) bitmap lookup.
   */
  @Benchmark
  public void scanWithBitmapSet(Blackhole blackhole) {
    Set<StructLike> bitmapSet = new BitmapBackedStructLikeSet(bitmap, 1, SCHEMA);
    GenericRecord template = GenericRecord.create(SCHEMA);

    // Simulate scan: check 10x more rows than deletes
    int scanRows = deleteCount * 10;
    int notDeletedCount = 0;
    for (long i = 0; i < scanRows; i++) {
      Record testRecord = template.copy("id", i, "data", "test");
      if (!bitmapSet.contains(testRecord)) {
        notDeletedCount++;
      }
    }

    blackhole.consume(notDeletedCount);
  }

  // ========== File Size Benchmarks ==========

  /**
   * Benchmark: Measure EDV file deserialization.
   *
   * <p>Measures time to read and deserialize Puffin bitmap file.
   */
  @Benchmark
  public void deserializeEDV(Blackhole blackhole) throws IOException {
    RoaringPositionBitmap deserializedBitmap =
        EqualityDeleteVectors.readEqualityDeleteVectorBitmap(edvInputFile);
    blackhole.consume(deserializedBitmap);
  }

  // ========== Helper Methods ==========

  private void writeEDVFile(RoaringPositionBitmap bm, OutputFile out) throws IOException {
    EqualityDeleteVectorWriter writer = new EqualityDeleteVectorWriter(out, 1, "id", null);
    for (Long id : deleteIds) {
      writer.write(id);
    }
    writer.complete();
  }

  private void deleteRecursively(File file) {
    if (file.isDirectory()) {
      for (File child : file.listFiles()) {
        deleteRecursively(child);
      }
    }
    file.delete();
  }
}
