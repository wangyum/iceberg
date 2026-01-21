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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Benchmark tests to validate storage savings of Equality Delete Vectors compared to traditional
 * Parquet equality deletes.
 */
public class TestEqualityDeleteVectorBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  @TempDir private File tempDir;

  private File tableDir;
  private Table table;

  @BeforeEach
  public void setupTable() {
    tableDir = new File(tempDir, "test_table");
    table =
        TestTables.create(
            tableDir,
            "test",
            SCHEMA,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            3 /* format version */);

    // Enable EDV
    table.updateProperties().set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true").commit();
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testSequentialDeletesBenchmark() throws IOException {
    // Sequential deletes should compress very well with RLE
    int deleteCount = 10000;
    long[] deletedIds = new long[deleteCount];
    for (int i = 0; i < deleteCount; i++) {
      deletedIds[i] = i;
    }

    BenchmarkResult result = benchmarkDeleteFormat(deletedIds);

    System.out.println("\n=== Sequential Deletes Benchmark ===");
    System.out.println("Delete count: " + deleteCount);
    System.out.println("Traditional (Parquet) size: " + result.traditionalSize + " bytes");
    System.out.println("EDV (Puffin/Bitmap) size: " + result.edvSize + " bytes");
    System.out.println(
        "Compression ratio: " + String.format("%.1f", result.compressionRatio) + "x");
    System.out.println("Space savings: " + String.format("%.1f", result.spaceSavings) + "%");

    // Sequential deletes should achieve very high compression (>40x with Parquet baseline)
    assertThat(result.compressionRatio).isGreaterThan(40.0);
    assertThat(result.spaceSavings).isGreaterThan(97.0);
  }

  @Test
  public void testSparseDeletesBenchmark() throws IOException {
    // Sparse deletes (every 100th value) - demonstrates worst case
    // Note: For very sparse patterns, bitmap overhead can be similar to Parquet
    int deleteCount = 1000;
    long[] deletedIds = new long[deleteCount];
    for (int i = 0; i < deleteCount; i++) {
      deletedIds[i] = i * 100L; // Sparse: 0, 100, 200, 300, ...
    }

    BenchmarkResult result = benchmarkDeleteFormat(deletedIds);

    System.out.println("\n=== Sparse Deletes Benchmark ===");
    System.out.println("Delete count: " + deleteCount);
    System.out.println("Traditional (Parquet) size: " + result.traditionalSize + " bytes");
    System.out.println("EDV (Puffin/Bitmap) size: " + result.edvSize + " bytes");
    System.out.println(
        "Compression ratio: " + String.format("%.1f", result.compressionRatio) + "x");
    System.out.println("Space savings: " + String.format("%.1f", result.spaceSavings) + "%");

    // Sparse deletes may not compress much, but still not worse than traditional
    // (EDV adds ~400 bytes overhead for Puffin header, so roughly break-even for small sparse sets)
    assertThat(result.edvSize).isLessThan((long) (result.traditionalSize * 2.0));
  }

  @Test
  public void testMixedPatternBenchmark() throws IOException {
    // Mixed pattern: some sequential runs, some sparse
    List<Long> deletedIdsList = Lists.newArrayList();

    // Add sequential run 1: 0-999
    for (long i = 0; i < 1000; i++) {
      deletedIdsList.add(i);
    }

    // Add sparse values: 10000, 10100, 10200, ...
    for (long i = 10000; i < 11000; i += 100) {
      deletedIdsList.add(i);
    }

    // Add sequential run 2: 20000-20999
    for (long i = 20000; i < 21000; i++) {
      deletedIdsList.add(i);
    }

    long[] deletedIds = deletedIdsList.stream().mapToLong(Long::longValue).toArray();

    BenchmarkResult result = benchmarkDeleteFormat(deletedIds);

    System.out.println("\n=== Mixed Pattern Deletes Benchmark ===");
    System.out.println("Delete count: " + deletedIds.length);
    System.out.println("Traditional (Parquet) size: " + result.traditionalSize + " bytes");
    System.out.println("EDV (Puffin/Bitmap) size: " + result.edvSize + " bytes");
    System.out.println(
        "Compression ratio: " + String.format("%.1f", result.compressionRatio) + "x");
    System.out.println("Space savings: " + String.format("%.1f", result.spaceSavings) + "%");

    // Mixed pattern should achieve good compression (>8x) - RLE handles sequential runs well
    assertThat(result.compressionRatio).isGreaterThan(8.0);
    assertThat(result.spaceSavings).isGreaterThan(85.0);
  }

  @Test
  public void testLargeScaleBenchmark() throws IOException {
    // Large-scale test: 100k sequential deletes
    int deleteCount = 100000;
    long[] deletedIds = new long[deleteCount];
    for (int i = 0; i < deleteCount; i++) {
      deletedIds[i] = i;
    }

    BenchmarkResult result = benchmarkDeleteFormat(deletedIds);

    System.out.println("\n=== Large Scale Benchmark (100k deletes) ===");
    System.out.println("Delete count: " + deleteCount);
    System.out.println("Traditional (Parquet) size: " + result.traditionalSize + " bytes");
    System.out.println("EDV (Puffin/Bitmap) size: " + result.edvSize + " bytes");
    System.out.println(
        "Compression ratio: " + String.format("%.1f", result.compressionRatio) + "x");
    System.out.println("Space savings: " + String.format("%.1f", result.spaceSavings) + "%");

    // Large sequential deletes should achieve excellent compression
    // RLE makes bitmap extremely compact for sequential ranges
    assertThat(result.compressionRatio).isGreaterThan(100.0);
    assertThat(result.spaceSavings).isGreaterThan(99.0);
  }

  @Test
  public void testWorstCaseBenchmark() throws IOException {
    // Worst case: highly sparse, random-ish values
    // Note: Very sparse patterns may not benefit from bitmap compression
    int deleteCount = 1000;
    long[] deletedIds = new long[deleteCount];
    // Use a deterministic "random" pattern: prime number multiples
    long multiplier = 997; // Large prime
    for (int i = 0; i < deleteCount; i++) {
      deletedIds[i] = i * multiplier;
    }

    BenchmarkResult result = benchmarkDeleteFormat(deletedIds);

    System.out.println("\n=== Worst Case Benchmark (highly sparse) ===");
    System.out.println("Delete count: " + deleteCount);
    System.out.println("Traditional (Parquet) size: " + result.traditionalSize + " bytes");
    System.out.println("EDV (Puffin/Bitmap) size: " + result.edvSize + " bytes");
    System.out.println(
        "Compression ratio: " + String.format("%.1f", result.compressionRatio) + "x");
    System.out.println("Space savings: " + String.format("%.1f", result.spaceSavings) + "%");

    // Worst case may not compress well, but should still be comparable to Parquet
    // (not more than 2x larger due to Puffin overhead)
    assertThat(result.edvSize).isLessThan((long) (result.traditionalSize * 2.0));
  }

  private BenchmarkResult benchmarkDeleteFormat(long[] deletedIds) throws IOException {
    // Write traditional Parquet equality delete file
    long traditionalSize = writeTraditionalEqualityDelete(deletedIds);

    // Write EDV (Puffin/bitmap) delete file
    long edvSize = writeEqualityDeleteVector(deletedIds);

    double compressionRatio = (double) traditionalSize / edvSize;
    double spaceSavings = ((double) (traditionalSize - edvSize) / traditionalSize) * 100.0;

    return new BenchmarkResult(traditionalSize, edvSize, compressionRatio, spaceSavings);
  }

  private long writeTraditionalEqualityDelete(long[] idsToDelete) throws IOException {
    // Temporarily disable EDV to force traditional Parquet
    table.updateProperties().set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "false").commit();

    List<Record> deleteRecords = Lists.newArrayList();
    GenericRecord deleteRecord = GenericRecord.create(table.schema().select("id"));

    for (long id : idsToDelete) {
      deleteRecords.add(deleteRecord.copy("id", id));
    }

    OutputFile output =
        org.apache.iceberg.Files.localOutput(
            new File(tableDir, "delete-traditional-" + System.nanoTime() + ".parquet"));

    Schema deleteSchema = table.schema().select("id");
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, output, null, deleteRecords, deleteSchema);

    // Verify it's Parquet
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PARQUET);

    // Re-enable EDV for next test
    table.updateProperties().set(TableProperties.EQUALITY_DELETE_VECTOR_ENABLED, "true").commit();

    return deleteFile.fileSizeInBytes();
  }

  private long writeEqualityDeleteVector(long[] idsToDelete) throws IOException {
    // EDV is already enabled from setup

    List<Record> deleteRecords = Lists.newArrayList();
    GenericRecord deleteRecord = GenericRecord.create(table.schema().select("id"));

    for (long id : idsToDelete) {
      deleteRecords.add(deleteRecord.copy("id", id));
    }

    OutputFile output =
        org.apache.iceberg.Files.localOutput(
            new File(tableDir, "delete-edv-" + System.nanoTime() + ".puffin"));

    Schema deleteSchema = table.schema().select("id");
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, output, null, deleteRecords, deleteSchema);

    // Verify it's Puffin (EDV)
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PUFFIN);

    return deleteFile.fileSizeInBytes();
  }

  private static class BenchmarkResult {
    final long traditionalSize;
    final long edvSize;
    final double compressionRatio;
    final double spaceSavings;

    BenchmarkResult(
        long traditionalSize, long edvSize, double compressionRatio, double spaceSavings) {
      this.traditionalSize = traditionalSize;
      this.edvSize = edvSize;
      this.compressionRatio = compressionRatio;
      this.spaceSavings = spaceSavings;
    }
  }
}
