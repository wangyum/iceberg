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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for Spark SQL DELETE operations with equality delete strategy.
 *
 * <p>This test verifies that when {@code write.delete.strategy=equality} is set, Spark SQL DELETE
 * operations create EQUALITY_DELETES instead of POSITION_DELETES.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteStrategySparkSQL extends SparkRowLevelOperationsTestBase {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName(),
        TableProperties.DELETE_STRATEGY,
        TableProperties.DELETE_STRATEGY_EQUALITY);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testEqualityDeleteStrategy() throws NoSuchTableException {
    // Format version must be >= 3 for PUFFIN support
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");

    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }");

    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));
    assertThat(deleteFiles).isNotEmpty();

    for (DeleteFile df : deleteFiles) {
      System.out.println("=== DELETE FILE METADATA ==");
      System.out.println("Format: " + df.format());
      System.out.println("Content: " + df.content());
      System.out.println("Equality field IDs: " + df.equalityFieldIds());
      System.out.println("===========================");
    }

    // Verify it's an equality delete
    assertThat(deleteFiles.get(0).format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFiles.get(0).content().toString()).isEqualTo("EQUALITY_DELETES");
    assertThat(deleteFiles.get(0).equalityFieldIds()).isNotNull();
    assertThat(deleteFiles.get(0).equalityFieldIds()).containsExactly(1);
  }

  @TestTemplate
  public void testMultipleEqualityDeletes() throws NoSuchTableException {
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");

    // Insert more data to test bitmap compression
    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }\n"
            + "{ \"id\": 4, \"data\": \"d\" }\n"
            + "{ \"id\": 5, \"data\": \"e\" }\n"
            + "{ \"id\": 10, \"data\": \"j\" }\n"
            + "{ \"id\": 20, \"data\": \"t\" }\n"
            + "{ \"id\": 30, \"data\": \"x\" }");

    createBranchIfNeeded();

    // Delete multiple rows - should accumulate in single EDV file
    sql("DELETE FROM %s WHERE id = 2", commitTarget());
    sql("DELETE FROM %s WHERE id = 4", commitTarget());
    sql("DELETE FROM %s WHERE id = 10", commitTarget());

    assertEquals(
        "Should have expected rows after deletes",
        ImmutableList.of(
            row(1L, "a"), row(3L, "c"), row(5L, "e"), row(20L, "t"), row(30L, "x")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    List<DeleteFile> allDeleteFiles = Lists.newArrayList();
    for (Snapshot snap : table.snapshots()) {
      allDeleteFiles.addAll(Lists.newArrayList(snap.addedDeleteFiles(table.io())));
    }

    System.out.println("\n=== MULTIPLE EQUALITY DELETES ===");
    System.out.println("Total delete files created: " + allDeleteFiles.size());
    for (DeleteFile df : allDeleteFiles) {
      System.out.println("  - Format: " + df.format() + ", Content: " + df.content());
      System.out.println("    Equality field IDs: " + df.equalityFieldIds());
      System.out.println("    Record count: " + df.recordCount());
      System.out.println("    File size: " + df.fileSizeInBytes() + " bytes");
    }
    System.out.println("=================================");

    // All should be equality deletes
    for (DeleteFile df : allDeleteFiles) {
      assertThat(df.content().toString()).isEqualTo("EQUALITY_DELETES");
      assertThat(df.equalityFieldIds()).containsExactly(1);
      assertThat(df.format()).isEqualTo(FileFormat.PUFFIN);
    }
  }

  @TestTemplate
  public void testEqualityDeleteVectorCompression() throws NoSuchTableException {
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");

    // Insert enough data to demonstrate bitmap compression benefits
    StringBuilder data = new StringBuilder();
    for (long i = 1; i <= 100; i++) {
      data.append(String.format("{ \"id\": %d, \"data\": \"data_%d\" }\n", i, i));
    }
    append(tableName, data.toString());

    createBranchIfNeeded();

    // Delete scattered rows (good for bitmap compression)
    sql("DELETE FROM %s WHERE id IN (2, 5, 10, 15, 20, 25, 30, 40, 50, 60)", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    assertThat(deleteFiles).hasSize(1);
    DeleteFile edvFile = deleteFiles.get(0);

    System.out.println("\n=== EDV COMPRESSION VERIFICATION ===");
    System.out.println("Deleted 10 rows from 100 total rows");
    System.out.println("EDV file size: " + edvFile.fileSizeInBytes() + " bytes");
    System.out.println("Record count: " + edvFile.recordCount());
    System.out.println("Format: " + edvFile.format());
    System.out.println("Content: " + edvFile.content());
    System.out.println("Equality field IDs: " + edvFile.equalityFieldIds());
    System.out.println("====================================");

    // Verify EDV characteristics
    assertThat(edvFile.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(edvFile.content().toString()).isEqualTo("EQUALITY_DELETES");
    assertThat(edvFile.equalityFieldIds()).containsExactly(1);
    assertThat(edvFile.recordCount()).isEqualTo(10);

    // EDV file should be very small due to bitmap compression
    // A PUFFIN file with bitmap should be < 1KB for this data
    assertThat(edvFile.fileSizeInBytes())
        .as("EDV file should be small due to bitmap compression")
        .isLessThan(5000); // 5KB threshold

    // Verify data correctness
    List<Object[]> remainingRows = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    assertThat(remainingRows).hasSize(90);
  }

  @TestTemplate
  public void testComparePositionVsEqualityStrategy() throws NoSuchTableException {
    if (formatVersion < 3) {
      return;
    }

    // Test 1: With equality strategy (current test)
    createAndInitTable("id LONG, data STRING");

    append(tableName, "{ \"id\": 1, \"data\": \"a\" }\n" + "{ \"id\": 2, \"data\": \"b\" }");

    createBranchIfNeeded();
    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    System.out.println("\n=== EQUALITY DELETE STRATEGY ===");
    for (DeleteFile df : deleteFiles) {
      System.out.println("Content: " + df.content());
      System.out.println("Equality field IDs: " + df.equalityFieldIds());
    }

    assertThat(deleteFiles.get(0).content().toString()).isEqualTo("EQUALITY_DELETES");
    assertThat(deleteFiles.get(0).equalityFieldIds()).containsExactly(1);

    // Clean up
    sql("DROP TABLE %s", tableName);

    // Test 2: Create another table WITHOUT equality strategy
    sql(
        "CREATE TABLE %s (id LONG, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='%d', 'write.delete.mode'='merge-on-read', "
            + "'write.delete.strategy'='position')",
        tableName, formatVersion);

    append(tableName, "{ \"id\": 1, \"data\": \"a\" }\n" + "{ \"id\": 2, \"data\": \"b\" }");

    createBranchIfNeeded();
    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    table = validationCatalog.loadTable(tableIdent);
    snapshot = SnapshotUtil.latestSnapshot(table, branch);
    deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    System.out.println("\n=== POSITION DELETE STRATEGY ===");
    for (DeleteFile df : deleteFiles) {
      System.out.println("Content: " + df.content());
      System.out.println("Equality field IDs: " + df.equalityFieldIds());
    }

    // Should be position deletes
    assertThat(deleteFiles.get(0).content().toString()).isEqualTo("POSITION_DELETES");
    assertThat(deleteFiles.get(0).equalityFieldIds()).isNull();
  }
}
