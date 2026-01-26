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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Spark SQL integration tests for Equality Delete Vectors (EDV).
 *
 * <p>Tests end-to-end Spark SQL DELETE statements with format version 3 tables to verify that
 * Equality Delete Vectors are automatically created and used.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteVectorSparkSQL extends SparkRowLevelOperationsTestBase {

  @Override
  protected Map<String, String> extraTableProperties() {
    return java.util.Collections.emptyMap();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testBasicEqualityDeleteVector() throws NoSuchTableException {
    // Create table with LONG id field (EDV-compatible) and format version 3
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3', 'write.format.default' = 'parquet')",
        tableName);

    // Insert test data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", tableName);

    // Verify initial data
    List<Object[]> initialRows = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(initialRows)
        .containsExactly(
            row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));

    // Delete using equality condition - should create EDV
    sql("DELETE FROM %s WHERE id IN (2, 4)", tableName);

    // Verify deletes were applied
    List<Object[]> afterDelete = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertThat(afterDelete).containsExactly(row(1L, "a"), row(3L, "c"), row(5L, "e"));

    // Verify EDV files were created (PUFFIN format, equality delete)
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = table.currentSnapshot();
    assertThat(currentSnapshot).isNotNull();

    List<DeleteFile> deleteFiles = getDeleteFiles(table, currentSnapshot);

    // Should have equality delete files in PUFFIN format
    assertThat(deleteFiles)
        .isNotEmpty()
        .anyMatch(
            df ->
                df.format() == FileFormat.PUFFIN
                    && df.equalityFieldIds() != null
                    && df.equalityFieldIds().contains(1),
            "Should have at least one PUFFIN EDV with field ID 1 (id column)");
  }

  @TestTemplate
  public void testMultipleDeleteOperations() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", tableName);

    // First delete
    sql("DELETE FROM %s WHERE id = 2", tableName);
    List<Object[]> afterFirst = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(afterFirst).extracting(row -> row[0]).containsExactly(1L, 3L, 4L, 5L);

    // Second delete
    sql("DELETE FROM %s WHERE id = 4", tableName);
    List<Object[]> afterSecond = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(afterSecond).extracting(row -> row[0]).containsExactly(1L, 3L, 5L);

    // Third delete
    sql("DELETE FROM %s WHERE id IN (1, 5)", tableName);
    List<Object[]> afterThird = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(afterThird).extracting(row -> row[0]).containsExactly(3L);

    // Verify multiple snapshots created
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSizeGreaterThan(3);
  }

  @TestTemplate
  public void testEqualityDeleteVectorWithPredicates() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, category STRING, value INT) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert test data
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'A', 100), (2, 'A', 200), (3, 'B', 300), "
            + "(4, 'B', 400), (5, 'C', 500), (6, 'C', 600)",
        tableName);

    // Delete with predicate
    sql("DELETE FROM %s WHERE category = 'B'", tableName);

    // Verify correct rows deleted
    List<Object[]> result = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(result).extracting(row -> row[0]).containsExactly(1L, 2L, 5L, 6L);

    // Verify snapshot summary
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.summary()).containsEntry("deleted-records", "2");
  }

  @TestTemplate
  public void testEqualityDeleteVectorCompression() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert 1000 sequential records (good for run-length encoding)
    StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES ");
    for (int i = 0; i < 1000; i++) {
      if (i > 0) insertQuery.append(", ");
      insertQuery.append("(").append(i).append(", 'data").append(i).append("')");
    }
    sql(insertQuery.toString());

    // Delete sequential range (should compress well with RLE bitmap)
    sql("DELETE FROM %s WHERE id >= 0 AND id < 500", tableName);

    // Verify correct count
    List<Object[]> count = sql("SELECT COUNT(*) FROM %s", tableName);
    assertThat(count.get(0)[0]).isEqualTo(500L);

    // Verify EDV is small due to compression
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    List<DeleteFile> deleteFiles = getDeleteFiles(table, snapshot);

    assertThat(deleteFiles).isNotEmpty();
    for (DeleteFile deleteFile : deleteFiles) {
      if (deleteFile.format() == FileFormat.PUFFIN) {
        // Sequential deletes should compress to < 10KB with Roaring bitmap
        assertThat(deleteFile.fileSizeInBytes())
            .isLessThan(10_000L)
            .withFailMessage(
                "EDV should be well-compressed for sequential deletes, but was %d bytes",
                deleteFile.fileSizeInBytes());
      }
    }
  }

  @TestTemplate
  public void testEqualityDeleteVectorWithSparseValues() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert sparse IDs
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'a'), (1000, 'b'), (1000000, 'c'), (1000000000, 'd')",
        tableName);

    // Delete sparse values
    sql("DELETE FROM %s WHERE id IN (1000, 1000000000)", tableName);

    // Verify correct rows remain
    List<Object[]> result = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(result).extracting(row -> row[0]).containsExactly(1L, 1000000L);

    // Verify EDV remains small despite sparse values (bitmap compression)
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    List<DeleteFile> deleteFiles = getDeleteFiles(table, snapshot);

    assertThat(deleteFiles)
        .isNotEmpty()
        .anyMatch(
            df -> df.format() == FileFormat.PUFFIN && df.fileSizeInBytes() < 5000,
            "Sparse EDV should be small due to Roaring bitmap compression");
  }

  @TestTemplate
  public void testSchemaEvolutionWithEDV() throws NoSuchTableException {
    // Create table with INT id (not EDV-compatible initially)
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    // Evolve schema: INT → LONG (now EDV-compatible)
    sql("ALTER TABLE %s ALTER COLUMN id TYPE BIGINT", tableName);

    // Delete after schema evolution - should use EDV
    sql("DELETE FROM %s WHERE id = 2", tableName);

    // Verify deletes work
    List<Object[]> result = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(result).extracting(row -> row[0]).containsExactly(1L, 3L);

    // Verify EDV files created after schema evolution
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    List<DeleteFile> deleteFiles = getDeleteFiles(table, snapshot);

    long edvCount = deleteFiles.stream().filter(df -> df.format() == FileFormat.PUFFIN).count();
    assertThat(edvCount)
        .isPositive()
        .withFailMessage("Should have EDVs after INT→LONG schema evolution");
  }

  @TestTemplate
  public void testPartitionedTableWithEDV() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, category STRING, data STRING) USING iceberg "
            + "PARTITIONED BY (category) "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert partitioned data
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'A', 'data1'), (2, 'A', 'data2'), "
            + "(3, 'B', 'data3'), (4, 'B', 'data4'), "
            + "(5, 'C', 'data5'), (6, 'C', 'data6')",
        tableName);

    // Delete from specific partition
    sql("DELETE FROM %s WHERE category = 'B' AND id = 3", tableName);

    // Verify delete
    List<Object[]> result = sql("SELECT id FROM %s WHERE category = 'B' ORDER BY id", tableName);
    assertThat(result).extracting(row -> row[0]).containsExactly(4L);

    // Verify EDV created
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    List<DeleteFile> deleteFiles = getDeleteFiles(table, snapshot);

    assertThat(deleteFiles)
        .isNotEmpty()
        .anyMatch(df -> df.format() == FileFormat.PUFFIN, "Should have at least one EDV");
  }

  @TestTemplate
  public void testDeleteWithTimeTravel() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert initial data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotBeforeDelete = table.currentSnapshot().snapshotId();

    // Delete data
    sql("DELETE FROM %s WHERE id = 2", tableName);

    // Verify current state
    List<Object[]> currentResult = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(currentResult).extracting(row -> row[0]).containsExactly(1L, 3L);

    // Time travel to before delete
    List<Object[]> historicalResult =
        sql("SELECT id FROM %s VERSION AS OF %d ORDER BY id", tableName, snapshotBeforeDelete);
    assertThat(historicalResult).extracting(row -> row[0]).containsExactly(1L, 2L, 3L);
  }

  @TestTemplate
  public void testDeleteWithComplexPredicates() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, value INT, category STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert test data
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 100, 'A'), (2, 200, 'A'), (3, 300, 'B'), "
            + "(4, 400, 'B'), (5, 500, 'C'), (6, 600, 'C')",
        tableName);

    // Delete with complex predicate
    sql("DELETE FROM %s WHERE value > 250 AND category IN ('B', 'C')", tableName);

    // Verify results
    List<Object[]> result = sql("SELECT id FROM %s ORDER BY id", tableName);
    assertThat(result).extracting(row -> row[0]).containsExactly(1L, 2L, 3L);

    // Verify snapshot summary
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.summary()).containsEntry("deleted-records", "3");
  }

  @TestTemplate
  public void testDeleteAllRows() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version' = '3')",
        tableName);

    // Insert data
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    // Delete all rows
    sql("DELETE FROM %s WHERE id > 0", tableName);

    // Verify empty
    List<Object[]> result = sql("SELECT * FROM %s", tableName);
    assertThat(result).isEmpty();

    // Verify delete files created
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    assertThat(snapshot.summary()).containsEntry("deleted-records", "3");
  }

  // Helper method to extract delete files from snapshot
  private List<DeleteFile> getDeleteFiles(Table table, Snapshot snapshot) {
    return Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));
  }
}
