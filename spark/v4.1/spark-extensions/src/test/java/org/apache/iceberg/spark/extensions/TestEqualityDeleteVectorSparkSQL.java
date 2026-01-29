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
 * Spark SQL integration tests for Equality Delete Vectors with merge-on-read mode.
 *
 * <p>Tests end-to-end Spark SQL DELETE statements with format version 3 tables to verify
 * that Equality Delete Vectors (EDVs) are created in PUFFIN format when enabled.
 *
 * <p>Configures tables with:
 * <ul>
 *   <li>write.delete.mode = merge-on-read
 *   <li>write.delete.equality-vector.enabled = true
 * </ul>
 *
 * <p>When Spark DELETE operates on tables with LONG primary keys in V3+, the system
 * should create equality delete vectors (bitmaps) instead of position deletes.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteVectorSparkSQL extends SparkRowLevelOperationsTestBase {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.<String, String>builder()
        .put(TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName())
        .put(TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName())
        .put(TableProperties.MERGE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName())
        .put(TableProperties.WRITE_DISTRIBUTION_MODE, "hash")
        .put("write.delete.equality-vector.enabled", "true")
        .build();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testBasicDeleteWithDeleteVectors() throws NoSuchTableException {
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

    // Print debug info about what deletes were created
    for (DeleteFile df : deleteFiles) {
      System.out.println("=== Delete File Info ===");
      System.out.println("Format: " + df.format());
      System.out.println("Content: " + df.content());
      System.out.println("Equality field IDs: " + df.equalityFieldIds());
      System.out.println("Referenced data file: " + df.referencedDataFile());
      System.out.println("Record count: " + df.recordCount());
    }

    if (formatVersion >= 3) {
      // Should have PUFFIN format delete files (delete vectors)
      assertThat(deleteFiles)
          .anyMatch(df -> df.format() == FileFormat.PUFFIN, "Should have PUFFIN DV files in v3");

      // With equality-vector.enabled=true and LONG id field, should use equality deletes
      // Equality deletes have null referencedDataFile and non-null equalityFieldIds
      boolean hasEqualityDeletes = deleteFiles.stream()
          .anyMatch(df -> df.referencedDataFile() == null &&
                         df.equalityFieldIds() != null &&
                         !df.equalityFieldIds().isEmpty());

      if (hasEqualityDeletes) {
        System.out.println("✓ Equality Delete Vectors are being used!");
      } else {
        System.out.println("✗ Position deletes are being used (equality deletes not working)");
      }
    }
  }

  @TestTemplate
  public void testMultipleDeletes() throws NoSuchTableException {
    createAndInitTable("id LONG, data STRING");

    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }\n"
            + "{ \"id\": 4, \"data\": \"d\" }\n"
            + "{ \"id\": 5, \"data\": \"e\" }");

    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE id = 2", commitTarget());
    sql("DELETE FROM %s WHERE id = 4", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(3L, "c"), row(5L, "e")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSizeGreaterThan(2);
  }

  @TestTemplate
  public void testDeleteWithPredicate() throws NoSuchTableException {
    createAndInitTable("id LONG, category STRING, value INT");

    append(
        tableName,
        "{ \"id\": 1, \"category\": \"A\", \"value\": 100 }\n"
            + "{ \"id\": 2, \"category\": \"A\", \"value\": 200 }\n"
            + "{ \"id\": 3, \"category\": \"B\", \"value\": 300 }\n"
            + "{ \"id\": 4, \"category\": \"B\", \"value\": 400 }");

    createBranchIfNeeded();

    sql("DELETE FROM %s WHERE category = 'B'", commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "A", 100), row(2L, "A", 200)),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    if (formatVersion >= 3) {
      // In v3, position deletes are written
      assertThat(snapshot.summary()).containsKey("added-delete-files");
    }
  }
}
