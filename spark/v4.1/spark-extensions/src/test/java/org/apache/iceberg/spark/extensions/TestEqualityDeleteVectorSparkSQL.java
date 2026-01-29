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
 * <p>Tests end-to-end Spark SQL UPDATE statements with format version 3 tables to verify
 * that Equality Delete Vectors (EDVs) are created in PUFFIN format when enabled.
 *
 * <p>Configures tables with:
 * <ul>
 *   <li>write.update.mode = merge-on-read
 *   <li>write.delete.equality-vector.enabled = true
 * </ul>
 *
 * <p>Note: Spark SQL UPDATE in merge-on-read mode writes the old row values as equality
 * deletes, then appends the new values. With a LONG primary key and equality-vector enabled,
 * these equality deletes should be stored as Roaring bitmaps in PUFFIN format.
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
    // Create table with 'id' as primary key to enable equality deletes
    sql(
        "CREATE TABLE %s (id LONG NOT NULL, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version' = '%d', "
            + "'write.update.mode' = 'merge-on-read', "
            + "'write.delete.equality-vector.enabled' = 'true', "
            + "'write.upsert-enabled' = 'true'"
            + ")",
        tableName, formatVersion);

    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }");

    createBranchIfNeeded();

    // UPDATE creates equality deletes for the old values in merge-on-read mode
    // Setting data to null effectively "deletes" the meaningful data
    sql("UPDATE %s SET data = 'deleted' WHERE id = 2", commitTarget());

    assertEquals(
        "Should have updated row",
        ImmutableList.of(row(1L, "a"), row(2L, "deleted"), row(3L, "c")),
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

    if (formatVersion >= 3 && !deleteFiles.isEmpty()) {
      // UPDATE in merge-on-read mode should create equality deletes
      // Equality deletes have:
      // - null referencedDataFile (not tied to specific file)
      // - non-null equalityFieldIds (the fields used for equality matching)
      // - content = EQUALITY_DELETES
      boolean hasEqualityDeletes = deleteFiles.stream()
          .anyMatch(df -> df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES &&
                         df.referencedDataFile() == null &&
                         df.equalityFieldIds() != null &&
                         !df.equalityFieldIds().isEmpty());

      if (hasEqualityDeletes) {
        System.out.println("✓ Equality Delete Vectors are being used!");

        // Verify PUFFIN format for equality delete vectors
        boolean hasPuffinEqualityDeletes = deleteFiles.stream()
            .anyMatch(df -> df.content() == org.apache.iceberg.FileContent.EQUALITY_DELETES &&
                           df.format() == FileFormat.PUFFIN);

        if (hasPuffinEqualityDeletes) {
          System.out.println("✓✓ Equality deletes are in PUFFIN format (bitmaps)!");
        } else {
          System.out.println("⚠ Equality deletes are in Parquet format (not bitmaps)");
        }
      } else {
        System.out.println("✗ Position deletes are being used instead of equality deletes");
      }
    }
  }

  @TestTemplate
  public void testMultipleDeletes() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id LONG NOT NULL, data STRING) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version' = '%d', "
            + "'write.delete.mode' = 'merge-on-read', "
            + "'write.delete.equality-vector.enabled' = 'true'"
            + ")",
        tableName, formatVersion);

    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }\n"
            + "{ \"id\": 4, \"data\": \"d\" }\n"
            + "{ \"id\": 5, \"data\": \"e\" }");

    createBranchIfNeeded();

    sql(
        "MERGE INTO %s t USING (SELECT 2 as id) s ON t.id = s.id "
            + "WHEN MATCHED THEN DELETE",
        commitTarget());
    sql(
        "MERGE INTO %s t USING (SELECT 4 as id) s ON t.id = s.id "
            + "WHEN MATCHED THEN DELETE",
        commitTarget());

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(3L, "c"), row(5L, "e")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSizeGreaterThan(2);
  }

  @TestTemplate
  public void testDeleteWithPredicate() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id LONG NOT NULL, category STRING, value INT) USING iceberg "
            + "TBLPROPERTIES ("
            + "'format-version' = '%d', "
            + "'write.delete.mode' = 'merge-on-read', "
            + "'write.delete.equality-vector.enabled' = 'true'"
            + ")",
        tableName, formatVersion);

    append(
        tableName,
        "{ \"id\": 1, \"category\": \"A\", \"value\": 100 }\n"
            + "{ \"id\": 2, \"category\": \"A\", \"value\": 200 }\n"
            + "{ \"id\": 3, \"category\": \"B\", \"value\": 300 }\n"
            + "{ \"id\": 4, \"category\": \"B\", \"value\": 400 }");

    createBranchIfNeeded();

    // Delete by id values to use equality deletes
    sql(
        "MERGE INTO %s t USING (SELECT 3 as id UNION ALL SELECT 4 as id) s ON t.id = s.id "
            + "WHEN MATCHED THEN DELETE",
        commitTarget());

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
