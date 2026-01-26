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
 * Spark SQL integration tests for Delete Vectors (DV) with merge-on-read mode.
 *
 * <p>Tests end-to-end Spark SQL DELETE statements with format version 3 tables using merge-on-read
 * mode to verify that Position Delete Vectors (DVs) are created in PUFFIN format.
 *
 * <p>Note: Spark SQL DELETE creates position deletes, not equality deletes. For Equality Delete
 * Vector (EDV) tests using the programmatic API, see TestSparkEqualityDeleteVectors.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteVectorSparkSQL extends SparkRowLevelOperationsTestBase {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
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

    if (formatVersion >= 3) {
      assertThat(deleteFiles)
          .anyMatch(df -> df.format() == FileFormat.PUFFIN, "Should have PUFFIN DV files in v3");
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
    assertThat(snapshot.summary()).containsEntry("added-position-deletes", "2");
  }
}
