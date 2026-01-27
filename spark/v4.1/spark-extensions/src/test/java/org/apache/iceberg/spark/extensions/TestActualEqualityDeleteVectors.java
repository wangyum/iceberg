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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BitmapDeleteWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for ACTUAL Equality Delete Vectors (EDV) with Spark SQL.
 *
 * <p>These tests verify that Spark SQL can correctly READ tables with actual equality deletes
 * created using the programmatic BitmapDeleteWriter API.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestActualEqualityDeleteVectors extends SparkRowLevelOperationsTestBase {

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
  public void testSparkSQLReadWithActualEDV() throws NoSuchTableException, IOException {
    if (formatVersion < 3) {
      return;
    }

    createAndInitTable("id LONG, data STRING");

    // Write initial data using Spark SQL
    append(
        tableName,
        "{ \"id\": 1, \"data\": \"a\" }\n"
            + "{ \"id\": 2, \"data\": \"b\" }\n"
            + "{ \"id\": 3, \"data\": \"c\" }\n"
            + "{ \"id\": 4, \"data\": \"d\" }\n"
            + "{ \"id\": 5, \"data\": \"e\" }");

    createBranchIfNeeded();

    // Verify initial data
    List<Object[]> initialRows = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    System.out.println("\n=== INITIAL DATA ===");
    System.out.println("Row count: " + initialRows.size());
    for (Object[] row : initialRows) {
      System.out.println("  Row: id=" + row[0] + ", data=" + row[1]);
    }

    assertEquals(
        "Should have 5 rows initially",
        ImmutableList.of(
            row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e")),
        initialRows);

    // Now create ACTUAL equality deletes using BitmapDeleteWriter
    Table table = validationCatalog.loadTable(tableIdent);
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();

    BitmapDeleteWriter writer = new BitmapDeleteWriter(fileFactory);
    try (BitmapDeleteWriter w = writer) {
      // Delete rows where id=2 and id=4 using EQUALITY deletes
      w.deleteEquality(1, 2L, table.spec(), null); // field ID 1 = "id" column
      w.deleteEquality(1, 4L, table.spec(), null);
    }

    DeleteFile edvFile = Iterables.getOnlyElement(writer.result().deleteFiles());

    // Commit to the correct branch if needed
    if (branch != null) {
      table.newRowDelta().toBranch(branch).addDeletes(edvFile).commit();
    } else {
      table.newRowDelta().addDeletes(edvFile).commit();
    }

    // Verify the delete file metadata
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    assertThat(deleteFiles).hasSize(1);
    DeleteFile actualEDV = deleteFiles.get(0);

    System.out.println("\n=== ACTUAL EQUALITY DELETE VECTOR ===");
    System.out.println("Format: " + actualEDV.format());
    System.out.println("Content: " + actualEDV.content());
    System.out.println("Equality field IDs: " + actualEDV.equalityFieldIds());
    System.out.println("Record count: " + actualEDV.recordCount());
    System.out.println("File size: " + actualEDV.fileSizeInBytes() + " bytes");
    System.out.println("======================================");

    // Verify it's ACTUALLY an equality delete
    assertThat(actualEDV.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(actualEDV.content().toString()).isEqualTo("EQUALITY_DELETES");
    assertThat(actualEDV.equalityFieldIds()).isNotNull();
    assertThat(actualEDV.equalityFieldIds()).containsExactly(1);
    assertThat(actualEDV.recordCount()).isEqualTo(2);

    // IMPORTANT: Refresh the table in Spark's catalog to pick up the newly committed delete file
    // Without this, Spark may use cached metadata that doesn't include the delete file
    sql("REFRESH TABLE %s", tableName);

    // THE KEY TEST: Verify Spark SQL can READ the table with EDV correctly
    List<Object[]> afterDeleteRows = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    System.out.println("\n=== AFTER EQUALITY DELETES ===");
    System.out.println("Row count: " + afterDeleteRows.size());
    for (Object[] row : afterDeleteRows) {
      System.out.println("  Row: id=" + row[0] + ", data=" + row[1]);
    }
    System.out.println("Expected: 3 rows (id=1,3,5)");
    System.out.println("===============================");

    assertEquals(
        "Spark SQL should correctly apply equality deletes",
        ImmutableList.of(row(1L, "a"), row(3L, "c"), row(5L, "e")),
        afterDeleteRows);

    System.out.println("\n✓ SUCCESS: Spark SQL correctly read table with Equality Delete Vectors!");
  }
}
