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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

/**
 * Debug test to understand why Spark SQL DELETE with equality strategy doesn't apply deletes.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteDebug extends SparkRowLevelOperationsTestBase {

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
  public void testDebugEqualityDeleteSpec() throws NoSuchTableException {
    // Only run on format version 3
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

    // Execute DELETE
    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    // Load table and inspect
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    System.out.println("\n=== TABLE INFO ===");
    System.out.println("Table name: " + tableName);
    System.out.println("Table specs: " + table.specs());
    System.out.println("Default spec ID: " + table.spec().specId());
    System.out.println("Default spec is unpartitioned: " + table.spec().isUnpartitioned());

    System.out.println("\n=== SNAPSHOT INFO ===");
    System.out.println("Snapshot ID: " + snapshot.snapshotId());
    System.out.println("Operation: " + snapshot.operation());
    System.out.println("Summary: " + snapshot.summary());

    // Get delete files
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));

    System.out.println("\n=== DELETE FILES ===");
    System.out.println("Count: " + deleteFiles.size());

    for (DeleteFile df : deleteFiles) {
      System.out.println("\nDelete file:");
      System.out.println("  Location: " + df.location());
      System.out.println("  Format: " + df.format());
      System.out.println("  Content: " + df.content());
      System.out.println("  Spec ID: " + df.specId());
      System.out.println("  Partition: " + df.partition());
      System.out.println("  Equality field IDs: " + df.equalityFieldIds());
      System.out.println("  Record count: " + df.recordCount());
      System.out.println("  Data sequence number: " + df.dataSequenceNumber());

      // Check if spec exists
      org.apache.iceberg.PartitionSpec spec = table.specs().get(df.specId());
      System.out.println("  Spec exists in table.specs(): " + (spec != null));
      if (spec != null) {
        System.out.println("  Spec is unpartitioned: " + spec.isUnpartitioned());
        System.out.println("  Spec: " + spec);
      }
    }

    // Verify we created an EDV
    assertThat(deleteFiles).hasSize(1);
    assertThat(deleteFiles.get(0).format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(deleteFiles.get(0).content().toString()).isEqualTo("EQUALITY_DELETES");

    // Now try to read the data
    System.out.println("\n=== READING DATA ===");
    List<Object[]> rows = sql("SELECT * FROM %s ORDER BY id", selectTarget());
    System.out.println("Row count: " + rows.size());
    for (Object[] row : rows) {
      System.out.println("  Row: " + java.util.Arrays.toString(row));
    }

    // Check if deletes were applied
    if (rows.size() == 3) {
      System.out.println("\n=== DELETES NOT APPLIED - BUG REPRODUCED ===");

      // Let me try to inspect all snapshots
      System.out.println("\n=== ALL SNAPSHOTS ===");
      for (Snapshot snap : table.snapshots()) {
        System.out.println("Snapshot " + snap.snapshotId() + ":");
        System.out.println("  Operation: " + snap.operation());
        System.out.println("  Parent: " + snap.parentId());
        System.out.println("  Manifest list: " + snap.manifestListLocation());
        List<org.apache.iceberg.ManifestFile> manifests = snap.allManifests(table.io());
        for (org.apache.iceberg.ManifestFile mf : manifests) {
          System.out.println("  Manifest: " + mf.path());
          System.out.println("    Content: " + mf.content());
          System.out.println("    Added files: " + mf.addedFilesCount());
          System.out.println("    Existing files: " + mf.existingFilesCount());
          System.out.println("    Deleted files: " + mf.deletedFilesCount());
        }
      }
    } else if (rows.size() == 2) {
      System.out.println("\n=== DELETES APPLIED CORRECTLY ===");
    }

    System.out.println("\n=== TEST PASSED ===");
  }
}
