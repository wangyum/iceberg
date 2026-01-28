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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test to verify equality deletes work with direct Iceberg table scan (bypassing Spark SQL).
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestEqualityDeleteDirectScan extends SparkRowLevelOperationsTestBase {

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
  public void testDirectIcebergScan() throws NoSuchTableException, IOException {
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

    // Execute DELETE via Spark SQL
    sql("DELETE FROM %s WHERE id = 2", commitTarget());

    // Load table
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);

    // Verify EDV was created
    List<DeleteFile> deleteFiles = Lists.newArrayList(snapshot.addedDeleteFiles(table.io()));
    assertThat(deleteFiles).hasSize(1);
    assertThat(deleteFiles.get(0).format()).isEqualTo(FileFormat.PUFFIN);

    System.out.println("\n=== DIRECT ICEBERG SCAN ===");

    // Use Iceberg's table scan directly
    TableScan scan = table.newScan();
    if (branch != null) {
      scan = scan.useRef(branch);
    }

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        System.out.println("FileScanTask for: " + task.file().location());
        System.out.println("  Delete files count: " + task.deletes().size());

        for (DeleteFile df : task.deletes()) {
          System.out.println("  Delete file: " + df.location());
          System.out.println("    Format: " + df.format());
          System.out.println("    Content: " + df.content());
        }

        // THIS IS THE KEY CHECK
        if (task.deletes().isEmpty()) {
          System.out.println("\n=== BUG: FileScanTask.deletes() IS EMPTY ===");
          System.out.println("This confirms delete files are not being associated with data files during scan planning");
        } else {
          System.out.println("\n=== FileScanTask includes delete files correctly ===");
        }
      }
    }

    System.out.println("\n=== END DIRECT SCAN ===");
  }
}
