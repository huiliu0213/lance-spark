/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.update;

import org.lance.spark.LanceTable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/** Base test for DESCRIBE HISTORY command and LanceTable.history() API. */
public abstract class BaseDescribeHistoryTest {
  protected String catalogName = "lance_test";
  protected String tableName;
  protected String fullTable;

  protected SparkSession spark;

  @TempDir Path tempDir;
  protected String tableDir;
  protected String testRoot;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    testRoot = rootPath.toString();
    spark =
        SparkSession.builder()
            .appName("lance-describe-history-test")
            .master("local[10]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    tableName = "history_test_" + UUID.randomUUID().toString().replace("-", "");
    fullTable = catalogName + ".default." + tableName;
    tableDir = FileSystems.getDefault().getPath(testRoot, tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  private void createAndPopulateTable() {
    spark.sql(String.format("CREATE TABLE %s (id INT, name STRING) USING lance", fullTable));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", fullTable));
    spark.sql(String.format("INSERT INTO %s VALUES (3, 'charlie')", fullTable));
  }

  @Test
  public void testDescribeHistorySchema() {
    createAndPopulateTable();

    Dataset<Row> result = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable));

    String expectedSchema =
        "StructType("
            + "StructField(version,LongType,false),"
            + "StructField(timestamp,TimestampType,false),"
            + "StructField(total_rows,LongType,true),"
            + "StructField(total_data_files,LongType,true),"
            + "StructField(total_files_size,LongType,true),"
            + "StructField(total_fragments,LongType,true),"
            + "StructField(total_deletion_files,LongType,true))";
    Assertions.assertEquals(expectedSchema, result.schema().toString());
  }

  @Test
  public void testDescribeHistoryReturnsAllVersions() {
    createAndPopulateTable();

    // CREATE TABLE = v1, first INSERT = v2, second INSERT = v3
    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    Assertions.assertTrue(
        rows.size() >= 3, "Expected at least 3 versions (create + 2 inserts), got " + rows.size());
  }

  @Test
  public void testDescribeHistoryDescendingOrder() {
    createAndPopulateTable();

    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    // Verify descending order: each version should be greater than the next
    for (int i = 0; i < rows.size() - 1; i++) {
      long currentVersion = rows.get(i).getLong(0);
      long nextVersion = rows.get(i + 1).getLong(0);
      Assertions.assertTrue(
          currentVersion > nextVersion,
          String.format(
              "Expected descending order: version %d should be > %d", currentVersion, nextVersion));
    }
  }

  @Test
  public void testDescribeHistoryLimit() {
    createAndPopulateTable();

    List<Row> rows =
        spark.sql(String.format("DESCRIBE HISTORY %s LIMIT 1", fullTable)).collectAsList();

    Assertions.assertEquals(1, rows.size(), "LIMIT 1 should return exactly 1 row");

    // The single row should be the latest version (highest version number)
    List<Row> allRows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();
    long latestVersion = allRows.get(0).getLong(0);
    Assertions.assertEquals(
        latestVersion, rows.get(0).getLong(0), "LIMIT 1 should return the latest version");
  }

  @Test
  public void testDescribeHistoryLimit2() {
    createAndPopulateTable();

    List<Row> rows =
        spark.sql(String.format("DESCRIBE HISTORY %s LIMIT 2", fullTable)).collectAsList();

    Assertions.assertEquals(2, rows.size(), "LIMIT 2 should return exactly 2 rows");
    Assertions.assertTrue(
        rows.get(0).getLong(0) > rows.get(1).getLong(0), "Results should be in descending order");
  }

  @Test
  public void testDescribeHistoryRowCountProgression() {
    spark.sql(String.format("CREATE TABLE %s (id INT, name STRING) USING lance", fullTable));
    // v2: insert 2 rows
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'alice'), (2, 'bob')", fullTable));
    // v3: insert 1 more row
    spark.sql(String.format("INSERT INTO %s VALUES (3, 'charlie')", fullTable));

    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    // Find versions by their version number
    // Latest version (v3) should have 3 total_rows
    Row latestRow = rows.get(0);
    long latestTotalRows = latestRow.getLong(2); // total_rows column
    Assertions.assertEquals(3L, latestTotalRows, "Latest version should have 3 total rows");
  }

  @Test
  public void testDescribeHistoryTimestampsAreNonNull() {
    createAndPopulateTable();

    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    for (Row row : rows) {
      Assertions.assertFalse(
          row.isNullAt(1), "Timestamp should not be null for version " + row.getLong(0));
    }
  }

  @Test
  public void testDescribeHistoryEmptyTable() {
    // CREATE TABLE only — no data inserted
    spark.sql(String.format("CREATE TABLE %s (id INT, name STRING) USING lance", fullTable));

    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    // Should have at least version 1 (the CREATE)
    Assertions.assertFalse(rows.isEmpty(), "Empty table should still have at least one version");

    // The version should have 0 total_rows
    Row row = rows.get(0);
    Assertions.assertEquals(0L, row.getLong(2), "Empty table should have 0 total_rows");
  }

  @Test
  public void testDescribeHistoryVersionsAreSequential() {
    createAndPopulateTable();

    List<Row> rows = spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).collectAsList();

    // Versions should be sequential with no gaps
    for (int i = 0; i < rows.size() - 1; i++) {
      long currentVersion = rows.get(i).getLong(0);
      long nextVersion = rows.get(i + 1).getLong(0);
      Assertions.assertEquals(
          1,
          currentVersion - nextVersion,
          String.format(
              "Versions should be sequential: %d and %d have a gap", currentVersion, nextVersion));
    }
  }

  // ==========================================
  // LanceTable.history() Scala/Java API tests
  // ==========================================

  @Test
  public void testLanceTableHistoryFromPath() {
    createAndPopulateTable();

    org.apache.spark.sql.Dataset<Row> history = LanceTable.forPath(spark, tableDir).history();

    Assertions.assertTrue(history.count() >= 3, "history() should return at least 3 versions");

    // Verify schema matches SQL output
    Assertions.assertEquals(
        spark.sql(String.format("DESCRIBE HISTORY %s", fullTable)).schema().toString(),
        history.schema().toString(),
        "LanceTable.history() schema should match DESCRIBE HISTORY schema");
  }

  @Test
  public void testLanceTableHistoryFromPathWithLimit() {
    createAndPopulateTable();

    org.apache.spark.sql.Dataset<Row> history = LanceTable.forPath(spark, tableDir).history(1);

    List<Row> rows = history.collectAsList();
    Assertions.assertEquals(1, rows.size(), "history(1) should return exactly 1 row");
  }

  @Test
  public void testLanceTableHistoryFromPathDescendingOrder() {
    createAndPopulateTable();

    List<Row> rows = LanceTable.forPath(spark, tableDir).history().collectAsList();

    for (int i = 0; i < rows.size() - 1; i++) {
      long currentVersion = rows.get(i).getLong(0);
      long nextVersion = rows.get(i + 1).getLong(0);
      Assertions.assertTrue(
          currentVersion > nextVersion, "history() should return versions in descending order");
    }
  }

  @Test
  public void testLanceTableHistoryFromName() {
    createAndPopulateTable();

    org.apache.spark.sql.Dataset<Row> history = LanceTable.forName(spark, fullTable).history();

    Assertions.assertTrue(
        history.count() >= 3, "forName().history() should return at least 3 versions");
  }

  @Test
  public void testLanceTableHistoryFromNameWithLimit() {
    createAndPopulateTable();

    org.apache.spark.sql.Dataset<Row> history = LanceTable.forName(spark, fullTable).history(2);

    List<Row> rows = history.collectAsList();
    Assertions.assertEquals(2, rows.size(), "forName().history(2) should return exactly 2 rows");
  }
}
