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
package org.lance.spark;

import org.lance.ManifestSummary;
import org.lance.ReadOptions;
import org.lance.Version;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Programmatic API for Lance table operations.
 *
 * <p>Provides path-based and catalog-based access to Lance table metadata, analogous to Delta
 * Lake's {@code DeltaTable} class.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * // From path
 * DataFrame history = LanceTable.forPath(spark, "/path/to/table.lance").history();
 * DataFrame recent = LanceTable.forPath(spark, "/path/to/table.lance").history(10);
 *
 * // From catalog
 * DataFrame history = LanceTable.forName(spark, "lance_catalog.db.my_table").history();
 * }</pre>
 */
public class LanceTable {

  private final SparkSession spark;
  private final String tablePathOrName;
  private final boolean isPath;

  private LanceTable(SparkSession spark, String tablePathOrName, boolean isPath) {
    this.spark = spark;
    this.tablePathOrName = tablePathOrName;
    this.isPath = isPath;
  }

  /**
   * Creates a LanceTable instance from a file system path.
   *
   * @param spark Active SparkSession
   * @param path Path to the Lance dataset (e.g., "s3://bucket/path/to/table.lance")
   * @return A LanceTable instance for the given path
   */
  public static LanceTable forPath(SparkSession spark, String path) {
    return new LanceTable(spark, path, true);
  }

  /**
   * Creates a LanceTable instance from a catalog-qualified table name.
   *
   * @param spark Active SparkSession
   * @param tableName Fully qualified table name (e.g., "lance_catalog.db.my_table")
   * @return A LanceTable instance for the given table name
   */
  public static LanceTable forName(SparkSession spark, String tableName) {
    return new LanceTable(spark, tableName, false);
  }

  /**
   * Returns the full version history of the Lance dataset as a DataFrame. Results are ordered by
   * version descending (most recent first).
   *
   * @return DataFrame with columns: version, timestamp, total_rows, total_data_files,
   *     total_files_size, total_fragments, total_deletion_files
   */
  public org.apache.spark.sql.Dataset<Row> history() {
    return history(Integer.MAX_VALUE);
  }

  /**
   * Returns the most recent {@code limit} versions of the Lance dataset as a DataFrame. Results are
   * ordered by version descending (most recent first).
   *
   * @param limit Maximum number of versions to return
   * @return DataFrame with columns: version, timestamp, total_rows, total_data_files,
   *     total_files_size, total_fragments, total_deletion_files
   */
  public org.apache.spark.sql.Dataset<Row> history(int limit) {
    if (isPath) {
      return historyFromPath(limit);
    } else {
      return historyFromCatalog(limit);
    }
  }

  private org.apache.spark.sql.Dataset<Row> historyFromPath(int limit) {
    ReadOptions readOptions = new ReadOptions.Builder().setSession(LanceRuntime.session()).build();

    org.lance.Dataset dataset =
        org.lance.Dataset.open()
            .allocator(LanceRuntime.allocator())
            .uri(tablePathOrName)
            .readOptions(readOptions)
            .build();

    try {
      return buildHistoryDataFrame(dataset, limit);
    } finally {
      dataset.close();
    }
  }

  private org.apache.spark.sql.Dataset<Row> historyFromCatalog(int limit) {
    // For catalog-based access, use DESCRIBE HISTORY SQL
    String sql = "DESCRIBE HISTORY " + tablePathOrName;
    if (limit < Integer.MAX_VALUE) {
      sql += " LIMIT " + limit;
    }
    return spark.sql(sql);
  }

  private org.apache.spark.sql.Dataset<Row> buildHistoryDataFrame(
      org.lance.Dataset dataset, int limit) {
    // Copy to mutable list — listVersions() may return an immutable list
    List<Version> versions = new ArrayList<>(dataset.listVersions());

    // Sort descending by version ID (most recent first)
    versions.sort(Comparator.comparingLong(Version::getId).reversed());

    // Apply limit
    int actualLimit = Math.min(limit, versions.size());
    List<Row> rows = new ArrayList<>(actualLimit);

    for (int i = 0; i < actualLimit; i++) {
      Version version = versions.get(i);
      ManifestSummary summary = version.getManifestSummary();

      rows.add(
          RowFactory.create(
              version.getId(),
              Timestamp.from(version.getDataTime().toInstant()),
              summary.getTotalRows(),
              summary.getTotalDataFiles(),
              summary.getTotalFilesSize(),
              summary.getTotalFragments(),
              summary.getTotalDeletionFiles()));
    }

    return spark.createDataFrame(rows, HISTORY_SCHEMA);
  }

  /** Schema for the history DataFrame. */
  static final StructType HISTORY_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("version", DataTypes.LongType, false),
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
            DataTypes.createStructField("total_rows", DataTypes.LongType, true),
            DataTypes.createStructField("total_data_files", DataTypes.LongType, true),
            DataTypes.createStructField("total_files_size", DataTypes.LongType, true),
            DataTypes.createStructField("total_fragments", DataTypes.LongType, true),
            DataTypes.createStructField("total_deletion_files", DataTypes.LongType, true),
          });
}
