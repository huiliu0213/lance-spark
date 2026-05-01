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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.DescribeHistoryOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

import scala.collection.JavaConverters._

/**
 * Physical execution of DESCRIBE HISTORY for Lance datasets.
 *
 * Opens the dataset via lance-core and calls Dataset.listVersions() to retrieve
 * version history from the manifest. This is an O(1) metadata read — no data
 * files are scanned.
 *
 * Results are returned in descending order (most recent version first) to match
 * Delta Lake's DESCRIBE HISTORY behavior and make LIMIT 1 return the latest version.
 */
case class DescribeHistoryExec(
    catalog: TableCatalog,
    ident: Identifier,
    limit: Option[Int]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = DescribeHistoryOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = catalog.loadTable(ident) match {
      case ds: LanceDataset => ds
      case _ =>
        throw new UnsupportedOperationException("DESCRIBE HISTORY only supports LanceDataset")
    }

    val readOptions = lanceDataset.readOptions()

    val dataset = Utils.openDatasetBuilder(readOptions)
      .initialStorageOptions(lanceDataset.getInitialStorageOptions)
      .build()
    try {
      val versions = dataset.listVersions().asScala.toSeq

      // Sort descending by version ID (most recent first), then apply LIMIT
      val sorted = versions.sortBy(_.getId)(Ordering[Long].reverse)
      val limited = limit.map(n => sorted.take(n)).getOrElse(sorted)

      limited.map { version =>
        val summary = version.getManifestSummary
        // Convert ZonedDateTime to microseconds since epoch for Spark's TimestampType
        val timestampMicros = version.getDataTime.toInstant.getEpochSecond * 1000000L +
          version.getDataTime.toInstant.getNano / 1000L

        new GenericInternalRow(Array[Any](
          version.getId,
          timestampMicros,
          summary.getTotalRows,
          summary.getTotalDataFiles,
          summary.getTotalFilesSize,
          summary.getTotalFragments,
          summary.getTotalDeletionFiles))
      }
    } finally {
      dataset.close()
    }
  }
}
