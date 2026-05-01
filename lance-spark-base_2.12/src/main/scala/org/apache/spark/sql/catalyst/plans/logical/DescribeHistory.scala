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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * DescribeHistory logical plan representing listing version history of a Lance dataset.
 *
 * Returns version metadata from the Lance manifest: version number, timestamp,
 * and table-level statistics (row count, file count, sizes) at each version.
 *
 * @param table The resolved table identifier
 * @param limit Optional maximum number of versions to return (most recent first)
 */
case class DescribeHistory(table: LogicalPlan, limit: Option[Int] = None) extends Command {

  override def children: Seq[LogicalPlan] = Seq(table)

  override def output: Seq[Attribute] = DescribeHistoryOutputType.SCHEMA

  override def simpleString(maxFields: Int): String = {
    s"DescribeHistoryLanceDataset${limit.map(l => s" LIMIT $l").getOrElse("")}"
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[LogicalPlan])
      : DescribeHistory = {
    copy(table = newChildren(0))
  }
}

object DescribeHistoryOutputType {
  val SCHEMA: Seq[Attribute] = StructType(
    Array(
      StructField("version", DataTypes.LongType, nullable = false),
      StructField("timestamp", DataTypes.TimestampType, nullable = false),
      StructField("total_rows", DataTypes.LongType, nullable = true),
      StructField("total_data_files", DataTypes.LongType, nullable = true),
      StructField("total_files_size", DataTypes.LongType, nullable = true),
      StructField("total_fragments", DataTypes.LongType, nullable = true),
      StructField("total_deletion_files", DataTypes.LongType, nullable = true)))
    .map(field => AttributeReference(field.name, field.dataType, field.nullable, field.metadata)())
}
