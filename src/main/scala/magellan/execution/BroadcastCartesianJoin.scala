/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

case class BroadcastCartesianJoin(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    condition: Option[Expression]) extends BinaryNode {

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  @transient private lazy val boundCondition =
    newPredicate(condition.getOrElse(Literal(true)), left.output ++ right.output)

  override protected def doExecute(): RDD[InternalRow] = {
    val bc = broadcast.execute()
    val broadcastedRelation = sparkContext.broadcast(bc.map(_.copy()).collect().toIndexedSeq)

    streamed.execute().mapPartitions { streamedIter =>
      val v = broadcastedRelation.value
      val matchedRows = new CompactBuffer[InternalRow]
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0

        while (i < v.size) {
          // TODO: One bitset per partition instead of per row.
          val broadcastedRow = v(i)
          buildSide match {
            case BuildRight if boundCondition(joinedRow(streamedRow, broadcastedRow)) =>
              matchedRows += joinedRow(streamedRow, broadcastedRow).copy()
            case BuildLeft if boundCondition(joinedRow(broadcastedRow, streamedRow)) =>
              matchedRows += joinedRow(broadcastedRow, streamedRow).copy()
            case _ =>
          }
          i += 1
        }

      }
      matchedRows.iterator
    }

  }

  override def output: Seq[Attribute] = left.output ++ right.output

}
