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

package org.apache.spark.sql.magellan

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.{Inner, logical}
import org.apache.spark.sql.execution.{Filter, SparkPlan, joins}
import org.apache.spark.sql.{SQLContext, Strategy}

trait MagellanStrategies {

  self: SQLContext#SparkPlanner =>

  @Experimental
  object BroadcastCartesianJoin extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Join(left, right, Inner, condition) =>
        val buildSide =
          if (right.statistics.sizeInBytes < left.statistics.sizeInBytes) {
            joins.BuildRight
          } else {
            joins.BuildLeft
          }
        val broadcastCartesianJoin = joins.BroadcastCartesianJoin(
          planLater(left), planLater(right), buildSide, condition)
        condition.map(Filter(_, broadcastCartesianJoin)).getOrElse(broadcastCartesianJoin) :: Nil

      case _ => Nil
    }
  }
}
