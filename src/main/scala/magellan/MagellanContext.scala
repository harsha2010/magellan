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

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.{SQLConf, SQLContext, Strategy}

class MagellanContext(sc: SparkContext) extends SQLContext(sc) {
  self =>

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  protected[sql] override lazy val conf: SQLConf = new SQLConf()

  @transient
  private val magellanPlanner = new SparkPlanner with MagellanStrategies {
    override val strategies: Seq[Strategy] =
      experimental.extraStrategies ++ (
        DataSourceStrategy ::
        DDLStrategy ::
        TakeOrderedAndProject ::
        HashAggregation ::
        Aggregation ::
        LeftSemiJoin ::
        EquiJoinSelection ::
        InMemoryScans ::
        BasicOperators ::
        BroadcastCartesianJoin ::
        CartesianProduct ::
        BroadcastNestedLoopJoin :: Nil)
  }

  @transient
  override protected[sql] val planner = magellanPlanner
}
