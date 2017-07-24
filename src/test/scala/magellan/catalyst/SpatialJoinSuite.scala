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

package magellan.catalyst

import magellan.{Point, Polygon, TestSparkContext, Utils}
import org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughJoin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class SpatialJoinSuite extends FunSuite with TestSparkContext {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("pushdown filter through join", Once, PushPredicateThroughJoin) ::
      Batch("spatial join", FixedPoint(100), new SpatialJoin(spark, Map())) :: Nil
  }

  override def beforeAll() {
    super.beforeAll()
    Utils.injectRules(spark, Map())
  }

  test("spatial join in plan") {

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      ("1", Polygon(Array(0), ring))
    )).toDF("id", "polygon")

    val points = sc.parallelize(Seq(
      ("a", 1, Point(0.0, 0.0)),
      ("b" , 2, Point(2.0, 2.0))
    )).toDF("name", "value", "point")

    val joined = points.join(polygons).where($"point" within $"polygon")

    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)

  }

  test("use existing indices in spatial join") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      ("1", Polygon(Array(0), ring))
    )).toDF("id", "polygon").withColumn("index", $"polygon" index 30)

    val points = sc.parallelize(Seq(
      ("a", 1, Point(0.0, 0.0)),
      ("b" , 2, Point(2.0, 2.0))
    )).toDF("name", "value", "point")

    val joined = polygons.join(points).where($"point" within $"polygon")

    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)

    assert(optimizedPlan.toString().contains("Generate inline(index#"))
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)
  }

  test("order of expressions in filter does not matter") {

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      ("1", Polygon(Array(0), ring))
    )).toDF("id", "polygon")

    val points = sc.parallelize(Seq(
      ("a", 1, Point(0.0, 0.0)),
      ("b" , 2, Point(2.0, 2.0))
    )).toDF("name", "value", "point")

    val joined = polygons.join(points).where($"point" within $"polygon")

    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)

  }

  test("Broadcast Join preserved under spatial join") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      ("1", Polygon(Array(0), ring))
    )).toDF("id", "polygon")

    val points = sc.parallelize(Seq(
      ("a", 1, Point(0.0, 0.0)),
      ("b" , 2, Point(2.0, 2.0))
    )).toDF("name", "value", "point")

    val joined = points.join(broadcast(polygons)).where($"point" within $"polygon")
    assert(joined.queryExecution.toString() contains "BroadcastExchange")

  }
}
