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
import org.apache.spark.sql.Row
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
      Batch("spatial join", FixedPoint(100), new SpatialJoin(spark)) :: Nil
  }

  override def beforeAll() {
    super.beforeAll()
    Utils.injectRules(spark)
  }

  test("spatial join in plan: within") {

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

    val joined = points.join(polygons index 5).where($"point" within $"polygon")

    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)

  }

  test("spatial join in plan: more complex within condition") {
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

    val joined = points.join(polygons index 5).
      where($"point" within $"polygon").
      where($"name" === "a")

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

    val joined = polygons.join(points index 5).where($"point" within $"polygon")

    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)

    assert(joined.queryExecution.analyzed.toString().contains("SpatialJoinHint"))
    assert(!optimizedPlan.toString().contains("SpatialJoinHint"))
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

    val joined = polygons.join(points index 5).where($"point" within $"polygon")

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

    val joined = points.join(broadcast(polygons index 5)).where($"point" within $"polygon")
    assert(joined.queryExecution.executedPlan.toString() contains "BroadcastExchange")

  }

  test("Spatial Join rewrite does not introduce additional output columns") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val path = this.getClass.getClassLoader.getResource("geojson/multipolygon/countries.geojson").getPath
    val countries = sqlContext.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
      .select($"polygon",
        $"metadata"("name").as("country"))
      .cache()

    val cities = sc.parallelize(Seq(("San Francisco", Point(-122.5076401, 37.7576793)))).toDF("city", "point")

    val results = cities.join(countries index 5).where($"point" within $"polygon").collect()(0)

    //polygon, name, point, city
    assert(results.length === 4)
  }

  test("Optimize Left Outer Join") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))

    val ring2 = Array(Point(1.1, -1.0), Point(2.0, -1.0),
      Point(2.0, -2.0), Point(1.1, -2.0), Point(1.1, -1.0))

    val polygons = sc.parallelize(Seq(
      ("1", Polygon(Array(0), ring1)),
      ("2", Polygon(Array(0), ring2))
    )).toDF("id", "polygon")

    val points = sc.parallelize(Seq(
      ("a", 1, Point(0.0, 0.0)),
      ("b", 2, Point(1.5, -1.5)),
      ("c" , 3, Point(2.0, 2.0))
    )).toDF("name", "value", "point")

    val joined = points.join(polygons index 5,
      points("point") within polygons("polygon"), "left_outer")
    val optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 3)
    val unmatched = joined.filter($"polygon" isNull).
      select("name").
      map { case Row(s: String) => s}.collect()

    assert(unmatched === Array("c"))
  }

  test("Complex Join Condition") {
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

    var joined = points.join(polygons index 5,
      points("point") within polygons("polygon")).filter(points("name") === "a")

    var optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)

    joined = points.join(polygons index 5).
      where(points("name") === "a" && (points("point") within polygons("polygon")))

    optimizedPlan = Optimize.execute(joined.queryExecution.analyzed)
    assert(optimizedPlan.toString().contains("Generate inline(indexer"))
    assert(joined.count() === 1)
  }
}
