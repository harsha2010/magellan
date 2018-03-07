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

package magellan

import magellan.TestingUtils._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ShapefileSuite extends FunSuite with TestSparkContext with BeforeAndAfterAll {

  override def beforeAll() {
    super.beforeAll()
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.split.maxsize", "10000")
  }

  test("shapefile-relation: points") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpoint/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    assert(df.count() === 1)
    val point = df.select($"point").first().get(0).asInstanceOf[Point]
    assert(point.getX() ~== -99.796 absTol 0.2)

    // query
    assert(df.filter($"point" within $"point").count() === 1)
  }

  test("shapefile-relation: polygons") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpolygon/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    assert(df.count() === 1)
    val polygon = df.select($"polygon").first().get(0).asInstanceOf[Polygon]
    assert(polygon.getNumRings() === 1)
  }

  test("shapefile-relation: Zillow Neighborhoods") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testzillow/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    assert(df.count() === 1932)  // 34 + 948 + 689 + 261

    // CA should have some metadata attached to it

    assert(df.filter($"metadata"("STATE") === "CA").count() === 948)

    assert(df.select($"metadata"("STATE").as("state")).filter($"state" === "CA").count() === 948)
    assert(df.select($"metadata"("STATE").as("state")).filter($"state" isNull).count() === 723)
  }

  test("shapefile-relation: polylines") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpolyline/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    assert(df.count() === 14959)
    // 5979762.107174277,2085850.5510566086,6024890.0635061115,2130875.5735391825
  }

  test("shapefile-relation: points and polygons") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testcomposite/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    assert(df.count() === 2)
    // each row should either contain a point or a polygon but not both
    import sqlCtx.implicits._
    assert(df.filter($"point" isNull).count() === 1)
    assert(df.filter($"polygon" isNull).count() === 1)
  }

  test("shapefile-relation: valid") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpolyline/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    assert(df.filter($"valid").count() == 14959)
  }

  test("shapefile-relation: parsing points") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testshapefile/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    val polygon = df.select("polygon").first().get(0).asInstanceOf[Polygon]
    assert(polygon.boundingBox == BoundingBox(-121.457213, 41.183484, -119.998287, 41.997613))
  }

  test("shapefile-relation: index") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testshapefile/").getPath
    val df = sqlCtx.read.
      format("magellan").
      option("magellan.index", "true").
      option("magellan.index.precision", "15").
      load(path)
    import org.apache.spark.sql.functions.explode
    import sqlCtx.implicits._
    assert(df.select(explode($"index")).count() === 2)
    assert(df.select(explode($"index").as("index")).groupBy($"index.relation").count().count() === 1)
  }

  test("shapefile-relation: use shx file to split") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("shapefiles/us_states/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    assert(df.count() === 56)
  }
}
