/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan.catalyst

import org.apache.spark.sql.magellan.MagellanContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.magellan.{Line, Point, Polygon, TestSparkContext}
import org.scalatest.FunSuite

import scala.language.implicitConversions

case class PointExample(point: Point)
case class PolygonExample(polygon: Polygon)

class PredicateSuite extends FunSuite with TestSparkContext {

  test("within: Expression") {

    val points = sc.parallelize(Seq(
      PointExample(new Point(0.0, 0.0)),
      PointExample(new Point(2.0, 2.0))
    ))

    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
        PolygonExample(new Polygon(Array(0), ring))
      ))

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._

    val pdf = points.toDF().as("pdf")
    val sdf = polygons.toDF().as("sdf")
    assert(pdf.count() === 2)
    assert(sdf.count() === 1)
    println(pdf.select($"point").show())
    assert(pdf.join(sdf).where($"pdf.point" within  $"sdf.polygon").count() === 1)

  }

  test("within: Literal") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    ))

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")
    def assertWithin(df: DataFrame, start: Point, end: Point, count: Int): Unit = {
      assert(df.where(shape(new Line(start, end)) within $"polygon").count() == count)
    }
    assertWithin(pdf, new Point(0.0, 0.0), new Point(1.0, 1.0), 1)
    assertWithin(pdf, new Point(2.0, 0.0), new Point(3.0, 1.0), 0)
  }

  test("contains: Literal") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    ))

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")
    def assertWithin(df: DataFrame, start: Point, end: Point, count: Int): Unit = {
      assert(df.where($"polygon" >? shape(new Line(start, end))).count() == count)
    }
    assertWithin(pdf, new Point(0.0, 0.0), new Point(1.0, 1.0), 1)
    assertWithin(pdf, new Point(2.0, 0.0), new Point(3.0, 1.0), 0)
  }

  test("intersection: Literal") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    ))

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")

    val ring2 = Array(new Point(2.0, 2.0), new Point(2.0, 0.0),
      new Point(0.0, 0.0), new Point(0.0, 2.0), new Point(2.0, 2.0))
    val polygon2 = new Polygon(Array(0), ring2)

    // the intersection region must be the square (0,0), (1,0), (1, 1), (0, 1)
    val df = pdf.select(($"polygon" intersection shape(polygon2)).as("intersection"))
    assert(df.count() == 1)
    assert(df.where($"intersection" >? new Point(0.5, 0.5)).count() == 1)
    assert(df.where($"intersection" >? new Point(-0.5, 0.5)).count() == 0)
  }

  test("transform") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = polygons.toDF().as("pdf")
    val scale: Point => Point = (p: Point) => {new Point(p.x * 2, p.y * 2)}
    val scaledDf = df.withColumn("scale", $"polygon" transform scale)
    assert(scaledDf.where(point(1.5, 1.5) within $"scale").count() == 1)
  }

  test("join") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    ))

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._
    val df = polygons.toDF().as("pdf")
    println(df.join(df.as("qdf")).explain())
  }
}
