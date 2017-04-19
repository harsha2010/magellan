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

import magellan.TestSparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.magellan.dsl.expressions._
import magellan.{Line, Point, Polygon}
import org.scalatest.FunSuite

import scala.language.implicitConversions

case class LineExample(line: Line)
case class PointExample(point: Point)
case class PolygonExample(polygon: Polygon)

class PredicateSuite extends FunSuite with TestSparkContext {

  test("within: Expression") {

    val points = sc.parallelize(Seq(
      PointExample( Point(0.0, 0.0)),
      PointExample( Point(2.0, 2.0))
    ))

    val ring = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = points.toDF().as("pdf")
    val sdf = polygons.toDF().as("sdf")
    assert(pdf.count() === 2)
    assert(sdf.count() === 1)
    println(pdf.select($"point").show())
    assert(pdf.join(sdf).where($"pdf.point" within  $"sdf.polygon").count() === 1)

  }

  test("Polygon contains points and Line") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
                       Point(-1.0, -1.0),  Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring1)

    assert(polygon.contains(Point(0.0, 0.0)))
    assert(polygon.contains(Point(1.0, 1.0)))
    assert(polygon.contains(Line(Point(0.0, 0.0), Point(0.9, 0.9))))
    assert(polygon.contains(Line(Point(0.0, 0.0), Point(1.0, 1.0))))
    assert(polygon.contains(Line(Point(0.0, 0.0), Point(-1.0, 0.5))))
    assert(!polygon.contains(Line(Point(0.0, 0.0), Point(1.2, 1.1))))
    assert(!polygon.contains(Line(Point(0.0, 0.0), Point(-1.2, -1.1))))
    assert(!polygon.contains(Line(Point(-1.3, -2), Point(-1.2, -1.1))))
  }

  test("within: Literal") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
    Point(-1.0, -1.0),  Point(-1.0, 1.0),
    Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
    PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")
    def assertWithin(df: DataFrame, start: Point, end: Point, count: Int): Unit = {
      assert(df.where(shape( Line(start, end)) within $"polygon").count() === count)
    }
    assertWithin(pdf,  Point(0.0, 0.0),  Point(1.0, 1.0), 1)
    assertWithin(pdf,  Point(2.0, 0.0),  Point(3.0, 1.0), 0)
  }

  test("contains: Literal") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")
    def assertWithin(df: DataFrame, start: Point, end: Point, count: Int): Unit = {
      assert(df.where($"polygon" >? shape( Line(start, end))).count() === count)
    }
    assertWithin(pdf,  Point(0.0, 0.0),  Point(1.0, 1.0), 1)
    assertWithin(pdf,  Point(2.0, 0.0),  Point(3.0, 1.0), 0)
  }

  test("intersection: Literal") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")

    val ring2 = Array( Point(2.0, 2.0),  Point(2.0, 0.0),
       Point(0.0, 0.0),  Point(0.0, 2.0),  Point(2.0, 2.0))
    val polygon2 =  Polygon(Array(0), ring2)

    // the intersection region must be the square (0,0), (1,0), (1, 1), (0, 1)
    val df = pdf.select(($"polygon" intersects shape(polygon2)).as("intersection"))
    assert(df.count() == 1)
    assert(df.where($"intersection" >?  Point(0.5, 0.5)).count() === 1)
    assert(df.where($"intersection" >?  Point(-0.5, 0.5)).count() === 0)
  }

  test("transform") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = polygons.toDF().as("pdf")
    //val scale: Point => Point = (p: Point) => { Point(p.x * 2, p.y * 2)}
    //val scaledDf = df.withColumn("scale", $"polygon" transform scale)
    //assert(scaledDf.where(point(1.5, 1.5) within $"scale").count() === 1)
  }

  test("point conversion") {
    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = polygons.toDF().as("pdf")
    val df = sc.parallelize(Seq((0.5, 0.5))).toDF("x", "y")
    assert(df.join(pdf).where(point($"x", $"y") within $"polygon").count() === 1)
  }

  test("line queries") {
    val lines = sc.parallelize(Seq(
      LineExample( Line( Point(-1.0, -1.0),  Point(1.0, 1.0)))
    ))

    val ring1 = Array( Point(1.0, 1.0),  Point(1.0, -1.0),
       Point(-1.0, -1.0),  Point(-1.0, 1.0),
       Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample( Polygon(Array(0), ring1))
    ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ldf = lines.toDF()
    val pdf = polygons.toDF()
    assert(ldf.join(pdf).where($"line" intersects $"polygon").count() === 1)
    assert(ldf.join(pdf).where($"line" within $"polygon").count() === 1)
    assert(ldf.where($"line" intersects  Point(0.0, 0.0)).count() === 1)
    assert(ldf.where($"line" intersects  Point(-2.0, 0.0)).count() === 0)
  }

}
