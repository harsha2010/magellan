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

import magellan._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

case class PointExample(point: Point)
case class PolygonExample(polygon: Polygon)

class ExpressionSuite extends FunSuite with TestSparkContext {

  test("Point Converter") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val df = sc.parallelize(Seq((35.7, -122.3))).toDF("lat", "lon")
    val p = df.withColumn("point", point($"lon", $"lat"))
      .select('point)
      .first()(0).asInstanceOf[Point]

    assert(p.getX() === -122.3)
    assert(p.getY() === 35.7)
  }

  test("Within") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    val points = sc.parallelize(Seq(
      PointExample(Point(0.0, 0.0)),
      PointExample(Point(2.0, 2.0))
    )).toDF()

    val joined = points.join(polygons).where($"point" within $"polygon")
    assert(joined.count() === 1)

  }

  test("Intersects") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    val points = sc.parallelize(Seq(
      PointExample(Point(0.0, -1.0)),
      PointExample(Point(2.0, 2.0))
    )).toDF()

    val joined = points.join(polygons).where($"point" intersects $"polygon")
    assert(joined.count() === 1)

  }


  test("Contains") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    val points = sc.parallelize(Seq(
      PointExample(Point(0.0, 0.0)),
      PointExample(Point(2.0, 2.0))
    )).toDF()

    val joined = points.join(polygons).where($"polygon" >? $"point")
    assert(joined.count() === 1)

  }

  test("Polygon intersects Line") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    val lines = sc.parallelize(Seq(
      (1, Line(Point(0.0, 0.0), Point(0.0, 5.0))), // proper intersection, yes
      (2, Line(Point(0.0, 0.0), Point(1.0, 0.0))), // contained within and touches boundary, yes
      (3, Line(Point(1.0, 1.0), Point(1.0, 0.0))), // lies on boundary, yes
      (4, Line(Point(1.0, 1.0), Point(2.0, 2.0))), // touches, yes
      (5, Line(Point(0.0, 0.0), Point(0.5, 0.5))), //  contained entirely within, yes
      (6, Line(Point(2.0, 2.0), Point(3.0, 3.0)))  // outside, no
    )).toDF("id", "line")

    val joined = lines.join(polygons).where($"polygon" intersects  $"line")
    assert(joined.select($"id").map { case Row(s: Int) => s }.collect().sorted === Array(1, 2, 3, 4, 5))
  }

  test("PolyLine intersects Line") {

    val line = Line(Point(0,0), Point(2,2))

    val polyline1 = PolyLine(new Array[Int](3), Array(
      Point(0.0, 0.0), Point(2.0, 2.0), Point(-2.0, -2.0)
    ))

    val polyline2 = PolyLine(new Array[Int](3), Array(
      Point(0.0, 3.0), Point(3.0, 1.0), Point(-2.0, -2.0)
    ))

    val polyline3 = PolyLine(new Array[Int](3), Array(
              Point(3.0, 3.0), Point(3.0, 11.0), Point(5.0, 0.0)
            ))

    assert(polyline1.intersects(line) === true)
    assert(polyline2.intersects(line) === true)
    assert(polyline3.intersects(line) === false)

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val lines = sc.parallelize(Seq(("1", line))).toDF("id", "line")

    val polylines = sc.parallelize(Seq(
        (true, polyline1),
        (true, polyline2),
        (false, polyline3))).toDF("cond", "polyline")

    val joined = polylines.join(lines, $"polyline" intersects  $"line", "leftOuter").
      select("cond", "line").
      collect().
      map {
        case Row(cond: Boolean, line: Line) =>
          (cond, Some(line))

        case Row(cond: Boolean, null) =>
          (cond, None)
      }

    joined foreach { case (cond, line) => assert(cond || line.isEmpty)}
  }

  test("PolyLine intersects Polygon") {

    val polyline = PolyLine(Array(0), Array(Point(0.0, 0.0), Point(1.0, 1.0)))
    val polygon1 = Polygon(Array(0), Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0)))
    val polygon2 = Polygon(Array(0), Array(Point(3.0, 3.0), Point(3.0, 2.0),
      Point(2.0, 2.0), Point(2.0, 3.0), Point(3.0, 3.0)))
    val polygon3 = Polygon(Array(0), Array(Point(3.0, 3.0), Point(3.0, -3.0),
      Point(-3.0, -3.0), Point(-3.0, 3.0), Point(3.0, 3.0)))

    assert(polygon1.intersects(polyline) === true)
    assert(polygon2.intersects(polyline) === false)
    assert(polygon3.intersects(polyline) === true)

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val polylines = sc.parallelize(Seq(("1", polyline))).toDF("id", "polyline")

    val polygons = sc.parallelize(Seq(
      (true, polygon1),
      (false, polygon2),
      (true, polygon3))).toDF("cond", "polygon")

    val joined = polygons.join(polylines, $"polygon" intersects  $"polyline", "leftOuter").
      select("cond", "polyline").
      collect().
      map {
        case Row(cond: Boolean, polyline: PolyLine) =>
          (cond, Some(polyline))

        case Row(cond: Boolean, null) =>
          (cond, None)
      }

    joined foreach { case (cond, polyline) => assert(cond || polyline.isEmpty)}
  }


  test("PolyLine contains Point") {

    val polyline = PolyLine(new Array[Int](3), Array(
      Point(0.0, 0.0), Point(3.0, 3.0), Point(-2.0, -2.0)
    ))

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val polylines = sc.parallelize(Seq(("1", polyline))).toDF("id", "polyline")

    val points = sc.parallelize(Seq(
        (true, Point(1.0, 1.0)),
        (false, Point(2.0, 1.0)))).toDF("cond", "point")

    val joined = points.join(polylines, $"point" within $"polyline", "leftOuter").
      select("cond", "polyline").
      collect().
      map {
        case Row(cond: Boolean, polyline: PolyLine) =>
          (cond, Some(polyline))

        case Row(cond: Boolean, null) =>
          (cond, None)
      }

    joined foreach { case (cond, polyline) => assert(cond || polyline.isEmpty)}
  }

  test("Point within Range") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val points = sc.parallelize(Seq(
      PointExample(Point(0.0, 0.0)),
      PointExample(Point(2.0, 2.0))
    )).toDF()

    val boundingBox = BoundingBox(0.0, 0.0, 1.0, 1.0)
    assert(points.where($"point" withinRange boundingBox).count() === 1)

  }

  test("Point within Circle Range") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val points = sc.parallelize(Seq(
      PointExample(Point(0.0, 0.0)),
      PointExample(Point(2.0, 2.0))
    )).toDF()

    assert(points.where($"point" withinRange (Point(0.0, 0.0), 1.0)).count() === 1)

  }

  test("Polygon within Range") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    val boundingBox = BoundingBox(-1.0, -1.0, 1.0, 1.0)
    assert(polygons.where($"polygon" withinRange boundingBox).count() === 1)

  }

  test("Polygon within Circle Range") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
      PolygonExample(Polygon(Array(0), ring))
    )).toDF()

    assert(polygons.where($"polygon" withinRange (Point(0.0, 0.0), 1.42)).count() === 1)

  }

  test("eval: point in range") {
    val expr = WithinRange(MockPointExpr(Point(0.0, 0.0)), BoundingBox(0.0, 0.0, 1.0, 1.0))
    assert(expr.eval(null) === true)
  }

  test("eval: point in circle range") {
    val expr = WithinCircleRange(MockPointExpr(Point(0.0, 0.0)), Point(0.5, 0.5), 0.5)
    assert(expr.eval(null) === false)
  }

  test("eval: point within polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))

    val polygon = Polygon(Array(0), ring)
    var point = Point(0.0, 0.0)
    var expr = Within(MockPointExpr(point), MockPolygonExpr(polygon))
    assert(expr.eval(null) === true)

    point = Point(1.5, 1.5)
    expr = Within(MockPointExpr(point), MockPolygonExpr(polygon))
    assert(expr.eval(null) === false)
  }

  test("eval: point intersects polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0),
      Point(1.0, 1.0))

    val polygon = Polygon(Array(0), ring)
    var point = Point(0.0, 0.0)
    var expr = Intersects(MockPointExpr(point), MockPolygonExpr(polygon))
    assert(expr.eval(null) === false)

    point = Point(1.0, 1.0)
    expr = Intersects(MockPointExpr(point), MockPolygonExpr(polygon))
    assert(expr.eval(null) === true)
  }

  test("Polygon intersects Polygon") {
    /**
      *  +---------+ 1,1
      *  +   0,0   +     2,0
      *  +     +---+----+
      *  +     +   +    +
      *  +-----+---+    +
      *        +--------+
      */

    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon1 = Polygon(Array(0), ring1)

    val ring2 = Array(Point(0.0, 0.0), Point(2.0, 0.0),
      Point(2.0, -2.0), Point(0.0, -2.0), Point(0.0, 0.0))
    val polygon2 = Polygon(Array(0), ring2)

    val ring3 = Array(Point(1.0, 0.0), Point(2.0, 0.0),
      Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, 0.0))
    val polygon3 = Polygon(Array(0), ring3)

    val ring4 = Array(Point(1.0, -1.0), Point(2.0, -1.0),
      Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, -1.0))
    val polygon4 = Polygon(Array(0), ring4)

    val ring5 = Array(Point(1.1, -1.0), Point(2.0, -1.0),
      Point(2.0, -2.0), Point(1.1, -2.0), Point(1.1, -1.0))
    val polygon5 = Polygon(Array(0), ring5)


    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val x = sc.parallelize(Seq(
      PolygonExample(polygon1)
    )).toDF()

    val y = sc.parallelize(Seq(polygon2, polygon3, polygon4, polygon5)).zipWithIndex().toDF("polygon", "index")

    val results = x.join(y).where(x("polygon") intersects y("polygon")).
      select($"index").
      map {case Row (s: Long) => s}.
      collect().
      sorted

    assert(results === Array(0, 1, 2))
  }
}
