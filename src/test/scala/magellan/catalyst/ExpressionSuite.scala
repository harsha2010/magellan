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
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

/*case class PointExample(point: Point)
case class PolygonExample(polygon: Polygon)*/

class ExpressionSuite extends FunSuite with TestSparkContext {

  test("Point Converter") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val df = sc.parallelize(Seq((35.7, -122.3))).toDF("lat", "lon")
    val p = df.withColumn("point", point($"lon", $"lat"))
      .select('point)
      .map { case Row(p: Point) => p}
      .first()

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
  }

  test("PolyLine contains Point") {

    val polyline = PolyLine(new Array[Int](3), Array(
      Point(0.0, 0.0), Point(3.0, 3.0), Point(-2.0, -2.0)
    ))
    assert(polyline.contains(Point(1.0, 1.0)) === true)
    assert(polyline.contains(Point(2.0, 1.0)) === false)
  }
}
