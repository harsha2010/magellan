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
import org.apache.spark.sql.types.PolyLineUDT
import org.scalatest.FunSuite

class PolyLineSuite extends FunSuite with TestSparkContext {

  test("bounding box") {
    val polyline = PolyLine(Array(0),Array(Point(0.0, 1.0), Point(1.0, 0.5)))
    val BoundingBox(xmin, ymin, xmax, ymax) = polyline.boundingBox
    assert(xmin === 0.0)
    assert(ymin === 0.5)
    assert(xmax === 1.0)
    assert(ymax === 1.0)
  }

  test("point touches polyline") {
    val line = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0))
    val polyline = PolyLine(Array(0), line)
    assert(polyline.contains(Point(1.0, 1.0)))
    assert(polyline.contains(Point(1.0, 0.0)))
  }

  test("line intersects polyline") {
    val line = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0))
    val polyline = PolyLine(Array(0), line)

    val candidates = Seq(
      (true, "0:0,2:0"),
      (true, "1:1,2:2"),
      (false, "1.1:1.1,2:2"),
      (false, "0:0,0:1")
    ) map {
      case (cond, str) =>
        (cond, makeLine(str))
    }

    candidates foreach { case (cond, line) => assert(cond === polyline.intersects(line)) }
  }

  test("polyline intersects line") {
    val x = PolyLine(Array(0),Array(Point(0.0, 0.0), Point(1.0, 1.0)))
    val y = Line(Point(1.0, 0.0), Point(0.0, 0.1))
    val z = Line(Point(0.5, 0.0), Point(1.0, 0.5))
    val w = Line(Point(1.0, -1.0), Point(-1.0, -1.0))
    val l = Line(Point(0.0, -1.0), Point(0.0, -1.0))
    val t = Line(Point(0.5, 0.5), Point(0.5, 0.0))
    val u = Line(Point(1.0, 1.0), Point(2.0, 2.0))
    val s = Line(Point(1.0, 1.0), Point(1.0, -1.0))
    assert(x.intersects(y))
    assert(!x.intersects(z))
    assert(w.intersects(l))
    assert(x.intersects(t))
    assert(u.intersects(s))
  }

  test("polyline intersects polygon") {
    val x = PolyLine(Array(0),Array(Point(0.0, 0.0), Point(1.0, 1.0)))
    val w = Polygon(Array(0), Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0)))
    val l = Polygon(Array(0), Array(Point(3.0, 3.0), Point(3.0, 2.0),
      Point(2.0, 2.0), Point(2.0, 3.0), Point(3.0, 3.0)))
    val t = Polygon(Array(0), Array(Point(3.0, 3.0), Point(3.0, -3.0),
      Point(-3.0, -3.0), Point(-3.0, 3.0), Point(3.0, 3.0)))
    assert(x.intersects(w))
    assert(!x.intersects(l))
    assert(x.intersects(t))
  }

  test("serialization") {
    val polylineUDT = new PolyLineUDT
    val polyline = PolyLine(Array(0), Array(Point(0.0, 1.0), Point(1.0, 0.5)))
    val BoundingBox(xmin, ymin, xmax, ymax) = polyline.boundingBox
    val row = polylineUDT.serialize(polyline)
    assert(row.getInt(0) === polyline.getType())
    assert(row.getDouble(1) === xmin)
    assert(row.getDouble(2) === ymin)
    assert(row.getDouble(3) === xmax)
    assert(row.getDouble(4) === ymax)
    val serializedLine = polylineUDT.deserialize(row)
    assert(polyline.equals(serializedLine))
  }

  test("polyline contains line") {
    val x = Line(Point(0.0, 0.0), Point(1.0, 1.0))
    val y = Line(Point(0.0, 0.0), Point(0.5, 0.5))
    val z = Line(Point(0.0, 0.0), Point(1.5, 1.5))
    val w = Line(Point(0.5, 0.5), Point(0.0, 0.0))
    val u = Line(Point(0.0, 0.0), Point(0.0, 1.0))
    assert(x.contains(y))
    assert(!x.contains(z))
    assert(x.contains(w))
    assert(!x.contains(u))
  }

  test("polyline contains point") {
    val y = PolyLine(Array(0), Array(Point(0.5, 0.0), Point(0.0, 0.5)))
    assert(y.contains(Point(0.5, 0.0)))
  }

  test("buffer polyline") {
    /**
     *    +-------+ 1.5,1.5
     *    +----+  +
     *         +  +
     *    +----+  +
     *    +-------+
     *
     */

    val ring = Array(Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(1.0, -1.0), Point(-1.0, -1.0))
    val polyline = PolyLine(Array(0), ring)

    val bufferedPolygon = polyline.buffer(0.5)
    assert(bufferedPolygon.getNumRings() === 1)
    assert(bufferedPolygon.contains(Point(1.3, 1.3)))
    assert(!bufferedPolygon.contains(Point(0.5, 0.5)))
  }
}
