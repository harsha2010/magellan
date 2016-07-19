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

import org.scalatest.FunSuite

class PolygonSuite extends FunSuite {

  test("bounding box") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val ((xmin, ymin), (xmax, ymax)) = polygon.boundingBox
    assert(xmin === -1.0)
    assert(ymin === -1.0)
    assert(xmax === 1.0)
    assert(ymax === 1.0)
  }

  test("point in polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    assert(!polygon.contains(Point(2.0, 0.0)))
    assert(polygon.contains(Point(0.0, 0.0)))
  }

  test("point in polygon: 2 rings") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )
    val polygon = Polygon(Array(0, 5), ring)
    assert(!polygon.contains(Point(2.0, 0.0)))
    assert(!polygon.contains(Point(0.0, 0.0)))
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


  test("line in polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    assert(polygon.contains(line))
  }

  test("line in polygon: 2 rings") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )
    var line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    val polygon = Polygon(Array(0, 5), ring)
    assert(polygon.intersects(line))
    assert(!polygon.contains(line))

    line = Line(Point(-0.5, 0.0), Point(0.5, 0.0))
    assert(!polygon.contains(line))

    line = Line(Point(-0.25, -0.25), Point(0.25, 0.25))
    assert(!polygon.contains(line))

    line = Line(Point(-0.90, 0.0), Point(-0.55, 0.0))
    assert(polygon.contains(line))

    line = Line(Point(-0.90, 0.75), Point(0.95, 0.75))
    assert(polygon.contains(line))

    line = Line(Point(-0.90, -0.75), Point(0.95, -0.75))
    assert(polygon.contains(line))

    line = Line(Point(-0.75, -1.0), Point(-0.75, 1.0))
    assert(polygon.contains(line))

    line = Line(Point(-1.0, -1.0), Point(-1.0, 1.0))
    assert(polygon.contains(line))

    line = Line(Point(0.75, -1.0), Point(0.75, 1.0))
    assert(polygon.contains(line))

    line = Line(Point(1.0, -1.0), Point(1.0, 1.0))
    assert(polygon.contains(line))

    line = Line(Point(-0.501, -1.0), Point(-0.501, 1.0))
    assert(polygon.contains(line))

    //TODO: this test case still needs to be addressed
    /*line = Line(Point(-0.5, -1.0), Point(-0.5, 1.0))
    assert(polygon.contains(line))*/

    line = Line(Point(-0.49, -1.0), Point(-0.49, 1.0))
    assert(!polygon.contains(line))

    line = Line(Point(-0.9990, 0.0), Point(-0.55, 0.0))
    assert(polygon.contains(line))

    line = Line(Point(-0.5, 0.0), Point(0.5, 0.0))
    assert(!polygon.contains(line))


  }

  test("serialization") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val ((xmin, ymin), (xmax, ymax)) = polygon.boundingBox

    val polygonUDT = new PolygonUDT
    val row = polygonUDT.serialize(polygon)
    assert(row.getInt(0) === polygon.getType())
    assert(row.getDouble(1) === xmin)
    assert(row.getDouble(2) === ymin)
    assert(row.getDouble(3) === xmax)
    assert(row.getDouble(4) === ymax)
  }
}
