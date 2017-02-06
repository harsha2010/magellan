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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class PolygonSuite extends FunSuite {

  test("bounding box") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val BoundingBox(xmin, ymin, xmax, ymax) = polygon.boundingBox
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

  test("line in polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    assert(polygon.contains(line))
  }

  test("line intersects polygon") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    assert(polygon.intersects(Line(Point(0.0, 0.0), Point(1.5, 1.5))))
    assert(polygon.intersects(Line(Point(0.0, 0.0), Point(1.5, 0.0))))
    assert(polygon.intersects(Line(Point(0.0, 0.0), Point(0.0, 1.5))))
    assert(!polygon.intersects(Line(Point(-0.5, -0.5), Point(0.5, 0.5))))
    assert(!polygon.intersects(Line(Point(0.5, 0.5), Point(0.5, 0.5))))
    assert(polygon.intersects(Line(Point(0.0, -1.0), Point(1.0, -1.0))))
  }

  test("line in polygon: 2 rings") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )
    val line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    val polygon = Polygon(Array(0, 5), ring)
    assert(!polygon.contains(line))
  }

  test("serialization") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val BoundingBox(xmin, ymin, xmax, ymax) = polygon.boundingBox

    val polygonUDT = new PolygonUDT
    val row = polygonUDT.serialize(polygon)
    assert(row.getInt(0) === polygon.getType())
    assert(row.getDouble(1) === xmin)
    assert(row.getDouble(2) === ymin)
    assert(row.getDouble(3) === xmax)
    assert(row.getDouble(4) === ymax)
  }

  test("contains box") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val box1 = BoundingBox(0.1, 0.1, 0.5, 0.5)
    assert(polygon.contains(box1))
    val box2 = BoundingBox(0.1, 0.1, 1.5, 1.5)
    assert(!polygon.contains(box2))
  }

  test("intersects box") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val box1 = BoundingBox(0.0, 0.0, 1.5, 1.5)
    assert(!polygon.contains(box1))
    assert(polygon.intersects(box1))

    val box2 = BoundingBox(1.0, 1.0, 1.5, 1.5)
    assert(polygon.intersects(box2))
  }

  test("intersects box - more complex") {
    val ring = Array(Point(0.0, 0.0), Point(2.0, 0.0),
      Point(1.5, 1.0), Point(1.0, 0.5), Point(0.5, 1.0), Point(0.0, 0.0))
    val polygon = Polygon(Array(0), ring)
    val box1 = BoundingBox(0.5, 0.25, 1.5, 0.75)
    assert(!polygon.contains(box1))
    assert(polygon.intersects(box1))
  }
}
