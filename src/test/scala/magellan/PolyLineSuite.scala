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

class PolyLineSuite extends FunSuite {

  test("intersects") {
    val polyline = new PolyLine(Array(0), Array(
        new Point(0.0, 0.0), new Point(0.0, 1.0), new Point(1.0, 1.0)
      ))

    assert(polyline.intersects(new Line(new Point(-1.0, -1.0), new Point(2.0, 2.0))))
    assert(polyline.intersects(new Line(new Point(-1.1, -1), new Point(1.9, 2.0))))
  }

  test("transform") {
    val polyline1 = new PolyLine(Array(0), Array(
      new Point(0.0, 0.0), new Point(0.0, 1.0), new Point(1.0, 1.0)
    ))
    val polyline2 = polyline1.transform((p: Point) => new Point(3 * p.x, 3 * p.y))
    assert(polyline2.points(0).equals(new Point(0.0, 0.0)))
    assert(polyline2.points(2).equals(new Point(3.0, 3.0)))

  }

  test("difference") {
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0))
    val polygon = new Polygon(Array(0), ring)

    val polyline = new PolyLine(Array(0), Array(
      new Point(2.0, 2.0), new Point(0.0, 0.0), new Point(-2.0, -2.0)
    ))

    val diff = polyline.difference(polygon)
    assert(diff.isInstanceOf[PolyLine])
    val p = diff.asInstanceOf[PolyLine]
    assert(p.indices.diff(Array[Int](0)).isEmpty)
    assert(p.points.size === 4)
    assert(p.points.contains(new Point(-2.0, -2.0)))
    assert(p.points.contains(new Point(2.0, 2.0)))
    assert(!p.points.contains(new Point(0.0, 0.0)))
  }
}
