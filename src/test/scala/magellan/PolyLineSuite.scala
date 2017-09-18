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
import org.scalatest.FunSuite

class PolyLineSuite extends FunSuite {

  test("bounding box") {
    val line = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0))
    val polyline = Polygon(Array(0), line)
    val BoundingBox(xmin, ymin, xmax, ymax) = polyline.boundingBox
    assert(xmin === -1.0)
    assert(ymin === -1.0)
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

    candidates foreach { case (cond, line) => assert(cond === polyline.intersects(line))}
  }
}
