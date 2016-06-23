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
    val polyline = PolyLine(Array(0), Array(
      Point(0.0, 0.0), Point(0.0, 1.0), Point(1.0, 1.0)
    ))

    assert(polyline.intersects(Line( Point(-1.0, -1.0),  Point(2.0, 2.0))))
    assert(polyline.intersects( Line( Point(-1.1, -1),  Point(1.9, 2.0))))
  }

  /*test("transform") {
    val polyline1 =  PolyLine(Array(0), Array(
       Point(0.0, 0.0),  Point(0.0, 1.0),  Point(1.0, 1.0)
    ))
    val polyline2 = polyline1.transform((p: Point) =>  Point(3 * p.x, 3 * p.y))
    assert(polyline2.points(0).equals( Point(0.0, 0.0)))
    assert(polyline2.points(2).equals( Point(3.0, 3.0)))

  }*/

}
