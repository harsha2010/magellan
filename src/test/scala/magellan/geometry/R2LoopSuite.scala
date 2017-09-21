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
package magellan.geometry

import magellan.Relate.{Contains, Disjoint, Touches}
import magellan.TestingUtils._
import magellan.{Line, Point}
import org.scalatest.FunSuite

class R2LoopSuite extends FunSuite {

  test("Loop contains Point") {
    val r2Loop = makeLoop("1.0:1.0,1.0:-1.0,-1.0:-1.0,-1.0:1.0,1.0:1.0")
    assert(r2Loop.contains(Point(0.0, 0.0)))
    assert(r2Loop.contains(Point(0.5, 0.5)))
    assert(!r2Loop.contains(Point(1.0, 0.0)))
  }

  test("Loop containsOrCrosses Point") {
    val r2Loop = makeLoop("1.0:1.0,1.0:-1.0,-1.0:-1.0,-1.0:1.0,1.0:1.0")
    assert(r2Loop.containsOrCrosses(Point(1.0, 0.0)) === Touches)
    assert(r2Loop.containsOrCrosses(Point(0.0, 0.0)) === Contains)
    assert(r2Loop.containsOrCrosses(Point(1.0, 1.0)) === Touches)
    assert(r2Loop.containsOrCrosses(Point(2.0, 0.0)) === Disjoint)
  }

  test("Loop intersects line") {
    val r2Loop = makeLoop("1.0:1.0,1.0:-1.0,-1.0:-1.0,-1.0:1.0,1.0:1.0")
    assert(!r2Loop.intersects(Line(Point(0.0, 0.0), Point(0.5, 0.5))))
    assert(r2Loop.intersects(Line(Point(-2.0, 0.0), Point(2.0, 0.0))))
  }

  test("Loop intersects Loop") {
    val loop1 = makeLoop("1.0:1.0,1.0:-1.0,-1.0:-1.0,-1.0:1.0,1.0:1.0")
    val loop2 = makeLoop("0.0:0.0,2.0:0.0,2.0:-2.0,0.0:-2.0,0.0:0.0")
    assert(loop1 intersects loop2)
  }

  test("Ray intersects line") {

    /**
      * Given
      *     + + + + + + + +
      *           .
      *           .
      *           P
      *
      * ray from P should  intersect line
      */

    assert(R2Loop.intersects(Point(0.0, -2.0), makeLine("-1.0:-1.0,1.0:-1.0")))

    /**
      * Given
      *     +
      *     +
      *     +  .
      *     +  .
      *     +  P
      *
      * ray from P should not intersect line (parallel)
      */
    assert(!R2Loop.intersects(Point(0.0, 0.0), makeLine("-1.0:-1.0,-1.0:1.0")))

    /**
      * Given
      *     +
      *     +
      *     +
      *     +
      *     P
      *
      * ray from P should not intersect line (collinear)
      */

    assert(!R2Loop.intersects(Point(-1.0, 0.0), makeLine("-1.0:-1.0,-1.0:1.0")))

    /**
      * Given
      *     + + + +
      *   + .
      * +   .
      *     .
      *     P
      *
      * Ray from P should intersect polyline exactly once
      */

    assert(!R2Loop.intersects(Point(2.0, 0.0), makeLine("0.0:0.0,2.0:2.0")))
    assert(R2Loop.intersects(Point(2.0, 0.0), makeLine("2.0:2.0,4.0:2.0")))

    /**
      * Given
      *     + + + +
      *   + .
      * + + + + + +
      *     .
      *     P
      *
      * Ray from P should intersect the polyline exactly twice
      */

    assert(R2Loop.intersects(Point(2.0, 0.0), makeLine("1.0:1.0,4.0:1.0")))

  }
}
