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

package magellan.coord

import magellan.TestingUtils
import magellan.Point
import org.scalatest.FunSuite
import TestingUtils._


class NAD83Suite extends FunSuite {

  test("to") {
    def assertUptoTol(coords: Tuple2[Double, Double],
      zone: Int,
      expected: Tuple2[Double, Double]): Unit = {

      val originalPoint = new Point(coords._1, coords._2)
      val nad83 = new NAD83(Map("zone" -> zone))
      val to = nad83.to()
      val toPoint = originalPoint.transform(to)
      assert(toPoint.x ~== expected._1 absTol 0.01)
      assert(toPoint.y ~== expected._2 absTol 0.01)

      val expectedPoint = new Point(expected._1, expected._2)
      val from = nad83.from()
      val fromPoint = expectedPoint.transform(from)

      assert(fromPoint.x ~== coords._1 absTol 1000.0)
      assert(fromPoint.y ~== coords._2 absTol 1000.0)
    }

    // San Francisco
    assertUptoTol((1834072.94, 625933.96), 403, (-122.38, 37.62))

    // Santa Clara
    assertUptoTol((1871077.8, 595798.5), 403, (-121.955, 37.35))

    // Santa Cruz
    assertUptoTol((1863695.19, 553738.99), 403, (-122.03, 36.97))

    val feet2Meters = 0.3048006096012192
    assertUptoTol((6351504 * feet2Meters, 2153727 * feet2Meters),
      403, (-121.2284594353128, 37.9075149470656))

  }
}
