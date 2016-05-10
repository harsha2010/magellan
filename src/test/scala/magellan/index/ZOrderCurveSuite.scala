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

package magellan.index

import magellan.{BoundingBox, Point}
import magellan.TestingUtils._
import org.scalatest.FunSuite

class ZOrderCurveSuite extends FunSuite {

  test("GeoHash globe") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.4517249, 37.765315), 5)
    assert(index.bits === (9L << 59))
    val child = index.children().filter(_.code() === "0100110")
    assert(!child.isEmpty)
    val c = child.head
    assert(c.bits === 5476377146882523136L)
  }

  test("GeoHash") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.3959313, 37.7912976), 25)
    assert(index.toBase32() === "9q8yy")
    assert(index.code() === "0100110110010001111011110")
    val BoundingBox(xmin, ymin, xmax, ymax) = index.boundingBox
    assert(xmin ~== -122.41 absTol 0.5)
    assert(xmax ~== -122.41 absTol 0.5)
    assert(ymin ~== 37.771 absTol 0.5)
    assert(ymax ~== 37.771 absTol 0.5)

    val anotherIndex = indexer.index(Point(116.6009234, 40.0798573), 30)
    assert(anotherIndex.toBase32() === "wx4uj2")
  }

  test("GeoHash children") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.3959313, 37.7912976), 23)
    val children = index.children()
    assert(children.filter(_.toBase32() === "9q8yy").nonEmpty)
  }
}

