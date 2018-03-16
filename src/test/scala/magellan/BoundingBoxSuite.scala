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

class BoundingBoxSuite extends FunSuite {

  test("contains point") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    val y = BoundingBox(0.0, 0.0, 0.5, 0.5)
    assert(x contains Point(0.5, 0.5))
    assert(y contains Point(0.5, 0.5))
    assert(!(x contains Point(1.5, 0.5)))
  }

  test("intersects point") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    val y = BoundingBox(0.0, 0.0, 0.5, 0.5)
    assert(x intersects Point(0.0, 0.25))
  }

  test("contains bounding box") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    val y = BoundingBox(0.0, 0.0, 0.5, 0.5)
    assert(x.contains(y))
  }

  test("intersects bounding box") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    val y = BoundingBox(0.0, 0.0, 0.5, 0.5)
    assert(x.intersects(y))
    val z = BoundingBox(0.5, 0.5, 1.5, 1.5)
    assert(z.intersects(z))
    val w = BoundingBox(0.0, 1.1, 2.0, 2.0)
    assert(!x.intersects(w))

    val a = BoundingBox(0.0, 22.5, 45.0, 45.0)
    val b = BoundingBox(-122.4517249, 37.765315, -122.4173497, 37.7771202)
    assert(!a.intersects(b))
  }

  test("disjoint bounding box") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    val y = BoundingBox(1.0, 1.0, 2.0, 2.0)
    val z = BoundingBox(0.5, 1.0, 2.0, 2.0)
    val w = BoundingBox(0.5, 1.1, 2.0, 2.0)
    val u = BoundingBox(0.5, -1.0, 1.5, 0.0)
    val t = BoundingBox(0.5, -1.0, 1.5, -0.01)
    assert(!x.disjoint(y))
    assert(!x.disjoint(z))
    assert(x.disjoint(w))
    assert(!x.disjoint(u))
    assert(x.disjoint(t))
  }

  test("within circle") {
    val x = BoundingBox(0.0, 0.0, 1.0, 1.0)
    assert(x.withinCircle(Point(0.0, 0.0), 2.0))
    assert(!x.withinCircle(Point(0.5, 0.75), 0.5))
    assert(!x.withinCircle(Point(0.5, 0.5), 0.2))
  }

  test("Relate") {
    /**
     *  +---------+ 1,1
     *  + +----+  +
     *  + +    +  +
     *  + +----+  +
     *  +---------+
     *
     */
    val box = BoundingBox(0.0, 0.0, 0.5, 0.5)

    val outerPolygon = Polygon(
      Array(0),
      Array(Point(1.0, 1.0), Point(1.0, -1.0),
        Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0)))

    assert(box.relate(outerPolygon) === Relate.Within)

    /**
     *  +---------+ 1,1
     *  +         +
     *  +         +
     *  +         +
     *  +-----+---+
     *             +----+
     *             +    +
     *             +----+
     */

    val disjointPolygon = Polygon(
      Array(0),
      Array(Point(1.1, -1.0), Point(2.0, -1.0),
        Point(2.0, -2.0), Point(1.1, -2.0), Point(1.1, -1.0)))

    assert(box.relate(disjointPolygon) === Relate.Disjoint)

    /**
     *  +---------+ 1,1
     *  +         +
     *  +         +
     *  +         +
     *  +-----+---+----+
     *            +    +
     *            +----+
     */


    val touchesPolygon = Polygon(
      Array(0),
      Array(Point(1.0, -1.0), Point(2.0, -1.0),
        Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, -1.0)))

    /**
     *  +---------+ 1,1
     *  +         +
     *  +         +----+
     *  +         +    +
     *  +-----+---+    +
     *            +----+
     */

    val touchesPolygon2 = Polygon(
      Array(0),
      Array(Point(1.0, 0.0), Point(2.0, 0.0),
        Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, 0.0)))

    assert(box.relate(touchesPolygon2) == Relate.Disjoint)

    // the interiors of the boxes do not intersect
    assert(box.relate(touchesPolygon) === Relate.Disjoint)

    /**
     *  +---------+ 1,1
     *  +   0,0   +     2,0
     *  +     +---+----+
     *  +     +   +    +
     *  +-----+---+    +
     *        +--------+
     */

    val intersectsPolygon = Polygon(
      Array(0),
      Array(Point(0.0, 0.0), Point(2.0, 0.0),
      Point(2.0, -2.0), Point(0.0, -2.0), Point(0.0, 0.0)))

    assert(box.relate(intersectsPolygon) == Relate.Intersects)

  }
}
