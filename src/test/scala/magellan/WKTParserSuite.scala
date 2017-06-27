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

class WKTParserSuite extends FunSuite {

  test("parse point") {
    val point =
      WKTParser.parse(
        WKTParser.point,
        "POINT (30 10)")
    assert(point.successful)
    val p: Point = point.get
    assert(p.getX() === 30.0)
    assert(p.getY() === 10.0)
  }

  test("parse linestring") {
    var linestring =
      WKTParser.parse(
        WKTParser.linestring,
        "LINESTRING (30 10, 10 30, 40 40)")

    assert(linestring.successful)
    val p: PolyLine = linestring.get
    assert(p.indices.length === 1)
    assert(p.xcoordinates.length === 3)

    linestring =
      WKTParser.parse(
        WKTParser.linestring,
        "LINESTRING (-79.470579 35.442827,-79.469465 35.444889,-79.468907 35.445829,-79.468294 35.446608,-79.46687 35.447893)")

    assert(linestring.successful)

  }

  test("parse polygon without holes") {
    val polygon =
      WKTParser.parse(
        WKTParser.polygonWithoutHoles,
        "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))")

    assert(polygon.successful)
    val p: Polygon = polygon.get
    assert(p.xcoordinates.length === 5)
  }

  test("parse polygon with holes") {
    val polygon =
      WKTParser.parse(
        WKTParser.polygonWithHoles,
        "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
      )

    assert(polygon.successful)
    val p: Polygon = polygon.get
    assert(p.indices.length == 2)
    assert(p.indices(1) == 5)
    assert(p.xcoordinates(4) === 35.0)
    assert(p.xcoordinates(5) === 20.0)
  }

  test("parse") {
    val shape = WKTParser.parseAll(WKTParser.expr, "LINESTRING (30 10, 10 30, 40 40)")
    assert(shape.successful)
    val s = shape.get
    assert(s.isInstanceOf[PolyLine])
  }

}
