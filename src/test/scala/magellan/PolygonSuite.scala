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

import com.esri.core.geometry.{GeometryEngine, Point => ESRIPoint, Polygon => ESRIPolygon}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import magellan.TestingUtils._
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

  test("loops in polyon: no holes") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    val loops = polygon.loops
    assert(loops.size === 1)
    val loop = loops(0)
    assert(loop.iterator().size === 4)
    val iter = loop.iterator()
    val first = iter.next()
    assert(first === Line(Point(1.0, 1.0), Point(1.0, -1.0)))
    (0 until 2) foreach { _ => iter.next() }
    val last = iter.next()
    assert(last === Line(Point(-1.0, 1.0), Point(1.0, 1.0)))
  }

  test("point in polygon: no holes") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    assert(!polygon.contains(Point(2.0, 0.0)))
    assert(polygon.contains(Point(0.0, 0.0)))

    // contains is a strict check. agrees with definition in ESRI

    val esriPolygon = toESRI(polygon)

    assert(!polygon.contains(Point(1.0, 1.0)))
    assert(!GeometryEngine.contains(esriPolygon, new ESRIPoint(1.0, 1.0), null))

    assert(!polygon.contains(Point(1.0, 0.0)))
    assert(!GeometryEngine.contains(esriPolygon, new ESRIPoint(1.0, 0.0), null))

  }

  test("point in polygon: one hole") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )
    val polygon = Polygon(Array(0, 5), ring)
    assert(!polygon.contains(Point(2.0, 0.0)))
    assert(!polygon.contains(Point(0.0, 0.0)))

    // contains is a strict check. agrees with definition in ESRI

    val esriPolygon = toESRI(polygon)

    assert(!polygon.contains(Point(1.0, 1.0)))
    assert(!GeometryEngine.contains(esriPolygon, new ESRIPoint(1.0, 1.0), null))

    assert(!GeometryEngine.contains(esriPolygon, new ESRIPoint(0.5, 0.0), null))
    assert(!polygon.contains(Point(0.5, 0.0)))

  }

  test("fromESRI") {
    val esriPolygon = new ESRIPolygon()
    // outer ring1
    esriPolygon.startPath(-200, -100)
    esriPolygon.lineTo(200, -100)
    esriPolygon.lineTo(200, 100)
    esriPolygon.lineTo(-190, 100)
    esriPolygon.lineTo(-190, 90)
    esriPolygon.lineTo(-200, 90)

    // hole
    esriPolygon.startPath(-100, 50)
    esriPolygon.lineTo(100, 50)
    esriPolygon.lineTo(100, -40)
    esriPolygon.lineTo(90, -40)
    esriPolygon.lineTo(90, -50)
    esriPolygon.lineTo(-100, -50)

    // island
    esriPolygon.startPath(-10, -10)
    esriPolygon.lineTo(10, -10)
    esriPolygon.lineTo(10, 10)
    esriPolygon.lineTo(-10, 10)

    esriPolygon.reverseAllPaths()

    val polygon = fromESRI(esriPolygon)
    assert(polygon.getRings() === Array(0, 6, 12))
    assert(polygon.getVertex(6) === Point(-200.0, -100.0))
    assert(polygon.getVertex(13) === Point(-100.0, 50.0))
  }

  test("toESRI") {

    // no hole
    var ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    var polygon = Polygon(Array(0), ring)
    var esriPolygon = toESRI(polygon)
    assert(esriPolygon.calculateRingArea2D(0) ~== 4.0 absTol 0.001)
    assert(esriPolygon.getPathCount === 1)
    assert(esriPolygon.getPoint(0).getX === 1.0)
    assert(esriPolygon.getPoint(0).getY === 1.0)
    assert(esriPolygon.getPoint(1).getX === 1.0)
    assert(esriPolygon.getPoint(1).getY === -1.0)
    assert(esriPolygon.getPoint(3).getX === -1.0)
    assert(esriPolygon.getPoint(3).getY === 1.0)

    val esriPoint = new ESRIPoint()

    esriPoint.setXY(0.0, 0.0)
    assert(GeometryEngine.contains(esriPolygon, esriPoint, null))

    esriPoint.setXY(1.0, 1.0)
    // strict contains does not allow for points to lie on the boundary
    assert(!GeometryEngine.contains(esriPolygon, esriPoint, null))

    // one hole

    ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )

    polygon = Polygon(Array(0, 5), ring)
    esriPolygon = toESRI(polygon)

    assert(esriPolygon.calculateRingArea2D(0) ~== 4.0 absTol 0.001)
    assert(esriPolygon.calculateRingArea2D(1) ~== -0.5 absTol 0.001)
    assert(esriPolygon.calculateArea2D() ~== 3.5 absTol 0.001)

    esriPoint.setXY(0.0, 0.0)
    assert(!GeometryEngine.contains(esriPolygon, esriPoint, null))

    esriPoint.setXY(0.75, 0.75)
    assert(GeometryEngine.contains(esriPolygon, esriPoint, null))
  }

  test("jackson serialization") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )

    val polygon = Polygon(Array(0, 5), ring)
    val s = new ObjectMapper().writeValueAsString(polygon)
    assert(s.contains("boundingBox"))
    assert(s.contains("xcoordinates"))
    assert(s.contains("ycoordinates"))

    // read back into Polygon to test deserialization
    val mapper = new ObjectMapper()
    val module = new SimpleModule()
    module.addDeserializer(classOf[Polygon], new PolygonDeserializer())
    mapper.registerModule(module)
    val deserializedPolygon: Polygon = mapper.readerFor(classOf[Polygon]).readValue(s)
    assert(deserializedPolygon === polygon)
    assert(deserializedPolygon.boundingBox === polygon.boundingBox)
  }

  test("point touches polygon: no holes") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    assert(polygon.touches(Point(1.0, 0.0)))
    assert(polygon.touches(Point(1.0, 1.0)))
  }

  test("point touches polygon: holes") {
    val  ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )

    val polygon = Polygon(Array(0, 5), ring)

    assert(polygon.touches(Point(1.0, 0.0)))
    assert(polygon.touches(Point(0.5, 0.0)))
  }

  test("polygon intersects line: no holes") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    var line = Line(Point(-2.0, 0.0), Point(2.0, 0.0))
    assert(polygon.intersects(line))

    line = Line(Point(1.0, 1.0), Point(1.0, -2.0))
    assert(polygon.intersects(line))
  }

  test("polygon intersects line: holes") {
    val  ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )

    val polygon = Polygon(Array(0, 5), ring)
    var line = Line(Point(-2.0, 0.0), Point(2.0, 0.0))
    assert(polygon.intersects(line))

    line = Line(Point(0.0, 0.0), Point(0.5, 0.0))
    assert(polygon.intersects(line))
  }

  test("polygon intersects polygon") {

    /**
      *  +---------+ 1,1
      *  +   0,0   +     2,0
      *  +     +---+----+
      *  +     +   +    +
      *  +-----+---+    +
      *        +--------+
      */

    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon1 = Polygon(Array(0), ring1)

    val ring2 = Array(Point(0.0, 0.0), Point(2.0, 0.0),
      Point(2.0, -2.0), Point(0.0, -2.0), Point(0.0, 0.0))
    val polygon2 = Polygon(Array(0), ring2)

    assert(polygon1.intersects(polygon2))

    /**
      *  +---------+ 1,1
      *  +         +
      *  +         +----+
      *  +         +    +
      *  +-----+---+    +
      *            +----+
      */

    val ring3 = Array(Point(1.0, 0.0), Point(2.0, 0.0),
      Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, 0.0))
    val polygon3 = Polygon(Array(0), ring3)

    assert(polygon1.intersects(polygon3))

    /**
      *  +---------+ 1,1
      *  +         +
      *  +         +
      *  +         +
      *  +-----+---+----+
      *            +    +
      *            +----+
      */

    val ring4 = Array(Point(1.0, -1.0), Point(2.0, -1.0),
      Point(2.0, -2.0), Point(1.0, -2.0), Point(1.0, -1.0))
    val polygon4 = Polygon(Array(0), ring4)

    assert(polygon1.intersects(polygon4))

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

    val ring5 = Array(Point(1.1, -1.0), Point(2.0, -1.0),
      Point(2.0, -2.0), Point(1.1, -2.0), Point(1.1, -1.0))
    val polygon5 = Polygon(Array(0), ring5)

    assert(!polygon1.intersects(polygon5))

  }
}
