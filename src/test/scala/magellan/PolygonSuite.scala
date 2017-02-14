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

import com.esri.core.geometry.{GeometryEngine, Point => ESRIPoint, Polygon => ESRIPolygon, Polyline => ESRIPolyLine}
import com.fasterxml.jackson.databind.ObjectMapper
import magellan.TestingUtils._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

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

  test("line in polygon: no holes") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    val polygon = Polygon(Array(0), ring)
    var line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    assert(polygon.contains(line))
    line = Line(Point(-0.5, -1.0), Point(-0.5, 0.0))
    assert(polygon.contains(line))
  }

  test("line in polygon: 1 hole") {
    val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(0.5, 0), Point(0, 0.5), Point(-0.5, 0),
      Point(0, -0.5), Point(0.5, 0)
    )
    var line = Line(Point(-0.5, -0.5), Point(0.5, 0.5))
    val polygon = Polygon(Array(0, 5), ring)

    val esriPolygon = toESRI(polygon)

    line = Line(Point(-0.5, 0.0), Point(0.5, 0.0))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.25, -0.25), Point(0.25, 0.25))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.90, 0.0), Point(-0.55, 0.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.90, 0.75), Point(0.95, 0.75))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.90, -0.75), Point(0.95, -0.75))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.75, -1.0), Point(-0.75, 1.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-1.0, -1.0), Point(-1.0, 1.0))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(0.75, -1.0), Point(0.75, 1.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(1.0, -1.0), Point(1.0, 1.0))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.501, -1.0), Point(-0.501, 1.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.5, -1.0), Point(-0.5, 1.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.49, -1.0), Point(-0.49, 1.0))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.9990, 0.0), Point(-0.55, 0.0))
    assert(polygon.contains(line))
    assert(GeometryEngine.contains(esriPolygon, toESRI(line), null))

    line = Line(Point(-0.5, 0.0), Point(0.5, 0.0))
    assert(!polygon.contains(line))
    assert(!GeometryEngine.contains(esriPolygon, toESRI(line), null))

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
    assert(polygon.indices === Array(0, 6, 12))
    assert(polygon.xcoordinates(6) === -200.0)
    assert(polygon.ycoordinates(6) === -100.0)
    assert(polygon.xcoordinates(13) === -100.0)
    assert(polygon.ycoordinates(13) === 50.0)

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

    val deserializedPolygon: Polygon = new ObjectMapper().readerFor(classOf[Polygon]).readValue(s)
    assert(deserializedPolygon.xcoordinates.deep === polygon.xcoordinates.deep)
    assert(deserializedPolygon.ycoordinates.deep === polygon.ycoordinates.deep)
    assert(deserializedPolygon.boundingBox === polygon.boundingBox)
  }

  def fromESRI(esriPolygon: ESRIPolygon): Polygon = {
    val length = esriPolygon.getPointCount
    if (length == 0) {
      Polygon(Array[Int](), Array[Point]())
    } else {
      val indices = ArrayBuffer[Int]()
      indices.+=(0)
      val points = ArrayBuffer[Point]()
      var start = esriPolygon.getPoint(0)
      var currentRingIndex = 0
      points.+=(Point(start.getX(), start.getY()))

      for (i <- (1 until length)) {
        val p = esriPolygon.getPoint(i)
        val j = esriPolygon.getPathEnd(currentRingIndex)
        if (j < length) {
          val end = esriPolygon.getPoint(j)
          if (p.getX == end.getX && p.getY == end.getY) {
            indices.+=(i)
            currentRingIndex += 1
            // add start point
            points.+= (Point(start.getX(), start.getY()))
            start = end
          }
        }
        points.+=(Point(p.getX(), p.getY()))
      }
      Polygon(indices.toArray, points.toArray)
    }
  }

  def toESRI(polygon: Polygon): ESRIPolygon = {
    val p = new ESRIPolygon()
    val indices = polygon.indices
    val length = polygon.xcoordinates.length
    if (length > 0) {
      var startIndex = 0
      var endIndex = 1
      var currentRingIndex = 0
      p.startPath(
        polygon.xcoordinates(startIndex),
        polygon.ycoordinates(startIndex))

      while (endIndex < length) {
        p.lineTo(polygon.xcoordinates(endIndex),
          polygon.ycoordinates(endIndex))
        startIndex += 1
        endIndex += 1
        // if we reach a ring boundary skip it
        val nextRingIndex = currentRingIndex + 1
        if (nextRingIndex < indices.length) {
          val nextRing = indices(nextRingIndex)
          if (endIndex == nextRing) {
            startIndex += 1
            endIndex += 1
            currentRingIndex = nextRingIndex
            p.startPath(
              polygon.xcoordinates(startIndex),
              polygon.ycoordinates(startIndex))
          }
        }
      }
    }
    p
  }

  def toESRI(line: Line): ESRIPolyLine = {
    val l = new ESRIPolyLine()
    l.startPath(line.getStart().getX(), line.getStart().getY())
    l.lineTo(line.getEnd().getX(), line.getEnd().getY())
    l
  }
}
