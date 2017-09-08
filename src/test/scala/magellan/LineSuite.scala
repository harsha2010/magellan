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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class LineSuite extends FunSuite with TestSparkContext {

  test("bounding box") {
    val line = Line(Point(0.0, 1.0), Point(1.0, 0.5))
    val BoundingBox(xmin, ymin, xmax, ymax) = line.boundingBox
    assert(xmin === 0.0)
    assert(ymin === 0.5)
    assert(xmax === 1.0)
    assert(ymax === 1.0)
  }

  test("touches") {
    val x = Line(Point(0.0, 0.0), Point(1.0, 1.0))
    val y = Line(Point(1.0, 1.0), Point(2.0, 2.0))
    val z = Line(Point(0.5, 0.5), Point(0.5, 0.0))
    val w = Line(Point(1.0, -1.0), Point(-1.0, -1.0))

    // test touches line
    assert(x.touches(y))
    assert(x.touches(z))
    assert(!x.touches(w))
  }

  test("intersects") {
    val x = Line(Point(0.0, 0.0), Point(1.0, 1.0))
    val y = Line(Point(1.0, 0.0), Point(0.0, 0.1))
    val z = Line(Point(0.5, 0.0), Point(1.0, 0.5))
    val w = Line(Point(1.0, -1.0), Point(-1.0, -1.0))
    val l = Line(Point(0.0, -1.0), Point(0.0, -1.0))
    val t = Line(Point(0.5, 0.5), Point(0.5, 0.0))
    val u = Line(Point(1.0, 1.0), Point(2.0, 2.0))
    val s = Line(Point(1.0, 1.0), Point(1.0, -1.0))
    assert(x.intersects(y))
    assert(!x.intersects(z))
    assert(w.intersects(l))
    assert(x.intersects(t))
    assert(u.intersects(s))
  }

  test("serialization") {
    val lineUDT = new LineUDT
    val line = Line(Point(0.0, 1.0), Point(1.0, 0.5))
    val BoundingBox(xmin, ymin, xmax, ymax) = line.boundingBox
    val row = lineUDT.serialize(line)
    assert(row.getInt(0) === line.getType())
    assert(row.getDouble(1) === xmin)
    assert(row.getDouble(2) === ymin)
    assert(row.getDouble(3) === xmax)
    assert(row.getDouble(4) === ymax)
    val serializedLine = lineUDT.deserialize(row)
    assert(line.equals(serializedLine))
  }

  test("contains line") {
    val x = Line(Point(0.0, 0.0), Point(1.0, 1.0))
    val y = Line(Point(0.0, 0.0), Point(0.5, 0.5))
    val z = Line(Point(0.0, 0.0), Point(1.5, 1.5))
    val w = Line(Point(0.5, 0.5), Point(0.0, 0.0))
    val u = Line(Point(0.0, 0.0), Point(0.0, 1.0))
    assert(x.contains(y))
    assert(!x.contains(z))
    assert(x.contains(w))
    assert(!x.contains(u))
  }

  test("contains point") {
    val y = Line(Point(0.5, 0.0), Point(0.0, 0.5))
    assert(y.contains(Point(0.5, 0.0)))
  }

  test("jackson serialization") {
    val s = new ObjectMapper().writeValueAsString(Line(Point(0.0, 0.0), Point(1.0, 1.0)))
    assert(s.contains("boundingBox"))
    assert(s.contains("start"))
    assert(s.contains("end"))
  }
}
