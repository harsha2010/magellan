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

class LineSuite extends FunSuite with TestSparkContext {

  test("bounding box") {
    val line = Line(Point(0.0, 1.0), Point(1.0, 0.5))
    val BoundingBox(xmin, ymin, xmax, ymax) = line.boundingBox
    assert(xmin === 0.0)
    assert(ymin === 0.5)
    assert(xmax === 1.0)
    assert(ymax === 1.0)
  }

  test("intersects") {
    val x = Line(Point(0.0, 0.0), Point(1.0, 1.0))
    val y = Line(Point(1.0, 0.0), Point(0.0, 0.1))
    val z = Line(Point(0.5, 0.0), Point(1.0, 0.5))
    assert(x.intersects(y))
    assert(!x.intersects(z))
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
}
