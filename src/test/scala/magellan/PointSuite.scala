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

class PointSuite extends FunSuite with TestSparkContext {

  test("bounding box") {
    val point = Point(1.0, 1.0)
    val BoundingBox(xmin, ymin, xmax, ymax) = point.boundingBox
    assert(xmin === 1.0)
    assert(ymin === 1.0)
    assert(xmax === 1.0)
    assert(ymax === 1.0)
  }

  test("serialization") {
    val point = Point(1.0, 1.0)
    val pointUDT = new PointUDT
    val BoundingBox(xmin, ymin, xmax, ymax) = point.boundingBox
    val row = pointUDT.serialize(point)
    assert(row.getInt(0) === point.getType())
    assert(row.getDouble(1) === xmin)
    assert(row.getDouble(2) === ymin)
    assert(row.getDouble(3) === xmax)
    assert(row.getDouble(4) === ymax)
    val serializedPoint = pointUDT.deserialize(row)
    assert(point.equals(serializedPoint))
  }

  test("point udf") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._
    val points = sc.parallelize(Seq((-1.0, -1.0), (-1.0, 1.0), (1.0, -1.0))).toDF("x", "y")
    import org.apache.spark.sql.functions.udf
    val toPointUDF = udf{(x:Double,y:Double) => Point(x,y) }
    val point = points.withColumn("point", toPointUDF('x, 'y))
      .select('point)
      .first()(0)
      .asInstanceOf[Point]

    assert(point.getX() === -1.0)
    assert(point.getY() === -1.0)
  }

  test("jackson serialization") {
    val s = new ObjectMapper().writeValueAsString(Point(1.0, 1.0))
    assert(s.contains("boundingBox"))
    assert(s.contains("x"))
    assert(s.contains("y"))
  }

  test("within circle") {
    assert(Point(0.0, 0.0) withinCircle (Point(0.5, 0.5), 0.75))
    assert(!(Point(0.0, 0.0) withinCircle (Point(0.5, 0.5), 0.5)))
  }

  test("buffer point") {
    val polygon = Point(0.0, 1.0).buffer(0.5)
    assert(polygon.getNumRings() === 1)
    // check that [0.0, 0.75] is within this polygon
    assert(polygon.contains(Point(0.0, 0.75)))
    // check that [0.4, 1.0] is within this polygon
    assert(polygon.contains(Point(0.4, 1.0)))
    // check that [0.6, 1.0] is outside this polygon
    assert(!polygon.contains(Point(0.6, 1.0)))
  }
}
