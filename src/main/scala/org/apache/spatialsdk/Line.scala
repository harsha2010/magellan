/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spatialsdk

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * Line segment between two points.
 *
 * @param start
 * @param end
 */
@SQLUserDefinedType(udt = classOf[LineUDT])
class Line(val start: Point, val end: Point) extends Serializable with Shape {

  def intersects(other: Line): Boolean = {
    // test for degeneracy
    if (start == other.start ||
        end == other.end ||
        start == other.end ||
        end == other.start) {
      true
    } else {
      def ccw(a: Point, b: Point, c: Point) = {
        (c.y - a.y) * (b.x - a.x) > (b.y - a.y) * (c.x - a.x)
      }
      ccw(start, other.start, other.end) != ccw(end, other.start, other.end) &&
        ccw(start, end, other.start) != ccw(start, end, other.end)
    }
  }

  override val shapeType: Int = 2

  /**
   *
   * @param point
   * @return true if this shape envelops the given point
   */
  override def contains(point: Point): Boolean = ???
}

class LineUDT extends UserDefinedType[Line] {

  private val pointDataType = new PointUDT().sqlType

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("start", pointDataType, nullable = true),
      StructField("end", pointDataType, nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(7)
    val line = obj.asInstanceOf[Line]
    row(0) = line.shapeType
    row(1) = line.start
    row(2) = line.end
    row
  }

  override def userClass: Class[Line] = classOf[Line]

  override def deserialize(datum: Any): Line = {
    datum match {
      case x: Line => x
      case r: Row => {
        val start = r(1).asInstanceOf[Point]
        val end = r(2).asInstanceOf[Point]
        new Line(start, end)
      }
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "spatialsdk.types.LineUDT"

}
