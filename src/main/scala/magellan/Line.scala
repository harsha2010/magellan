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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * Line segment between two points.
 */
@SQLUserDefinedType(udt = classOf[LineUDT])
class Line extends Shape {

  private var start: Point = _
  private var end: Point = _

  def setStart(start: Point) { this.start = start }
  def setEnd(end: Point) { this.end = end }

  def getStart() = start
  def getEnd() = end

  private [magellan] def intersects(other: Line): Boolean = {
    // test for degeneracy
    if (start == other.start ||
      end == other.end ||
      start == other.end ||
      end == other.start) {
      true
    } else {
      def ccw(a: Point, b: Point, c: Point) = {
        (c.getY() - a.getY()) * (b.getX() - a.getX()) > (b.getY() - a.getY()) * (c.getX() - a.getX())
      }
      ccw(start, other.start, other.end) != ccw(end, other.start, other.end) &&
        ccw(start, end, other.start) != ccw(start, end, other.end)
    }
  }

  override def getType(): Int = 2

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Shape = ???

  override def boundingBox: ((Double, Double), (Double, Double)) = {
    val (xmin, xmax) = if (start.getX() < end.getX()) {
      (start.getX(), end.getX())
    } else {
      (end.getX(), start.getX())
    }
    val (ymin, ymax) = if (start.getY() < end.getY()) {
      (start.getY(), end.getY())
    } else {
      (end.getY(), start.getY())
    }
    ((xmin, ymin), (xmax, ymax))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Line]

  override def equals(other: Any): Boolean = other match {
    case that: Line =>
      (that canEqual this) &&
        start == that.start &&
        end == that.end
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(start, end)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class LineUDT extends UserDefinedType[Line] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("startX", DoubleType, nullable = false),
      StructField("startY", DoubleType, nullable = false),
      StructField("endX", DoubleType, nullable = false),
      StructField("endY", DoubleType, nullable = false)
    ))

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(9)
    val line = obj.asInstanceOf[Line]
    row.setInt(0, 2)
    val ((xmin, ymin), (xmax, ymax)) = line.boundingBox
    row.setDouble(1, xmin)
    row.setDouble(2, ymin)
    row.setDouble(3, xmax)
    row.setDouble(4, ymax)
    row.setDouble(5, line.getStart().getX())
    row.setDouble(6, line.getStart().getY())
    row.setDouble(7, line.getEnd().getX())
    row.setDouble(8, line.getEnd().getY())
    row
  }

  override def userClass: Class[Line] = classOf[Line]

  override def deserialize(datum: Any): Line = {
    val row = datum.asInstanceOf[InternalRow]
    val startX = row.getDouble(5)
    val startY = row.getDouble(6)
    val endX = row.getDouble(7)
    val endY = row.getDouble(8)
    val line = new Line()
    val start = Point(startX, startY)
    val end = Point(endX, endY)
    line.setStart(start)
    line.setEnd(end)
    line
  }

  override def pyUDT: String = "magellan.types.LineUDT"

}

object Line {

  def apply(start: Point, end: Point): Line = {
    val line = new Line()
    line.setStart(start)
    line.setEnd(end)
    line
  }
}