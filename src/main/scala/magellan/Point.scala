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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

/**
 * A point is a zero dimensional shape.
 * The coordinates of a point can be in linear units such as feet or meters,
 * or they can be in angular units such as degrees or radians.
 * The associated spatial reference specifies the units of the coordinates.
 * In the case of a geographic coordinate system, the x-coordinate is the longitude
 * and the y-coordinate is the latitude.
 */
@SQLUserDefinedType(udt = classOf[PointUDT])
class Point extends Shape {

  private var x: Double = _
  private var y: Double = _

  def equalToTol(other: Point, eps: Double): Boolean = {
    math.abs(x - other.x) < eps && math.abs(y - other.y) < eps
  }

  override def equals(other: Any): Boolean = {
    other match {
      case p: Point => x == p.x && y == p.y
      case _ => false
    }
  }

  override def hashCode(): Int = {
    var code = 0
    code = code * 41 + x.hashCode()
    code = code * 41 + y.hashCode()
    code
  }

  def intersects(line: Line):Boolean = {
    val start = line.getStart()
    val end = line.getEnd()
    val slope = (end.getY() - start.getY())/(end.getX() - start.getX())
    (end.getY() - this.getY())/(end.getX()-this.getX()) == slope
  }

  override def toString = s"Point($x, $y)"

  def setX(x: Double): Unit = {
    this.x = x
  }

  def setY(y: Double): Unit = {
    this.y = y
  }

  def getX(): Double = x

  def getY(): Double = y

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Point = fn(this)

  override def getType(): Int = 1

  override def jsonValue: JValue =
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> "magellan.types.PointUDT") ~
      ("x" -> x) ~
      ("y" -> y)

  override def boundingBox: ((Double, Double), (Double, Double)) = ((x, y), (x, y))

}

class PointUDT extends UserDefinedType[Point] {

  override val sqlType: DataType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType, nullable = false)
    ))

  override def serialize(obj: Any): InternalRow = {
    val p = obj.asInstanceOf[Point]
    val row = new GenericMutableRow(7)
    row.setInt(0, p.getType())
    row.setDouble(1, p.getX())
    row.setDouble(2, p.getY())
    row.setDouble(3, p.getX())
    row.setDouble(4, p.getY())
    row.setDouble(5, p.getX())
    row.setDouble(6, p.getY())
    row
  }

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    val row = datum.asInstanceOf[InternalRow]
    require(row.numFields == 7)
    Point(row.getDouble(5), row.getDouble(6))
  }

  override def pyUDT: String = "magellan.types.PointUDT"

  def serialize(x: Double, y: Double): InternalRow = {
    val row = new GenericMutableRow(7)
    row.setInt(0, 1)
    row.setDouble(1, x)
    row.setDouble(2, y)
    row.setDouble(3, x)
    row.setDouble(4, y)
    row.setDouble(5, x)
    row.setDouble(6, y)
    row
  }

}

object Point {

  def apply(x: Double, y: Double) = {
    val p = new Point()
    p.setX(x)
    p.setY(y)
    p
  }
}

