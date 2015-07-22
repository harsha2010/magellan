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

import com.esri.core.geometry.{Point => ESRIPoint}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * A point is a zero dimensional shape.
 * The coordinates of a point can be in linear units such as feet or meters,
 * or they can be in angular units such as degrees or radians.
 * The associated spatial reference specifies the units of the coordinates.
 * In the case of a geographic coordinate system, the x-coordinate is the longitude
 * and the y-coordinate is the latitude.
 */
@SQLUserDefinedType(udt = classOf[PointUDT])
class Point(val x: Double, val y: Double) extends Shape {

  override private[magellan] val delegate = {
    val p = new ESRIPoint()
    p.setX(x)
    p.setY(y)
    p
  }

  override final val shapeType: Int = 1

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


  override def toString = s"Point($x, $y)"

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Point = fn(this)

}

private[magellan] class PointUDT extends UserDefinedType[Point] {

  override def sqlType: DataType = Point.EMPTY

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(1)
    obj match {
      case p: Point => row(0) = p
      case _ => ???
    }
    row
  }

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    datum match {
      case row: Row => {
        row(0).asInstanceOf[Point]
      }
      // TODO: There is a bug in UDT serialization in Spark.This should never happen.
      case p: Point => p
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "magellan.types.PointUDT"

}

object Point {

  val EMPTY = new Point(0.0, 0.0)

  private[magellan] def fromESRI(esriPoint: ESRIPoint): Point = {
    new Point(esriPoint.getX(), esriPoint.getY())
  }

  def apply(x: Double, y: Double) = new Point(x, y)
}
