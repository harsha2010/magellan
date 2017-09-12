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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
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


  override def toString = s"Point($x, $y)"

  def setX(x: Double): Unit = {
    this.x = x
  }

  def setY(y: Double): Unit = {
    this.y = y
  }

  @JsonProperty
  def getX(): Double = x

  @JsonProperty
  def getY(): Double = y

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Point = fn(this)

  def withinCircle(origin: Point, radius: Double): Boolean = {
    val sqrdL2Norm = Math.pow((origin.getX() - getX()), 2) + Math.pow((origin.getY() - getY()), 2)
    sqrdL2Norm <= Math.pow(radius, 2)
  }

  @JsonProperty
  override def getType(): Int = 1

  override def jsonValue: JValue =
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> "magellan.types.PointUDT") ~
      ("x" -> x) ~
      ("y" -> y)

  @JsonProperty
  override def boundingBox = BoundingBox(x, y, x, y)

  @JsonIgnore
  override def isEmpty(): Boolean = true

}

object Point {

  def apply(x: Double, y: Double) = {
    val p = new Point()
    p.setX(x)
    p.setY(y)
    p
  }
}

