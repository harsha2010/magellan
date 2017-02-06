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

import org.apache.spark.sql.types._

import scala.util.control.Breaks._

/**
  * A PolyLine is an ordered set of vertices that consists of one or more parts.
  * A part is a connected sequence of two or more points.
  * Parts may or may not be connected to one another.
  * Parts may or may not intersect one another
  */
@SQLUserDefinedType(udt = classOf[PolyLineUDT])
class PolyLine(
    val indices: Array[Int],
    val xcoordinates: Array[Double],
    val ycoordinates: Array[Double],
    override val boundingBox: BoundingBox) extends Shape {

  override def getType(): Int = 3

  private [magellan] def contains(point:Point): Boolean = {
    var startIndex = 0
    var endIndex = 1
    var contains = false
    val length = xcoordinates.size

    if(!exceedsBounds(point))
    breakable {
      while(endIndex < length) {
        val startX = xcoordinates(startIndex)
        val startY = ycoordinates(startIndex)
        val endX = xcoordinates(endIndex)
        val endY = ycoordinates(endIndex)
        val slope =  (endY - startY)/(endX - startX)
        val pointSlope = (endY - point.getY())/(endX - point.getX())
        if(slope == pointSlope) {
          contains = true
          break
        }
        startIndex += 1
        endIndex += 1
      }
    }
    contains
  }

  def exceedsBounds(point:Point):Boolean = {
    val BoundingBox(pt_xmin, pt_ymin, pt_xmax, pt_ymax) = point.boundingBox
    val BoundingBox(xmin, ymin, xmax, ymax) = boundingBox

    pt_xmin < xmin && pt_ymin < ymin ||
    pt_xmax > xmax && pt_ymax > ymax
  }

  def intersects(line:Line):Boolean = {
    var startIndex = 0
    var endIndex = 1
    var intersects = false
    val length = xcoordinates.size

    breakable {

      while(endIndex < length) {
        val startX = xcoordinates(startIndex)
        val startY = ycoordinates(startIndex)
        val endX = xcoordinates(endIndex)
        val endY = ycoordinates(endIndex)
        // check if any segment intersects incoming line
        if(line.intersects(Line(Point(startX, startY), Point(endX, endY)))) {
          intersects = true
          break
        }
        startIndex += 1
        endIndex += 1
      }
    }
    intersects
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PolyLine]

  override def equals(other: Any): Boolean = other match {
    case that: PolyLine =>
      (that canEqual this) &&
        getType() == that.getType() &&
        indices.deep == that.indices.deep &&
        xcoordinates.deep == that.xcoordinates.deep &&
        ycoordinates.deep == that.ycoordinates.deep

    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(getType(), indices, xcoordinates, ycoordinates)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  /**
    * Applies an arbitrary point wise transformation to a given shape.
    *
    * @param fn
    * @return
    */
  override def transform(fn: (Point) => Point): PolyLine = {
    /*val transformedPoints = points.map(point => point.transform(fn))
    PolyLine(indices, transformedPoints)*/
    ???
  }

  /*override def jsonValue: JValue =
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> "magellan.types.PolyLineUDT") ~
      ("indices" -> JArray(indices.map(index => JInt(index)).toList)) ~
      ("points" -> JArray(points.map(_.jsonValue).toList))*/
}

private[magellan] object PolyLine {

  def apply(indices: Array[Int], points: Array[Point]): PolyLine = {
    // look for the extremities
    var xmin: Double = Double.MaxValue
    var ymin: Double = Double.MaxValue
    var xmax: Double = Double.MinValue
    var ymax: Double = Double.MinValue
    val size = points.length
    var i = 0
    while (i < size) {
      val point = points(i)
      val (x, y) = (point.getX(), point.getY())
      if (xmin > x) {
        xmin = x
      }
      if (ymin > y) {
        ymin = y
      }
      if (xmax < x) {
        xmax = x
      }
      if (ymax < y) {
        ymax = y
      }
      i += 1
    }
    new PolyLine(
      indices,
      points.map(_.getX()),
      points.map(_.getY()),
      BoundingBox(xmin, ymin, xmax, ymax)
    )
  }
}