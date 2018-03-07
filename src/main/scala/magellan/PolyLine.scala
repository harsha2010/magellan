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
import magellan.geometry.{Curve, R2Loop}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * A PolyLine is an ordered set of vertices that consists of one or more parts.
  * A part is a connected sequence of two or more points.
  * Parts may or may not be connected to one another.
  * Parts may or may not intersect one another
  */
@SQLUserDefinedType(udt = classOf[PolyLineUDT])
class PolyLine extends Shape {

  private var indices: Array[Int] = _
  private var xcoordinates: Array[Double] = _
  private var ycoordinates: Array[Double] = _

  @transient var curves = new ArrayBuffer[Curve]()

  @JsonIgnore private var _boundingBox: BoundingBox = _

  private[magellan] def init(
                              indices: Array[Int],
                              xcoordinates: Array[Double],
                              ycoordinates: Array[Double],
                              boundingBox: BoundingBox): Unit = {

    this.indices = indices
    this.xcoordinates = xcoordinates
    this.ycoordinates = ycoordinates
    this._boundingBox = boundingBox
    // initialize the loops
    val offsets = indices.zip(indices.drop(1) ++ Array(xcoordinates.length))
    for ((start, end) <- offsets) {
      curves += ({
        val curve = new R2Loop()
        curve.init(xcoordinates, ycoordinates, start, end - 1)
        curve
      })
    }
  }
  override def getType(): Int = 3

  def init(row: InternalRow): Unit = {
    init(row.getArray(5).toIntArray(),
      row.getArray(6).toDoubleArray(),
      row.getArray(7).toDoubleArray(),
      BoundingBox(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4)))
  }

  def serialize(): InternalRow = {
    val row = new GenericInternalRow(8)
    val BoundingBox(xmin, ymin, xmax, ymax) = boundingBox
    row.update(0, getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(indices))
    row.update(6, new DoubleArrayData(xcoordinates))
    row.update(7, new DoubleArrayData(ycoordinates))
    row
  }

  @JsonProperty
  private def getXCoordinates(): Array[Double] = xcoordinates

  @JsonProperty
  private def getYCoordinates(): Array[Double] = ycoordinates

  @JsonProperty
  override def boundingBox = _boundingBox

  private[magellan] def contains(point: Point): Boolean = {
    val numLoops = curves.size
    var touches = false
    var i = 0
    while (i < numLoops && !touches) {
      touches |= curves(i).touches(point)
      i += 1
    }
    touches
  }


  /**
    * A polygon intersects a line iff it is a proper intersection,
    * or if either vertex of the line touches the polygon.
    *
    * @param line
    * @return
    */
  private [magellan] def intersects(line: Line): Boolean = {
    curves exists (_.intersects(line))
  }

  @JsonIgnore
  override def isEmpty(): Boolean = xcoordinates.length == 0

  def length(): Int = xcoordinates.length

  def getVertex(index: Int) = Point(xcoordinates(index), ycoordinates(index))

  @JsonProperty
  def getRings(): Array[Int] = indices

  @JsonIgnore
  def getNumRings(): Int = indices.length

  def getRing(index: Int): Int = indices(index)

  def intersects(polygon:Polygon):Boolean = {
    // a polyline intersects a polygon iff any line intersects a polygon
    curves exists (_.iterator().exists(polygon intersects))
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

  def readResolve(): AnyRef = {
    curves = new ArrayBuffer[Curve]()
    this.init(indices, xcoordinates, ycoordinates, boundingBox)
    this
  }
}

object PolyLine {

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
    val polyline = new PolyLine()
    polyline.init(
      indices,
      points.map(_.getX()),
      points.map(_.getY()),
      BoundingBox(xmin, ymin, xmax, ymax))
    polyline
  }
}