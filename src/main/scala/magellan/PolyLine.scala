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
    override val boundingBox: Tuple2[Tuple2[Double, Double], Tuple2[Double, Double]]) extends Shape {

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
    val ((pt_xmin, pt_ymin), (pt_xmax, pt_ymax)) = point.boundingBox
    val ((xmin, ymin), (xmax, ymax)) = boundingBox

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

private[magellan] class PolyLineUDT extends UserDefinedType[PolyLine] {

  override val sqlType: DataType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("xcoordinates", ArrayType(DoubleType, containsNull = false), nullable = true),
      StructField("ycoordinates", ArrayType(DoubleType, containsNull = false), nullable = true)
    ))

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(8)
    val polyLine = obj.asInstanceOf[PolyLine]
    val ((xmin, ymin), (xmax, ymax)) = polyLine.boundingBox
    row.update(0, polyLine.getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(polyLine.indices))
    row.update(6, new DoubleArrayData(polyLine.xcoordinates))
    row.update(7, new DoubleArrayData(polyLine.ycoordinates))
    row
  }

  override def userClass: Class[PolyLine] = classOf[PolyLine]

  override def deserialize(datum: Any): PolyLine = {
    val row = datum.asInstanceOf[InternalRow]
    val polyLine = new PolyLine(
      row.getArray(5).toIntArray(),
      row.getArray(6).toDoubleArray(),
      row.getArray(7).toDoubleArray(),
      ((row.getDouble(1), row.getDouble(2)), (row.getDouble(3), row.getDouble(4)))
    )
    polyLine
  }

  override def pyUDT: String = "magellan.types.PolyLineUDT"
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
      ((xmin, ymin), (xmax, ymax))
    )
  }
}