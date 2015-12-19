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

import com.esri.core.geometry.{Polyline => ESRIPolyline}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JInt, JArray, JValue}
import org.json4s.JsonDSL._

import scala.collection.mutable.ArrayBuffer

/**
 * A PolyLine is an ordered set of vertices that consists of one or more parts.
 * A part is a connected sequence of two or more points.
 * Parts may or may not be connected to one another.
 * Parts may or may not intersect one another
 */
@SQLUserDefinedType(udt = classOf[PolyLineUDT])
class PolyLine(
    val indices: IndexedSeq[Int],
    val points: IndexedSeq[Point])
  extends Shape {

  override val shapeType: Int = 3

  override private[magellan] val delegate = {
    val p = new ESRIPolyline()
    var startIndex = 0
    var endIndex = 1
    val length = points.size
    var currentRingIndex = 0

    while (endIndex < length) {
      val start = points(startIndex)
      p.startPath(start.delegate)
      val end = points(endIndex)
      p.lineTo(end.delegate)
      startIndex += 1
      endIndex += 1
      // if we reach a ring boundary skip it
      val nextRingIndex = currentRingIndex + 1
      if (nextRingIndex < indices.length) {
        val nextRing = indices(nextRingIndex)
        if (endIndex == nextRing) {
          startIndex += 1
          endIndex += 1
          currentRingIndex = nextRingIndex
        }
      }
    }
    p
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PolyLine]

  override def equals(other: Any): Boolean = other match {
    case that: PolyLine =>
      (that canEqual this) &&
        shapeType == that.shapeType &&
        indices == that.indices &&
        points == that.points
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(shapeType, indices, points)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"PolyLine($shapeType, $indices, $points)"

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): PolyLine = {
    val transformedPoints = points.map(point => point.transform(fn))
    new PolyLine(indices, transformedPoints)
  }

  override def jsonValue: JValue =
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> "magellan.types.PolyLineUDT") ~
      ("indices" -> JArray(indices.map(index => JInt(index)).toList)) ~
      ("points" -> JArray(points.map(_.jsonValue).toList))

}

class PolyLineUDT extends UserDefinedType[PolyLine] {

  override def sqlType: DataType = PolyLine.EMPTY

  override def serialize(obj: Any): PolyLine = {
    obj.asInstanceOf[PolyLine]
  }

  override def userClass: Class[PolyLine] = classOf[PolyLine]

  override def deserialize(datum: Any): PolyLine = {
    datum match {
      case x: PolyLine => x
      case r: Row => r(0).asInstanceOf[PolyLine]
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "magellan.types.PolyLineUDT"
}

object PolyLine {

  val EMPTY = new PolyLine(Array[Int](), Array[Point]())

  def fromESRI(esriPolyline: ESRIPolyline): PolyLine = {
    val length = esriPolyline.getPointCount
    val indices = ArrayBuffer[Int]()
    indices.+=(0)
    val points = ArrayBuffer[Point]()
    var currentRing = esriPolyline.getPoint(0)
    points.+=(Point.fromESRI(currentRing))

    for (i <- (1 until length)) {
      val p = esriPolyline.getPoint(i)
      if (p.getX == currentRing.getX && p.getY == currentRing.getY) {
        if (i < length - 1) {
          indices.+=(i)
          currentRing = esriPolyline.getPoint(i + 1)
        }
      }
      points.+=(Point.fromESRI(p))
    }
    new PolyLine(indices.toIndexedSeq, points.toIndexedSeq)
  }

}
