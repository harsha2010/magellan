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

import java.util.{List => JList}

import com.esri.core.geometry.{Polygon => ESRIPolygon}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JInt, JValue}
import org.json4s.JsonDSL._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * A polygon consists of one or more rings. A ring is a connected sequence of four or more points
 * that form a closed, non-self-intersecting loop. A polygon may contain multiple outer rings.
 * The order of vertices or orientation for a ring indicates which side of the ring is the interior
 * of the polygon. The neighborhood to the right of an observer walking along the ring
 * in vertex order is the neighborhood inside the polygon.
 * Vertices of rings defining holes in polygons are in a counterclockwise direction.
 * Vertices for a single, ringed polygon are, therefore, always in clockwise order.
 * The rings of a polygon are referred to as its parts.
 *
 * @param indices
 * @param points
 */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class Polygon(
    val indices: IndexedSeq[Int],
    val points: IndexedSeq[Point])
  extends Shape {

  override val shapeType: Int = 5

  override private[magellan] val delegate = {
    val p = new ESRIPolygon()
    if (points.length > 0) {
      var startIndex = 0
      var endIndex = 1
      val length = points.size
      var currentRingIndex = 0
      var start = points(startIndex)

      p.startPath(start.delegate)

      while (endIndex < length) {
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
            p.closePathWithLine()
            start = points(startIndex)
          }
        }
      }
    }
    p
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Polygon]

  override def equals(other: Any): Boolean = other match {
    case that: Polygon =>
      (that canEqual this) &&
        indices == that.indices &&
        points == that.points
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(indices, points)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"Polygon($shapeType, $indices, $points)"

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Polygon = {
    val transformedPoints = points.map(point => point.transform(fn))
    new Polygon(indices, transformedPoints)
  }

  override def jsonValue: JValue =
    ("type" -> "udt") ~
      ("class" -> this.getClass.getName) ~
      ("pyClass" -> "magellan.types.PolygonUDT") ~
      ("indices" -> JArray(indices.map(index => JInt(index)).toList)) ~
      ("points" -> JArray(points.map(_.jsonValue).toList))
}

class PolygonUDT extends UserDefinedType[Polygon] {

  override def sqlType: DataType = Polygon.EMPTY

  override def serialize(obj: Any): Polygon = {
    obj.asInstanceOf[Polygon]
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    datum match {
      case x: Polygon => x
      case r: Row => r(0).asInstanceOf[Polygon]
      case null => null
      case Array(t: Int, indices: JList[Int], points: JList[Point]) => {
        new Polygon(indices.toIndexedSeq, points.toIndexedSeq)
      }
      case _ => ???
    }
  }

  override def pyUDT: String = "magellan.types.PolygonUDT"

}

object Polygon {

  val EMPTY = new Polygon(Array[Int](), Array[Point]())

  def fromESRI(esriPolygon: ESRIPolygon): Polygon = {
    val length = esriPolygon.getPointCount
    if (length == 0) {
      new Polygon(Array[Int](), Array[Point]())
    } else {
      val indices = ArrayBuffer[Int]()
      indices.+=(0)
      val points = ArrayBuffer[Point]()
      var currentRing = esriPolygon.getPoint(0)
      points.+=(Point.fromESRI(currentRing))

      for (i <- (1 until length)) {
        val p = esriPolygon.getPoint(i)
        if (p.getX == currentRing.getX && p.getY == currentRing.getY) {
          if (i < length - 1) {
            indices.+=(i)
            currentRing = esriPolygon.getPoint(i + 1)
          }
        }
        points.+=(Point.fromESRI(p))
      }
      new Polygon(indices.toIndexedSeq, points.toIndexedSeq)
    }
  }
}
