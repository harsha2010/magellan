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
 * A polygon consists of one or more rings. A ring is a connected sequence of four or more points
 * that form a closed, non-self-intersecting loop. A polygon may contain multiple outer rings.
 * The order of vertices or orientation for a ring indicates which side of the ring is the interior
 * of the polygon. The neighborhood to the right of an observer walking along the ring
 * in vertex order is the neighborhood inside the polygon.
 * Vertices of rings defining holes in polygons are in a counterclockwise direction.
 * Vertices for a single, ringed polygon are, therefore, always in clockwise order.
 * The rings of a polygon are referred to as its parts.
 *
 * @param box
 * @param indices
 * @param points
 */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class Polygon(val box: Box,
    val indices: IndexedSeq[Int],
    val points: IndexedSeq[Point])
  extends Serializable with Shape {

  override final val shapeType: Int = 5

  def contains(point: Point): Boolean = {
    // simple test: if the point is outside the bounding box, it cannot be in the polygon
    if (!box.contains(point)) {
      false
    } else {
      // pick a point q uniformly at random a distance epsilon away from the box
      // the line segment between point and q should intersect the polygon
      // an odd # of times if the point is within the polygon.
      val q = box.away(0.1)
      val line = Line(point, q)
      var startIndex = 0
      var endIndex = 1
      val length = points.size
      var intersections = 0
      var currentRingIndex = 0
      while (endIndex < length) {
        val start = points(startIndex)
        val end = points(endIndex)
        if (line.intersects(Line(start, end))) {
          intersections += 1
        }
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
      intersections % 2 != 0
    }
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

  override def toString = s"Polygon($shapeType, $box, $indices, $points)"

}

private[spatialsdk] class PolygonUDT extends UserDefinedType[Polygon] {

  private val pointDataType = new PointUDT().sqlType

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("points", ArrayType(pointDataType, containsNull = false), nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(7)
    val polygon = obj.asInstanceOf[Polygon]
    row(0) = polygon.shapeType
    row(1) = polygon.box.xmin
    row(2) = polygon.box.ymin
    row(3) = polygon.box.xmax
    row(4) = polygon.box.ymax
    row(5) = polygon.indices
    row(6) = polygon.points
    row
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    datum match {
      case x: Polygon => x
      case r: Row => {
        r.getInt(0)
        val box = Box(r.getDouble(1), r.getDouble(2),
            r.getDouble(3), r.getDouble(4)
          )
        val indices = r.get(5).asInstanceOf[IndexedSeq[Int]]
        val points = r.get(6).asInstanceOf[IndexedSeq[Point]]
        new Polygon(box, indices, points)
      }
      case _ => ???
    }
  }

  override def pyUDT: String = "spatialsdk.types.PolygonUDT"

}
