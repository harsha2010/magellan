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
 * @param indices
 * @param points
 */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class Polygon(
    override val indices: IndexedSeq[Int],
    override val points: IndexedSeq[Point])
  extends Multipath {

  override val shapeType: Int = 5

  override def contains(point: Point): Boolean = {
    // simple test: if the point is outside the bounding box, it cannot be in the polygon
    if (!box.contains(point)) {
      false
    } else {
      // pick a point q uniformly at random a distance epsilon away from the box
      // the line segment between point and q should intersect the polygon
      // an odd # of times if the point is within the polygon.
      val q = box.away(0.1)
      val line = new Line(point, q)
      intersections(line) % 2 != 0
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

  override def toString = s"Polygon($shapeType, $indices, $points)"

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Polygon = {
    val transformedPoints = points.map(fn)
    new Polygon(indices, transformedPoints)
  }

}

private[spatialsdk] class PolygonUDT extends UserDefinedType[Polygon] {

  private val pointUDT = new PointUDT()
  private val pointDataType = pointUDT.sqlType

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("points", ArrayType(pointDataType, containsNull = false), nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(3)
    val polygon = obj.asInstanceOf[Polygon]
    row(0) = polygon.shapeType
    row.update(1, polygon.indices.toSeq)
    row.update(2, polygon.points.map(pointUDT.serialize).toSeq)
    row
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    datum match {
      case x: Polygon => x
      case r: Row => {
        r.getInt(0)
        val indices = r.get(1).asInstanceOf[Seq[Int]]
        val points = r.get(2).asInstanceOf[Seq[_]]
        new Polygon(indices.toIndexedSeq, points.map(pointUDT.deserialize).toIndexedSeq)
      }
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "spatialsdk.types.PolygonUDT"

}
