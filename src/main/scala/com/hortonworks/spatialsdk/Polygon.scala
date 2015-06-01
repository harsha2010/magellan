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

package com.hortonworks.spatialsdk

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
 * @param numRings
 * @param numPoints
 * @param indices
 * @param points
 */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
case class Polygon(box: Box,
    numRings: Int,
    numPoints: Int,
    indices: IndexedSeq[Int],
    points: IndexedSeq[Point])
  extends Serializable with Shape {

}

private[spatialsdk] class PolygonUDT extends UserDefinedType[Polygon] {

  private val pointDataType = new PointUDT().sqlType

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("numRings", IntegerType, nullable = true),
      StructField("numPoints", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("points", ArrayType(pointDataType, containsNull = false), nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(8)
    val polygon = obj.asInstanceOf[Polygon]
    row(0) = polygon.box.xmin
    row(1) = polygon.box.ymin
    row(2) = polygon.box.xmax
    row(3) = polygon.box.ymax
    row(4) = polygon.numRings
    row(5) = polygon.numPoints
    row(6) = polygon.indices
    row(7) = polygon.points
    row
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    datum match {
      case x: Polygon => x
      case r: Row => {
        val box = Box(r.getDouble(0), r.getDouble(1),
            r.getDouble(2), r.getDouble(3)
          )
        val numRings = r.getInt(4)
        val numPoints = r.getInt(5)
        val indices = r.get(6).asInstanceOf[IndexedSeq[Int]]
        val points = r.get(7).asInstanceOf[IndexedSeq[Point]]
        Polygon(box, numRings, numPoints, indices, points)
      }
      case _ => ???
    }
  }

}