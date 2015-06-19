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

package org.apache.magellan

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.esri.core.geometry.{Polyline => ESRIPolyline}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * A PolyLine is an ordered set of vertices that consists of one or more parts.
 * A part is a connected sequence of two or more points.
 * Parts may or may not be connected to one another.
 * Parts may or may not intersect one another
 */
@SQLUserDefinedType(udt = classOf[PolyLineUDT])
class PolyLine(
    override val indices: IndexedSeq[Int],
    override val points: IndexedSeq[Point])
  extends Multipath {

  override val shapeType: Int = 3

  override lazy val delegate = {
    val p = new ESRIPolyline()
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
          start = points(startIndex)
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
    val transformedPoints = points.map(fn)
    new PolyLine(indices, transformedPoints)
  }

}

private[magellan] class PolyLineUDT extends UserDefinedType[PolyLine] {

  private val pointDataType = new PointUDT().sqlType

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("points", ArrayType(pointDataType, containsNull = false), nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(7)
    val polyline = obj.asInstanceOf[PolyLine]
    row(0) = polyline.shapeType
    row(1) = polyline.indices
    row(2) = polyline.points
    row
  }

  override def userClass: Class[PolyLine] = classOf[PolyLine]

  override def deserialize(datum: Any): PolyLine = {
    datum match {
      case x: PolyLine => x
      case r: Row => {
        r.getInt(0)
        val indices = r.get(1).asInstanceOf[IndexedSeq[Int]]
        val points = r.get(2).asInstanceOf[IndexedSeq[Point]]
        new PolyLine(indices, points)
      }
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "magellan.types.PolyLineUDT"

}
