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

import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point => JTSPoint, PrecisionModel}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * A point is a zero dimensional shape.
 * The coordinates of a point can be in linear units such as feet or meters,
 * or they can be in angular units such as degrees or radians.
 * The associated spatial reference specifies the units of the coordinates.
 * In the case of a geographic coordinate system, the x-coordinate is the longitude
 * and the y-coordinate is the latitude.
 */
@SQLUserDefinedType(udt = classOf[PointUDT])
class Point(val x: Double, val y: Double) extends Shape {

  override val delegate = {
    val precisionModel = new PrecisionModel()
    val geomFactory = new GeometryFactory(precisionModel)
    val csf = CoordinateArraySequenceFactory.instance()
    val cs = csf.create(Array(new Coordinate(x, y)))
    new JTSPoint(cs, geomFactory)
  }

  override final val shapeType: Int = 1

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


  override def toString = s"Point($shapeType, $x, $y)"

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Point = fn(this)

}

private[magellan] class PointUDT extends UserDefinedType[Point] {

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("x", DoubleType, nullable = true),
      StructField("y", DoubleType, nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(3)
    obj match {
      case p: Point => row(0) = p.shapeType; row(1) = p.x; row(2) = p.y
      case _ => ???
    }
    row
  }

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    datum match {
      case row: Row => {
        val t = row(0)
        t match {
          case 1 => new Point(row.getDouble(1), row.getDouble(2))
          case _ => ???
        }
      }
      // TODO: There is a bug in UDT serialization in Spark.This should never happen.
      case p: Point => p
      case null => null
      case _ => ???
    }
  }

  override def pyUDT: String = "magellan.types.PointUDT"

}