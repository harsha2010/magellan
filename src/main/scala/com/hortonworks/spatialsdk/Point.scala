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
 * A point is a zero dimensional shape.
 * The coordinates of a point can be in linear units such as feet or meters,
 * or they can be in angular units such as degrees or radians.
 * The associated spatial reference specifies the units of the coordinates.
 * In the case of a geographic coordinate system, the x-coordinate is the longitude
 * and the y-coordinate is the latitude.
 */
@SQLUserDefinedType(udt = classOf[PointUDT])
sealed trait Point extends Serializable with Shape {

}

case class Point1D(x: Double) extends Point
case class Point2D(x: Double, y: Double) extends Point
case class Point3D(x: Double, y: Double, z: Double) extends Point

private[spatialsdk] class PointUDT extends UserDefinedType[Point] {

  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("x", DoubleType, nullable = true),
      StructField("y", DoubleType, nullable = true),
      StructField("z", DoubleType, nullable = true)))
  }

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(4)
    obj match {
      case Point1D(x) => row(0) = 0; row(1) = x
      case Point2D(x, y) => row(0) = 1; row(1) = x; row(2) = y
      case Point3D(x, y, z) => row(0) = 0; row(1) = x; row(2) = y; row(3) = z;
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
          case 0 => Point1D(row.getDouble(1))
          case 1 => Point2D(row.getDouble(1), row.getDouble(2))
          case 2 => Point3D(row.getDouble(1), row.getDouble(2), row.getDouble(3))
          case _ => ???
        }
      }
      // TODO: There is a bug in UDT serialization in Spark.This should never happen.
      case p: Point => p
      case _ => ???
    }
  }
}