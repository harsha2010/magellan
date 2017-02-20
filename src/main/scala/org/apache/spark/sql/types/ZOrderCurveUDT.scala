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

package org.apache.spark.sql.types

import magellan.BoundingBox
import magellan.index.ZOrderCurve
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class ZOrderCurveUDT extends UserDefinedType[ZOrderCurve] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("precision", IntegerType, nullable = false),
      StructField("bits", LongType, nullable = false)
    ))

  override def serialize(obj: ZOrderCurve): Any = {
    val row = new GenericInternalRow(6)
    val BoundingBox(xmin, ymin, xmax, ymax) = obj.boundingBox
    row.setDouble(0, xmin)
    row.setDouble(1, ymin)
    row.setDouble(2, xmax)
    row.setDouble(3, ymax)
    row.setInt(4, obj.precision)
    row.setLong(5, obj.bits)
    row
  }

  override def deserialize(datum: Any): ZOrderCurve = {
    val row = datum.asInstanceOf[InternalRow]
    val boundingBox = BoundingBox(row.getDouble(0),
      row.getDouble(1),
      row.getDouble(2),
      row.getDouble(3))

    new ZOrderCurve(boundingBox, row.getInt(4), row.getLong(5))
  }

  override def userClass: Class[ZOrderCurve] = classOf[ZOrderCurve]

}
