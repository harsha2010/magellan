package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class PolyLineUDT extends UserDefinedType[PolyLine] with GeometricUDT {

  override val sqlType: StructType = StructType(
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

  override def serialize(polyLine: PolyLine): InternalRow = {
   polyLine.serialize()
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[PolyLine])

  override def userClass: Class[PolyLine] = classOf[PolyLine]

  override def deserialize(datum: Any): PolyLine = {
    val row = datum.asInstanceOf[InternalRow]
    val polyline = new PolyLine()
    polyline.init(row)
    polyline
  }

  override def pyUDT: String = "magellan.types.PolyLineUDT"

  override val geometryType = new PolyLine().getType()
}
