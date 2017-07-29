package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class PolyLineUDT extends UserDefinedType[PolyLine] with GeometricUDT {

  override val sqlType: DataType = StructType(
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
    val row = new GenericInternalRow(8)
    val BoundingBox(xmin, ymin, xmax, ymax) = polyLine.boundingBox
    row.update(0, polyLine.getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(polyLine.indices))
    row.update(6, new DoubleArrayData(polyLine.xcoordinates))
    row.update(7, new DoubleArrayData(polyLine.ycoordinates))
    row
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[PolyLine])

  override def userClass: Class[PolyLine] = classOf[PolyLine]

  override def deserialize(datum: Any): PolyLine = {
    val row = datum.asInstanceOf[InternalRow]
    val polyLine = new PolyLine(
      row.getArray(5).toIntArray(),
      row.getArray(6).toDoubleArray(),
      row.getArray(7).toDoubleArray(),
      BoundingBox(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))
    )
    polyLine
  }

  override def pyUDT: String = "magellan.types.PolyLineUDT"

  override val geometryType = new PolyLine().getType()
}
