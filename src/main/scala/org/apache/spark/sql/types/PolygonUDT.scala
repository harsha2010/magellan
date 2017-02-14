package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class PolygonUDT extends UserDefinedType[Polygon] with GeometricUDT {

  override val sqlType: DataType = StructType(Seq(
    StructField("type", IntegerType, nullable = false),
    StructField("xmin", DoubleType, nullable = false),
    StructField("ymin", DoubleType, nullable = false),
    StructField("xmax", DoubleType, nullable = false),
    StructField("ymax", DoubleType, nullable = false),
    StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
    StructField("xcoordinates", ArrayType(DoubleType, containsNull = false), nullable = true),
    StructField("ycoordinates", ArrayType(DoubleType, containsNull = false), nullable = true)
  ))

  override def serialize(polygon: Polygon): InternalRow = {
    val row = new GenericInternalRow(8)
    val BoundingBox(xmin, ymin, xmax, ymax) = polygon.boundingBox
    row.update(0, polygon.getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(polygon.indices))
    row.update(6, new DoubleArrayData(polygon.xcoordinates))
    row.update(7, new DoubleArrayData(polygon.ycoordinates))
    row
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[Polygon])

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val row = datum.asInstanceOf[InternalRow]
    val polygon = new Polygon(
      row.getArray(5).toIntArray(),
      row.getArray(6).toDoubleArray(),
      row.getArray(7).toDoubleArray(),
      BoundingBox(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))
    )

    polygon
  }

}
