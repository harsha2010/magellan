package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class PolygonUDT extends UserDefinedType[Polygon] with GeometricUDT {

  override val sqlType: StructType = StructType(Seq(
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
    polygon.serialize()
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[Polygon])

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val row = datum.asInstanceOf[InternalRow]
    val polygon = new Polygon()
    polygon.init(row)
    polygon
  }

  override val geometryType = new Polygon().getType()
}
