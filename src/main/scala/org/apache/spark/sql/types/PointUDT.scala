package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class PointUDT extends UserDefinedType[Point] with GeometricUDT {

  override val sqlType: StructType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("x", DoubleType, nullable = false),
      StructField("y", DoubleType, nullable = false)
    ))

  override def serialize(point: Point): InternalRow = {
    val row = new GenericInternalRow(7)
    row.setInt(0, point.getType())
    row.setDouble(1, point.getX())
    row.setDouble(2, point.getY())
    row.setDouble(3, point.getX())
    row.setDouble(4, point.getY())
    row.setDouble(5, point.getX())
    row.setDouble(6, point.getY())
    row
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[Point])

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    val row = datum.asInstanceOf[InternalRow]
    require(row.numFields == 7)
    Point(row.getDouble(5), row.getDouble(6))
  }

  override def pyUDT: String = "magellan.types.PointUDT"

  def serialize(x: Double, y: Double): InternalRow = {
    val row = new GenericInternalRow(7)
    row.setInt(0, 1)
    row.setDouble(1, x)
    row.setDouble(2, y)
    row.setDouble(3, x)
    row.setDouble(4, y)
    row.setDouble(5, x)
    row.setDouble(6, y)
    row
  }

  override val geometryType = new Point().getType()

}

