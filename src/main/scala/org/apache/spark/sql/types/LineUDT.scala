package org.apache.spark.sql.types

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class LineUDT extends UserDefinedType[Line] with GeometricUDT {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false),
      StructField("xmin", DoubleType, nullable = false),
      StructField("ymin", DoubleType, nullable = false),
      StructField("xmax", DoubleType, nullable = false),
      StructField("ymax", DoubleType, nullable = false),
      StructField("startX", DoubleType, nullable = false),
      StructField("startY", DoubleType, nullable = false),
      StructField("endX", DoubleType, nullable = false),
      StructField("endY", DoubleType, nullable = false)
    ))

  override def serialize(line: Line): InternalRow = {
    val row = new GenericInternalRow(9)
    row.setInt(0, 2)
    val BoundingBox(xmin, ymin, xmax, ymax) = line.boundingBox
    row.setDouble(1, xmin)
    row.setDouble(2, ymin)
    row.setDouble(3, xmax)
    row.setDouble(4, ymax)
    row.setDouble(5, line.getStart().getX())
    row.setDouble(6, line.getStart().getY())
    row.setDouble(7, line.getEnd().getX())
    row.setDouble(8, line.getEnd().getY())
    row
  }

  override def serialize(shape: Shape) = serialize(shape.asInstanceOf[Line])

  override def userClass: Class[Line] = classOf[Line]

  override def deserialize(datum: Any): Line = {
    val row = datum.asInstanceOf[InternalRow]
    val startX = row.getDouble(5)
    val startY = row.getDouble(6)
    val endX = row.getDouble(7)
    val endY = row.getDouble(8)
    val line = new Line()
    val start = Point(startX, startY)
    val end = Point(endX, endY)
    line.setStart(start)
    line.setEnd(end)
    line
  }

  override def pyUDT: String = "magellan.types.LineUDT"

  override val geometryType = new Line().getType()

}
