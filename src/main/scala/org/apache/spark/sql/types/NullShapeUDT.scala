package org.apache.spark.sql.types

import magellan.Shape
import magellan.NullShape
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class NullShapeUDT extends UserDefinedType[NullShape] with GeometricUDT {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", IntegerType, nullable = false)
    )
  )

  override def serialize(nullShape: NullShape): InternalRow = {
    val row = new GenericInternalRow(1)
    row.setInt(0, nullShape.getType())
    row
  }

  override def serialize(shape: Shape): InternalRow = serialize(shape.asInstanceOf[NullShape])

  override def deserialize(datum: Any): NullShape = {
    val row = datum.asInstanceOf[InternalRow]
    require(row.numFields == 1)
    NullShape
  }

  override def userClass: Class[NullShape] = classOf[NullShape]

  override val geometryType: Int = NullShape.getType()

}
