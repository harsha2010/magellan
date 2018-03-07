package org.apache.spark.sql.catalyst.expressions

import magellan._
import magellan.catalyst.MagellanExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._


case class MagellanSerializer(
    override val child: Expression,
    _dataType: DataType)
  extends UnaryExpression
  with MagellanExpression
  with CodegenFallback
  with NonSQLExpression {

  override def nullable: Boolean = false

  override protected def nullSafeEval(input: Any): Any = {
    val shape = input.asInstanceOf[Shape]
    serialize(shape)
  }

  override def dataType: DataType = _dataType
}

case class MagellanDeserializer(
    override val child: Expression, klass: Class[_ <: Shape])
  extends UnaryExpression
  with MagellanExpression
  with CodegenFallback
  with NonSQLExpression {

  override def nullable: Boolean = false

  override protected def nullSafeEval(input: Any): Any = {
    newInstance(input.asInstanceOf[InternalRow])
  }

  override def dataType: DataType = ObjectType(klass)
}
