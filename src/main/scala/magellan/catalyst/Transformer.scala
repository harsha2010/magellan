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

import magellan._
import magellan.catalyst._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}

case class Transformer(
    override val child: Expression,
    fn: Point => Point)
  extends UnaryExpression with MagellanExpression {

  protected override def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    serialize(shape.transform(fn))
  }

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        "serializers.put(1, new org.apache.spark.sql.types.PointUDT());" +
        "serializers.put(2, new org.apache.spark.sql.types.LineUDT());" +
        "serializers.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        "serializers.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    val idx = ctx.references.length
    ctx.addReferenceObj("fn", fn);

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
      s"Integer childType = $c1.getInt(0); \n" +
      "org.apache.spark.sql.types.UserDefinedType<magellan.Shape> serializer = " +
      s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
      s"serializers.get(childType)); \n" +
      s"magellan.Shape childShape = (magellan.Shape)" +
      s"serializer.deserialize($c1); \n" +
      s"magellan.Shape shape = childShape.transform((scala.Function1<magellan.Point>)references[$idx]); \n" +
      s"${ev.value} = (org.apache.spark.sql.catalyst.InternalRow)serializer.serialize(shape); \n"
    })
  }

}
