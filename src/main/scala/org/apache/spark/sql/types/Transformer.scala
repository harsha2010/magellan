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
    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    val shapeVar = ctx.freshName("shape")
    val childTypeVar = ctx.freshName("childType")
    val childShapeVar = ctx.freshName("childShape")
    val serializerVar = ctx.freshName("serializer")


    val idx = ctx.references.length
    ctx.addReferenceObj("fn", fn);

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
      s"Integer $childTypeVar = $c1.getInt(0); \n" +
      s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $serializerVar = " +
      s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
      s"$serializersVar.get($childTypeVar)); \n" +
      s"magellan.Shape $childShapeVar = (magellan.Shape)" +
      s"$serializerVar.deserialize($c1); \n" +
      s"magellan.Shape $shapeVar = $childShapeVar.transform((scala.Function1<magellan.Point>)references[$idx]); \n" +
      s"${ev.value} = (org.apache.spark.sql.catalyst.InternalRow)$serializerVar.serialize($shapeVar); \n"
    })
  }

}
