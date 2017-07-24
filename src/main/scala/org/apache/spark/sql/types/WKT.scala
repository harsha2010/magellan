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

import magellan.Shape
import magellan.catalyst.MagellanExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}

case class WKT(override val child: Expression)
  extends UnaryExpression with MagellanExpression{

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    val childTypeVar = ctx.freshName("childType")
    val childShapeVar = ctx.freshName("childShape")
    val serializerVar = ctx.freshName("serializer")

    val indexVar = ctx.freshName("index")
    val resultVar = ctx.freshName("result")

    val idx = ctx.references.length
    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"String text = ${c1}.toString();\n" +
        s"magellan.Shape $childShapeVar = (magellan.Shape) " +
        s"magellan.WKTParser.parseAll(text);\n" +
        s"Integer $childTypeVar = $childShapeVar.getType();\n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $serializerVar =" +
        s" (org.apache.spark.sql.types.UserDefinedType<magellan.Shape>) $serializersVar.get($childTypeVar);\n" +
        s"Integer $indexVar = -1; \n" +
        s"if ($childTypeVar == 1) {$indexVar = 0;}\n" +
        s"else if ($childTypeVar == 2 || $childTypeVar == 3) {$indexVar = 1;} \n" +
        s"else {$indexVar = 2;} \n" +
        s"org.apache.spark.sql.catalyst.expressions.GenericInternalRow $resultVar = " +
        s" new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(3);\n" +
        s"$resultVar.update($indexVar, $serializerVar.serialize($childShapeVar)); \n" +
        s"${ev.value} = $resultVar; \n"
    })
  }

  override def dataType: DataType = {
      StructType(List(StructField("point", new PointUDT(), true),
      StructField("polyline", new PolyLineUDT(), true),
      StructField("polygon", new PolygonUDT(), true)))
  }

}
