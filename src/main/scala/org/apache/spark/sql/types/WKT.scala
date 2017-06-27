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
    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ; \n" +
        "serializers.put(1, new org.apache.spark.sql.types.PointUDT()); \n" +
        "serializers.put(2, new org.apache.spark.sql.types.LineUDT()); \n" +
        "serializers.put(3, new org.apache.spark.sql.types.PolyLineUDT()); \n" +
        "serializers.put(5, new org.apache.spark.sql.types.PolygonUDT()); \n" +
        "")

    val idx = ctx.references.length
    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"String text = ${c1}.toString();\n" +
        s"magellan.Shape shape = (magellan.Shape) " +
        s"magellan.WKTParser.parseAll(magellan.WKTParser.expr(), text).get();\n" +
        s"Integer t = shape.getType();\n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> serializer =" +
        s" (org.apache.spark.sql.types.UserDefinedType<magellan.Shape>) serializers.get(t);\n" +
        s"Integer index = -1; \n" +
        s"if (t == 1) {index = 0;}\n" +
        s"else if (t == 2 || t == 3) {index = 1;} \n" +
        s"else {index = 2;} \n" +
        s"org.apache.spark.sql.catalyst.expressions.GenericInternalRow row = " +
        s" new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(3);\n" +
        s"row.update(index, serializer.serialize(shape)); \n" +
        s"${ev.value} = row; \n"
    })
  }

  override def dataType: DataType = {
      StructType(List(StructField("point", new PointUDT(), true),
      StructField("polyline", new PolyLineUDT(), true),
      StructField("polygon", new PolygonUDT(), true)))
  }

}
