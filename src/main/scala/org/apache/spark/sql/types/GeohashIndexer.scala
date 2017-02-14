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

import magellan.catalyst.MagellanExpression
import magellan.index.ZOrderCurveIndexer
import magellan.{BoundingBox, Shape}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}

case class GeohashIndexer(
    override val child: Expression,
    precision: Int)
  extends UnaryExpression with MagellanExpression {

  private val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))

  assert(precision % 5 == 0)

  protected override def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    indexer.index(shape, precision).map(_.toBase32())
  }

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(StringType)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ; \n" +
        "serializers.put(1, new org.apache.spark.sql.types.PointUDT()); \n" +
        "serializers.put(2, new org.apache.spark.sql.types.LineUDT()); \n" +
        "serializers.put(3, new org.apache.spark.sql.types.PolyLineUDT()); \n" +
        "serializers.put(5, new org.apache.spark.sql.types.PolygonUDT()); \n" +
        "")

    val idx = ctx.references.length
    ctx.addReferenceObj("indexer", indexer)
    ctx.addReferenceObj("precision", precision)

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"magellan.index.Indexer indexer = (magellan.index.Indexer)references[$idx]; \n" +
        s"Integer childType = $c1.getInt(0); \n" +
        s"Integer precision = (Integer) references[$idx + 1]; \n" +
        "org.apache.spark.sql.types.UserDefinedType<magellan.Shape> serializer = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"serializers.get(childType)); \n" +
        s"magellan.Shape childShape = (magellan.Shape)" +
        s"serializer.deserialize($c1); \n" +
        s"java.util.Collection<magellan.index.Index> indices = indexer.indexAsJava(childShape, precision); \n" +
        s"java.util.List<String> codes = new java.util.ArrayList<String>(); \n" +
        s"for (magellan.index.Index index : indices) { \n" +
        s"  codes.add(org.apache.spark.unsafe.types.UTF8String.fromString(index.toBase32())) ; \n" +
        s"} \n" +
        s"${ev.value} = new org.apache.spark.sql.catalyst.util.GenericArrayData(codes); \n"
    })
  }
}
