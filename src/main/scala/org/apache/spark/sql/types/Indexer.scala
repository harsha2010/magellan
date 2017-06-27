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
import magellan.index.{ZOrderCurve, ZOrderCurveIndexer}
import magellan.{Relate, Shape}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}

import scala.collection.mutable.ListBuffer

case class Indexer(
    override val child: Expression,
    precision: Int)
  extends UnaryExpression with MagellanExpression {

  private val indexer = new ZOrderCurveIndexer()

  assert(precision % 5 == 0)

  protected override def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    val curves = new ListBuffer[ZOrderCurve]()
    val relations = new ListBuffer[String]()

    indexer.indexWithMeta(shape, precision) foreach {
      case (index: ZOrderCurve, relation: Relate) =>
        curves.+= (index)
        relations.+= (relation.name())
    }

    Row.fromSeq(Seq(curves, relations))
  }

  override def nullable: Boolean = false

  override def dataType: DataType = StructType(List(
    StructField("curve", ArrayType(new ZOrderCurveUDT()), false),
    StructField("relation", ArrayType(StringType), false)
  ))

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
    ctx.addReferenceObj("zordercurveudt", new ZOrderCurveUDT)

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"magellan.index.ZOrderCurveIndexer indexer = (magellan.index.ZOrderCurveIndexer)references[$idx]; \n" +
        s"Integer childType = $c1.getInt(0); \n" +
        s"Integer precision = (Integer) references[$idx + 1]; \n" +
        s"org.apache.spark.sql.types.ZOrderCurveUDT udt = " +
        s"  (org.apache.spark.sql.types.ZOrderCurveUDT) references[$idx + 2]; \n" +
        "org.apache.spark.sql.types.UserDefinedType<magellan.Shape> serializer = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"serializers.get(childType)); \n" +
        s"magellan.Shape childShape = (magellan.Shape)" +
        s"serializer.deserialize($c1); \n" +
        s"scala.Tuple2<java.util.Collection<ZOrderCurve>, java.util.Collection<String>> v =" +
        s" indexer.indexWithMetaAsJava(childShape, precision); \n" +
        s"java.util.List<magellan.index.ZOrderCurve> curves = (java.util.List<magellan.index.ZOrderCurve>)v._1(); \n" +
        s"java.util.List<String> relations = (java.util.List<String>)v._2(); \n" +
        s"java.util.List<InternalRow> c = new java.util.ArrayList<InternalRow>(); \n" +
        s"java.util.List<org.apache.spark.unsafe.types.UTF8String> r = " +
        s"  new java.util.ArrayList<org.apache.spark.unsafe.types.UTF8String>(); \n" +
        s"for(magellan.index.ZOrderCurve curve : curves) { \n" +
        s"  c.add(udt.serialize(curve));\n" +
        s"}\n" +
        s"for(String relation : relations) { \n" +
        s"  r.add(org.apache.spark.unsafe.types.UTF8String.fromString(relation));\n" +
        s"}\n" +
        s"org.apache.spark.sql.catalyst.expressions.GenericInternalRow row = " +
        s" new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(2);\n" +
        s"row.update(0, new org.apache.spark.sql.catalyst.util.GenericArrayData(c));\n" +
        s"row.update(1, new org.apache.spark.sql.catalyst.util.GenericArrayData(r));\n" +
        s"${ev.value} = row; \n"
    })
  }
}
