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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, UnaryExpression}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

case class Indexer(
    override val child: Expression,
    precision: Int)
  extends UnaryExpression with MagellanExpression {

  private val indexer = new ZOrderCurveIndexer()
  private val indexUDT = Indexer.indexUDT

  assert(precision % 5 == 0)

  protected override def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    val rows = ListBuffer[InternalRow]()
    indexer.indexWithMeta(shape, precision) foreach  {
      case (index: ZOrderCurve, relation: Relate) => {
        val row = new GenericInternalRow(2)
        row.update(0, indexUDT.serialize(index))
        row.update(1, UTF8String.fromString(relation.name()))
        rows.+= (row)
      }
    }

    new GenericArrayData(rows)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = Indexer.dataType

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childTypeVar = ctx.freshName("childType")
    val childShapeVar = ctx.freshName("childShape")
    val shapeSerializerVar = ctx.freshName("shapeSerializer")
    val indexerVar = ctx.freshName("indexer")
    val precisionVar = ctx.freshName("precision")
    val resultsVar = ctx.freshName("results")
    val indexSerializerVar = ctx.freshName("indexSerializer")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ; \n" +
        "serializers.put(1, new org.apache.spark.sql.types.PointUDT()); \n" +
        "serializers.put(2, new org.apache.spark.sql.types.LineUDT()); \n" +
        "serializers.put(3, new org.apache.spark.sql.types.PolyLineUDT()); \n" +
        "serializers.put(5, new org.apache.spark.sql.types.PolygonUDT()); \n" +
        "")

    val idx = ctx.references.length
    ctx.addReferenceObj(s"$indexerVar", indexer)
    ctx.addReferenceObj(s"$precisionVar", precision)
    ctx.addReferenceObj(s"$indexSerializerVar", new ZOrderCurveUDT)

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"magellan.index.ZOrderCurveIndexer $indexerVar = (magellan.index.ZOrderCurveIndexer)references[$idx]; \n" +
        s"Integer $childTypeVar = $c1.getInt(0); \n" +
        s"Integer $precisionVar = (Integer) references[$idx + 1]; \n" +
        s"org.apache.spark.sql.types.ZOrderCurveUDT $indexSerializerVar = " +
        s"  (org.apache.spark.sql.types.ZOrderCurveUDT) references[$idx + 2]; \n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $shapeSerializerVar = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"serializers.get($childTypeVar)); \n" +
        s"magellan.Shape $childShapeVar = (magellan.Shape)" +
        s"$shapeSerializerVar.deserialize($c1); \n" +
        s"java.util.List<scala.Tuple2<magellan.index.ZOrderCurve, String>> v =" +
        s" indexer.indexWithMetaAsJava($childShapeVar, $precisionVar); \n" +
        s"java.util.List<InternalRow> $resultsVar = new java.util.ArrayList<InternalRow>(v.size()); \n" +
        s"for(scala.Tuple2<magellan.index.ZOrderCurve, String> i : v) { \n" +
        s"  org.apache.spark.sql.catalyst.expressions.GenericInternalRow row =\n" +
        s"  new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(2);\n" +
        s"  row.update(0, $indexSerializerVar.serialize(i._1()));\n" +
        s"  row.update(1, org.apache.spark.unsafe.types.UTF8String.fromString((String)i._2())); \n" +
        s"  $resultsVar.add(row); \n" +
        s"}\n" +
        s"${ev.value} = new org.apache.spark.sql.catalyst.util.GenericArrayData($resultsVar); \n"
    })
  }
}

object Indexer {

  val indexUDT = new ZOrderCurveUDT()

  val dataType = ArrayType(new StructType()
    .add("curve", indexUDT, false)
    .add("relation", StringType, false))
}
