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

package org.apache.spark.sql.catalyst.expressions

import magellan.catalyst.MagellanExpression
import magellan.index.{ZOrderCurve, ZOrderCurveIndexer}
import magellan.{GeoJSON, Point, Relate, Shape}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ListBuffer

/**
  * Convert x and y coordinates to a `Point`
  *
  * @param left
  * @param right
  */
case class PointConverter(
    override val left: Expression,
    override val right: Expression) extends BinaryExpression {

  override def nullable: Boolean = false

  override val dataType = new PointUDT

  override def nullSafeEval(leftEval: Any, rightEval: Any): Any = {
    val x = leftEval.asInstanceOf[Double]
    val y = rightEval.asInstanceOf[Double]
    dataType.serialize(x, y)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ctx.addMutableState(classOf[PointUDT].getName, "pointUDT", "pointUDT = new org.apache.spark.sql.types.PointUDT();")
    defineCodeGen(ctx, ev, (c1, c2) => s"pointUDT.serialize($c1, $c2)")
  }
}

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

/**
  * Extracts shapes from WKT Text.
  *
  * @param child
  */
case class WKT(override val child: Expression)
  extends UnaryExpression with MagellanExpression {

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

/**
  * Geohash Indexes a given shape expression to a specified precision.
  *
  * @param child
  * @param precision
  */
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
    val indexVar = ctx.freshName("v")

    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
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
        s"$serializersVar.get($childTypeVar)); \n" +
        s"magellan.Shape $childShapeVar = (magellan.Shape)" +
        s"$shapeSerializerVar.deserialize($c1); \n" +
        s"java.util.List<scala.Tuple2<magellan.index.ZOrderCurve, String>> $indexVar =" +
        s" $indexerVar.indexWithMetaAsJava($childShapeVar, $precisionVar); \n" +
        s"java.util.List<InternalRow> $resultsVar = new java.util.ArrayList<InternalRow>($indexVar.size()); \n" +
        s"for(scala.Tuple2<magellan.index.ZOrderCurve, String> i : $indexVar) { \n" +
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

case class AsGeoJSON(override val child: Expression)
  extends UnaryExpression with MagellanExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override protected def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    val json = GeoJSON.writeJson(shape)
    org.apache.spark.unsafe.types.UTF8String.fromString(json)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childTypeVar = ctx.freshName("childType")
    val childShapeVar = ctx.freshName("childShape")
    val shapeSerializerVar = ctx.freshName("shapeSerializer")
    val resultVar = ctx.freshName("result")
    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"Integer $childTypeVar = $c1.getInt(0); \n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $shapeSerializerVar = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"$serializersVar.get($childTypeVar)); \n" +
        s"magellan.Shape $childShapeVar = (magellan.Shape)" +
        s"$shapeSerializerVar.deserialize($c1); \n" +
        s"java.lang.String $resultVar = magellan.GeoJSON.writeJson($childShapeVar); \n" +
        s"${ev.value} = org.apache.spark.unsafe.types.UTF8String.fromString($resultVar); \n"
    })
  }
}

case class Buffer(override val child: Expression, distance: Double)
  extends UnaryExpression with MagellanExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = new PolygonUDT()

  override protected def nullSafeEval(input: Any): Any = {
    val shape = newInstance(input.asInstanceOf[InternalRow])
    val bufferedShape = shape.buffer(distance)
    serialize(bufferedShape)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childTypeVar = ctx.freshName("childType")
    val childShapeVar = ctx.freshName("childShape")
    val shapeSerializerVar = ctx.freshName("shapeSerializer")
    val resultSerializerVar = ctx.freshName("resultSerializer")
    val distanceVar = ctx.freshName("distance")
    val resultVar = ctx.freshName("result")
    val resultTypeVar = ctx.freshName("resultType")
    val serializersVar = ctx.freshName("serializers")
    val idx = ctx.references.length
    ctx.addReferenceObj("distance", distance);

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    nullSafeCodeGen(ctx, ev, (c1) => {
      s"" +
        s"Integer $childTypeVar = $c1.getInt(0); \n" +
        s"Double $distanceVar = (Double) references[$idx]; \n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $shapeSerializerVar = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"$serializersVar.get($childTypeVar)); \n" +
        s"magellan.Shape $childShapeVar = (magellan.Shape)" +
        s"$shapeSerializerVar.deserialize($c1); \n" +
        s"magellan.Shape $resultVar = $childShapeVar.buffer($distanceVar); \n" +
        s"Integer $resultTypeVar = $resultVar.getType(); \n" +
        s"org.apache.spark.sql.types.UserDefinedType<magellan.Shape> $resultSerializerVar = " +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"$serializersVar.get($resultTypeVar)); \n" +
        s"${ev.value} = (org.apache.spark.sql.catalyst.InternalRow)$resultSerializerVar.serialize($resultVar); \n"
    })

  }
}

object Indexer {

  val indexUDT = new ZOrderCurveUDT()

  val dataType = ArrayType(new StructType()
    .add("curve", indexUDT, false)
    .add("relation", StringType, false))
}
