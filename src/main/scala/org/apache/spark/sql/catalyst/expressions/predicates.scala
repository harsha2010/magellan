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

import magellan._
import magellan.catalyst._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{BooleanType, DataType, UserDefinedType}


/**
  * A function that returns the intersection between the left and right shapes.
  * @param left
  * @param right
  */
case class Intersects(left: Expression, right: Expression)
  extends BinaryExpression with MagellanExpression {

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = left.nullable || right.nullable

  override protected def nullSafeEval(leftEval: Any, rightEval: Any): Any = {

    val leftRow = leftEval.asInstanceOf[InternalRow]
    val rightRow = rightEval.asInstanceOf[InternalRow]

    // check if the right bounding box intersects left bounding box.
    val ((lxmin, lymin), (lxmax, lymax)) = (
      (leftRow.getDouble(1), leftRow.getDouble(2)),
      (leftRow.getDouble(3), leftRow.getDouble(4))
      )

    val ((rxmin, rymin), (rxmax, rymax)) = (
      (rightRow.getDouble(1), rightRow.getDouble(2)),
      (rightRow.getDouble(3), rightRow.getDouble(4))
      )

    if (!(rxmax < lxmin || rxmin > lxmax) &&
        !(rymax < lymin || rymin > lymax)) {
      val leftShape = newInstance(leftRow)
      val rightShape = newInstance(rightRow)
      rightShape.intersects(leftShape)
    } else {
      false
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    val lxminVar = ctx.freshName("lxmin")
    val lyminVar = ctx.freshName("lymin")
    val lxmaxVar = ctx.freshName("lxmax")
    val lymaxVar = ctx.freshName("lymax")

    val rxminVar = ctx.freshName("rxmin")
    val ryminVar = ctx.freshName("rymin")
    val rxmaxVar = ctx.freshName("rxmax")
    val rymaxVar = ctx.freshName("rymax")

    val ltypeVar = ctx.freshName("ltype")
    val rtypeVar = ctx.freshName("rtype")

    val leftShapeVar = ctx.freshName("leftShape")
    val rightShapeVar = ctx.freshName("rightShape")

    nullSafeCodeGen(ctx, ev, (c1, c2) => {
      s"" +
        s"Double $lxminVar = $c1.getDouble(1);" +
        s"Double $lyminVar = $c1.getDouble(2);" +
        s"Double $lxmaxVar = $c1.getDouble(3);" +
        s"Double $lymaxVar = $c1.getDouble(4);" +
        s"Double $rxminVar = $c2.getDouble(1);" +
        s"Double $ryminVar = $c2.getDouble(2);" +
        s"Double $rxmaxVar = $c2.getDouble(3);" +
        s"Double $rymaxVar = $c2.getDouble(4);" +
        s"Boolean intersects = false;" +
        s"if(!($rxmaxVar < $lxminVar || $rxminVar > $lxmaxVar) && !($rymaxVar < $lyminVar || $ryminVar > $lymaxVar)) {" +
        s"Integer $ltypeVar = $c1.getInt(0);" +
        s"Integer $rtypeVar = $c2.getInt(0);" +
        s"magellan.Shape $leftShapeVar = (magellan.Shape)" +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"$serializersVar.get($ltypeVar)).deserialize($c1);" +
        s"magellan.Shape $rightShapeVar = (magellan.Shape)" +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"$serializersVar.get($rtypeVar)).deserialize($c2);" +
        s"intersects = $rightShapeVar.intersects($leftShapeVar);" +
        s"}" +
        s"${ev.value} = intersects;"
    })
  }
}

/**
 * A function that returns true if the shape `left` is within the shape `right`.
 */
case class Within(left: Expression, right: Expression)
  extends BinaryExpression with MagellanExpression {

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = BooleanType

  override def nullSafeEval(leftEval: Any, rightEval: Any): Any = {
    val leftRow = leftEval.asInstanceOf[InternalRow]
    val rightRow = rightEval.asInstanceOf[InternalRow]

    // check if the right bounding box contains left bounding box.
    val ((lxmin, lymin), (lxmax, lymax)) = (
        (leftRow.getDouble(1), leftRow.getDouble(2)),
        (leftRow.getDouble(3), leftRow.getDouble(4))
      )

    val ((rxmin, rymin), (rxmax, rymax)) = (
      (rightRow.getDouble(1), rightRow.getDouble(2)),
      (rightRow.getDouble(3), rightRow.getDouble(4))
      )

    if (rxmin <= lxmin && rymin <= lymin && rxmax >= lxmax && rymax >= lymax) {
      val leftShape = newInstance(leftRow)
      val rightShape = newInstance(rightRow)
      rightShape.contains(leftShape)
    } else {
      false
    }

  }

  override def nullable: Boolean = left.nullable || right.nullable

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val serializersVar = ctx.freshName("serializers")

    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, s"$serializersVar",
      s"$serializersVar = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        s"$serializersVar.put(1, new org.apache.spark.sql.types.PointUDT());" +
        s"$serializersVar.put(2, new org.apache.spark.sql.types.LineUDT());" +
        s"$serializersVar.put(3, new org.apache.spark.sql.types.PolyLineUDT());" +
        s"$serializersVar.put(5, new org.apache.spark.sql.types.PolygonUDT());" +
        "")

    val lxminVar = ctx.freshName("lxmin")
    val lyminVar = ctx.freshName("lymin")
    val lxmaxVar = ctx.freshName("lxmax")
    val lymaxVar = ctx.freshName("lymax")

    val rxminVar = ctx.freshName("rxmin")
    val ryminVar = ctx.freshName("rymin")
    val rxmaxVar = ctx.freshName("rxmax")
    val rymaxVar = ctx.freshName("rymax")

    val ltypeVar = ctx.freshName("ltype")
    val rtypeVar = ctx.freshName("rtype")

    val leftShapeVar = ctx.freshName("leftShape")
    val rightShapeVar = ctx.freshName("rightShape")

    nullSafeCodeGen(ctx, ev, (c1, c2) => {
        s"" +
        s"Double $lxminVar = $c1.getDouble(1);" +
        s"Double $lyminVar = $c1.getDouble(2);" +
        s"Double $lxmaxVar = $c1.getDouble(3);" +
        s"Double $lymaxVar = $c1.getDouble(4);" +
        s"Double $rxminVar = $c2.getDouble(1);" +
        s"Double $ryminVar = $c2.getDouble(2);" +
        s"Double $rxmaxVar = $c2.getDouble(3);" +
        s"Double $rymaxVar = $c2.getDouble(4);" +
        s"Boolean within = false;" +
        s"if ($rxminVar <= $lxminVar && $ryminVar <= $lyminVar && $rxmaxVar >= $lxmaxVar && $rymaxVar >= $lymaxVar) {" +
        s"Integer $ltypeVar = $c1.getInt(0);" +
        s"Integer $rtypeVar = $c2.getInt(0);" +
        s"magellan.Shape $leftShapeVar = (magellan.Shape)" +
          s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
          s"$serializersVar.get($ltypeVar)).deserialize($c1);" +
        s"magellan.Shape $rightShapeVar = (magellan.Shape)" +
          s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
          s"$serializersVar.get($rtypeVar)).deserialize($c2);" +
        s"within = $rightShapeVar.contains($leftShapeVar);" +
        s"}" +
        s"${ev.value} = within;"
      })

  }
}

/**
  * A function that returns true if the Shape defined by the child expression
  * lies within the bounding box.
  *
  * @param child
  * @param boundingBox
  */
case class WithinRange(child: Expression, boundingBox: BoundingBox)
  extends UnaryExpression with MagellanExpression {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = child.nullable

  override def nullSafeEval(leftEval: Any): Any = {
    val leftRow = leftEval.asInstanceOf[InternalRow]

    // check if the bounding box contains the child's bounding box.
    val ((lxmin, lymin), (lxmax, lymax)) = (
      (leftRow.getDouble(1), leftRow.getDouble(2)),
      (leftRow.getDouble(3), leftRow.getDouble(4))
    )

    val childBoundingBox = BoundingBox(lxmin, lymin, lxmax, lymax)
    boundingBox.contains(childBoundingBox)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val lxminVar = ctx.freshName("lxmin")
    val lyminVar = ctx.freshName("lymin")
    val lxmaxVar = ctx.freshName("lxmax")
    val lymaxVar = ctx.freshName("lymax")

    val ltypeVar = ctx.freshName("ltype")


    val idx = ctx.references.length
    ctx.addReferenceObj("boundingBox", boundingBox)

    val boundingBoxVar = ctx.freshName("boundingBox")
    val otherBoundingBoxVar = ctx.freshName("boundingBox")

    nullSafeCodeGen(ctx, ev, c1 => {
      s"" +
        s"Double $lxminVar = $c1.getDouble(1);" +
        s"Double $lyminVar = $c1.getDouble(2);" +
        s"Double $lxmaxVar = $c1.getDouble(3);" +
        s"Double $lymaxVar = $c1.getDouble(4);" +
        s"magellan.BoundingBox $boundingBoxVar = (magellan.BoundingBox)references[$idx];" +
        s"magellan.BoundingBox $otherBoundingBoxVar = new magellan.BoundingBox($lxminVar, $lyminVar, $lxmaxVar, $lymaxVar);" +
        s"${ev.value} = $boundingBoxVar.contains($otherBoundingBoxVar);"
    })
  }
}

/**
  * An Expression that returns true if the shape is within a circle of
  * prescribed radius around the given point.
  *
  * @param child
  * @param point
  * @param radius
  */
case class WithinCircleRange(child: Expression, point: Point, radius: Double)
  extends UnaryExpression with MagellanExpression {

  override def dataType: DataType = BooleanType

  override def nullable: Boolean = child.nullable

  override def nullSafeEval(leftEval: Any): Any = {
    val leftRow = leftEval.asInstanceOf[InternalRow]

    // check if the bounding box intersects the given circle.
    val ((lxmin, lymin), (lxmax, lymax)) = (
      (leftRow.getDouble(1), leftRow.getDouble(2)),
      (leftRow.getDouble(3), leftRow.getDouble(4))
    )

    val childBoundingBox = BoundingBox(lxmin, lymin, lxmax, lymax)
    childBoundingBox.withinCircle(point, radius)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val lxminVar = ctx.freshName("lxmin")
    val lyminVar = ctx.freshName("lymin")
    val lxmaxVar = ctx.freshName("lxmax")
    val lymaxVar = ctx.freshName("lymax")

    val ltypeVar = ctx.freshName("ltype")


    val idx = ctx.references.length
    ctx.addReferenceObj("point", point)
    ctx.addReferenceObj("radius", radius)

    val originVar = ctx.freshName("origin")
    val radiusVar = ctx.freshName("radius")
    val otherBoundingBoxVar = ctx.freshName("boundingBox")

    nullSafeCodeGen(ctx, ev, c1 => {
      s"" +
        s"Double $lxminVar = $c1.getDouble(1);" +
        s"Double $lyminVar = $c1.getDouble(2);" +
        s"Double $lxmaxVar = $c1.getDouble(3);" +
        s"Double $lymaxVar = $c1.getDouble(4);" +
        s"magellan.Point $originVar = (magellan.Point)references[$idx];" +
        s"Double $radiusVar = (Double)references[$idx + 1];" +
        s"magellan.BoundingBox $otherBoundingBoxVar = new magellan.BoundingBox($lxminVar, $lyminVar, $lxmaxVar, $lymaxVar);" +
        s"${ev.value} = $otherBoundingBoxVar.withinCircle($originVar, $radiusVar);"
    })
  }
}
