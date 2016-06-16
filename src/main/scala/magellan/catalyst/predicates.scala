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

package magellan.catalyst

import magellan._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, CodegenFallback, GeneratedExpressionCode}
import org.apache.spark.sql.types.{BooleanType, DataType, UserDefinedType}

import scala.collection.immutable.HashMap


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

    if (
        (lxmin <= rxmin && lxmax >= rxmin && lymin <= rymin && lymax >= rymin) ||
        (rxmin <= lxmin && rxmax >= lxmin && rymin <= lymin && rymax >= lymin)) {
      val leftShape = newInstance(leftRow)
      val rightShape = newInstance(rightRow)
      rightShape.intersects(leftShape)
    } else {
      false
    }
  }

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        "serializers.put(1, new magellan.PointUDT());" +
        "serializers.put(2, new magellan.LineUDT());" +
        "serializers.put(3, new magellan.PolyLineUDT());" +
        "serializers.put(5, new magellan.PolygonUDT());" +
        "")

    nullSafeCodeGen(ctx, ev, (c1, c2) => {
      s"" +
        s"Double lxmin = $c1.getDouble(1);" +
        s"Double lymin = $c1.getDouble(2);" +
        s"Double lxmax = $c1.getDouble(3);" +
        s"Double lymax = $c1.getDouble(4);" +
        s"Double rxmin = $c2.getDouble(1);" +
        s"Double rymin = $c2.getDouble(2);" +
        s"Double rxmax = $c2.getDouble(3);" +
        s"Double rymax = $c2.getDouble(4);" +
        s"Boolean intersects = false;" +
        s"if ((lxmin <= rxmin && lxmax >= rxmin && lymin <= rymin && lymax >= rymin) ||" +
        s"(rxmin <= lxmin && rxmax >= lxmin && rymin <= lymin && rymax >= lymin)) {" +
        s"Integer ltype = $c1.getInt(0);" +
        s"Integer rtype = $c2.getInt(0);" +
        s"magellan.Shape leftShape = (magellan.Shape)" +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"serializers.get(ltype)).deserialize($c1);" +
        s"magellan.Shape rightShape = (magellan.Shape)" +
        s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
        s"serializers.get(rtype)).deserialize($c2);" +
        s"intersects = rightShape.intersects(leftShape);" +
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

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    ctx.addMutableState(classOf[java.util.HashMap[Integer, UserDefinedType[Shape]]].getName, "serializers",
      "serializers = new java.util.HashMap<Integer, org.apache.spark.sql.types.UserDefinedType<magellan.Shape>>() ;" +
        "serializers.put(1, new magellan.PointUDT());" +
        "serializers.put(3, new magellan.PolyLineUDT());" +
        "serializers.put(5, new magellan.PolygonUDT());" +
      "")

    nullSafeCodeGen(ctx, ev, (c1, c2) => {
        s"" +
        s"Double lxmin = $c1.getDouble(1);" +
        s"Double lymin = $c1.getDouble(2);" +
        s"Double lxmax = $c1.getDouble(3);" +
        s"Double lymax = $c1.getDouble(4);" +
        s"Double rxmin = $c2.getDouble(1);" +
        s"Double rymin = $c2.getDouble(2);" +
        s"Double rxmax = $c2.getDouble(3);" +
        s"Double rymax = $c2.getDouble(4);" +
        s"Boolean within = false;" +
        s"if (rxmin <= lxmin && rymin <= lymin && rxmax >= lxmax && rymax >= lymax) {" +
        s"Integer ltype = $c1.getInt(0);" +
        s"Integer rtype = $c2.getInt(0);" +
        s"magellan.Shape leftShape = (magellan.Shape)" +
          s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
          s"serializers.get(ltype)).deserialize($c1);" +
        s"magellan.Shape rightShape = (magellan.Shape)" +
          s"((org.apache.spark.sql.types.UserDefinedType<magellan.Shape>)" +
          s"serializers.get(rtype)).deserialize($c2);" +
        s"within = rightShape.contains(leftShape);" +
        s"}" +
        s"${ev.value} = within;"
      })

  }
}

