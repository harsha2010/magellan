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

import magellan.BoundingBox
import magellan.catalyst.MagellanExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}

/**
  * A function that returns true if the Shape defined by the child expression
  * lies within the bounding box.
  *
  * @param child
  * @param boundingBox
  */
case class PointInRange(child: Expression, boundingBox: BoundingBox)
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
