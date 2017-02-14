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

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}

/**
 * Convert x and y coordinates to a `Point`
 *
 * @param left
 * @param right
 */
case class PointConverter(override val left: Expression,
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
