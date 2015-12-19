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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, BinaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * A function that returns the intersection between the left and right shapes.
 * @param left
 * @param right
 */
case class Intersection(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = left.dataType

  override def eval(input: InternalRow): Shape = {
    val leftEval = left.eval(input)
    if (leftEval == null) {
      NullShape
    } else {
      val rightEval = right.eval(input)
      val leftShape = leftEval.asInstanceOf[Shape]
      val rightShape = rightEval.asInstanceOf[Shape]
      if (rightEval == null) NullShape else leftShape.intersection(rightShape)
    }
  }

  override def nullable: Boolean = left.nullable || right.nullable
}

case class Buffer(left: Expression, right: Expression)
  extends BinaryExpression with CodegenFallback {

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = Polygon.EMPTY

  override def eval(input: InternalRow): Shape = {
    val leftEval = left.eval(input)
    if (leftEval == null) {
      NullShape
    } else {
      val rightEval = right.eval(input)
      val shape = leftEval.asInstanceOf[Shape]
      val distance = rightEval.asInstanceOf[Double]
      if (shape == null) NullShape else shape.buffer(distance)
    }
  }
}
