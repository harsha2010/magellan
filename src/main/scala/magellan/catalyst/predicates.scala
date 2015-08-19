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

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Row}
import org.apache.spark.sql.types.{BooleanType, DataType}

import magellan.Shape

/**
 * A function that returns true if the shape `left` is within the shape `right`.
 */
case class Within(left: Expression, right: Expression)
  extends BinaryExpression {

  override type EvaluatedType = Any

  override def symbol: String = nodeName

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = BooleanType

  override def eval(input: Row): Any = {
    val leftEval = left.eval(input)
    if (leftEval == null) {
      null
    } else {
      val rightEval = right.eval(input)
      val leftShape = leftEval.asInstanceOf[Shape]
      val rightShape = rightEval.asInstanceOf[Shape]
      if (rightEval == null) null else rightShape.contains(leftShape)
    }
  }

  override def nullable: Boolean = left.nullable || right.nullable

  protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val leftShape = input1.asInstanceOf[Shape]
    if (leftShape == null) {
      null
    } else {
      val rightShape = input2.asInstanceOf[Shape]
      if (rightShape == null) null else rightShape.contains(leftShape)
    }
  }
}

/**
 * A function that returns the number of times the left shape intersects the right line.
 * @param left
 * @param right
 */
case class Intersects(left: Expression, right: Expression)
  extends BinaryExpression {

  override type EvaluatedType = Boolean

  override def symbol: String = nodeName

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = BooleanType

  override def eval(input: Row): Boolean = {
    val leftEval = left.eval(input)
    if (leftEval == null) {
      false
    } else {
      val rightEval = right.eval(input)
      val leftShape = leftEval.asInstanceOf[Shape]
      val rightShape = rightEval.asInstanceOf[Shape]
      if (rightEval == null) false else leftShape.intersects(rightShape)
    }
  }

  override def nullable: Boolean = left.nullable || right.nullable

  protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val leftShape = input1.asInstanceOf[Shape]
    if (leftShape == null) {
      null
    } else {
      val rightShape = input2.asInstanceOf[Shape]
      if (rightShape == null) null else rightShape.intersects(leftShape)
    }
  }

}
