/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan.catalyst

import org.apache.magellan.{NullShape, Shape}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Row}
import org.apache.spark.sql.types._

/**
 * A function that returns the intersection between the left and right shapes.
 * @param left
 * @param right
 */
case class Intersection(left: Expression, right: Expression)
  extends BinaryExpression {

  override type EvaluatedType = Shape

  override def symbol: String = nodeName

  override def toString: String = s"$nodeName($left, $right)"

  override def dataType: DataType = left.dataType

  override def eval(input: Row): Shape = {
    val leftEval = left.eval(input)
    if (leftEval == null) {
      NullShape
    } else {
      val rightEval = right.eval(input)
      val leftShape = Shape.deserialize(leftEval)
      val rightShape = Shape.deserialize(rightEval)
      if (rightEval == null) NullShape else leftShape.intersection(rightShape)
    }
  }

  override def nullable: Boolean = left.nullable || right.nullable

  protected def nullSafeEval(input1: Any, input2: Any): Any = {
    val leftShape = Shape.deserialize(input1)
    if (leftShape == null) {
      null
    } else {
      val rightShape = Shape.deserialize(input2)
      if (rightShape == null) null else rightShape.intersection(leftShape)
    }
  }

}