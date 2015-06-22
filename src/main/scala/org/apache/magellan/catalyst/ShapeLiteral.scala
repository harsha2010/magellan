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

import org.apache.magellan.Shape
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Row}
import org.apache.spark.sql.types.DataType

case class ShapeLiteral(shape: Shape) extends LeafExpression {

  override def foldable: Boolean = true

  override def nullable: Boolean = false

  type EvaluatedType = Shape

  override def eval(input: Row): Shape = shape

  override val dataType: DataType = Shape.TYPES(shape.shapeType)

}
