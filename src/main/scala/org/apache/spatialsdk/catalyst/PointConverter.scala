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

package org.apache.spatialsdk.catalyst

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType
import org.apache.spatialsdk.{Point, PointUDT}

/**
 * Convert x and y coordinates to a `Point`
 *
 * @param left
 * @param right
 */
case class PointConverter(override val left: Expression,
    override val right: Expression) extends BinaryExpression {


  override def nullable: Boolean = false

  override def eval(input: Row): Point = {
    val x = left.eval(input).asInstanceOf[Double]
    val y = right.eval(input).asInstanceOf[Double]
    new Point(x, y)
  }

  override type EvaluatedType = Point

  override val dataType: DataType = new PointUDT

  override def symbol: String = "point"
}
