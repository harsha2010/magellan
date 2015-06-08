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

import org.apache.spatialsdk
import org.apache.spatialsdk.PointUDT

/**
 * Convert x and y coordinates to a `Point`
 * 
 * @param x
 * @param y
 */
case class PointConverter(x: Double, y: Double) extends LeafExpression {

  private lazy val value = new spatialsdk.Point(x, y)

  override def foldable: Boolean = true

  override def nullable: Boolean = false

  override def toString: String = "(%.3d, %.3d)".format(x, y)

  override def eval(input: Row): spatialsdk.Point = value

  override type EvaluatedType = spatialsdk.Point

  override val dataType: DataType = new PointUDT

}
