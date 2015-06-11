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

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Row, UnaryExpression}
import org.apache.spark.sql.types.{BooleanType, IntegerType, DataType, MapType}
import org.apache.spatialsdk.{Line, Shape}

/**
 * Extract value out of map by key or return null if key does not exist
 * 
 * @param child
 * @param key
 */
case class GetMapValue(child: Expression, key: Expression) extends UnaryExpression {

  override type EvaluatedType = Any

  override def foldable: Boolean = child.foldable && key.foldable
  override def toString: String = s"$child[$key]"
  override def children: Seq[Expression] = child :: key :: Nil

  override def eval(input: Row): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      val o = key.eval(input)
      if (o == null) {
        null
      } else {
        evalNotNull(value, o)
      }
    }
  }

  protected def evalNotNull(value: Any, key: Any) = {
    val map = value.asInstanceOf[Map[Any, _]]
    map.get(key).orNull
  }

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType.asInstanceOf[MapType].valueType

}
