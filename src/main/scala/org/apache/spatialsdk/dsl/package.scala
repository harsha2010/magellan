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

package org.apache.spark.sql.spatialsdk

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spatialsdk.catalyst.{PointConverter, GetMapValue, Within}

package object dsl {
  trait ImplicitOperators {

    def expr: Expression

    def within(other: Expression): Expression = Within(expr, other)

    def within(other: Any): Column = Column(Within(expr, lit(other).expr))

    def apply(other: Any): Expression = GetMapValue(expr, lit(other).expr)

    def apply(other: Expression): Expression = GetMapValue(expr, other)

  }

  trait ExpressionConversions {

    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit class DslColumn(c: Column) {
      def col: Column = c

      def within(other: Any): Column = Column(Within(lit(c).expr, lit(other).expr))

      def apply(other: Any): Column = Column(GetMapValue(col.expr, lit(other).expr))

      def apply(other: Expression): Column = Column(GetMapValue(col.expr, other))

    }

    implicit def point(x: Double, y: Double): Expression = PointConverter(lit(x).expr, lit(y).expr)

    implicit def point(x: Expression, y: Expression) = PointConverter(x, y)

    implicit def point(x: Column, y: Column) = Column(PointConverter(x.expr, y.expr))

  }

  object expressions extends ExpressionConversions  // scalastyle:ignore

}
