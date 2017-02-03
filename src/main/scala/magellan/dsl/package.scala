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

package org.apache.spark.sql.magellan

import magellan._
import magellan.catalyst._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

package object dsl {
  trait ImplicitOperators {

    def expr: Expression

    def within(other: Expression): Expression = Within(expr, other)

    def within(other: Column): Column = Column(Within(expr, other.expr))

    def intersects(other: Expression): Expression = Intersects(expr, other)

    def >?(other: Expression): Expression = Within(other, expr)

    def >?(other: Column): Column = Column(Within(other.expr, expr))

    def apply(other: Any): Expression = GetMapValue(expr, lit(other).expr)

    def apply(other: Expression): Expression = GetMapValue(expr, other)

    def transform(fn: Point => Point) = Transformer(expr, fn)

  }

  trait ExpressionConversions {

    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit class DslColumn(c: Column) {
      def col: Column = c

      def within(other: Column): Column = Column(Within(col.expr, other.expr))

      def intersects(other: Column): Column = Column(Intersects(c.expr, other.expr))

      def >?(other: Column): Column = Column(Within(other.expr, col.expr))

      def >?(other: Expression): Column = Column(Within(other, col.expr))

      def apply(other: Any): Column = Column(GetMapValue(col.expr, lit(other).expr))

      def apply(other: Expression): Column = Column(GetMapValue(col.expr, other))

      def transform(fn: Point => Point): Column = Column(Transformer(c.expr, fn))

    }

    implicit def point(x: Expression, y: Expression) = PointConverter(x, y)

    implicit def point(x: Column, y: Column) = Column(PointConverter(x.expr, y.expr))

  }


  object expressions extends ExpressionConversions  // scalastyle:ignore

}

