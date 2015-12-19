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

package object dsl {
  trait ImplicitOperators {

    def expr: Expression

    def within(other: Expression): Expression = Within(expr, other)

    def within(other: Column): Column = Column(Within(expr, other.expr))

    def within(other: Shape): Column = Column(Within(expr, ShapeLiteral(other)))

    def >?(other: Expression): Expression = Within(other, expr)

    def >?(other: Shape): Expression = Within(ShapeLiteral(other), expr)

    def >?(other: Column): Column = Column(Within(other.expr, expr))

    def apply(other: Any): Expression = GetMapValue(expr, lit(other).expr)

    def apply(other: Expression): Expression = GetMapValue(expr, other)

    def intersects(other: Expression): Expression = Intersects(expr, other)

    def intersects(other: Shape): Column = Column(Intersects(expr, ShapeLiteral(other)))

    def intersection(other: Expression): Expression = Intersection(expr, other)

    def intersection(other: Shape): Column = Column(Intersection(expr, ShapeLiteral(other)))

    def transform(fn: Point => Point) = Transformer(expr, fn)

    def buffer(other: Expression): Expression = Buffer(expr, other)

    def buffer(distance: Double): Column = Column(Buffer(expr, Literal(distance)))

  }

  trait ExpressionConversions {

    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit class DslColumn(c: Column) {
      def col: Column = c

      def within(other: Column): Column = Column(Within(col.expr, other.expr))

      def within(other: Shape): Column = Column(Within(col.expr, ShapeLiteral(other)))

      def >?(other: Shape): Column = Column(Within(ShapeLiteral(other), col.expr))

      def >?(other: Column): Column = Column(Within(other.expr, col.expr))

      def >?(other: Expression): Column = Column(Within(other, col.expr))

      def apply(other: Any): Column = Column(GetMapValue(col.expr, lit(other).expr))

      def apply(other: Expression): Column = Column(GetMapValue(col.expr, other))

      def intersects(other: Column): Column = Column(Intersects(c.expr, other.expr))

      def intersects(other: Shape): Column = Column(Intersects(c.expr, ShapeLiteral(other)))

      def intersection(other: Column): Column = Column(Intersection(c.expr, other.expr))

      def intersection(other: Shape): Column = Column(Intersection(c.expr, ShapeLiteral(other)))

      def intersection(other: Expression): Column = Column(Intersection(c.expr, other))

      def transform(fn: Point => Point): Column = Column(Transformer(c.expr, fn))

      def buffer(distance: Double): Column = Column(Buffer(c.expr, Literal(distance)))

      def buffer(other: Expression): Column = Column(Buffer(c.expr, other))

    }

    implicit def point(x: Double, y: Double): Expression = ShapeLiteral(new Point(x, y))

    implicit def point(x: Expression, y: Expression) = PointConverter(x, y)

    implicit def point(x: Column, y: Column) = Column(PointConverter(x.expr, y.expr))

    implicit def shape(shape: Shape) = ShapeLiteral(shape)
  }


  object expressions extends ExpressionConversions  // scalastyle:ignore

}

