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

import magellan.catalyst.SpatialJoinHint
import magellan.{BoundingBox, Point}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.{Column, Dataset}

package object dsl {

  trait ExpressionConversions {

    implicit class DslExpression(e: Expression) {
      def expr: Expression = e
    }

    implicit class DslColumn(c: Column) {
      def col: Column = c

      def within(other: Column): Column = Column(Within(col.expr, other.expr))

      def intersects(other: Column): Column = Column(Intersects(c.expr, other.expr))

      def >?(other: Column): Column = Column(Within(other.expr, col.expr))

      def transform(fn: Point => Point): Column = Column(Transformer(c.expr, fn))

      def index(precision: Int): Column = Column(Indexer(c.expr, precision))

      def wkt(): Column = Column(WKT(c.expr))

      def withinRange(boundingBox: BoundingBox): Column = Column(WithinRange(c.expr, boundingBox))

      def withinRange(origin: Point, radius: Double): Column = Column(WithinCircleRange(c.expr, origin, radius))

      def asGeoJSON(): Column = Column(AsGeoJSON(c.expr))

      def buffer(distance: Double): Column = Column(Buffer(c.expr, distance))
    }
    
    implicit def point(x: Column, y: Column) = Column(PointConverter(x.expr, y.expr))

    implicit def wkt(x: Column) = Column(WKT(x.expr))

    implicit def asGeoJSON(x: Column) = Column(AsGeoJSON(x.expr))

    implicit def buffer(x: Column, distance: Double) = Column(Buffer(x.expr, distance))

    implicit class DslDataset[T](c: Dataset[T]) {
      def df: Dataset[T] = c

      def index(precision: Int): Dataset[T] = {
        Dataset[T](df.sparkSession,
          SpatialJoinHint(df.logicalPlan, Map("magellan.index.precision" -> precision.toString)))(df.exprEnc)
      }
    }

  }

  object expressions extends ExpressionConversions  // scalastyle:ignore

}

