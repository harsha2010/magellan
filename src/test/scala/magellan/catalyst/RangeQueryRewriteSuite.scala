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

import magellan.{BoundingBox, Point, TestSparkContext}
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types.DoubleType
import org.scalatest.FunSuite

class RangeQueryRewriteSuite extends FunSuite with TestSparkContext {

  test("Rewrite point($x, $y) within Range Query") {

    object Optimize extends RuleExecutor[LogicalPlan] {
      val batches =
        Batch("rewrite range query", FixedPoint(100), RangeQueryRewrite(spark)) :: Nil
    }

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val points = sc.parallelize(Seq(
      (0.0, 0.0),
      (2.0, 2.0)
    )).toDF("x", "y")

    val boundingBox = BoundingBox(-1.0, -1.0, 1.0, 1.0)

    val query = points.where(point($"x", $"y") withinRange boundingBox)
    val optimizedPlan = Optimize.execute(query.queryExecution.analyzed)
    val filter = optimizedPlan(0).asInstanceOf[Filter]
    val condition = filter.condition
    val And(xpredicate, ypredicate) = condition
    val And(xupperbound, xlowerbound) = xpredicate
    val And(yupperbound, ylowerbound) = ypredicate

    xupperbound match {
      case LessThanOrEqual(_, Literal(1.0, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    xlowerbound match {
      case GreaterThanOrEqual(_, Literal(-1.0, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    yupperbound match {
      case LessThanOrEqual(_, Literal(1.0, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    ylowerbound match {
      case GreaterThanOrEqual(_, Literal(-1.0, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    // execute the query and verify the results
    assert(query.count() === 1)
  }

  test("Rewrite point($x, $y) within Circle Range Query") {

    object Optimize extends RuleExecutor[LogicalPlan] {
      val batches =
        Batch("rewrite range query", FixedPoint(100), new RangeQueryRewrite(spark)) :: Nil
    }

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._

    val points = sc.parallelize(Seq(
      (0.0, 0.0),
      (1.4, 1.4),
      (2.0, 2.0)
    )).toDF("x", "y")


    val query = points.where(point($"x", $"y") withinRange (Point(0.5, 0.5), 1.0))
    val optimizedPlan = Optimize.execute(query.queryExecution.analyzed)
    val filter = optimizedPlan(0).asInstanceOf[Filter]
    val condition = filter.condition
    val And(And(xpredicate, ypredicate), other) = condition
    val And(xupperbound, xlowerbound) = xpredicate
    val And(yupperbound, ylowerbound) = ypredicate

    xupperbound match {
      case LessThanOrEqual(_, Literal(1.5, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    xlowerbound match {
      case GreaterThanOrEqual(_, Literal(-0.5, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    yupperbound match {
      case LessThanOrEqual(_, Literal(1.5, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    ylowerbound match {
      case GreaterThanOrEqual(_, Literal(-0.5, DoubleType)) => assert(true)
      case _ => assert(false)
    }

    // execute the query and verify the results
    assert(query.count() === 1)

  }
}
