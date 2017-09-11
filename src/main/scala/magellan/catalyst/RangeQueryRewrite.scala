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

import magellan.{BoundingBox, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Performs the following Query Rewrites:
  * 1) Rewrite point($x, $y) withinRange box -> ($x \in [Xmin, Xmax] and $y \in [Ymin, Ymax]
  *    This allows column pruning on $x and $y where possible, and min/ max pruning where possible
  * 2) Rewrite point($x, $y) withinRange (point, radius) to first prune by the bounding box
  *    of the circle (thereby applying the pruning in step (1) followed by the actual filter.
  *
  * @param session
  */
private[magellan] case class RangeQueryRewrite(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {

      case p @ Filter(condition, child) =>
        val transformedCondition = condition transformUp {

          case WithinRange((PointConverter(xexpr, yexpr)), boundingBox) =>
            prune(boundingBox, xexpr, yexpr)

          case q @ WithinCircleRange((PointConverter(xexpr, yexpr)), point, radius) =>
            val (x, y) = (point.getX(), point.getY())
            val boundingBox = BoundingBox(x - radius, y - radius, x + radius, y + radius)
            And(prune(boundingBox, xexpr, yexpr),
              new WithinCircleRangePostOpt((PointConverter(xexpr, yexpr)), point, radius))

          case q : WithinCircleRangePostOpt => q
        }

        Filter(transformedCondition, child)
    }
  }

  private def prune(boundingBox: BoundingBox, xexpr: Expression, yexpr: Expression) = {

    val xpredicate = And(LessThanOrEqual(xexpr, Literal(boundingBox.xmax)),
      GreaterThanOrEqual(xexpr, Literal(boundingBox.xmin)))

    val ypredicate = And(LessThanOrEqual(yexpr, Literal(boundingBox.ymax)),
      GreaterThanOrEqual(yexpr, Literal(boundingBox.ymin)))
    And(xpredicate, ypredicate)
  }
}

private [magellan] class WithinCircleRangePostOpt(child: Expression, point: Point, radius: Double)
  extends WithinCircleRange(child, point, radius)