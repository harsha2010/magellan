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

import magellan.{Point, Polygon}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Generate, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._

private[magellan] case class SpatialJoin(session: SparkSession)
  extends Rule[LogicalPlan] with MagellanExpression {

  private val indexerType = Indexer.dataType
  private val curveType = new ZOrderCurveUDT().sqlType
  private var prec: Option[Int] = None

  override def apply(plan: LogicalPlan): LogicalPlan = {

    plan transformUp {
      case p@SpatialJoinHint(child, hints) =>
        prec = hints.get("magellan.index.precision").map(_.toInt)
        logDebug(s"Spatial Join Hint provided, precision $prec")

        child
      case p@Join(
      Generate(Inline(_: Indexer), _, _, _, _, _),
      Generate(Inline(_: Indexer), _, _, _, _, _), _, _) => p
      case p@Join(l, r, joinType @ (Inner | LeftOuter), Some(cond)) =>
        val trigger = matchesTrigger(cond)
        if (trigger.isEmpty) p else {
          /**
            * Given a Logical Query Plan of the form
            * 'Project [...]
            * +- Filter Within(point, polygon)
            * +- Join Inner
            * :- Relation[point, ...]
            * :- Relation[polygon,...]
            * *
            * Convert it into
            * 'Project [...]
            * +- Filter Or (('relation == "Within"), Within(point, polygon))
            * +- Join Inner JoinKey = 'curve
            * :- Generate(Inline(index ($"point" , precision)))
            * :- Generate(Inline(index ($"point" , precision)))
            *
            */

          val Some(Within(a, b)) = trigger
          // determine which is the left project and which is the right projection in Within
          val (leftProjection, rightProjection) =
          l.outputSet.find(a.references.contains(_)) match {
            case Some(_) => (a, b)
            case None => (b, a)
          }

          // left hand side attributes
          val c1 = attr("curve", curveType)
          val r1 = attr("relation", StringType)

          // right hand side attributes
          val c2 = attr("curve", curveType)
          val r2 = attr("relation", StringType)

          // if a curve is within the geometry on the right hand side then we shortcircuit
          val shortcutRelation = EqualTo(r2, Literal("Within"))
          val transformedCondition = cond transformUp {
            case p @ Within(_, _) =>
              Or(shortcutRelation, p)
          }
          // check if the precision is available as a hint, otherwise use default
          val precision = prec.fold(30)(identity)

          logDebug(s"Spatial Join Rule using precision $precision")

          val leftIndexer = l.outputSet.find(_.dataType == indexerType)
            .fold[Expression](Indexer(leftProjection, precision))(identity)

          val rightIndexer = r.outputSet.find(_.dataType == indexerType)
            .fold[Expression](Indexer(rightProjection, precision))(identity)

          val join = Join(
            Generate(Inline(leftIndexer), true, false, None, Seq(c1, r1), l),
            Generate(Inline(rightIndexer), true, false, None, Seq(c2, r2), r),
            joinType,
            Some(And(EqualTo(c1, c2), transformedCondition)))

          Project(p.outputSet.toSeq, join)
        }
    }
  }

  private def attr(name: String, dt: DataType): Attribute = {
    // name: String, dataType: DataType, nullable: Boolean = true,
    // metadata: Metadata = Metadata.empty), exprId: ExprId = NamedExpression.newExprId,
    // qualifier: Option[String] = None,
    // isGenerated: java.lang.Boolean = false
    val klass = Class.forName("org.apache.spark.sql.catalyst.expressions.AttributeReference")
    val ctor = klass.getConstructors.apply(0)
    val nullable = true.asInstanceOf[AnyRef]
    val metadata = Metadata.empty
    val exprId = NamedExpression.newExprId
    val qualifier = None
    val isGenerated = false.asInstanceOf[AnyRef]
    if (ctor.getParameterCount == 7) {
      // prior to Spark 2.3
      ctor.newInstance(name, dt, nullable, metadata, exprId, qualifier, isGenerated)
        .asInstanceOf[Attribute]
    } else {
      // Spark 2.3  +
      ctor.newInstance(name, dt, nullable, metadata, exprId, qualifier)
        .asInstanceOf[Attribute]
    }

  }

  private def matchesTrigger(cond: Expression) = cond find {
    case p @ Within(left, _) =>
      val pointType = sqlType(classOf[Point])
      left.dataType == pointType

    case _ => false
  }
}
