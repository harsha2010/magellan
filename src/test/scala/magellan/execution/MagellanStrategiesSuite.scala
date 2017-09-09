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

package magellan.execution

import magellan.catalyst.{PointExample, PolygonExample}
import magellan.{Polygon, Point, TestSparkContext}
import org.apache.spark.sql.execution.joins.BroadcastCartesianJoin
import org.apache.spark.sql.magellan.MagellanContext
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class MagellanStrategiesSuite extends FunSuite with TestSparkContext {

  test("broadcast join") {

    val sqlContext = new MagellanContext(sc)
    import sqlContext.implicits._

    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))

    val polygons = sc.parallelize(Seq(
      PolygonExample(new Polygon(Array(0), ring1))
    )).toDF()

    val points = sc.parallelize(0 until 10000 map (_ => PointExample(new Point(0.0, 0.0)))).toDF()

    val plan = points.join(polygons).where($"point" within $"polygon").queryExecution.sparkPlan
    val operator = plan.collect {
      case j: BroadcastCartesianJoin => j
    }

    assert(operator.size === 1)
    val join = operator(0)

    assert(join.isInstanceOf[BroadcastCartesianJoin])
  }
}
