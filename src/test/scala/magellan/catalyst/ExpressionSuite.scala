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

import magellan.{Point, TestSparkContext}
import org.apache.spark.sql.magellan.MagellanContext
import org.apache.spark.sql.magellan.dsl.expressions._

import org.scalatest.FunSuite

class ExpressionSuite extends FunSuite with TestSparkContext {

  test("buffer") {
    val sqlCtx = new MagellanContext(sc)
    import sqlCtx.implicits._

    val pointsdf = sc.parallelize(Seq(
      PointExample(new Point(0.0, 0.0)),
      PointExample(new Point(2.0, 2.0))
    )).toDF()

    val buffereddf = pointsdf.withColumn("polygon", $"point" buffer 0.5)
    buffereddf.show()
  }
}
