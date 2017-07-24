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
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class WKTSuite extends FunSuite with TestSparkContext {

  test("convert points to WKT") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val df = sc.parallelize(Seq(
        (1, "POINT (3 15)"),
        (2, "POINT (25 5)"),
        (3, "POINT (30 10)")
      )).toDF("id", "text")

    val points = df.withColumn("shape", wkt($"text")).select($"shape"("point"))
    assert(points.count() === 3)
    val point = points.first()(0).asInstanceOf[Point]
    assert(point.getX() === 3.0)
    assert(point.getY() === 15.0)
  }
}
