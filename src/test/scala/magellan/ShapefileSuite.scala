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

package magellan

import magellan.TestingUtils._
import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import org.apache.spark.sql.magellan.dsl.expressions._

case class UberRecord(tripId: String, timestamp: String, point: Point)

class ShapefileSuite extends FunSuite with TestSparkContext {


  test("shapefile-relation: points") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpoint/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    assert(df.count() === 1)
    val point = df.select($"point").map {case Row(x: Point) => x}.first()
    assert(point.getX() ~== -99.796 absTol 0.2)

    // query
    assert(df.filter($"point" within $"point").count() === 1)
  }
}

