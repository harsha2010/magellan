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

import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FunSuite

class GeoJSONSuite extends FunSuite with TestSparkContext {

  test("Read Point") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("geojson/point").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1)
    import sqlCtx.implicits._
    val p = df.select($"point").map { case Row(p: Point) => p}.first()
    assert(p.equals(Point(102.0, 0.5)))
  }

  test("Read Polygon") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("geojson/polygon").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").map { case Row(p: Polygon) => p}.first()
    val indices = p.indices
    assert(indices(0) === 0)
    assert(indices(1) === 5)
  }
}
