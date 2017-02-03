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
    val p = df.select($"point").first()(0)
    assert(p.equals(Point(102.0, 0.5)))
  }

  test("Read Line String") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("geojson/linestring").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1018)
    import sqlCtx.implicits._
    val p = df.select($"polyline").first()(0).asInstanceOf[PolyLine]
    // [ -122.04864044239585, 37.408617050391001 ], [ -122.047741818556602, 37.408915362324983 ]
    assert(p.indices.size === 2)
    assert(p.xcoordinates.head == -122.04864044239585)
    assert(p.ycoordinates.head == 37.408617050391001)
    assert(p.xcoordinates.last == -122.047741818556602)
    assert(p.ycoordinates.last == 37.408915362324983)
  }

  test("Read Polygon") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("geojson/polygon").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    val indices = p.indices
    assert(indices(0) === 0)
    assert(indices(1) === 5)
  }
}
