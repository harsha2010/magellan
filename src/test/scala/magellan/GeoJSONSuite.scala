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

import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class GeoJSONSuite extends FunSuite with TestSparkContext {

  test("Read Point") {
    val sqlCtx = this.sqlContext
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

  test("Read Point with Double-Int Coordinates") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/point-double-int").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1)
    import sqlCtx.implicits._
    val p = df.select($"point").first()(0)
    assert(p.equals(Point(102.0, 1.0)))
  }

  test("Read Point with Int-Double Coordinates") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/point-int-double").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1)
    import sqlCtx.implicits._
    val p = df.select($"point").first()(0)
    assert(p.equals(Point(102.0, 0.5)))
  }

  test("Read Point with Int-Int Coordinates") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/point-int-int").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1)
    import sqlCtx.implicits._
    val p = df.select($"point").first()(0)
    assert(p.equals(Point(102.0, 5.0)))
  }

  test("Read Line String") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/linestring").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
    assert(df.count() === 1018)
    import sqlCtx.implicits._
    val p = df.select($"polyline").first()(0).asInstanceOf[PolyLine]
    // [ -122.04864044239585, 37.408617050391001 ], [ -122.047741818556602, 37.408915362324983 ]
    assert(p.getNumRings() === 2)
    assert(p.getVertex(0) == Point(-122.04864044239585, 37.408617050391001))
    assert(p.getVertex(1) == Point(-122.047741818556602, 37.408915362324983))
  }

  test("Read Polygon") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/polygon").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    assert(p.getRing(0) === 0)
    assert(p.getRing(1) === 5)
  }

  test("Read Polygon with Int-Double coordinate pairs") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/polygon-int-double").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    assert(p.getVertex(1) === Point(101, 0))
  }

  test("Read Polygon with Double-Int coordinate pairs") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/polygon-double-int").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    assert(p.getVertex(1) === Point(101, 0))
  }

  test("Read Polygon with Int-Int coordinate pairs") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/polygon-int-int").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    assert(p.getVertex(0) === Point(100, 0))
  }

  test("Read Multipolygon") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val path = this.getClass.getClassLoader.getResource("geojson/multipolygon/example.geojson").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
      .select($"polygon")

    assert(df.count() === 2)

    // check that the second polygon has holes
    assert(df.filter { row => row match { case Row(polygon: Polygon) => polygon.getNumRings() == 2 }}.count() === 1)
  }

  test("Read Multipolygon: more complex example") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val path = this.getClass.getClassLoader.getResource("geojson/multipolygon/countries.geojson").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)
      .select($"polygon", $"metadata"("name").as("name"))

    assert(df.groupBy($"name").count().count() === 180)
    // check if USA is present
    val point = Point(-122.5076401, 37.7576793)
    val usa = df.filter { row => row match {
      case Row(p: Polygon, name: String) => p.contains(point)
    }}

    assert(usa.count() === 1 && usa.filter($"name" === "United States of America").count() === 1)

    // Angola is a Multipolygon.
    val angola = df.filter {$"name" === "Angola"}
    assert(angola.count() === 2)

    // check if Luanda is present here
    val luanda = Point(13.2140638, -8.8535258)
    assert(angola.filter { row => row match {
      case Row(p: Polygon, name: String) => p.contains(luanda)
    }}.count() === 1)
  }

  test("Write GeoJSON Point") {
    val point = Point(0.5, 0.0)
    val json = s"""
        {
          "type": "Feature",
          "geometry": ${GeoJSON.writeJson(point)},
          "properties": {}
        }
       """

    implicit val formats = org.json4s.DefaultFormats
    val result = parse(json).extract[Feature]
    val shapes = result.geometry.shapes
    // expect a single point
    assert(shapes.size == 1)
    assert(shapes.head.asInstanceOf[Point] === point)
  }

  test("Write GeoJSON Polygon") {
    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))

    val ring2 = Array(Point(5.0, 5.0), Point(5.0, 4.0),
      Point(4.0, 4.0), Point(4.0, 5.0), Point(5.0, 5.0))

    val polygon = Polygon(Array(0, 5), ring1 ++ ring2)

    val json = s"""
        {
          "type": "Feature",
          "geometry": ${GeoJSON.writeJson(polygon)},
          "properties": {}
        }
       """

    implicit val formats = org.json4s.DefaultFormats
    val result = parse(json).extract[Feature]
    val shapes = result.geometry.shapes
    // expect a single polygon
    assert(shapes.size == 1)
    assert(shapes.head.asInstanceOf[Polygon] === polygon)
  }

  test("Write GeoJSON PolyLine") {
    val polyline = PolyLine(Array(0), Array(Point(0.0, 1.0), Point(1.0, 0.5)))

    val json = s"""
        {
          "type": "Feature",
          "geometry": ${GeoJSON.writeJson(polyline)},
          "properties": {}
        }
       """

    implicit val formats = org.json4s.DefaultFormats
    val result = parse(json).extract[Feature]
    val shapes = result.geometry.shapes
    // expect a single polyline
    assert(shapes.size == 1)
    assert(shapes.head.asInstanceOf[PolyLine] === polyline)
  }

  test("Read compressed geojson") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("geojson/compressed/example.geojson.gz").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "geojson")
      .load(path)

    import sqlCtx.implicits._
    val p = df.select($"polygon").first()(0).asInstanceOf[Polygon]
    assert(p.getVertex(0) === Point(100, 0))
  }
}
