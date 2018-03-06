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

import com.esri.core.geometry.GeometryEngine
import magellan.esri.ESRIUtil
import magellan.{Point, Polygon, TestSparkContext}
import org.apache.spark.sql.Row
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

  test("ISSUE-108") {

    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val points = sc.parallelize(Seq(
      "600000001,5.4588922996667,5.4588922996667",
      "600000001,5.4588922996667,5.4588922996667",
      "6000000SS,5.4588922996667,5.4588922996667",
      "600000033,5.4588922996669,5.4588922996669",
      "600000000,5.4588922996667,5.4588922996667",
      "600000002,5.4588922996668,5.45889229966681",
      "6000000SS,5.4588922996667,5.4588922996667",
      "100000000,15.4588922996667,47.0500470991844",
      "100000001,15.4427295917601,47.0628953964286"
    )).map { line =>
      val Array(pointId, x, y) = line.split(",")
      (pointId, Point(x.toDouble, y.toDouble))
    }.toDF("pointId", "point")


    val polygons = sc.parallelize(Seq(
      "location2;456;POLYGON ((0.4588922996667 0.4588922996667, 10.4588922996667 0.4588922996667, 10.4588922996667 10.4588922996667, 0.4588922996667 10.4588922996667, 0.4588922996667 0.4588922996667))",
      "handDrawn;-55;POLYGON((16.5132943323988 47.85121641510742,16.523422353639035 47.83923477616023,16.558183782472042 47.84741484867311,16.558183782472042 47.86106463188969,16.53106128491345 47.865210689885984,16.52951633252087 47.85565122375945,16.516985052003292 47.857378968615265,16.5132943323988 47.85121641510742))"
    )).map { line =>
      val Array(polygonId, value, text) = line.split(";")
      (polygonId, value, text)
    }.toDF("polygonId", "value", "text")
      .withColumn("polygon", wkt($"text")("polygon"))
    
    val actual = points.join(polygons)
      .where($"point" within $"polygon")
      .select($"pointId", $"polygonId")
      .map { case Row(pointId: String, polygonId: String) =>
        (pointId, polygonId)
      }
      .collect()
      .sortBy(_._1)

    // compare with ESRI

    val esriPoints = points.collect().map { case Row(id: String, point: Point) =>
      val esriPoint = ESRIUtil.toESRIGeometry(point)
      (id, esriPoint)
    }

    val esriResults = polygons.flatMap {
      case Row(polygonId: String, value: String, text: String, polygon: Polygon) =>
        val esriPolygon = ESRIUtil.toESRIGeometry(polygon)
        esriPoints.map {case (pointId, esriPoint) =>
          val within = GeometryEngine.contains(esriPolygon, esriPoint, null)
          (within, pointId, polygonId)
        }.filter(_._1)
    }
    val expected = esriResults.collect().sortBy(_._2)

    assert(expected.length === actual.length)
    assert(expected.map(x => (x._2, x._3)).deep === actual.deep)
  }
}
