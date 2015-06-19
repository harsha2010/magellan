/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan

import org.scalatest.FunSuite

import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types.StringType

import TestingUtils._

class ShapefileSuite extends FunSuite with TestSparkContext {

  test("shapefile-relation: points") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("testpoint/").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() === 1)
    val point = df.select($"point").map {case Row(x: Point) => x}.first()
    assert(point.x ~== -99.796 absTol 0.2)
  }

  test("shapefile-relation: polygons") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("testpolygon/").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() === 1)
    val polygon = df.select($"polygon").map {case Row(x: Polygon) => x}.first()
    assert(polygon.indices.size === 1)
    assert(polygon.points.size === 6)
  }

  test("shapefile-relation: Zillow Neighborhoods") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("testzillow/").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() === 1932)  // 34 + 948 + 689 + 261

    // CA should have some metadata attached to it
    val extractValue: (Map[String, String], String) => String =
      (map: Map[String, String], key: String) => {
        map.getOrElse(key, null)
      }
    val stateUdf = callUDF(extractValue, StringType, col("metadata"), lit("STATE"))
    val dfwithmeta = df.withColumn("STATE", stateUdf)
    assert(dfwithmeta.filter($"STATE" === "CA").count() === 948)

    assert(df.select($"metadata"("STATE").as("state")).filter($"state" === "CA").count() === 948)
    assert(df.select($"metadata"("STATE").as("state")).filter($"state" isNull).count() === 723)
  }

  test("shapefile-relation: polylines") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("testpolyline/").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() === 14959)
    // 5979762.107174277,2085850.5510566086,6024890.0635061115,2130875.5735391825
    val start = new Point(5979764.107174277, 2085848.5510566086)
    val end = new Point(6024892.0635061115, 4184000.396185901)
    assert(df.filter(point(6024892.0635061115, 4184000.396185901) within $"polyline").count() > 0)
    // assert(df.filter($"polyline" intersects line(start, end)).count() > 0)
  }

  test("shapefile-relation: points and polygons") {
    val sqlCtx = new SQLContext(sc)
    val path = this.getClass.getClassLoader.getResource("testcomposite/").getPath
    val df = sqlCtx.shapeFile(path)
    assert(df.count() === 2)
    // each row should either contain a point or a polygon but not both
    import sqlCtx.implicits._
    assert(df.filter($"point" isNull).count() === 1)
    assert(df.filter($"polygon" isNull).count() === 1)
  }
}
