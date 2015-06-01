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

package com.hortonworks.spatialsdk

import org.apache.spark.sql.Row
import org.scalatest.FunSuite

import com.hortonworks.spatialsdk.TestingUtils._

class ShapefileSuite extends FunSuite with TestSparkContext {

  test("shapefile-relation: points") {
    val sqlCtx = new SpatialContext(sc)
    val path = this.getClass.getClassLoader.getResource("testpoint.shp").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() == 1)
    val point = df.select($"point").map {case Row(x: Point) => x}.first()
    assert(point.x ~== -99.796 absTol 0.2)
  }

  test("shapefile-relation: polygons") {
    val sqlCtx = new SpatialContext(sc)
    val path = this.getClass.getClassLoader.getResource("testpolygon.shp").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() == 1)
    val polygon = df.select($"polygon").map {case Row(x: Polygon) => x}.first()
    assert(polygon.indices.size === 1)
    assert(polygon.points.size === 6)
  }

  test("shapefile-relation: Zillow DC Neighborhoods") {
    val sqlCtx = new SpatialContext(sc)
    val path = this.getClass.getClassLoader.getResource("zillow_dc.shp").getPath
    val df = sqlCtx.shapeFile(path)
    import sqlCtx.implicits._
    assert(df.count() == 34)
    val polygon = df.select($"polygon").map {case Row(x: Polygon) => x}.first()
    val box = polygon.box
    assert(box.xmax ~== -77.026 absTol 0.01)
  }

}
