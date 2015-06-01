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

package com.hortonworks.spatialsdk.catalyst

import scala.language.implicitConversions

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.spatialsdk.dsl.expressions._
import org.scalatest.FunSuite

import com.hortonworks.spatialsdk._

case class PointExample(point: Point)
case class PolygonExample(polygon: Polygon)

class PredicateSuite extends FunSuite with TestSparkContext {

  test("within") {

    val points = sc.parallelize(Seq(
      PointExample(new Point(0.0, 0.0)),
      PointExample(new Point(2.0, 2.0))
    ))

    val box = Box(-1.0,-1.0, 1.0, 1.0)
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
        PolygonExample(new Polygon(box, Array(0), ring))
      ))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val pdf = points.toDF()
    val sdf = polygons.toDF()
    assert(pdf.count() === 2)
    assert(sdf.count() === 1)
    println(pdf.select($"point").show())
    assert(pdf.join(sdf).where($"point" within  $"polygon").count() === 1)

  }
}
