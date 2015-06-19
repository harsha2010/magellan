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

import com.vividsolutions.jts.geom.impl.CoordinateArraySequenceFactory
import com.vividsolutions.jts.geom.{Coordinate, LinearRing,
  GeometryFactory, PrecisionModel, Polygon => JTSPolygon}

import scala.collection.mutable.ArrayBuffer

trait Multipath extends Shape {

  val indices: IndexedSeq[Int]
  val points: IndexedSeq[Point]

  override val delegate = {
    val precisionModel = new PrecisionModel()
    val geomFactory = new GeometryFactory(precisionModel)
    val csf = CoordinateArraySequenceFactory.instance()

    val rings = ArrayBuffer[LinearRing]()
    for (Seq(i, j) <- (indices ++ Seq(points.size)).sliding(2)) {
      val coords = points.slice(i, j).map(point => new Coordinate(point.x, point.y))
      val csf = CoordinateArraySequenceFactory.instance()
      rings+= new LinearRing(csf.create(coords.toArray), geomFactory)
    }
    val shell = rings(0)
    val holes = rings.drop(1).toArray
    new JTSPolygon(shell, holes, geomFactory)
  }

}
