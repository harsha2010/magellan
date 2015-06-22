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

class PolygonSuite extends FunSuite {

  test("point in polygon") {
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0))
    val polygon = new Polygon(Array(0), ring)
    assert(!polygon.contains(new Point(2.0, 0.0)))
    assert(polygon.contains(new Point(0.0, 0.0)))
  }

  test("point in polygon: 2 rings") {
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0),
      new Point(0.5, 0), new Point(0, 0.5), new Point(-0.5, 0),
      new Point(0, -0.5), new Point(0.5, 0)
      )
    val polygon = new Polygon(Array(0, 5), ring)
    assert(!polygon.contains(new Point(2.0, 0.0)))
    assert(!polygon.contains(new Point(0.0, 0.0)))
  }

  test("point in polygon: OH") {
    val ring = Array(new Point(-81.734260559082, 41.4910373687744),
        new Point(-81.7333030700684, 41.4907093048096),
        new Point(-81.7333488464355, 41.4905986785889),
        new Point( -81.7331447601318, 41.49045753479),
        new Point(-81.7330646514893, 41.4903812408447),
        new Point(-81.7328319549561, 41.4901103973389),
        new Point(-81.734260559082, 41.4910373687744)
    )

    val polygon = new Polygon(Array(0), ring)
    assert(!polygon.contains(new Point(-80.2, 25.77)))

  }

  test("polygon intersection") {
    val ring1 = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0))
    val polygon1 = new Polygon(Array(0), ring1)

    val ring2 = Array(new Point(2.0, 2.0), new Point(2.0, 0.0),
      new Point(0.0, 0.0), new Point(0.0, 2.0), new Point(2.0, 2.0))
    val polygon2 = new Polygon(Array(0), ring2)

    assert(polygon2.intersects(polygon1))
    // the intersection region must be the square (0,0), (1,0), (1, 1), (0, 1)
    val square = polygon2.intersection(polygon1)
    assert(square.isInstanceOf[Polygon])
    assert(square.contains(new Point(0.75, 0.75)))


    val ring3 = Array(new Point(1.0, -1.0), new Point(2.0, -1.0),
      new Point(2.0, 1.0), new Point(1.0, 1.0), new Point(1.0, -1.0))
    val polygon3 = new Polygon(Array(0), ring3)
    // the intersection region must be the line (1,-1), (1,1)
    val line = polygon3.intersection(polygon1)
    assert(line.isInstanceOf[Line])
    assert(line.contains(new Point(1.0, 0.0)))

    val ring4 = Array(new Point(1.0, 1.0), new Point(3.0, 1.0),
      new Point(3.0, 3.0), new Point(1.0, 3.0), new Point(1.0, 1.0))
    val polygon4 = new Polygon(Array(0), ring4)
    // the intersection region must be the point (1,1)
    val point = polygon4.intersection(polygon1)
    assert(point.equals(new Point(1.0, 1.0)))

    val ring5 = Array(new Point(2.0, -1.0), new Point(3.0, -1.0),
      new Point(3.0, 0.0), new Point(2.0, 0.0), new Point(2.0, -1.0))
    val polygon5 = new Polygon(Array(0), ring5)
    // the intersection region must be empty
    val empty = polygon5.intersection(polygon1)
    assert(empty.isEmpty())
  }
}
