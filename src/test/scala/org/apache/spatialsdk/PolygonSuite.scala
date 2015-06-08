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

package org.apache.spatialsdk

import org.scalatest.FunSuite

class PolygonSuite extends FunSuite {

  test("point in polygon") {
    val box = Box(-1.0,-1.0, 1.0, 1.0)
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0))
    val polygon = new Polygon(box, Array(0), ring)
    assert(!polygon.contains(new Point(2.0, 0.0)))
    assert(polygon.contains(new Point(0.0, 0.0)))
  }

  test("point in polygon: 2 rings") {
    val box = Box(-1.0,-1.0, 1.0, 1.0)
    val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0), new Point(1.0, 1.0),
      new Point(0.5, 0), new Point(0, 0.5), new Point(-0.5, 0),
      new Point(0, -0.5), new Point(0.5, 0)
      )
    val polygon = new Polygon(box, Array(0, 5), ring)
    assert(!polygon.contains(new Point(2.0, 0.0)))
    assert(!polygon.contains(new Point(0.0, 0.0)))
  }

  test("point in polygon: OH") {
    val box = Box(-81.7519912719727,3.143E-319,3.143E-319,41.4934368133545)
    val ring = Array(new Point(-81.734260559082, 41.4910373687744),
        new Point(-81.7333030700684, 41.4907093048096),
        new Point(-81.7333488464355, 41.4905986785889),
        new Point( -81.7331447601318, 41.49045753479),
        new Point(-81.7330646514893, 41.4903812408447),
        new Point(-81.7328319549561, 41.4901103973389),
        new Point(-81.734260559082, 41.4910373687744)
    )

    val polygon = new Polygon(box, Array(0), ring)
    assert(!polygon.contains(new Point(-80.2, 25.77)))

  }
}
