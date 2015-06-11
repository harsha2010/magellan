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

class LineSuite extends FunSuite {

  test("intersects") {
    val first = new Line(new Point(0.0, 0.0), new Point(1.0, 1.0))
    val second = new Line(new Point(0.0, 1.0), new Point(1.0, 0.0))
    assert(first.intersects(second))
    assert(second.intersects(first))

    //check degenerate points
    val startDegenerate = new Line(new Point(0.0, 0.0), new Point(1.0, 0.0))
    assert(first.intersects(startDegenerate))

    val third = new Line(new Point(0.0, 1.0), new Point(0.9, 1.0))
    assert(!first.intersects(third))
  }
}
