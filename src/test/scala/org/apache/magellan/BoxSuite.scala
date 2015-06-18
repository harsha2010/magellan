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

class BoxSuite extends FunSuite {

  test("contains") {
    val box = Box(-1.0,-1.0, 1.0, 1.0)
    assert(box.contains(new Point(0.0, 0.0)))
    assert(!box.contains(new Point(2.0, 0.0)))
    assert(box.contains(new Point(1.0, 1.0)))
  }

  test("away") {
    val box = Box(-1.0,-1.0, 1.0, 1.0)
    val point = box.away(0.1)
    assert(!box.contains(point))
    assert(Box(-1.1, -1.1, 1.1, 1.1).contains(point))
  }
}
