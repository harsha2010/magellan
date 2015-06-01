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

/**
 * Line segment between two points.
 *
 * @param start
 * @param end
 */
case class Line(start: Point, end: Point) {

  def intersects(other: Line): Boolean = {
    // test for degeneracy
    if (start == other.start ||
        end == other.end ||
        start == other.end ||
        end == other.start) {
      true
    } else {
      def ccw(a: Point, b: Point, c: Point) = {
        (c.y - a.y) * (b.x - a.x) > (b.y - a.y) * (c.x - a.x)
      }
      ccw(start, other.start, other.end) != ccw(end, other.start, other.end) &&
        ccw(start, end, other.start) != ccw(start, end, other.end)
    }
  }
}
