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

trait Multipath extends Shape {

  val indices: IndexedSeq[Int]
  val points: IndexedSeq[Point]

  val box: Box = {
    var xmin: Double = Double.MaxValue
    var ymin: Double = Double.MaxValue
    var xmax: Double = Double.MinValue
    var ymax: Double = Double.MinValue
    for (point <- points) {
      val x = point.x
      val y = point.y
      if (x < xmin) {
        xmin = x
      }
      if (x > xmax) {
        xmax = x
      }
      if (y < ymin) {
        ymin = y
      }
      if (y > ymax) {
        ymax = y
      }
    }
    Box(xmin, ymin, xmax, ymax)
  }

  /**
   *
   * @param line
   * @return number of times this shape intersects the given line.
   */
  def intersections(line: Line): Int = {
    var startIndex = 0
    var endIndex = 1
    val length = points.size
    var intersections = 0
    var currentRingIndex = 0
    while (endIndex < length) {
      val start = points(startIndex)
      val end = points(endIndex)
      if (line.intersects(new Line(start, end))) {
        intersections += 1
      }
      startIndex += 1
      endIndex += 1
      // if we reach a ring boundary skip it
      val nextRingIndex = currentRingIndex + 1
      if (nextRingIndex < indices.length) {
        val nextRing = indices(nextRingIndex)
        if (endIndex == nextRing) {
          startIndex += 1
          endIndex += 1
          currentRingIndex = nextRingIndex
        }
      }
    }
    intersections
  }

  def intersects(line: Line): Boolean = intersections(line) > 0

}
