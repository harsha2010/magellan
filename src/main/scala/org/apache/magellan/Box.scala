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

import scala.util.Random

/**
 * An axis parallel rectangle that can serve as boundaries of geometric shapes.
 *
 * @param xmin
 * @param ymin
 * @param xmax
 * @param ymax
 */
case class Box(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

  /**
   * Picks a point x uniformly at random in the epsilon annulus surrounding the box
   *
   * +----------------+
   * |  +---+-----+ x |
   * |  |         |   |
   * |  +   Box   +   |
   * |  |         |   |
   * |  +---+-----+   |
   * +----------------+
   * @param eps
   * @param seed
   * @return
   */
  def away(
      eps: Double,
      seed: Int = 12345): Point = {
    val rnd = new Random(seed)

    def randomPointInInterval(start: Double, end: Double): Double = {
      val l = end - start
      val x = rnd.nextDouble() //(0,1)
      start + x * (end - start)
    }
    val p = rnd.nextBoolean()
    val q = rnd.nextBoolean()
    val (x, y) = if (!p & !q) {
      // left portion of the annulus [x - epsilon, x], [y - epsilon, y + epsilon]
      (randomPointInInterval(xmin - eps, xmin),
       randomPointInInterval(ymin - eps, ymax + eps))
    } else if (!p && q) {
      // top portion of annulus
      (randomPointInInterval(xmin, xmax),
       randomPointInInterval(ymax, ymax + eps))
    } else if (p && !q) {
      // right portion of annulus
      (randomPointInInterval(xmax, xmax + eps),
       randomPointInInterval(ymin - eps, ymax + eps))
    } else {
      // bottom portion of annulus
      (randomPointInInterval(xmin, xmax),
       randomPointInInterval(ymin - eps, ymin))
    }
    new Point(x, y)
  }

  /**
   * Checks whether a point is contained within the box.
   * @param point
   * @return
   */
  def contains(point: Point): Boolean = {
    !(point.x < xmin || point.x > xmax || point.y < ymin || point.y > ymax)
  }
}
