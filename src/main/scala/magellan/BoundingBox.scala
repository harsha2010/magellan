/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package magellan

/**
 * A bounding box is an axis parallel rectangle. It is completely specified by
 * the bottom left and top right points.
 *
 * @param xmin the minimum x coordinate of the rectangle.
 * @param ymin the minimum y coordinate of the rectangle.
 * @param xmax the maximum x coordinate of the rectangle.
 * @param ymax the maximum y coordinate of the rectangle.
 */
case class BoundingBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double) extends Shape {

  override def getType(): Int = 4

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): BoundingBox = ???

  override def boundingBox: BoundingBox = this

  private [magellan] def intersects(other: BoundingBox): Boolean = {
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other
    !(otherxmin > xmax || otherymin > ymax || otherymax < ymin || otherxmax < xmin)
  }

  private [magellan] def contains(other: BoundingBox): Boolean = {
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other
    (xmin <= otherxmin && ymin <= otherymin && xmax >= otherxmax && ymax >= otherymax)
  }

  private [magellan] def contains(point: Point): Boolean = {
    val (x, y) = (point.getX(), point.getY())
    (xmin <= x && ymin <= y && xmax >= x && ymax >= y)
  }

  private [magellan] def intersects(point: Point): Boolean = {
    val lines = Array(
      Line(Point(xmin, ymin), Point(xmax, ymin)),
      Line(Point(xmin, ymin), Point(xmin, ymax)),
      Line(Point(xmax, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymax)))

    lines exists (_ contains point)
  }

}

