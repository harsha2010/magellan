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

import magellan.Relate._

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * A bounding box is an axis parallel rectangle. It is completely specified by
 * the bottom left and top right points.
 *
 * @param xmin the minimum x coordinate of the rectangle.
 * @param ymin the minimum y coordinate of the rectangle.
 * @param xmax the maximum x coordinate of the rectangle.
 * @param ymax the maximum y coordinate of the rectangle.
 */
case class BoundingBox(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

  private def this() {this(0, 0, 0, 0)}

  @JsonProperty
  def getXmin(): Double = xmin

  @JsonProperty
  def getYmin(): Double = ymin

  @JsonProperty
  def getXmax(): Double = xmax

  @JsonProperty
  def getYmax(): Double = ymax

  private [magellan] def intersects(other: BoundingBox): Boolean = {
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other
    !(otherxmin >= xmax || otherymin >= ymax || otherymax <= ymin || otherxmax <= xmin)
  }

  def contains(other: BoundingBox): Boolean = {
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other
    (xmin <= otherxmin && ymin <= otherymin && xmax >= otherxmax && ymax >= otherymax)
  }

  /**
    * Checks if this bounding box is contained within the given circle.
    *
    * @param origin
    * @param radius
    * @return
    */
  def withinCircle(origin: Point, radius: Double): Boolean = {
    val vertices = Array(
      Point(xmin, ymin),
      Point(xmax, ymin),
      Point(xmax, ymax),
      Point(xmin, ymax)
    )
    !(vertices exists  (vertex => !(vertex withinCircle(origin, radius))))
  }

  private [magellan] def disjoint(other: BoundingBox): Boolean = {
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other
    (otherxmin > xmax || otherxmax < xmin || otherymin > ymax || otherymax < ymin)
  }

  def contains(point: Point): Boolean = {
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

  private [magellan] def disjoint(shape: Shape): Boolean = {
    relate(shape) == Disjoint
  }

  private [magellan] def relate(shape: Shape): Relate = {
    val vertices = Array(Point(xmin, ymin),
      Point(xmax, ymin),
      Point(xmax, ymax),
      Point(xmin, ymax)
    )

    // include both edges and diagonals
    // diagonals catch corner cases of bounding box in annulus
    val lines = Array(
      Line(Point(xmin, ymin), Point(xmax, ymin)),
      Line(Point(xmin, ymin), Point(xmin, ymax)),
      Line(Point(xmax, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymax)),
      Line(Point(xmin, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymin))
    )

    // look for strict intersections between the edges of the bounding box and the shape
    val lineIntersections = lines count (shape intersects (_, true))
    val vertexContained = vertices count (shape contains _)

    if (contains(shape.boundingBox)) {
      Contains
    } else if (lineIntersections == 0 && vertexContained == 4) {
      Within
    } else if (lineIntersections > 0 || vertexContained > 0) {
      Intersects
    } else {
      Disjoint
    }
  }

}
