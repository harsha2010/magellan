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

import org.apache.spark.sql.types._

/**
 * Line segment between two points.
 */
@SQLUserDefinedType(udt = classOf[LineUDT])
class Line extends Shape {

  private var start: Point = _
  private var end: Point = _

  def setStart(start: Point) { this.start = start }
  def setEnd(end: Point) { this.end = end }

  def getStart() = start
  def getEnd() = end

  private [magellan] def intersects(other: Line): Boolean = {
    def area(a: Point, b: Point, c: Point) = {
      ((c.getY() - a.getY()) * (b.getX() - a.getX())) - ((b.getY() - a.getY()) * (c.getX() - a.getX()))
    }

    def ccw(a: Point, b: Point, c: Point) = {
      area(a, b, c) > 0
    }

    // test for degeneracy
    if (start == end) {
      area(start, other.start, other.end) == 0
    } else if (other.start == other.end) {
      area(start, end, other.start) == 0
    } else if (start == other.start ||
      end == other.end ||
      start == other.end ||
      end == other.start) {
      true
    } else {

      ccw(start, other.start, other.end) != ccw(end, other.start, other.end) &&
        ccw(start, end, other.start) != ccw(start, end, other.end)
    }
  }

  override def getType(): Int = 2

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Shape = ???

  override def boundingBox: ((Double, Double), (Double, Double)) = {
    val (xmin, xmax) = if (start.getX() < end.getX()) {
      (start.getX(), end.getX())
    } else {
      (end.getX(), start.getX())
    }
    val (ymin, ymax) = if (start.getY() < end.getY()) {
      (start.getY(), end.getY())
    } else {
      (end.getY(), start.getY())
    }
    ((xmin, ymin), (xmax, ymax))
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Line]

  override def equals(other: Any): Boolean = other match {
    case that: Line =>
      (that canEqual this) &&
        start == that.start &&
        end == that.end
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(start, end)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Line {

  def apply(start: Point, end: Point): Line = {
    val line = new Line()
    line.setStart(start)
    line.setEnd(end)
    line
  }
}