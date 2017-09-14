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

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import magellan.Shape.{area, ccw}
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

  @JsonProperty
  def getStart() = start

  @JsonProperty
  def getEnd() = end

  @inline private [magellan] def contains(point: Point): Boolean = {
    area(start, end, point) == 0 && {
      val startX = start.getX()
      val endX = end.getX()
      val pointX = point.getX()
      val (lower, upper) = if (startX < endX) (startX, endX) else (endX, startX)
      lower <= pointX && pointX <= upper
    }
  }

  @inline private [magellan] def collinear(line: Line): Boolean = {
    val (lineStart, lineEnd) = (line.getStart(), line.getEnd())

    area(start, end, line.getStart()) == 0 && area(start, end, line.getEnd()) == 0
  }

  @inline private [magellan] def contains(line: Line): Boolean = {
    val c = collinear(line)
    if (c) {
      // check if both points of the line are within the start and end
      val (lineStart, lineEnd) = (line.getStart(), line.getEnd())
      val (leftX, rightX) = if (start.getX() < end.getX()) {
        (start.getX(), end.getX())
      } else {
        (end.getX(), start.getX())
      }

      val (leftY, rightY) = if (start.getY() < end.getY()) {
        (start.getY(), end.getY())
      } else {
        (end.getY(), start.getY())
      }

      val (lineStartX, lineEndX) = (lineStart.getX(), lineEnd.getX())
      val (lineStartY, lineEndY) = (lineStart.getY(), lineEnd.getY())

      (lineStartX <= rightX) && (lineStartX >= leftX) &&
      (lineStartY <= rightY) && (lineStartY >= leftY) &&
      (lineEndX <= rightX) && (lineEndX >= leftX) &&
      (lineEndY <= rightY) && (lineEndY >= leftY)
    } else {
      false
    }
  }

  @inline private [magellan] def touches(other: Line): Boolean = {
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
    } else if (
      contains(other.start) ||
      contains(other.end) ||
      other.contains(this.start) ||
      other.contains(this.end)) {
      true
    } else {
      false
    }
  }

  @inline private [magellan] def intersects(other: Line): Boolean = {

    // test for degeneracy
    if (this.touches(other)) {
      true
    } else {

      ccw(start, other.start, other.end) != ccw(end, other.start, other.end) &&
        ccw(start, end, other.start) != ccw(start, end, other.end)
    }
  }

  @JsonIgnore @inline private [magellan] def getMid(): Point = {
    Point(start.getX() + (end.getX() - start.getX())/ 2, start.getY() + (end.getY() - start.getY())/ 2)
  }

  override def getType(): Int = 2

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Shape = ???

  override def boundingBox: BoundingBox = {
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
    BoundingBox(xmin, ymin, xmax, ymax)
  }

  @JsonIgnore
  override def isEmpty(): Boolean = start == null || end == null

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


  override def toString = s"Line($start, $end)"
}

object Line {

  def apply(start: Point, end: Point): Line = {
    val line = new Line()
    line.setStart(start)
    line.setEnd(end)
    line
  }
}