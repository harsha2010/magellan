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
package magellan.geometry
import magellan.Relate.{Contains, Disjoint, Touches}
import magellan.{Line, Point, Relate}

class R2Loop extends Loop {

  private var xcoordinates: Array[Double] = _
  private var ycoordinates: Array[Double] = _
  private var startIndex: Int = _
  private var endIndex: Int = _

  private [magellan] def init(
      xcoordinates: Array[Double],
      ycoordinates: Array[Double],
      startIndex: Int,
      endIndex: Int): Unit = {
    this.xcoordinates = xcoordinates
    this.ycoordinates = ycoordinates
    this.startIndex = startIndex
    this.endIndex = endIndex
  }

  /**
    * A loop contains the given point iff the point is properly contained within the
    * interior of the loop.
    *
    * @param point
    * @return
    */
  @inline override final def contains(point: Point): Boolean = {
    containsOrCrosses(point) == Contains
  }

  @inline override final def containsOrCrosses(point: Point) = {

    var inside = false
    var touches = false

    val loopIterator = new LoopIterator()
    while (loopIterator.hasNext && !touches) {
      val edge = loopIterator.next()
      inside ^= R2Loop.intersects(point, edge)
      touches |= edge.contains(point)
    }
    if (touches) Touches else if (inside) Contains else Disjoint
  }

  @inline override def intersects(line: Line): Boolean = {
    var lineIntersects = false
    val loopIterator = new LoopIterator()
    while (loopIterator.hasNext && !lineIntersects) {
      val edge = loopIterator.next()
      lineIntersects = edge.intersects(line)
    }
    lineIntersects
  }

  override def iterator() = new LoopIterator()

  override def toString = s"R2Loop(${xcoordinates.mkString(",")}," +
    s" ${ycoordinates.mkString(",")}," +
    s" $startIndex," +
    s" $endIndex)"

  class LoopIterator extends Iterator[Line] {

    private val start: Point = new Point()
    private val end: Point = new Point()
    private val edge = new Line()
    private var i = startIndex
    private var j = startIndex + 1

    @inline override final def hasNext: Boolean = i < endIndex

    @inline override final def next(): Line = {
      start.setX(xcoordinates(i))
      start.setY(ycoordinates(i))

      end.setX(xcoordinates(j))
      end.setY(ycoordinates(j))

      edge.setStart(start)
      edge.setEnd(end)
      i += 1
      j += 1
      edge
    }
  }
}

private [geometry] object R2Loop {

  /**
    * Checks if the ray from a given point out to infinity on Y axis intersects
    * this line. This function checks for strict intersection:
    * a ray that is collinear with the line is not considered to intersect the line.
    *
    * @param point
    * @param line
    * @return
    */
  @inline def intersects(point: Point, line: Line): Boolean = {
    val (start, end) = (line.getStart(), line.getEnd())
    val slope = (end.getY() - start.getY())/ (end.getX() - start.getX())
    val cond1 = (start.getX() <= point.getX()) && (point.getX() < end.getX())
    val cond2 = (end.getX() <= point.getX()) && (point.getX() < start.getX())
    val above = (point.getY() < slope * (point.getX() - start.getX()) + start.getY())
    ((cond1 || cond2) && above )
  }
}
