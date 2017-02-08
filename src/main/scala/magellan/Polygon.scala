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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.types._

/**
 * A polygon consists of one or more rings. A ring is a connected sequence of four or more points
 * that form a closed, non-self-intersecting loop. A polygon may contain multiple outer rings.
 * The order of vertices or orientation for a ring indicates which side of the ring is the interior
 * of the polygon. The neighborhood to the right of an observer walking along the ring
 * in vertex order is the neighborhood inside the polygon.
 * Vertices of rings defining holes in polygons are in a counterclockwise direction.
 * Vertices for a single, ringed polygon are, therefore, always in clockwise order.
 * The rings of a polygon are referred to as its parts.
 *
 */
@SQLUserDefinedType(udt = classOf[PolygonUDT])
class Polygon(
    val indices: Array[Int],
    val xcoordinates: Array[Double],
    val ycoordinates: Array[Double],
    override val boundingBox: BoundingBox) extends Shape {

  private def this() {this(Array(0), Array(), Array(), BoundingBox(0,0,0,0))}

  @inline private def intersects(point: Point, line: Line): Boolean = {
    val (start, end) = (line.getStart(), line.getEnd())
    val slope = (end.getY() - start.getY())/ (end.getX() - start.getX())
    val cond1 = (start.getX() <= point.getX()) && (point.getX() < end.getX())
    val cond2 = (end.getX() <= point.getX()) && (point.getX() < start.getX())
    val above = (point.getY() < slope * (point.getX() - start.getX()) + start.getY())
    ((cond1 || cond2) && above )
  }

  @JsonProperty
  def getXCoordinates(): Array[Double] = xcoordinates

  @JsonProperty
  def getYCoordinates(): Array[Double] = ycoordinates

  private [magellan] def contains(point: Point): Boolean = {
    var startIndex = 0
    var endIndex = 1
    val length = xcoordinates.length
    var intersections = 0
    var currentRingIndex = 0
    val start = new Point
    val end = new Point
    val line = new Line()
    var touches = false

    while (endIndex < length && !touches) {

      start.setX(xcoordinates(startIndex))
      start.setY(ycoordinates(startIndex))
      end.setX(xcoordinates(endIndex))
      end.setY(ycoordinates(endIndex))

      line.setStart(start)
      line.setEnd(end)
      touches = line.contains(point)

      intersections += {if (intersects(point, line)) 1 else 0}
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
    !touches && (intersections % 2 != 0)
  }

  private [magellan] def touches(point: Point): Boolean = {
    var startIndex = 0
    var endIndex = 1
    val length = xcoordinates.length
    var currentRingIndex = 0
    val start = new Point
    val end = new Point
    var touches = false
    val line = new Line()

    while (endIndex < length && !touches) {

      start.setX(xcoordinates(startIndex))
      start.setY(ycoordinates(startIndex))
      end.setX(xcoordinates(endIndex))
      end.setY(ycoordinates(endIndex))
      line.setStart(start)
      line.setEnd(end)
      touches = line.contains(point)
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
    touches
  }

  private [magellan] def intersects(line: Line): Boolean = {
    // Check if any edge intersects this line
    val length = xcoordinates.length
    var intersects = false
    val start: Point = new Point()
    val end: Point = new Point()
    val edge = new Line()
    var startIndex = 0
    var endIndex = 1
    var currentRingIndex = 0

    while (endIndex < length && !intersects) {

      start.setX(xcoordinates(startIndex))
      start.setY(ycoordinates(startIndex))

      end.setX(xcoordinates(endIndex))
      end.setY(ycoordinates(endIndex))

      edge.setStart(start)
      edge.setEnd(end)

      intersects = edge.intersects(line)

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
    intersects
  }

  private [magellan] def contains(line: Line): Boolean = {
    /**
      * Algorithm:
      * If a line does not intersect any edge of the polygon:
      *   If start and end points are not contained in the polygon then false
      *   Else true
      * Else if line touches an edge/ boundary of the polygon:
      *   False
      * Else if a midpoint is not contained in/ touches the polygon
      *   False
      * Else if start is not contained in or touches polygon
      *   end should touch or be contained in polygon
      * Else
      *   True
      */


    var startIndex = 0
    var endIndex = 1
    val length = xcoordinates.length
    var currentRingIndex = 0
    val start = new Point
    val end = new Point
    val edge = new Line()
    var intersectsAnEdge = false
    var onBoundary = false

    val lineMid = line.getMid()
    var startI = 0
    var endI = 0
    var midI = 0
    var startTouches = false
    var endTouches = false
    var midTouches = false

    while (endIndex < length) {

      start.setX(xcoordinates(startIndex))
      start.setY(ycoordinates(startIndex))
      end.setX(xcoordinates(endIndex))
      end.setY(ycoordinates(endIndex))

      edge.setStart(start)
      edge.setEnd(end)
      intersectsAnEdge |= line.intersects(edge)
      onBoundary |= edge.contains(line)

      startI += {if (intersects(line.getStart(), edge)) 1 else 0}
      endI += {if (intersects(line.getEnd(), edge)) 1 else 0}
      midI += {if (intersects(lineMid, edge)) 1 else 0}

      startTouches |= edge.contains(line.getStart())
      endTouches |= edge.contains(line.getEnd())
      midTouches |= edge.contains(lineMid)

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

    val startContained = (startI % 2 != 0)
    val endContained = (endI % 2 != 0)
    val midContained = (midI % 2 != 0)

    if (!intersectsAnEdge) {
      startContained && endContained
    } else if (!midContained && !midTouches) {
      false
    } else if (!startContained && !startTouches) {
      endContained || endTouches
    } else {
      !onBoundary
    }
  }

  private [magellan] def contains(box: BoundingBox): Boolean = {
    val BoundingBox(xmin, ymin, xmax, ymax) = box
    val lines = Array(
      Line(Point(xmin, ymin), Point(xmax, ymin)),
      Line(Point(xmin, ymin), Point(xmin, ymax)),
      Line(Point(xmax, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymax)))

    !(lines exists (!contains(_)))
  }

  private [magellan] def intersects(box: BoundingBox): Boolean = {
    val BoundingBox(xmin, ymin, xmax, ymax) = box
    val lines = Array(
      Line(Point(xmin, ymin), Point(xmax, ymin)),
      Line(Point(xmin, ymin), Point(xmin, ymax)),
      Line(Point(xmax, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymax)))

    lines exists (intersects(_))
  }

  private [magellan] def intersects(point: Point): Boolean = {
    // Check if any edge intersects this line
    var i = 0
    val length = xcoordinates.length
    var found = false
    var start:Point = null
    var end:Point = new Point()
    val edge = new Line()

    while (i < length && !found) {
      if (start == null) {
        start = new Point()
        start.setX(xcoordinates(i))
        start.setY(ycoordinates(i))
      } else {
        start = end
        end = new Point()
        end.setX(xcoordinates(i))
        end.setY(ycoordinates(i))
        edge.setStart(start)
        edge.setEnd(end)
        found = edge.contains(point)
      }
      i += 1
    }
    found
  }


  @JsonProperty
  override def getType(): Int = 5

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Shape = ???


  def canEqual(other: Any): Boolean = other.isInstanceOf[Polygon]

  override def equals(other: Any): Boolean = other match {
    case that: Polygon =>
      (that canEqual this) &&
        indices.deep == that.indices.deep &&
        xcoordinates.deep == that.xcoordinates.deep &&
        ycoordinates.deep == that.ycoordinates.deep
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(indices, xcoordinates, ycoordinates)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

}

object Polygon {

  def apply(indices: Array[Int], points: Array[Point]): Polygon = {
    // look for the extremities
    var xmin: Double = Double.MaxValue
    var ymin: Double = Double.MaxValue
    var xmax: Double = Double.MinValue
    var ymax: Double = Double.MinValue
    val size = points.length
    var i = 0
    while (i < size) {
      val point = points(i)
      val (x, y) = (point.getX(), point.getY())
      if (xmin > x) {
        xmin = x
      }
      if (ymin > y) {
        ymin = y
      }
      if (xmax < x) {
        xmax = x
      }
      if (ymax < y) {
        ymax = y
      }
      i += 1
    }
    new Polygon(
        indices,
        points.map(_.getX()),
        points.map(_.getY()),
        BoundingBox(xmin, ymin, xmax, ymax)
      )
  }
}
