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

import com.esri.core.geometry.{Point => ESRIPoint, Polygon => ESRIPolygon, Polyline => ESRIPolyLine}
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable.ArrayBuffer

object TestingUtils {

  val ABS_TOL_MSG = " using absolute tolerance"
  val REL_TOL_MSG = " using relative tolerance"

  /**
   * Private helper function for comparing two values using relative tolerance.
   * Note that if x or y is extremely close to zero, i.e., smaller than Double.MinPositiveValue,
   * the relative tolerance is meaningless, so the exception will be raised to warn users.
   */
  private def RelativeErrorComparison(x: Double, y: Double, eps: Double): Boolean = {
    val absX = math.abs(x)
    val absY = math.abs(y)
    val diff = math.abs(x - y)
    if (x == y) {
      true
    } else if (absX < Double.MinPositiveValue || absY < Double.MinPositiveValue) {
      throw new TestFailedException(
        s"$x or $y is extremely close to zero, so the relative tolerance is meaningless.", 0)
    } else {
      diff < eps * math.min(absX, absY)
    }
  }

  /**
   * Private helper function for comparing two values using absolute tolerance.
   */
  private def AbsoluteErrorComparison(x: Double, y: Double, eps: Double): Boolean = {
    math.abs(x - y) < eps
  }

  case class CompareDoubleRightSide(fun: (Double, Double, Double) => Boolean,
    y: Double, eps: Double, method: String)

  /**
   * Implicit class for comparing two double values using relative tolerance or absolute tolerance.
   */
  implicit class DoubleWithAlmostEquals(val x: Double) {

    /**
     * When the difference of two values are within eps, returns true; otherwise, returns false.
     */
    def ~=(r: CompareDoubleRightSide): Boolean = r.fun(x, r.y, r.eps)

    /**
     * When the difference of two values are within eps, returns false; otherwise, returns true.
     */
    def !~=(r: CompareDoubleRightSide): Boolean = !r.fun(x, r.y, r.eps)

    /**
     * Throws exception when the difference of two values are NOT within eps;
     * otherwise, returns true.
     */
    def ~==(r: CompareDoubleRightSide): Boolean = {
      if (!r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Expected $x and ${r.y} to be within ${r.eps}${r.method}.", 0)
      }
      true
    }

    /**
     * Throws exception when the difference of two values are within eps; otherwise, returns true.
     */
    def !~==(r: CompareDoubleRightSide): Boolean = {
      if (r.fun(x, r.y, r.eps)) {
        throw new TestFailedException(
          s"Did not expect $x and ${r.y} to be within ${r.eps}${r.method}.", 0)
      }
      true
    }

    /**
     * Comparison using absolute tolerance.
     */
    def absTol(eps: Double): CompareDoubleRightSide =
      CompareDoubleRightSide(AbsoluteErrorComparison, x, eps, ABS_TOL_MSG)

    /**
     * Comparison using relative tolerance.
     */
    def relTol(eps: Double): CompareDoubleRightSide =
      CompareDoubleRightSide(RelativeErrorComparison, x, eps, REL_TOL_MSG)

    override def toString: String = x.toString
  }

  def fromESRI(esriPolygon: ESRIPolygon): Polygon = {
    val length = esriPolygon.getPointCount
    if (length == 0) {
      Polygon(Array[Int](), Array[Point]())
    } else {
      val indices = ArrayBuffer[Int]()
      indices.+=(0)
      val points = ArrayBuffer[Point]()
      var start = esriPolygon.getPoint(0)
      var currentRingIndex = 0
      points.+=(Point(start.getX(), start.getY()))

      for (i <- (1 until length)) {
        val p = esriPolygon.getPoint(i)
        val j = esriPolygon.getPathEnd(currentRingIndex)
        if (j < length) {
          val end = esriPolygon.getPoint(j)
          if (p.getX == end.getX && p.getY == end.getY) {
            indices.+=(i)
            currentRingIndex += 1
            // add start point
            points.+= (Point(start.getX(), start.getY()))
            start = end
          }
        }
        points.+=(Point(p.getX(), p.getY()))
      }
      Polygon(indices.toArray, points.toArray)
    }
  }

  def toESRI(polygon: Polygon): ESRIPolygon = {
    val p = new ESRIPolygon()
    val indices = polygon.indices
    val length = polygon.xcoordinates.length
    if (length > 0) {
      var startIndex = 0
      var endIndex = 1
      var currentRingIndex = 0
      p.startPath(
        polygon.xcoordinates(startIndex),
        polygon.ycoordinates(startIndex))

      while (endIndex < length) {
        p.lineTo(polygon.xcoordinates(endIndex),
          polygon.ycoordinates(endIndex))
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
            p.startPath(
              polygon.xcoordinates(startIndex),
              polygon.ycoordinates(startIndex))
          }
        }
      }
    }
    p
  }

  def toESRI(line: Line): ESRIPolyLine = {
    val l = new ESRIPolyLine()
    l.startPath(line.getStart().getX(), line.getStart().getY())
    l.lineTo(line.getEnd().getX(), line.getEnd().getY())
    l
  }

  def toESRI(point: Point): ESRIPoint = {
    val esriPoint = new ESRIPoint()
    esriPoint.setXY(point.getX(), point.getY())
    esriPoint
  }

}
