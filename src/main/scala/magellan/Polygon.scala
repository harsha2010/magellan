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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
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
    override val boundingBox: Tuple2[Tuple2[Double, Double], Tuple2[Double, Double]]) extends Shape {

  private [magellan] def contains(point: Point): Boolean = {
    var startIndex = 0
    var endIndex = 1
    val length = xcoordinates.length
    var intersections = 0
    var currentRingIndex = 0
    val start = new Point
    val end = new Point
    while (endIndex < length) {

      start.setX(xcoordinates(startIndex))
      start.setY(ycoordinates(startIndex))
      end.setX(xcoordinates(endIndex))
      end.setY(ycoordinates(endIndex))
      val slope = (end.getY() - start.getY())/ (end.getX() - start.getX())
      val cond1 = (start.getX() <= point.getX()) && (point.getX() < end.getX())
      val cond2 = (end.getX() <= point.getX()) && (point.getX() < start.getX())
      val above = (point.getY() < slope * (point.getX() - start.getX()) + start.getY())
      if ((cond1 || cond2) && above ) intersections+= 1
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
    intersections % 2 != 0
  }

  private [magellan] def intersects(line: Line): Boolean = {
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
        found = edge.intersects(line)
      }
      i += 1
    }
    found
  }

  private [magellan] def contains(line: Line): Boolean = {
    !this.intersects(line) && this.contains(line.getStart()) && this.contains(line.getEnd())
  }

  override def getType(): Int = 5

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  override def transform(fn: (Point) => Point): Shape = {
    val transformedPoints = (xcoordinates zip ycoordinates)
      .map { case (x, y) => Point(x, y) }
      .map(point => point.transform(fn))
    new Polygon(
      indices,
      transformedPoints.map(p => p.getX()),
      transformedPoints.map(p => p.getY()),
      boundingBox)
  }


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

class PolygonUDT extends UserDefinedType[Polygon] {

  override val sqlType: DataType = StructType(Seq(
    StructField("type", IntegerType, nullable = false),
    StructField("xmin", DoubleType, nullable = false),
    StructField("ymin", DoubleType, nullable = false),
    StructField("xmax", DoubleType, nullable = false),
    StructField("ymax", DoubleType, nullable = false),
    StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
    StructField("xcoordinates", ArrayType(DoubleType, containsNull = false), nullable = true),
    StructField("ycoordinates", ArrayType(DoubleType, containsNull = false), nullable = true)
  ))

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(8)
    val polygon = obj.asInstanceOf[Polygon]
    val ((xmin, ymin), (xmax, ymax)) = polygon.boundingBox
    row.update(0, polygon.getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(polygon.indices))
    row.update(6, new DoubleArrayData(polygon.xcoordinates))
    row.update(7, new DoubleArrayData(polygon.ycoordinates))
    row
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val row = datum.asInstanceOf[InternalRow]
    val polygon = new Polygon(
        row.getArray(5).toIntArray(),
        row.getArray(6).toDoubleArray(),
        row.getArray(7).toDoubleArray(),
        ((row.getDouble(1), row.getDouble(2)), (row.getDouble(3), row.getDouble(4)))
      )

    polygon
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
        ((xmin, ymin), (xmax, ymax))
      )
  }
}