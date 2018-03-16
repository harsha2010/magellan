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
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonNode}
import magellan.Relate.{Contains, Touches}
import magellan.geometry.R2Loop
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

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
class Polygon extends Shape {

  private var indices: Array[Int] = _
  private var xcoordinates: Array[Double] = _
  private var ycoordinates: Array[Double] = _
  @transient var loops = new ArrayBuffer[R2Loop]()

  @JsonIgnore private var _boundingBox: BoundingBox = _

  private[magellan] def init(
      indices: Array[Int],
      xcoordinates: Array[Double],
      ycoordinates: Array[Double],
      boundingBox: BoundingBox): Unit = {
    this.indices = indices
    this.xcoordinates = xcoordinates
    this.ycoordinates = ycoordinates
    this._boundingBox = boundingBox
    // initialize the loops
    val offsets = indices.zip(indices.drop(1) ++ Array(xcoordinates.length))
    for ((start, end) <- offsets) {
      loops += ({
        val loop = new R2Loop()
        loop.init(xcoordinates, ycoordinates, start, end - 1)
        loop
      })
    }
  }

  def init(row: InternalRow): Unit = {
    init(row.getArray(5).toIntArray(),
      row.getArray(6).toDoubleArray(),
      row.getArray(7).toDoubleArray(),
      BoundingBox(row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4)))
  }

  def serialize(): InternalRow = {
    val row = new GenericInternalRow(8)
    val BoundingBox(xmin, ymin, xmax, ymax) = boundingBox
    row.update(0, getType())
    row.update(1, xmin)
    row.update(2, ymin)
    row.update(3, xmax)
    row.update(4, ymax)
    row.update(5, new IntegerArrayData(indices))
    row.update(6, new DoubleArrayData(xcoordinates))
    row.update(7, new DoubleArrayData(ycoordinates))
    row
  }

  @JsonProperty
  private def getXCoordinates(): Array[Double] = xcoordinates

  @JsonProperty
  private def getYCoordinates(): Array[Double] = ycoordinates

  @JsonProperty
  override def boundingBox = _boundingBox

  private[magellan] def contains(point: Point): Boolean = {
    val numLoops = loops.size
    var inside = false
    var touches = false
    var i = 0
    while (i < numLoops && !touches) {
      val c = loops(i).containsOrCrosses(point)
      if (c == Touches)
        touches |= true
      else
        inside ^= (c == Contains)

      i += 1
    }
    !touches && inside
  }

  private [magellan] def touches(point: Point): Boolean = {
    loops.exists(_.containsOrCrosses(point) == Touches)
  }

  /**
    * A polygon intersects a line iff it is a proper intersection(strict),
    * or if the interior of the polygon contains any part of the line.
    *
    * @param line
   *  @param strict
    * @return
    */
  private [magellan] def intersects(line: Line, strict: Boolean): Boolean = {
    val interior = this.contains(line.getStart()) || this.contains(line.getEnd())
    val strictIntersects = loops.exists(_.intersects(line))
    strictIntersects || (!strict && interior)
  }

  /**
    * A polygon intersects a polyline iff it is a proper intersection (strict),
    * or if either vertex of the polyline touches the polygon.
    *
    * @param polyline
   *  @param strict
    * @return
    */
  private [magellan] def intersects(polyline: PolyLine, strict: Boolean): Boolean = {
    polyline.intersects(this, strict)
  }

  /**
    * A polygon intersects another polygon iff at least one edge of the
    * other polygon intersects this polygon.
    *
    * @param polygon
    * @return
    */
  private [magellan] def intersects(polygon: Polygon, strict: Boolean): Boolean = {
    val touches =
      polygon.getVertexes().exists(other => this.contains(other)) ||
      this.getVertexes().exists(vertex => polygon.contains(vertex))

    val strictIntersects = polygon.loops
      .exists(otherLoop => this.loops.exists(_.intersects(otherLoop)))

    strictIntersects || (!strict && touches)
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

  /**
   * Checks if the polygon intersects the bounding box in a strict sense.
   *
   * @param box
   * @return
   */
  private [magellan] def intersects(box: BoundingBox): Boolean = {
    val BoundingBox(xmin, ymin, xmax, ymax) = box
    val lines = Array(
      Line(Point(xmin, ymin), Point(xmax, ymin)),
      Line(Point(xmin, ymin), Point(xmin, ymax)),
      Line(Point(xmax, ymin), Point(xmax, ymax)),
      Line(Point(xmin, ymax), Point(xmax, ymax)))

    lines exists (intersects(_))
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

  @JsonIgnore
  override def isEmpty(): Boolean = xcoordinates.length == 0

  def length(): Int = xcoordinates.length

  def getVertex(index: Int) = Point(xcoordinates(index), ycoordinates(index))

  @JsonProperty
  def getRings(): Array[Int] = indices

  @JsonIgnore
  def getNumRings(): Int = indices.length

  def getRing(index: Int): Int = indices(index)

  private def getVertexes():Array[Point]={
    var ring = 0
    val vertexes =  new ArrayBuffer[Point]()
    while(ring < getNumRings()) {
      var i = 0
      var ringPolygon = getRingPolygon(ring)
      while (i < ringPolygon.length() - 1) {
        vertexes += ringPolygon.getVertex(i)
        i += 1
      }
      ring += 1
    }
    vertexes.toArray
  }

  def getRingPolygon(index: Int): Polygon = {
    var startindex = getRing(index)
    if(indices.length==1){
      this
    }
    else {
      var endindex = {
        if (startindex == indices.last) length
        else getRing(index + 1)
      }
      val arrayPoints = new ArrayBuffer[Point]()

      while (startindex < endindex) {
        arrayPoints += getVertex(startindex)
        startindex += 1
      }
      Polygon(Array(0), arrayPoints.toArray)
    }
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

  def readResolve(): AnyRef = {
    loops = new ArrayBuffer[R2Loop]()
    this.init(indices, xcoordinates, ycoordinates, boundingBox)
    this
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
    val polygon = new Polygon()
    polygon.init(
      indices,
      points.map(_.getX()),
      points.map(_.getY()),
      BoundingBox(xmin, ymin, xmax, ymax))
    polygon
  }
}

class PolygonDeserializer extends JsonDeserializer[Polygon] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Polygon = {

    def readDoubleArray(arrayNode: ArrayNode): Array[Double] = {
      val len = arrayNode.size()
      val array = Array.fill(len)(0.0)
      var i = 0
      while (i < len) {
        array.update(i, arrayNode.get(i).asDouble())
        i += 1
      }
      array
    }

    def readIntArray(arrayNode: ArrayNode): Array[Int] = {
      val len = arrayNode.size()
      val array = Array.fill(len)(0)
      var i = 0
      while (i < len) {
        array.update(i, arrayNode.get(i).asInt())
        i += 1
      }
      array
    }

    val polygon = new Polygon()
    val node: JsonNode = p.getCodec.readTree(p)
    val xcoordinates = readDoubleArray(node.get("xcoordinates").asInstanceOf[ArrayNode])
    val ycoordinates = readDoubleArray(node.get("ycoordinates").asInstanceOf[ArrayNode])
    val rings = readIntArray(node.get("rings").asInstanceOf[ArrayNode])
    val boundingBox = node.get("boundingBox")
    val xmin = boundingBox.get("xmin").asDouble()
    val ymin = boundingBox.get("ymin").asDouble()
    val xmax = boundingBox.get("xmax").asDouble()
    val ymax = boundingBox.get("ymax").asDouble()

    polygon.init(rings, xcoordinates, ycoordinates, BoundingBox(xmin, ymin, xmax, ymax))
    polygon
  }

}
