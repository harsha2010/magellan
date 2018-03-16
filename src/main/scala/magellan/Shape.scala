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

import com.esri.core.geometry.OperatorBuffer
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import magellan.esri.ESRIUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends DataType with Serializable {

  override def defaultSize: Int = 4096

  override def asNullable: DataType = this

  @JsonIgnore
  def getType(): Int

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  def transform(fn: Point => Point): Shape

  /**
   * Tests whether this <code>Shape</code>
   * is topologically valid, according to the OGC SFS specification.
   * <p>
   * For validity rules see the Javadoc for the specific Shape subclass.
   *
   * @return <code>true</code> if this <code>Shape</code> is valid
   */
  @JsonIgnore
  def isValid(): Boolean = true

  /**
   * Tests whether this shape intersects the argument shape.
   * <p>
   * A strict intersection is one where
   * <ul>
   * <li>The two geometries have at least one point in common
   * <li><code>! (other.contains(this) || this.contains(other)) </code>
   * </ul>
   *
   * @param  other  the <code>Shape</code> with which to compare this <code>Shape</code>
   * @param  strict is this intersection strict?
   * @return        <code>true</code> if the two <code>Shape</code>s intersect
   *
   * @see Shape#disjoint
   */
  def intersects(other: Shape, strict: Boolean): Boolean = {
    if (!boundingBox.disjoint(other.boundingBox)) {
      (this, other) match {
        case (p: Point, q: Point) => p.equals(q)
        case (p: Point, q: Polygon) => q.touches(p)
        case (p: Polygon, q: Point) => p.touches(q)
        case (p: Polygon, q: Line) => p.intersects(q, strict)
        case (p: Polygon, q: PolyLine) => p.intersects(q, strict)
        case (p: Polygon, q: Polygon) => p.intersects(q, strict)
        case (p: PolyLine, q: Line) => p.intersects(q, strict)
        case (p: PolyLine, q: Polygon) => p.intersects(q, strict)
        case (p: Line, q: Polygon) => q.intersects(p, strict)
        case (p: Line, q: PolyLine) => q.intersects(p, strict)
        case _ => ???
      }
    } else  {
      false
    }
  }

  /**
   * Computes the non strict intersection between two shapes.
   * <p>
   * The <code>intersects</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>The two geometries have at least one point in common
   * <li><code>! other.disjoint(this) = true</code>
   * <br>(<code>intersects</code> is the inverse of <code>disjoint</code>)
   * </ul> *
   *
   * @param other
   * @return
   */
  def intersects(other: Shape): Boolean = this.intersects(other, false)

  /**
   * Tests whether this shape contains the
   * argument shape.
   * <p>
   * The <code>contains</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>Every point of the other shape is a point of this shape,
   * and the interiors of the two shapes have at least one point in common.
   * <li><code>other.within(this) = true</code>
   * <br>(<code>contains</code> is the converse of {@link #within} )
   * </ul>
   * An implication of the definition is that "Shapes do not
   * contain their boundary".  In other words, if a shape A is a subset of
   * the points in the boundary of a shape B, <code>B.contains(A) = false</code>.
   * (As a concrete example, take A to be a Line which lies in the boundary of a Polygon B.)
   * For a predicate with similar behaviour but avoiding
   * this subtle limitation, see {@link #covers}.
   *
   * @param other
   * @return true if this shape contains the other.
   */
  def contains(other: Shape): Boolean = {
    // check if the bounding box intersects other's bounding box.
    // if not, no need to check further

    if (boundingBox.contains(other.boundingBox)) {
      (this, other) match {

        case (p: Point, q: Point) => p.equals(q)
        case (p: Point, q: Line) => false
        case (p: Point, q: Polygon) => false
        case (p: Point, q: PolyLine) => false

        case (p: Polygon, q: Point) => p.contains(q)
        case (p: Polygon, q: Line) => ???

        case (p: Line, q: Point) => p.contains(q)
        case (p: Line, q: Line) => p.contains(q)

        case (p: PolyLine, q: Point) => p.contains(q)

        case _ => ???
      }
    } else  {
      false
    }

  }

  @JsonProperty
  def boundingBox: BoundingBox

  /**
   * Tests whether this shape is within the
   * specified shape.
   * <p>
   * The <code>within</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>Every point of this geometry is a point of the other geometry,
   * and the interiors of the two geometries have at least one point in common.
   * <li><code>other.contains(this) = true</code>
   * <br>(<code>within</code> is the converse of {@link #contains})
   * </ul>
   * An implication of the definition is that
   * "The boundary of a Shape is not within the Shape".
   * In other words, if a shape A is a subset of
   * the points in the boundary of a shape B, <code>A.within(B) = false</code>
   * (As a concrete example, take A to be a Line which lies in the boundary of a Polygon B.)
   * For a predicate with similar behaviour but avoiding
   * this subtle limitation, see {@link #coveredBy}.
   *
   * @param  other  the <code>Shape</code> with which to compare this <code>Shape</code>
   * @return        <code>true</code> if this <code>Shape</code> is within
   *                <code>other</code>
   *
   * @see Geometry#contains
   */
  def within(other: Shape): Boolean = other.contains(this)

  /**
   * Tests whether the set of points covered by this <code>Shape</code> is
   * empty.
   *
   * @return <code>true</code> if this <code>Shape</code> does not cover any points
   */
  @JsonIgnore
  def isEmpty(): Boolean

  def buffer(distance: Double): Polygon = {
    val esriGeometry = ESRIUtil.toESRIGeometry(this)
    val bufferedEsriGeometry = OperatorBuffer.local()
      .execute(esriGeometry, null, distance, null)

    ESRIUtil.fromESRIGeometry(bufferedEsriGeometry).asInstanceOf[Polygon]
  }
}

/**
 * A null shape indicates an absence of geometric data.
 */
object NullShape extends Shape {

  override def getType() = 0

  override def isEmpty() = true

  override def intersects(shape: Shape, strict: Boolean = false): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

  override def boundingBox = BoundingBox(
      Int.MinValue, Int.MinValue,
      Int.MaxValue, Int.MaxValue
    )
}

object Shape {

  @inline final def area(a: Point, b: Point, c: Point) = {
    ((c.getY() - a.getY()) * (b.getX() - a.getX())) - ((b.getY() - a.getY()) * (c.getX() - a.getX()))
  }

  @inline final def ccw(a: Point, b: Point, c: Point) = {
    area(a, b, c) > 0
  }
}

