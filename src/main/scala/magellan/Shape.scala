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
import org.apache.spark.sql.types._

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends DataType with Serializable {

  override def defaultSize: Int = 4096

  override def asNullable: DataType = this

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
  def isValid(): Boolean = true

  /**
   * Tests whether this shape intersects the argument shape.
   * <p>
   * The <code>intersects</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>The two geometries have at least one point in common
   * <li><code>! other.disjoint(this) = true</code>
   * <br>(<code>intersects</code> is the inverse of <code>disjoint</code>)
   * </ul>
   *
   * @param  other  the <code>Shape</code> with which to compare this <code>Shape</code>
   * @return        <code>true</code> if the two <code>Shape</code>s intersect
   *
   * @see Shape#disjoint
   */
  def intersects(other: Shape): Boolean = {
    intersects(other, 7)
  }

  /**
   * Tests whether this shape intersects the argument shape.
   * <p>
   * The <code>intersects</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>The two geometries have at least one point in common
   * <li><code>! other.disjoint(this) = true</code>
   * <br>(<code>intersects</code> is the inverse of <code>disjoint</code>)
   * </ul>
   *
   * @param  other  the <code>Shape</code> with which to compare this <code>Shape</code>
   * @param bitMask The dimension of the intersection. The value is either -1, or a bitmask mask of values (1 << dim).
   *                The value of -1 means the lower dimension in the intersecting pair.
   *                This is a fastest option when intersecting polygons with polygons or polylines.
   *                The bitmask of values (1 << dim), where dim is the desired dimension value, is used to indicate
   *                what dimensions of geometry one wants to be returned. For example, to return
   *                multipoints and lines only, pass (1 << 0) | (1 << 1), which is equivalen to 1 | 2, or 3.
   * @return        <code>true</code> if the two <code>Shape</code>s intersect
   *
   * @see Shape#disjoint
   */
  def intersects(other: Shape, bitMask: Int): Boolean = {
    val BoundingBox(xmin, ymin, xmax, ymax) = this.boundingBox
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other.boundingBox
    if ((xmin <= otherxmin && xmax >= otherxmin && ymin <= otherymin && ymax >= otherymin) ||
      (otherxmin <= xmin && otherxmax >= xmin && otherymin <= ymin && otherymax >= ymin)) {
      (this, other) match {
        case (p: Point, q: Point) => p.equals(q)
        case (p: Point, q: Polygon) => q.intersects(Line(p, p))
        case (p: Polygon, q: Point) => p.intersects(Line(q, q))
        case (p: Polygon, q: Line) => p.intersects(q)
        case (p: Polygon, q: PolyLine) => p.intersects(q)
        case (p: PolyLine, q: Line) => p.intersects(q)
        case (p: Line, q: PolyLine) => q.intersects(p)
        case _ => ???
      }
    } else  {
      false
    }
  }

  /**
   * Tests whether this shape touches the
   * argument shape.
   * <p>
   * The <code>touches</code> predicate has the following equivalent definitions:
   * <ul>
   * <li>The geometries have at least one point in common,
   * but their interiors do not intersect.
   * If both shapes have dimension 0, the predicate returns <code>false</code>,
   * since points have only interiors.
   * This predicate is symmetric.
   *
   *
   * @param  other  the <code>Shape</code> with which to compare this <code>Shape</code>
   * @return        <code>true</code> if the two <code>Shape</code>s touch;
   *                Returns <code>false</code> if both <code>Shape</code>s are points
   */
  def touches(other: Shape): Boolean = {
    ???
  }

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
    // check if the bounding box encompasses other's bounding box.
    // if not, no need to check further
    val BoundingBox(xmin, ymin, xmax, ymax) = this.boundingBox
    val BoundingBox(otherxmin, otherymin, otherxmax, otherymax) = other.boundingBox
    if (xmin <= otherxmin && ymin <= otherymin && xmax >= otherxmax && ymax >= otherymax) {
      (this, other) match {
        case (p: Point, q: Point) => p.equals(q)
        case (p: Point, q: Polygon) => false
        case (p: Polygon, q: Point) => p.contains(q)
        case (p: Polygon, q: Line) => p.contains(q)
        case (p: Line, q: Point) => p.contains(q)
        case (p: Line, q: Line) => p.contains(q)
        case _ => ???
      }
    } else  {
      false
    }

  }

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
   * Computes a <code>Shape</code> representing the point-set which is
   * common to both this <code>Shape</code> and the <code>other</code> Geometry.
   * <p>
   * The intersection of two shapes of different dimension produces a result
   * shape of dimension less than or equal to the minimum dimension of the input
   * shapes.
   *
   * @param  other the <code>Shape</code> with which to compute the intersection
   * @return a Shape representing the point-set common to the two <code>Shape</code>s
   */
  def intersection(other: Shape): Shape = {
   ???
  }

  /**
   * Computes a <code>Shape</code> representing the difference between
   * this <code>Shape</code> and the <code>other</code> Geometry.
   * <p>
   *
   * @param  other the <code>Shape</code> with which to compute the difference
   * @return a Shape representing the difference between to the two <code>Shape</code>s
   */
  def difference(other: Shape): Shape = {
    ???
  }

  /**
   * Tests whether the set of points covered by this <code>Shape</code> is
   * empty.
   *
   * @return <code>true</code> if this <code>Shape</code> does not cover any points
   */
  def isEmpty(): Boolean = ???

  /**
   * Computes the smallest convex <code>Polygon</code> that contains all the
   * points in the <code>Geometry</code>. This applies only to <code>Geometry</code>
   * s which contain 3 or more points;
   * For degenerate classes the Shape that is returned is specified as follows:
   * <TABLE>
   * <TR>
   * <TH>Number of <code>Point</code>s in argument <code>Shape</code></TH>
   * <TH><code>Shape</code></TH>
   * </TR>
   * <TR>
   * <TD>0</TD>
   * <TD><code>NullShape</code></TD>
   * <TD>1</TD>
   * <TD><code>Point</code></TD>
   * <TD>2</TD>
   * <TD><code>Line</code></TD>
   * <TD>3 or more</TD>
   * <TD><code>Polygon</code></TD>
   * </TR>
   * </TABLE>
   *
   * @return    the minimum-area convex Shape containing this <code>Shape</code>'
   *            s points
   */
  def convexHull(): Shape = {
    ???
  }

  /**
   * Computes a buffer area around this <code>Shape</code> having the given width.
   * The buffer of a <code>Shape</code> is the Minkowski sum or difference of the geometry
   * with a disc of radius abs(distance).
   *
   * @param distance
   * @return
   */
  def buffer(distance: Double): Shape = {
    ???
  }
}

/**
 * A null shape indicates an absence of geometric data.
 */
object NullShape extends Shape {

  override def getType() = 0

  override def intersects(shape: Shape): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

  override def boundingBox = BoundingBox(
      Int.MinValue, Int.MinValue,
      Int.MaxValue, Int.MaxValue
    )
}

object Shape {

  def area(a: Point, b: Point, c: Point) = {
    ((c.getY() - a.getY()) * (b.getX() - a.getX())) - ((b.getY() - a.getY()) * (c.getX() - a.getX()))
  }

  def ccw(a: Point, b: Point, c: Point) = {
    area(a, b, c) > 0
  }

}

