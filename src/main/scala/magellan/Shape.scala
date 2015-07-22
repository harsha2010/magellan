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

import java.io.ObjectInputStream

import com.esri.core.geometry.{Geometry => ESRIGeometry, Point => ESRIPoint,
  Polygon => ESRIPolygon, Polyline => ESRIPolyline, _}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends DataType with Serializable {

  val shapeType: Int

  private[magellan] val delegate: ESRIGeometry

  override def defaultSize: Int = 4096

  override def asNullable: DataType = this

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
    val factory = OperatorFactoryLocal.getInstance
    val op = factory.getOperator(Operator.Type.Intersection).asInstanceOf[OperatorIntersection]
    val result_cursor = op.execute(new SimpleGeometryCursor(
      delegate), new SimpleGeometryCursor(other.delegate), null, null, bitMask)
    var esriGeom: ESRIGeometry = null
    do {
      esriGeom = result_cursor.next()
    } while (esriGeom != null && esriGeom.isEmpty)
    esriGeom != null && !esriGeom.isEmpty
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
    GeometryEngine.touches(delegate, other.delegate, null)
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
    GeometryEngine.contains(delegate, other.delegate, null)
  }

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
    val factory = OperatorFactoryLocal.getInstance
    val op = factory.getOperator(Operator.Type.Intersection).asInstanceOf[OperatorIntersection]
    val result_cursor = op.execute(new SimpleGeometryCursor(
      delegate), new SimpleGeometryCursor(other.delegate), null, null, 7)
    var esriGeom: ESRIGeometry = null
    do {
      esriGeom = result_cursor.next()
    } while (esriGeom != null && esriGeom.isEmpty)
    Shape.fromESRI(esriGeom)
  }

  /**
   * Tests whether the set of points covered by this <code>Shape</code> is
   * empty.
   *
   * @return <code>true</code> if this <code>Shape</code> does not cover any points
   */
  def isEmpty(): Boolean = delegate.isEmpty

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

}

/**
 * A null shape indicates an absence of geometric data.
 */
object NullShape extends Shape {

  override final val shapeType: Int = 0

  override private[magellan] val delegate = null

  override def intersects(shape: Shape): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

}

private[magellan] object Shape {

  def deserialize(obj: Any): Shape = {
    obj match {
      case s: Shape => s
      case row: Row => row(0).asInstanceOf[Shape]
    }
  }

  def fromESRI(esriGeom: ESRIGeometry): Shape = {
    esriGeom match {
      case esriPoint: ESRIPoint => Point.fromESRI(esriPoint)
      case esriPolygon: ESRIPolygon => Polygon.fromESRI(esriPolygon)
      case esriPolyline: ESRIPolyline => {
        if (esriPolyline.getPointCount == 2) {
          Line.fromESRI(esriPolyline)
        } else {
          PolyLine.fromESRI(esriPolyline)
        }
      }
      case _ => ???
    }
  }

  private def toInt(row: Row, index: Int): Int = {
    row(index) match {
      case i: Int => i
      case i: Long => i.toInt
    }
  }
}
