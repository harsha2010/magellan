/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry, LineString, MultiLineString, Point => JTSPoint, Polygon => JTSPolygon}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends Serializable {

  val shapeType: Int

  private lazy val delegate = toJTS()

  private[magellan] def toJTS(): JTSGeometry

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
  def isValid(): Boolean = delegate.isValid
  /**
   *
   * @param shape
   * @return true if the two shapes intersect each other.
   */
  def intersects(shape: Shape): Boolean = {
    delegate.intersects(shape.delegate)
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
    delegate.contains(other.delegate)
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
    val jtsGeom = delegate.intersection(other.delegate)
    Shape.fromJTS(jtsGeom)
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
   *   <TR>
   *     <TH>Number of <code>Point</code>s in argument <code>Shape</code></TH>
   *     <TH><code>Shape</code></TH>
   *   </TR>
   *   <TR>
   *     <TD>0</TD>
   *     <TD><code>NullShape</code></TD>
   *     <TD>1</TD>
   *     <TD><code>Point</code></TD>
   *     <TD>2</TD>
   *     <TD><code>Line</code></TD>
   *     <TD>3 or more</TD>
   *     <TD><code>Polygon</code></TD>
   *   </TR>
   * </TABLE>
   *
   * @return    the minimum-area convex Shape containing this <code>Shape</code>'
   *            s points
   */
  def convexHull(): Shape = {
    Shape.fromJTS(delegate.convexHull())
  }
}

/**
 * A null shape indicates an absence of geometric data.
 */
object NullShape extends Shape {

  override final val shapeType: Int = 0

  override private[magellan] def toJTS() = null

  override def intersects(shape: Shape): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

}

private[magellan] object Shape {

  val TYPES = Map[Int, UserDefinedType[_ <: Shape]](
      1 -> new PointUDT,
      2 -> new LineUDT,
      3 -> new PolyLineUDT,
      5 -> new PolygonUDT
    )

  def deserialize(obj: Any): Shape = {
    obj match {
      case s: Shape => s
      case row: Row =>
        TYPES.get(toInt(row, 0)).fold[Shape](NullShape)(_.deserialize(row))
    }
  }

  def fromJTS(jtsGeom: JTSGeometry): Shape = {
    jtsGeom match {
      case jtsPoint: JTSPoint => Point.fromJTS(jtsPoint)
      case jtsLine: LineString => Line.fromJTS(jtsLine)
      case jtsPolygon: JTSPolygon => Polygon.fromJTS(jtsPolygon)
      case jtsMls: MultiLineString => PolyLine.fromJTS(jtsMls)
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
