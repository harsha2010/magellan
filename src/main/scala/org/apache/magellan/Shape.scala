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

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.esri.core.geometry.{Geometry => ESRIGeometry, SpatialReference, GeometryEngine}
import org.apache.spark.sql.Row

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends Serializable {

  val shapeType: Int

  val delegate: ESRIGeometry

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  def transform(fn: Point => Point): Shape

  /**
   *
   * @param shape
   * @return true if the two shapes intersect each other.
   */
  def intersects(shape: Shape): Boolean = {
    val sref = SpatialReference.create(26910)
    val intersection = GeometryEngine.intersect(delegate, shape.delegate, sref)
    ! intersection.isEmpty
  }

  /**
   *
   * @param shape
   * @return true if this shape contains the other.
   */
  def contains(shape: Shape): Boolean = {
    val sref = SpatialReference.create(26910)
    GeometryEngine.contains(delegate, shape.delegate, sref)
  }

}

/**
 * A null shape indicates an absence of geometric data.
 * Each feature type (point, line, polygon, etc.) supports nulls.
 */
object NullShape extends Shape {

  override final val shapeType: Int = 0

  override val delegate = null

  override def intersects(shape: Shape): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

}

object Shape {

  private val pointUDT = new PointUDT
  private val polyLineUDT = new PolyLineUDT
  private val polygonUDT = new PolygonUDT

  def deserialize(obj: Any): Shape = {
    obj match {
      case s: Shape => s
      case row: Row =>
        row(0) match {
          case 0 => NullShape
          case 1 => pointUDT.deserialize(row)
          case 3 => polyLineUDT.deserialize(row)
          case 5 => polygonUDT.deserialize(row)
          case _ => ???
        }
    }
  }
}
