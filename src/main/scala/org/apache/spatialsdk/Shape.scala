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

package org.apache.spatialsdk

import org.apache.spark.sql.Row

/**
 * An abstraction for a geometric shape.
 */
trait Shape extends Serializable {

  val shapeType: Int

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
  def transform(fn: Point => Point): Shape

  /**
   *
   * @param point
   * @return true if this shape envelops the given point
   */
  def contains(point: Point): Boolean

  /**
   *
   * @param line
   * @return number of times this shape intersects the given line.
   */
  def intersects(line: Line): Boolean

  def contains(shape: Shape): Boolean = {
    shape match {
      case point: Point => this.contains(point)
      case _ => ???
    }
  }

}

/**
 * A null shape indicates an absence of geometric data.
 * Each feature type (point, line, polygon, etc.) supports nulls.
 */
object NullShape extends Shape {

  override final val shapeType: Int = 0

  /**
   *
   * @param point
   * @return true if this shape envelops the given point
   */
  override def contains(point: Point): Boolean = false

  /**
   *
   * @param line
   * @return true if this shape intersects the given line.
   */
  override def intersects(line: Line): Boolean = false

  /**
   * Applies an arbitrary point wise transformation to a given shape.
   *
   * @param fn
   * @return
   */
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
          case 1 =>  pointUDT.deserialize(row)
          case 3 => polyLineUDT.deserialize(row)
          case 5 => polygonUDT.deserialize(row)
          case _ => ???
        }
    }
  }
}
