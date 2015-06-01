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
   *
   * @param other
   * @return true if this shape envelops the other
   */
  def contains(other: Shape): Boolean = {
    (this, other) match {
      case (x: Point, y: Point) => x.equals(y)
      case (x: Point, y: Polygon) => y.contains(x)
      case (x: Polygon, y: Point) => x.contains(y)
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
}

object Shape {

  private val pointUDT = new PointUDT
  private val polygonUDT = new PolygonUDT

  def deserialize(obj: Any): Shape = {
    obj match {
      case s: Shape => s
      case row: Row =>
        println("Type " + row)
        row(0) match {
          case 0 => NullShape
          case 1 =>  pointUDT.deserialize(row)
          case 5 => polygonUDT.deserialize(row)
          case _ => ???
        }
    }
  }
}
