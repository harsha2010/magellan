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
package magellan.geometry

import magellan.Relate.Touches
import magellan.{Line, Point, Relate}

/**
  *
  * A Loop represents a closed curve. It consists of a single
  * chain of vertices where the first vertex is explicitly connected to the last.
  *
  * Loops are not allowed to have any duplicate vertices (whether adjacent or
  * not), and non-adjacent edges are not allowed to intersect. Loops must have at
  * least 3 vertices. Although these restrictions are not enforced in optimized
  * code, you may get unexpected results if they are violated.
  *
  * Point containment is defined such that if the plane is subdivided into
  * faces (loops), every point is contained by exactly one face. This implies
  * that loops do not necessarily contain all (or any) of their vertices.
  *
  */
trait Loop extends Serializable with Curve {

  override def touches(point: Point) = {
    containsOrCrosses(point) == Touches
  }

  /**
    * A loop contains the given point iff the point is properly contained within the
    * interior of the loop.
    *
    * @param point
    * @return
    */
  def contains(point: Point): Boolean


  /**
    * Return Contains if loop contains point (i.e the interior of the loop contains the point),
    * Disjoint if the point lies in the exterior region of the loop,
    * and Touches if the point lies on the loop.
    *
    * @param point
    * @return
    */
  def containsOrCrosses(point: Point): Relate

  /**
    * Returns true if the two loops intersect (properly or vertex touching), false otherwise.
    * @param loop
    * @return
    */
  def intersects(loop: Loop): Boolean = {
    loop.iterator() exists (intersects(_))
  }

}
