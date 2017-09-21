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

import magellan.{Line, Point, Relate}

/**
  * A curve consists of a single chain of vertices represents a open curve on the plane.
  *
  * Curves are not allowed to have any duplicate vertices (whether adjacent or
  * not), and non-adjacent edges are not allowed to intersect. Curves must have at
  * least 2 vertices. Although these restrictions are not enforced in optimized
  * code, you may get unexpected results if they are violated.
  */
trait Curve extends Serializable {

  /**
    * Returns true if the curve touches the point, false otherwise.
    *
    * @param point
    * @return
    */
  def touches(point: Point): Boolean

  /**
    * Returns true if the line intersects (properly or vertex touching) loop, false otherwise.
    *
    * @param line
    * @return
    */
  def intersects(line: Line): Boolean

  /**
    * Returns an iterator over the loop.
    *
    * @return
    */
  def iterator(): Iterator[Line]

}