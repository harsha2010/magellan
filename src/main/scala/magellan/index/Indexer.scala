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

package magellan.index

import java.util

import magellan.{Point, Relate, Shape}

trait Indexer[T <: Index] extends Serializable {

  /**
   * Output the smallest spatial curve that covers given point at given precision
   *
   * @param point
   * @param precision
   * @return
   */
  def index(point: Point, precision: Int): T

  /**
   * Output the set of all spatial curves that cover the given shape at given precision
   *
   * @param shape
   * @param precision
   * @return
   */
  def index(shape: Shape, precision: Int): Seq[T]

  /**
    * Outputs the set of all spatial curves that cover the given shape at a given precision,
    * along with metadata about the nature of the relation. A spatial index can either Contain
    * the shape, Intersect the shape, or be Within the shape.
    *
    * @param shape
    * @param precision
    * @return
    */
  def indexWithMeta(shape: Shape, precision: Int): Seq[(T, Relate)]

  def indexWithMetaAsJava(shape: Shape, precision: Int): util.List[(T, String)] = {
    val indices = new util.ArrayList[(T, String)]()
    indexWithMeta(shape, precision) foreach {
      case (index: T, relation: Relate) =>
        indices.add((index, relation.name()))
    }
    indices
  }

}

