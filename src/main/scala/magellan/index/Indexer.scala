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

import magellan.{Point, Shape}
import org.apache.spark.sql.DataFrame

trait Indexer[T <: Index] {

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

}

