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

import magellan.BoundingBox
import org.apache.spark.sql.types.DataType

/**
 * An abstraction for a spatial curve based 2D Index.
 * A spatial curve represents a two dimensional grid of a given precision.
 */
trait Index extends DataType with Serializable {

  def precision(): Int

  def code(): String

  def bits(): Long

  def boundingBox(): BoundingBox

  def toBase32(): String

  override def defaultSize: Int = 4096

  override def asNullable: DataType = this

}
