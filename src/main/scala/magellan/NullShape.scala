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

/**
  * A null shape indicates an absence of geometric data.
  */
class NullShape extends Shape {

  override def getType() = 0

  override def isEmpty() = true

  override def intersects(shape: Shape, strict: Boolean = false): Boolean = false

  override def contains(shape: Shape): Boolean = false

  override def transform(fn: (Point) => Point): Shape = this

  override def boundingBox = BoundingBox(
    Int.MinValue, Int.MinValue,
    Int.MaxValue, Int.MaxValue
  )

  override def equals(other: Any): Boolean = other match {
    case _: NullShape => true
    case _ => false
  }

  override def hashCode(): Int = 0.hashCode()

}

object NullShape extends NullShape
