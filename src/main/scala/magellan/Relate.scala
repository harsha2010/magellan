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

sealed trait Relate {

  def name() = toString

}

object Relate {

  /**
    * This geometry is within the other.
    */
  case object Within extends Relate

  /**
    * The other geometry is within this.
    */
  case object Contains extends Relate

  /**
    * This geometry intersects the other.
    */
  case object Intersects extends Relate

  /**
    * This geometry touches the other.
    */
  case object Touches extends Relate

  /**
    * This geometry has no overlap with the other.
    */
  case object Disjoint extends Relate

  val values = List(Within, Contains, Intersects, Touches, Disjoint)

  def withValue(name: String) = values.find(_.name() == name).get

}
