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

package magellan.catalyst

import magellan._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

trait MagellanExpression {

  private val SERIALIZERS = Map(
    new Point().getType() -> new PointUDT,
    new Line().getType() -> new LineUDT,
    new PolyLine().getType() -> new PolyLineUDT,
    new Polygon().getType()  -> new PolygonUDT)

  def newInstance(row: InternalRow): Shape = {
    SERIALIZERS.get(row.getInt(0)).fold(NullShape.asInstanceOf[Shape])(_.deserialize(row))
  }

  def serialize(shape: Shape): Any = {
    SERIALIZERS.get(shape.getType()).get.serialize(shape)
  }

  def sqlType(klass: Class[_ <: Shape]): DataType = {
    SERIALIZERS.get(klass.newInstance().getType()).get
  }
}

