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

import org.apache.spark.sql.catalyst.expressions.{Row, Expression, UnaryExpression}
import org.apache.spark.sql.types.DataType

import magellan._

case class Transformer(
    override val child: Expression,
    fn: Point => Point)
  extends UnaryExpression {

  override type EvaluatedType = child.EvaluatedType

  override def eval(input: Row): EvaluatedType = {
    val row = child.eval(input).asInstanceOf[Row]
    val shape = Shape.deserialize(row)
    shape.transform(fn).asInstanceOf[EvaluatedType]
  }

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  protected def nullSafeEval(input: Any): Any = {
    if (input == null) {
      null
    } else {
      val row = input.asInstanceOf[Row]
      val shape = Shape.deserialize(row)
      shape.transform(fn)
    }
  }

}
