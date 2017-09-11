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

import magellan.TestingUtils._
import magellan.{MockPointExpr, Point, TestSparkContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Transformer}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite


class TransformerSuite extends FunSuite with TestSparkContext {

  test("transform") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpoint/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    val dbl = (x: Point) => Point(2 * x.getX(), 2 * x.getY())
    val point = df.withColumn("transformed", $"point".transform(dbl))
      .select($"transformed")
      .first()(0).asInstanceOf[Point]

    assert(point.getX() ~== -199.0 absTol 1.0)
  }

  test("eval: transform") {
    val fn = (p: Point) => Point(2 * p.getX(), 2 * p.getY())
    val expr = Transformer(MockPointExpr(Point(1.0, 2.0)), fn)
    val result = expr.eval(null).asInstanceOf[InternalRow]
    // skip the type
    assert(result.getDouble(1) === 2.0)
    assert(result.getDouble(2) === 4.0)
  }
}
