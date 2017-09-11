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

import magellan.{MockPointExpr, Point, TestSparkContext}
import magellan.index.ZOrderCurve
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Indexer}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class IndexerSuite extends FunSuite with TestSparkContext {

  test("index points") {
    val sqlCtx = this.sqlContext
    val path = this.getClass.getClassLoader.getResource("testpoint/").getPath
    val df = sqlCtx.read.format("magellan").load(path)
    import sqlCtx.implicits._
    val index = df.withColumn("index", $"point" index 25)
      .select($"index.curve")
      .take(1)(0)(0)
      .asInstanceOf[Seq[ZOrderCurve]]

    assert(index.map(_.toBase32()) === Seq("9z109"))

    try {
      df.withColumn("index", $"point" index 23)
      assert(false)
    } catch {
      case e: Error => assert(true)
    }
  }

  test("eval: Index") {
    val indexer = Indexer(MockPointExpr(Point(-122.3959313, 37.7912976)), 25)
    val result = indexer.eval(null).asInstanceOf[GenericArrayData]
    assert(result.numElements() === 1)
    val resultRow = result.get(0, Indexer.dataType).asInstanceOf[GenericInternalRow]
    val indexUDT = Indexer.indexUDT
    val curve = indexUDT.deserialize(resultRow.get(0, indexUDT))
    assert(curve.toBase32() === "9q8yy")
    val relation = resultRow.getString(1)
    assert(relation === "Contains")
  }
}
