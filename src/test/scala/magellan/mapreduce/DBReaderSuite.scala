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

package magellan.mapreduce

import scala.collection.JavaConversions._
import magellan.{TestingUtils, TestSparkContext}
import magellan.io.ShapeKey
import TestingUtils._
import org.apache.hadoop.io.{Text, MapWritable}
import org.scalatest.FunSuite

class DBReaderSuite extends FunSuite with TestSparkContext {

  test("read dBase format") {
    val path = this.getClass.getClassLoader.getResource("testzillow/zillow_ca.dbf").getPath
    val baseRdd = sc.newAPIHadoopFile(
      path,
      classOf[DBInputFormat],
      classOf[ShapeKey],
      classOf[MapWritable]
    )
    assert(baseRdd.count() == 948)
  }

  test("bug: Landtracs DBF") {
    val path = this.getClass.getClassLoader.getResource("landtracs/landtrac_units.dbf").getPath
    val baseRdd = sc.newAPIHadoopFile(
      path,
      classOf[DBInputFormat],
      classOf[ShapeKey],
      classOf[MapWritable]
    ).map { case (s: ShapeKey, v: MapWritable) =>
      v.entrySet().map { kv =>
        val k = kv.getKey.asInstanceOf[Text].toString
        val v = kv.getValue.asInstanceOf[Text].toString
        (k, v)
      }.toMap
    }
    assert(baseRdd.count() == 231)
    // what does the first row look like?
    val area = baseRdd.first()("SHAPE_area")
    assert(area.toDouble ~== 426442.116396 absTol 1.0)
  }

  test("ISSUE-167") {
    val path = this.getClass.getClassLoader.getResource("shapefiles/ISSUE-167/iri_shape.dbf").getPath
    val baseRdd = sc.newAPIHadoopFile(
      path,
      classOf[DBInputFormat],
      classOf[ShapeKey],
      classOf[MapWritable]
    ).map { case (s: ShapeKey, v: MapWritable) =>
      v.entrySet().map { kv =>
        val k = kv.getKey.asInstanceOf[Text].toString
        val v = kv.getValue.asInstanceOf[Text].toString
        (k, v)
      }.toMap
    }
    assert(baseRdd.count() == 22597)
    assert(baseRdd.first()("route_ident").trim() === "[u'025A_BR_2498#_1']")
  }
}
