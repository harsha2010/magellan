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

import magellan.TestSparkContext
import magellan.io.ShapeKey
import org.apache.hadoop.io.MapWritable
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

  test("read dBase format with Floats") {
    //val path = this.getClass.getClassLoader.getResource("testdbf/GBR_adm0.dbf").getPath
    val path = this.getClass.getClassLoader.getResource("testdbf/dbf.dbf").getPath
    val baseRdd = sc.newAPIHadoopFile(
      path,
      classOf[DBInputFormat],
      classOf[ShapeKey],
      classOf[MapWritable]
    )
    System.out.println(baseRdd.map(x => x._2.values.size()).collect())
    assert(baseRdd.count() == 1)
  }
}
