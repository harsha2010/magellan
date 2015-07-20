/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan.mapreduce

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.magellan.TestSparkContext
import org.scalatest.FunSuite

class WholeFileReaderSuite extends FunSuite with TestSparkContext {

  test("Read Whole File") {
    val path = this.getClass.getClassLoader.getResource("geojson/linestring").getPath
    val data = sc.newAPIHadoopFile(
      path,
      classOf[WholeFileInputFormat],
      classOf[NullWritable],
      classOf[Text]
    )
    assert(data.count() === 1)
  }
}
