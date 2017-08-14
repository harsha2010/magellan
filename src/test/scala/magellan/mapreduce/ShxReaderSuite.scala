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
import magellan.io.PolygonReader
import org.apache.commons.io.EndianUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{ArrayWritable, LongWritable, Text}
import org.scalatest.FunSuite

class ShxReaderSuite extends FunSuite with TestSparkContext {

  test("Read shx file") {
    val path = this.getClass.getClassLoader.getResource("shapefiles/us_states/tl_2016_us_state.shx").getPath
    val conf = new Configuration()
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "10000")

    val data = sc.newAPIHadoopFile(
      path,
      classOf[ShxInputFormat],
      classOf[Text],
      classOf[ArrayWritable],
      conf
    ).map { case (txt: Text, splits: ArrayWritable) =>
        val fileName = txt.toString
        val s = splits.get()
        val size = s.length
        var i = 0
        val v = Array.fill(size)(0L)
        while (i < size) {
          v.update(i, s(i).asInstanceOf[LongWritable].get())
          i += 1
        }
        (fileName, v)
      }
    assert(data.count() === 1)
    val (fileName, splits) = data.first()
    assert(fileName === "tl_2016_us_state")

    // the offsets should be correct
    val firstOffset = splits(0)
    val secondOffset = splits(1)

    // skipping to the first offset in the Shapefile should allow me to read the first polygon
    val shpFilePath = this.getClass.getClassLoader.getResource("shapefiles/us_states/tl_2016_us_state.shp").getPath

    val fs = FileSystem.get(sc.hadoopConfiguration)

    var dis = fs.open(new Path(shpFilePath))

    // skip  firstOffset # of bytes
    dis.seek(firstOffset)

    // skip record number
    assert(dis.readInt() === 1)

    // read content length
    var contentLength = 16 * (dis.readInt() + 4)

    // extract the shape type
    var shapeType = EndianUtils.swapInteger(dis.readInt())

    // expect a Polygon
    assert(shapeType === 5)

    // the first polygon's content should follow from here
    val polygonReader = new PolygonReader()
    val polygon = polygonReader.readFields(dis)
    assert(polygon != null)

    // seek to the second offset
    dis.seek(secondOffset)
    assert(dis.readInt() === 2)

  }
}
