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

package magellan.io

import java.io.{DataInputStream, File, FileInputStream}

import org.apache.commons.io.EndianUtils
import org.scalatest.FunSuite

class ShapeReaderSuite extends FunSuite {

  test("Read Polygon") {
    val path = this.getClass.getClassLoader.getResource("testpolygon/testpolygon.shp").getPath
    val dis = new DataInputStream(new FileInputStream(new File(path)))
    val header = new Array[Byte](100)
    dis.readFully(header, 0, 100) // discard the first 100 bytes

    // discard the record header and content length
    assert(dis.readInt() === 1)
    val contentLength = 2 * dis.readInt() //content length in bytes

    // contentlength = shapetype(int) + bounding box (4 doubles) + numParts (int) + numPoints (int) +
    // parts (int) + points (16 * length) bytes

    val expectedLength = (contentLength - 4 - 4 * 8 - 4 - 4 - 4) / 16

    println(expectedLength)

    // discard the geometry type
    assert(EndianUtils.swapInteger(dis.readInt()) === 5)

    // now read the polygon
    val polygonReader = new PolygonReader()
    val polygon = polygonReader.readFields(dis)
    assert(polygon.length() === expectedLength)
  }
}
