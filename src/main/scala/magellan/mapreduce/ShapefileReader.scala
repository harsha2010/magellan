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

import java.io.DataInputStream

import org.apache.commons.io.EndianUtils
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import magellan.io.{ShapeKey, ShapeWritable}

private[magellan] class ShapefileReader extends RecordReader[ShapeKey, ShapeWritable] {

  private val key: ShapeKey = new ShapeKey()

  private var value: ShapeWritable = _

  private var dis: DataInputStream = _

  private var length: Long = _

  private var remaining: Long = _

  override def getProgress: Float = remaining / length.toFloat

  override def nextKeyValue(): Boolean = {
    if (remaining <= 0) {
      false
    } else {
      // record header has fixed length of 8 bytes
      // byte 0 = record #, byte 4 = content length
      val recordNumber = dis.readInt()
      // record numbers begin at 1
      require(recordNumber > 0)
      val contentLength = 16 * (dis.readInt() + 4)
      value.readFields(dis)
      remaining -= contentLength
      key.setRecordIndex(key.getRecordIndex() + 1)
      true
    }
  }

  override def getCurrentValue: ShapeWritable = value

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext) {
    val split = inputSplit.asInstanceOf[FileSplit]
    val job = MapReduceUtils.getConfigurationFromContext(taskAttemptContext)
    val start = split.getStart()
    val end = start + split.getLength()
    val file = split.getPath()
    val fs = file.getFileSystem(job)
    val is = fs.open(split.getPath())
    dis = new DataInputStream(is)
    require(is.readInt() == 9994)
    // skip the next 20 bytes which should all be zero
    0 until 5 foreach {_ => require(is.readInt() == 0)}
    // file length in bits
    length = 16 * is.readInt().toLong - 50 * 16
    remaining = length
    val version = EndianUtils.swapInteger(is.readInt())
    require(version == 1000)
    // shape type: all the shapes in a given split have the same type
    val shapeType = EndianUtils.swapInteger(is.readInt())
    key.setFileNamePrefix(split.getPath.getName.split("\\.")(0))
    value = new ShapeWritable(shapeType)
    // skip the next 64 bytes
    0 until 8 foreach {_ => is.readDouble()}
  }

  override def getCurrentKey: ShapeKey = key

  override def close(): Unit = dis.close()

}
