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

  private var remaining: BigInt = _

  override def getProgress: Float = 0

  override def nextKeyValue(): Boolean = {
    if (remaining <= 0) {
      false
    } else {
      // record header has fixed length of 8 bytes
      // byte 0 = record #, byte 4 = content length
      val recordNumber = dis.readInt()
      // record numbers begin at 1
      require(recordNumber > 0)
      val contentLength = 2 * (dis.readInt() + 4)
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

    val path = split.getPath()
    val fs = path.getFileSystem(job)
    val is = fs.open(path)

    val (start, end) = {
      val v = split.getStart
      if (v == 0) {
        is.seek(24)
        (100L, 2 * is.readInt().toLong)
      } else {
        (v, v + split.getLength)
      }
    }

    is.seek(start)
    dis = new DataInputStream(is)
    key.setFileNamePrefix(split.getPath.getName.split("\\.")(0))
    value = new ShapeWritable()
    remaining = (end - start)
  }

  override def getCurrentKey: ShapeKey = key

  override def close(): Unit = dis.close()

}
