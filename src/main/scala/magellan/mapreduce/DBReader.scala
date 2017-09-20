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

import scala.collection.mutable.ListBuffer

import org.apache.commons.io.EndianUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import magellan.io.ShapeKey

private[magellan] class DBReader extends RecordReader[ShapeKey, MapWritable] {

  private var key: ShapeKey = new ShapeKey()

  private var value: MapWritable = new MapWritable()

  private var dis: DataInputStream = _

  private var numRecords: Int = _

  private var currentByte: Byte = _

  private var recordsRead: Int = 0

  private var numBytesInRecord: Int = _

  private var fieldDescriptors: ListBuffer[(Text, Byte, Int, Byte)] = _

  override def getProgress: Float = recordsRead / numRecords.toFloat

  override def nextKeyValue(): Boolean = {
    // handle record deleted flag
    if (recordsRead < numRecords) {
      currentByte = dis.readByte()
      while (currentByte == 0x2A) {
        (0 until numBytesInRecord).foreach(_ => dis.readInt())
      }
      if (currentByte != 0x1A) {
        extractKeyValue()
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  private def extractKeyValue(): Unit = {
    value.clear()
    for ((fieldName, fieldType, length, decimalCount) <- fieldDescriptors) {
      val v = fieldType match {
        case 'C' => {
          val b = Array.fill[Byte](length)(0)
          dis.readFully(b)
          val fld = new Text()
          fld.append(b, 0, length)
          fld
        }
        case 'N' | 'F' => {
          val b = Array.fill[Byte](length)(0)
          dis.readFully(b)
          val fld = new Text()
          fld.clear()
          fld.set(new String(b))
          fld
        }

        case _ => ???
      }
      value.put(fieldName, v)
    }
    recordsRead += 1
    key.setRecordIndex(key.getRecordIndex() + 1)
  }

  override def getCurrentValue: MapWritable = {
    value
  }

  override def initialize(inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext): Unit = {

    val split = inputSplit.asInstanceOf[FileSplit]
    val job = MapReduceUtils.getConfigurationFromContext(taskAttemptContext)
    val start = split.getStart()
    val end = start + split.getLength()
    val file = split.getPath()
    val fs = file.getFileSystem(job)
    val is = fs.open(split.getPath())
    dis = new DataInputStream(is)
    // version
    dis.readByte()
    // date in YYMMDD format
    val date = Array.fill[Byte](3)(0)
    dis.read(date)
    numRecords = EndianUtils.swapInteger(dis.readInt())
    val numBytesInHeader = EndianUtils.swapShort(dis.readShort())
    numBytesInRecord = EndianUtils.swapShort(dis.readShort())
    // skip the next 20 bytes
    (0 until 20).foreach {_ => dis.readByte()}

    // field descriptors
    fieldDescriptors = new ListBuffer()
    var first = dis.readByte()
    while (first != 0x0d) {
      // 48 byte field descriptor
      val f = Array.fill[Byte](11)(0)
      dis.readFully(f, 1, 10)
      f(0) = first
      val fieldName = new Text()
      val terminalIndex = f.indexWhere(_ == 0)
      fieldName.append(f, 0, if (terminalIndex == -1) f.length else terminalIndex)
      val fieldType = dis.readByte()
      dis.readInt()
      val fieldLength = dis.readUnsignedByte()
      val decimalCount = dis.readByte()
      fieldDescriptors.+= ((fieldName, fieldType, fieldLength, decimalCount))
      // drain 14 more bytes
      (0 until 14).foreach {_ => dis.readByte()}
      first = dis.readByte()
    }
    key.setFileNamePrefix(split.getPath.getName.split("\\.")(0))
  }

  override def getCurrentKey: ShapeKey = key

  override def close(): Unit = dis.close()

}
