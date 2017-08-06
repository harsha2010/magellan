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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

import scala.collection.mutable.ListBuffer

class ShxInputFormat extends FileInputFormat[Text, ArrayWritable] {

  override def createRecordReader(
      split: InputSplit,
      context: TaskAttemptContext): RecordReader[Text, ArrayWritable] = {
    new ShxReader()
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
}

class ShxReader extends RecordReader[Text, ArrayWritable] {

  private var dis: DataInputStream = _

  override def getProgress: Float = ???

  private var done: Boolean = false

  private var splits:ArrayWritable = _

  private var key: Text = new Text()

  private val MAX_SPLIT_SIZE = "mapreduce.input.fileinputformat.split.maxsize"

  private val MIN_SPLIT_SIZE = "mapreduce.input.fileinputformat.split.minsize"


  override def nextKeyValue(): Boolean = if (done) false else {
    done = true
    true
  }

  override def getCurrentValue: ArrayWritable = {
    splits
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val split = inputSplit.asInstanceOf[FileSplit]
    val job = MapReduceUtils.getConfigurationFromContext(context)
    val start = split.getStart()
    val end = start + split.getLength()
    val path = split.getPath()
    val fs = path.getFileSystem(job)
    key.set(split.getPath.getName.split("\\.")(0))
    val is = fs.open(path)
    dis = new DataInputStream(is)
    require(is.readInt() == 9994)
    // skip the next 20 bytes which should all be zero
    0 until 5 foreach {_ => require(is.readInt() == 0)}
    // file length in bits
    val len = is.readInt()
    val numRecords = (2 * len - 100) / 8

    val version = EndianUtils.swapInteger(is.readInt())
    require(version == 1000)
    // shape type: all the shapes in a given split have the same type
    is.readInt()

    // skip the next 64 bytes
    0 until 8 foreach {_ => is.readDouble()}

    // iterate over the offset and content length of each record
    var j = 0
    val minSplitSize = job.getLong(MIN_SPLIT_SIZE, 1L)
    val maxSplitSize = job.getLong(MAX_SPLIT_SIZE, Long.MaxValue)
    val shpFileName = path.getName.replace("\\.shx$", "\\.shp")
    val blockSize = fs.getFileStatus(new Path(path.getParent, shpFileName)).getBlockSize
    val splitSize = Math.max(minSplitSize, Math.min(maxSplitSize, blockSize))

    // num bytes
    val v = new ListBuffer[Writable]()

    var startOffset: Long = Long.MinValue

    while (j < numRecords) {
      val offset = dis.readInt()
      // skip the next 4 bytes (the content length)
      dis.readInt()

      if (startOffset == Long.MinValue) {
        startOffset = offset
      }
      else if (offset - startOffset > splitSize) {
        v.+= (new LongWritable(startOffset * 2))
        startOffset = offset
      }
      j += 1
    }

    // if empty add starting offset
    if (v.isEmpty) {
      v.+= (new LongWritable(startOffset * 2))
    }

    splits = new ArrayWritable(classOf[LongWritable], v.toArray)
  }

  override def getCurrentKey: Text = key

  override def close() {}
}
