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

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class WholeFileReader extends RecordReader[NullWritable, Text] {

  private val key = NullWritable.get()
  private val value = new Text()
  private var split: FileSplit = _
  private var conf: Configuration = _
  private var done: Boolean = false

  override def getProgress: Float = ???

  override def nextKeyValue(): Boolean = {
    if (done){
      false
    } else {
      val len = split.getLength.toInt
      val result = new Array[Byte](len)
      val fs = FileSystem.get(conf)
      var is: FSDataInputStream = null
      try {
        is = fs.open(split.getPath)
        IOUtils.readFully(is, result)
        value.clear()
        value.set(result)
        done = true
        true
      } finally {
        if (is != null) {
          IOUtils.closeQuietly(is)
        }
      }
    }
  }

  override def getCurrentValue: Text = value

  override def initialize(inputSplit: InputSplit,
    taskAttemptContext: TaskAttemptContext): Unit = {
    this.split = inputSplit.asInstanceOf[FileSplit]
    this.conf = taskAttemptContext.getConfiguration
  }

  override def getCurrentKey: NullWritable = key

  override def close() {}
}
