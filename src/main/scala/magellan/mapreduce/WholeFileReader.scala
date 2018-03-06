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

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, Decompressor}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

class WholeFileReader extends RecordReader[NullWritable, Text] {

  private val key = NullWritable.get()
  private val value = new Text()
  private var split: FileSplit = _
  private var conf: Configuration = _
  private var path: Path = _
  private var done: Boolean = false

  override def getProgress: Float = ???

  override def nextKeyValue(): Boolean = {
    if (done){
      false
    } else {
      val fs = path.getFileSystem(conf)
      var is: FSDataInputStream = null
      var in: InputStream = null
      var decompressor: Decompressor = null
      try {
        is = fs.open(split.getPath)
        val codec = new CompressionCodecFactory(conf).getCodec(path)
        if (codec != null) {
          decompressor = CodecPool.getDecompressor(codec)
          in = codec.createInputStream(is, decompressor)
        } else {
          in = is
        }
        val result = IOUtils.toByteArray(in)
        value.clear()
        value.set(result)
        done = true
        true
      } finally {
        if (in != null) {
          IOUtils.closeQuietly(in)
        }
        if (decompressor != null) {
          CodecPool.returnDecompressor(decompressor)
        }
      }
    }
  }

  override def getCurrentValue: Text = value

  override def initialize(inputSplit: InputSplit,
    taskAttemptContext: TaskAttemptContext): Unit = {
    this.split = inputSplit.asInstanceOf[FileSplit]
    this.conf = MapReduceUtils.getConfigurationFromContext(taskAttemptContext)
    this.path = this.split.getPath
  }

  override def getCurrentKey: NullWritable = key

  override def close() {}
}
