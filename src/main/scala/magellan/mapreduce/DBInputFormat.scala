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

import java.util

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}

import magellan.io.ShapeKey

private[magellan] class DBInputFormat extends FileInputFormat[ShapeKey, MapWritable] {

  override def createRecordReader(inputSplit: InputSplit,
      taskAttemptContext: TaskAttemptContext) = {
    new DBReader
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = false

  override def getSplits(job: JobContext): util.List[InputSplit] = {
    try {
      super.getSplits(job)
    }catch {
      case e: Exception => seqAsJavaList(List[InputSplit]())
    }
  }
}
