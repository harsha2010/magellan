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

import java.io.{DataOutput, DataInput}

import org.apache.hadoop.io
import org.apache.hadoop.io.{Text, LongWritable, Writable}

private[magellan] class ShapeKey extends Writable {

  var fileNamePrefix: Text = new Text()
  var recordIndex: LongWritable = new io.LongWritable()

  def setFileNamePrefix(fileNamePrefix: String) = {
    val f = fileNamePrefix.getBytes()
    this.fileNamePrefix.clear()
    this.fileNamePrefix.append(f, 0, f.length)
  }

  def setRecordIndex(recordIndex: Long) = {
    this.recordIndex.set(recordIndex)
  }

  def getRecordIndex(): Long = recordIndex.get()

  def getFileNamePrefix(): String = fileNamePrefix.toString()

  override def write(dataOutput: DataOutput): Unit = ???

  override def readFields(dataInput: DataInput): Unit = ???

}
