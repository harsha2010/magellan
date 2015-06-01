/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spatialsdk.io

import java.io.{DataOutput, DataInput}

import org.apache.commons.io.EndianUtils
import org.apache.hadoop.io.Writable

import com.hortonworks.spatialsdk.Shape

class ShapeWritable(shapeType: Int) extends Writable {

  private[spatialsdk] var shape: Shape = _

  override def write(dataOutput: DataOutput): Unit = {
    ???
  }

  override def readFields(dataInput: DataInput): Unit = {
    val shapeType = EndianUtils.swapInteger(dataInput.readInt())
    // all records share the same type or nullshape.
    require(this.shapeType == shapeType || shapeType == 0)
    val h = shapeType match {
      case 0 => new NullShapeReader()
      case 1 => new PointReader()
      case _ => ???
    }
    shape = h.readFields(dataInput)
  }

}
