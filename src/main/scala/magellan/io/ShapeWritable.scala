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

import java.io.{DataInput, DataOutput}

import magellan.Shape
import org.apache.commons.io.EndianUtils
import org.apache.hadoop.io.Writable

private[magellan] class ShapeWritable extends Writable {

  var shape: Shape = _

  override def write(dataOutput: DataOutput): Unit = {
    ???
  }

  override def readFields(dataInput: DataInput): Unit = {
    val shapeType = EndianUtils.swapInteger(dataInput.readInt())
    val h = shapeType match {
      case 0 => new NullShapeReader()
      case 1 => new PointReader()
      case 3 => new PolyLineReader()
      case 5 => new PolygonReader()
      case 13 => new PolyLineZReader()
      case _ => ???
    }
    shape = h.readFields(dataInput)
  }

}
