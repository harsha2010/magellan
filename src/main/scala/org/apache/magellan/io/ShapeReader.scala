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

package org.apache.magellan.io

import java.io.DataInput

import org.apache.commons.io.EndianUtils

import org.apache.magellan._

import scala.collection.mutable.ArrayBuffer

private[magellan] trait ShapeReader {

  def readFields(dataInput: DataInput): Shape

}

class NullShapeReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Shape = ???

}

private[magellan] class PointReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Shape = {
    val x = EndianUtils.swapDouble(dataInput.readDouble())
    val y = EndianUtils.swapDouble(dataInput.readDouble())
    new Point(x, y)
  }

}

private[magellan] class PolygonReader extends PolyLineReader {

  override def readFields(dataInput: DataInput): Shape = {
    val (indices, points) = extract(dataInput)
    new Polygon(indices, points)
  }
}

private[magellan] class PolyLineReader extends ShapeReader {

  def extract(dataInput: DataInput): (IndexedSeq[Int], IndexedSeq[Point]) = {
    // extract bounding box.
    val Seq(xmin, ymin, xmax, ymax) = (0 until 4).map { _ =>
      EndianUtils.swapDouble(dataInput.readDouble())
    }
    val box = Box(xmin, ymin, xmax, ymax)
    // numRings
    val numRings = EndianUtils.swapInteger(dataInput.readInt())
    val numPoints = EndianUtils.swapInteger(dataInput.readInt())

    val indices = Array.fill(numRings)(-1)

    def tryl2b(l: Integer): Int = {
      if ((0 <= l) && (l < numRings)) {
        l
      } else {
        EndianUtils.swapInteger(l)
      }
    }

    for (ring <- 0 until numRings) {
      val s = tryl2b(dataInput.readInt())
      indices(ring) = s
    }
    val points = ArrayBuffer[Point]()
    for (_ <- 0 until numPoints) {
      points.+= {
        val x = EndianUtils.swapDouble(dataInput.readDouble())
        val y = EndianUtils.swapDouble(dataInput.readDouble())
        new Point(x, y)
      }
    }
    (indices, points)
  }

  override def readFields(dataInput: DataInput): Shape = {
    val (indices, points) = extract(dataInput)
    new PolyLine(indices, points)
  }
}

private[magellan] class PolyLineZReader extends PolyLineReader {

  override def readFields(dataInput: DataInput): Shape = {
    val (indices, points) = extract(dataInput)
    // throw away the Z and M values
    val size = points.length
    (0 until (4 + 2 * size)).foreach(_ => dataInput.readDouble())
    new PolyLine(indices, points)
  }
}
