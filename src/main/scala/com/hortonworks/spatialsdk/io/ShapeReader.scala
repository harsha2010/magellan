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

import java.io.DataInput

import org.apache.commons.io.EndianUtils

import com.hortonworks.spatialsdk._

import scala.collection.mutable.ArrayBuffer

trait ShapeReader {

  def readFields(dataInput: DataInput): Shape

}

class NullShapeReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Shape = ???

}

class PointReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Shape = {
    val x = EndianUtils.swapDouble(dataInput.readDouble())
    val y = EndianUtils.swapDouble(dataInput.readDouble())
    new Point(x, y)
  }

}

class PolygonReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Shape = {
    // extract bounding box.
    val Seq(xmin, ymin, xmax, ymax) = (0 until 4).map { _ =>
      EndianUtils.swapDouble(dataInput.readDouble())
    }
    val box = Box(xmin, ymin, xmax, ymax)
    // numRings
    val numRings = EndianUtils.swapInteger(dataInput.readInt())
    val numPoints = EndianUtils.swapInteger(dataInput.readInt())
    val indices = Array[Int](numRings)
    for (ring <- 0 until numRings) {
      indices(ring) = EndianUtils.swapInteger(dataInput.readInt())
    }
    val points = ArrayBuffer[Point]()
    for (_ <- 0 until numPoints) {
      points.+= {
        val x = EndianUtils.swapDouble(dataInput.readDouble())
        val y = EndianUtils.swapDouble(dataInput.readDouble())
        new Point(x, y)
      }
    }
    new Polygon(box, indices, points)
  }
}
