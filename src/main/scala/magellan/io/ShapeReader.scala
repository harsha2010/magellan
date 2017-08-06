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

import java.io.DataInput

import java.lang.{Double, Long}

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.EndianUtils

import magellan._

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
    Point(x, y)
  }

}

private[magellan] class PolygonReader extends ShapeReader {

  override def readFields(dataInput: DataInput): Polygon = {
    // extract bounding box.
    val xmin = EndianUtils.swapDouble(dataInput.readDouble())
    val ymin = EndianUtils.swapDouble(dataInput.readDouble())
    val xmax = EndianUtils.swapDouble(dataInput.readDouble())
    val ymax = EndianUtils.swapDouble(dataInput.readDouble())

    val box = BoundingBox(xmin, ymin, xmax, ymax)

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
    val xcoordinates = Array.fill(numPoints)(0.0)
    val ycoordinates = Array.fill(numPoints)(0.0)

    for (index <- 0 until numPoints) {
      val x = Double.longBitsToDouble(Long.reverseBytes(dataInput.readLong()))
      val y = Double.longBitsToDouble(Long.reverseBytes(dataInput.readLong()))
      xcoordinates.update(index, x)
      ycoordinates.update(index, y)
    }
    val polygon = new Polygon()
    polygon.init(indices, xcoordinates, ycoordinates, box)
    polygon
  }
}

private[magellan] class PolyLineReader extends ShapeReader {

  def extract(dataInput: DataInput): (Array[Int], Array[Point]) = {
    // extract bounding box.
    (0 until 4).foreach { _ => EndianUtils.swapDouble(dataInput.readDouble())}

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
        Point(x, y)
      }
    }
    (indices, points.toArray)
  }

  override def readFields(dataInput: DataInput): Shape = {
    val (indices, points) = extract(dataInput)
    PolyLine(indices, points)
  }
}

private[magellan] class PolyLineZReader extends PolyLineReader {

  override def readFields(dataInput: DataInput): Shape = {
    val (indices, points) = extract(dataInput)
    // throw away the Z and M values
    val size = points.length
    (0 until (4 + 2 * size)).foreach(_ => dataInput.readDouble())
    if(indices.size != points.size)
      PolyLine( new Array[Int](points.size), points)
    else
      PolyLine(indices, points)
  }
}
