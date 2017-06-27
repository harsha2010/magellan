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

package magellan.index

import magellan._
import magellan.Relate._

import scala.collection.mutable.ListBuffer

class ZOrderCurveIndexer(
    boundingBox: BoundingBox)
  extends Indexer[ZOrderCurve] {

  def this() = this(BoundingBox(-180, -90, 180, 90))

  private val BoundingBox(xmin, ymin, xmax, ymax) = boundingBox

  override def index(point: Point, precision: Int): ZOrderCurve = {
    var currentPrecision = 0
    var evenBit = true
    var xrange = Array(xmin, xmax)
    var yrange = Array(ymin, ymax)
    var bits = 0L

    def encode(v: Double, range: Array[Double]): Unit = {
      val mid = range(0) + (range(1) - range(0))/ 2.0
      if (v < mid) {
        // add off bit
        bits <<= 1
        range.update(1, mid)
      } else {
        // add on bit
        bits <<= 1
        bits = bits | 0x1
        range.update(0, mid)
      }
      currentPrecision += 1
    }

    while (currentPrecision < precision) {
      if (evenBit) {
        encode(point.getX(), xrange)
      } else {
        encode(point.getY(), yrange)
      }
      evenBit = !evenBit
    }
    bits <<= (64 - precision)
    new ZOrderCurve(BoundingBox(xrange(0), yrange(0), xrange(1), yrange(1)), precision, bits)
  }

  override def index(shape: Shape, precision: Int): Seq[ZOrderCurve] = {
    indexWithMeta(shape, precision).map(_._1)
  }

  override def indexWithMeta(shape: Shape, precision: Int) = {
    shape match {
      case p: Point => ListBuffer((index(p, precision), Contains))
      case _ => {
        val candidates = cover(shape.boundingBox, precision)
        val results = new ListBuffer[(ZOrderCurve, Relate)]
        for (candidate <- candidates) {
          // check if the candidate actually lies within the shape
          val box = candidate.boundingBox
          val relation = box.relate(shape)
          if (relation != Disjoint) {
            results.+= ((candidate, relation))
          }
        }
        results
      }
    }
  }

  /**
    * Returns the curves of a given precision that cover the bounding box.
    *
    * @param box
    * @return
    */
  def cover(box: BoundingBox, precision: Int): ListBuffer[ZOrderCurve] = {
    val BoundingBox(xmin, ymin, xmax, ymax) = box
    val leftBottom = index(Point(xmin, ymin), precision)
    val BoundingBox(startX, startY, endX, endY) = leftBottom.boundingBox
    val xdelta = Math.abs(endX - startX)
    val ydelta = Math.abs(endY - startY)

    val cover = new ListBuffer[ZOrderCurve]()

    var i = startX

    while (i <= xmax) {
      var j = startY
      while (j <= ymax) {
        val candidate = index(Point(i, j), precision)
        // check if the candidate intersects the box
        if (box.intersects(candidate.boundingBox)) {
          cover.+= (index(Point(i, j), precision))
        }
        j += ydelta
      }

      i += xdelta
    }

    cover
  }

}



