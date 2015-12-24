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

package org.apache.spark.sql.magellan

import java.io.OutputStream
import java.util.{List => JList}

import scala.collection.JavaConversions._

import magellan.{PolyLine, Point, Polygon, Shape}
import net.razorvine.pickle._
import org.apache.spark.sql.catalyst.expressions._

object EvaluatePython {

  private val pysparkModule = "pyspark.sql.types"

  private val magellanModule = "magellan.types"

  /**
   * Pickler for Shape
   */
  private class ShapePickler extends IObjectPickler {

    def register(): Unit = {
      Pickler.registerCustomPickler(classOf[Point], this)
      Pickler.registerCustomPickler(classOf[Polygon], this)
      Pickler.registerCustomPickler(classOf[PolyLine], this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((magellanModule + "\n" + "_inbound_shape_converter" + "\n").getBytes("utf-8"))
      val point = obj.asInstanceOf[Shape]
      pickler.save(point.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  private class PointUnpickler extends IObjectConstructor {

    def register(): Unit = {
      Unpickler.registerConstructor("magellan.types", "Point", this)
    }

    override def construct(args: Array[AnyRef]): AnyRef = {
      require(args.length == 2)
      val x = args(0).asInstanceOf[Double]
      val y = args(1).asInstanceOf[Double]
      new Point(x, y)
    }
  }

  private class PolygonUnpickler extends IObjectConstructor {

    def register(): Unit = {
      Unpickler.registerConstructor("magellan.types", "Polygon", this)
    }

    override def construct(args: Array[AnyRef]): AnyRef = {
      val indices = args(0).asInstanceOf[JList[Int]]
      val points = args(1).asInstanceOf[JList[Point]]
      new Polygon(indices.toIndexedSeq, points.toIndexedSeq)
    }
  }

  private class PolyLineUnpickler extends IObjectConstructor {

    def register(): Unit = {
      Unpickler.registerConstructor("magellan.types", "PolyLine", this)
    }

    override def construct(args: Array[AnyRef]): AnyRef = {
      val indices = args(0).asInstanceOf[JList[Int]]
      val points = args(1).asInstanceOf[JList[Point]]
      new PolyLine(indices.toIndexedSeq, points.toIndexedSeq)
    }
  }

  private[this] var registered = false
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        new ShapePickler().register()
        new PointUnpickler().register()
        new PolygonUnpickler().register()
        new PolyLineUnpickler().register()
        registered = true
      }
    }
  }
}
