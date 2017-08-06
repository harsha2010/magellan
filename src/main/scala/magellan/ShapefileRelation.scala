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

package magellan

import java.util.Objects

import magellan.io._
import magellan.mapreduce._
import org.apache.hadoop.io.{ArrayWritable, LongWritable, MapWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * A Shapefile relation is the entry point for working with Shapefile formats.
 */
case class ShapeFileRelation(
    path: String,
    parameters: Map[String, String])
    (@transient val sqlContext: SQLContext)
  extends SpatialRelation {

  protected override def _buildScan(): RDD[Array[Any]] = {

    // read the shx files, if they exist
    val fileNameToFileSplits = Try(sc.newAPIHadoopFile(
      path + "/*.shx",
      classOf[ShxInputFormat],
      classOf[Text],
      classOf[ArrayWritable]
    ).map { case (txt: Text, splits: ArrayWritable) =>
      val fileName = txt.toString
      val s = splits.get()
      val size = s.length
      var i = 0
      val v = Array.fill(size)(0L)
      while (i < size) {
        v.update(i, s(i).asInstanceOf[LongWritable].get())
        i += 1
      }
      (fileName, v)
    }.collectAsMap())

    fileNameToFileSplits.map(SplitInfos.SPLIT_INFO_MAP.set(_))

    val shapefileRdd = sqlContext.sparkContext.newAPIHadoopFile(
      path + "/*.shp",
      classOf[ShapeInputFormat],
      classOf[ShapeKey],
      classOf[ShapeWritable]
    )

    val dbaseRdd = sqlContext.sparkContext.newAPIHadoopFile(
      path + "/*.dbf",
      classOf[DBInputFormat],
      classOf[ShapeKey],
      classOf[MapWritable]
    )

    val dataRdd = shapefileRdd.map { case (k, v) =>
      ((k.getFileNamePrefix(), k.getRecordIndex()), v.shape)
    }

    val metadataRdd = dbaseRdd.map { case (k, v) =>
      val meta = v.entrySet().map { kv =>
        val k = kv.getKey.asInstanceOf[Text].toString
        val v = kv.getValue.asInstanceOf[Text].toString
        (k, v)
      }.toMap
      ((k.getFileNamePrefix(), k.getRecordIndex()), meta)
    }
    dataRdd.leftOuterJoin(metadataRdd).map(f => Array(f._2._1, f._2._2))
  }

  override def hashCode(): Int = Objects.hash(path, schema)
}
