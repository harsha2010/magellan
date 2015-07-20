/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.magellan

import com.google.common.base.Objects
import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.magellan.io.{ShapeKey, ShapeWritable}
import org.apache.magellan.mapreduce.{DBInputFormat, ShapeInputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
 * A Shapefile relation is the entry point for working with Shapefile formats.
 */
case class ShapeFileRelation(path: String)(@transient val sqlContext: SQLContext)
  extends SpatialRelation {

  override def _buildScan(): RDD[(Shape, Option[Map[String, String]])] = {

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
    dataRdd.leftOuterJoin(metadataRdd).map(f => f._2)
  }

  override def hashCode(): Int = Objects.hashCode(path, schema)
}
