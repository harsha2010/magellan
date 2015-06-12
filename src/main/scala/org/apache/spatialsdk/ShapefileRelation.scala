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

package org.apache.spatialsdk

import org.apache.hadoop.io.{MapWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spatialsdk.io.{ShapeKey, ShapeWritable}
import org.apache.spatialsdk.mapreduce.{DBInputFormat, ShapeInputFormat}

import scala.collection.JavaConversions._

/**
 * A Shapefile relation is the entry point for working with Shapefile formats.
 */
case class ShapeFileRelation(path: String)
                            (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  @transient val sc = sqlContext.sparkContext

  override val schema = {
    StructType(List(StructField("point", new PointUDT(), true),
      StructField("polyline", new PolyLineUDT(), true),
      StructField("polygon", new PolygonUDT(), true),
      StructField("metadata", MapType(StringType, StringType, true), true)
    ))
  }

  override def buildScan(): RDD[Row] = {
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

    val numFields = schema.fields.length
    val row = new GenericMutableRow(numFields)
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

    dataRdd.leftOuterJoin(metadataRdd).mapPartitions { iter =>
      iter.flatMap { case ((filePrefix, recordIndex), (shape, meta)) =>
        // we are re-using rows. clear them before next call
        (0 until numFields).foreach(i => row.setNullAt(i))
        row(3) = meta.fold(Map[String, String]())(identity)
        shape match {
          case NullShape => None
          case _: Point =>
            row(0) = shape
            Some(row)
          case _: PolyLine =>
            row(1) = shape
            Some(row)
          case _: Polygon =>
            row(2) = shape
            Some(row)
          case _ => ???
        }
      }
    }
  }

}
