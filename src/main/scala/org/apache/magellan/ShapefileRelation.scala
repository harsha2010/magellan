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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, GenericMutableRow}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.magellan.io.{ShapeKey, ShapeWritable}
import org.apache.magellan.mapreduce.{DBInputFormat, ShapeInputFormat}

import scala.collection.JavaConversions._

/**
 * A Shapefile relation is the entry point for working with Shapefile formats.
 */
case class ShapeFileRelation(path: String)
                            (@transient val sqlContext: SQLContext)
  extends BaseRelation with CatalystScan {

  @transient val sc = sqlContext.sparkContext

  override val schema = {
    StructType(List(StructField("point", new PointUDT(), true),
      StructField("polyline", new PolyLineUDT(), true),
      StructField("polygon", new PolygonUDT(), true),
      StructField("metadata", MapType(StringType, StringType, true), true),
      StructField("valid", BooleanType, true)
    ))
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
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

    val indices = requiredColumns.map(attr => schema.fieldIndex(attr.name))
    val numFields = indices.size

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
      val row = new GenericMutableRow(numFields)
      iter.flatMap { case ((filePrefix, recordIndex), (shape, meta)) =>
        // we are re-using rows. clear them before next call
        (0 until numFields).foreach(i => row.setNullAt(i))
        indices.zipWithIndex.foreach { case (index, i) =>
          val v = index match {
            case 0 => if (shape.isInstanceOf[Point]) Some(shape) else None
            case 1 => if (shape.isInstanceOf[PolyLine]) Some(shape) else None
            case 2 => if (shape.isInstanceOf[Polygon]) Some(shape) else None
            case 3 => Some(meta.fold(Map[String, String]())(identity))
            case 4 => Some(shape.isValid())
          }
          v.foreach(x => row(i) = x)
        }
        Some(row)
      }
    }
  }

  override def hashCode(): Int = Objects.hashCode(path, schema)
}
