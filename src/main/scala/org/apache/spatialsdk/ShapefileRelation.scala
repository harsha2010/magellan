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

import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spatialsdk.io.ShapeWritable
import org.apache.spatialsdk.mapreduce.ShapeInputFormat

/**
 * A Shapefile relation is the entry point for working with Shapefile formats.
 */
case class ShapeFileRelation(path: String)
                            (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  @transient val sc = sqlContext.sparkContext

  override val schema = {
    StructType(List(StructField("point", new PointUDT(), true),
        StructField("polygon", new PolygonUDT(), true)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val baseRdd = sqlContext.sparkContext.newAPIHadoopFile(
      path,
      classOf[ShapeInputFormat],
      classOf[NullWritable],
      classOf[ShapeWritable]
    )
    val numFields = schema.fields.length
    val row = new GenericMutableRow(numFields)
    baseRdd.mapPartitions { iter =>
      iter.flatMap { case(k, v) =>
        val shape = v.shape
        shape match {
          case NullShape => None
          case _: Point =>
            row(0) = shape
            Some(row)
          case _: Polygon =>
            row(1) = shape
            Some(row)
          case _ => ???
        }
      }
    }
  }

}
