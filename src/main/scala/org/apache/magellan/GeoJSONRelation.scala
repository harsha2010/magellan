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

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.magellan.mapreduce.WholeFileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/**
 * A GeoJSON relation is the entry point for working with GeoJSON formats.
 */
case class GeoJSONRelation(path: String)
                          (@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  @transient val sc = sqlContext.sparkContext

  override val schema = {
    StructType(List(StructField("point", new PointUDT(), true),
      StructField("polyline", new PolyLineUDT(), true),
      StructField("polygon", new PolygonUDT(), true),
      StructField("metadata", MapType(StringType, StringType, true), true),
      StructField("valid", BooleanType, true)
    ))
  }

  override def buildScan(): RDD[Row] = {
    val numFields = schema.fields.length
    sc.newAPIHadoopFile(
      path,
      classOf[WholeFileInputFormat],
      classOf[NullWritable],
      classOf[Text]
    ).flatMap { case(k, v) =>
      val line = v.toString()
      parseShapeWithMeta(line)
    }.mapPartitions { iter =>
      val row = new GenericMutableRow(numFields)
      iter.flatMap { case (shape: Shape, meta: Option[Map[String, String]]) =>
        (0 until numFields).foreach(i => row.setNullAt(i))
        row(3) = meta.fold(Map[String, String]())(identity)
        row(4) = if (shape == NullShape) false else shape.isValid()
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

  private def parseShapeWithMeta(line: String) = {
    val tree = parse(line)
    implicit val formats = org.json4s.DefaultFormats
    val result = tree.extract[GeoJSON]
    result.features.map(f => (f.geometry.shape, f.properties))
  }
}

case class Geometry(`type`: String, coordinates: JValue) {
  def extractPoints(p: List[JValue]) = {
    p.map { case (JArray(List(JDouble(x), JDouble(y)))) => new Point(x, y)}
  }
  val shape = {
    `type` match {
      case "Point" => {
        val JArray(List(JDouble(x), JDouble(y))) = coordinates
        new Point(x, y)
      }
      case "LineString" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        val points = extractPoints(p)
        new PolyLine(Array.fill(1)(0), points.toIndexedSeq)
      }
      case "Polygon" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        val rings = p.map { case JArray(q) => extractPoints(q)}
        val indices = rings.scanLeft(0)((running, current) => running + current.size).dropRight(1)
        new Polygon(indices.toIndexedSeq, rings.flatten.toIndexedSeq)
      }
    }
  }

}
case class Feature(`type`: String, properties: Option[Map[String, String]], geometry: Geometry)
case class CRS(`type`: String, properties: Option[Map[String, String]])
case class GeoJSON(`type`: String, crs: Option[CRS], features: List[Feature])