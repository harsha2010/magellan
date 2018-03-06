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

import magellan.mapreduce.WholeFileInputFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * A GeoJSON relation is the entry point for working with GeoJSON formats.
  * GeoJSON is a format for encoding a variety of geographic data structures.
  * A GeoJSON object may represent a geometry, a feature, or a collection of features.
  * GeoJSON supports the following geometry types: Point, LineString, Polygon,
  * MultiPoint, MultiLineString, MultiPolygon, and GeometryCollection.
  * Features in GeoJSON contain a geometry object and additional properties,
  * and a feature collection represents a list of features.
  * A complete GeoJSON data structure is always an object (in JSON terms).
  * In GeoJSON, an object consists of a collection of name/value pairs -- also called members.
  * For each member, the name is always a string.
  * Member values are either a string, number, object, array
  * or one of the literals: true, false, and null.
  * An array consists of elements where each element is a value as described above.
  */
case class GeoJSONRelation(
    path: String,
    parameters: Map[String, String])
    (@transient val sqlContext: SQLContext)
  extends SpatialRelation {

  protected override def _buildScan(): RDD[Array[Any]] = {
    val conf = sc.hadoopConfiguration
    FileSystem.getLocal(conf)
    sc.newAPIHadoopFile(
      path,
      classOf[WholeFileInputFormat],
      classOf[NullWritable],
      classOf[Text]).flatMap {
      case (k, v) =>
        val line = v.toString()
        parseShapeWithMeta(line)
    }.map {
      case (shape: Shape, meta: Option[Map[String, String]]) =>
        Array(shape, meta)
    }
  }

  private def parseShapeWithMeta(line: String) = {
    val tree = parse(line)
    implicit val formats = org.json4s.DefaultFormats
    val result = tree.extract[GeoJSON]
    result.features.flatMap { f =>
      f.geometry.shapes.map(shape => (shape, f.properties))
    }
  }
}
