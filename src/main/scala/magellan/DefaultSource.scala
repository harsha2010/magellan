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

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, SchemaRelationProvider, RelationProvider}

/**
 * Provides access to Shapefile Formatted data from pure SQL statements.
 */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = createRelation(sqlContext, parameters, null)

  override def createRelation(sqlContext: SQLContext,
    parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified for Shapefiles."))
    val t = parameters.getOrElse("type", "shapefile")
    t match {
      case "shapefile" => new ShapeFileRelation(path, parameters)(sqlContext)
      case "geojson" => new GeoJSONRelation(path, parameters)(sqlContext)
      case "osm" => new OsmFileRelation(path, parameters)(sqlContext)
      case _ => ???
    }
  }

}
