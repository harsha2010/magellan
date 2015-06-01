/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spatialsdk

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, SchemaRelationProvider, RelationProvider}

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
    new ShapeFileRelation(path)(sqlContext)
  }

}
