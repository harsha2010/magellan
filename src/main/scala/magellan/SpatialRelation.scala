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

import magellan.index.{ZOrderCurve, ZOrderCurveIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

private[magellan] trait SpatialRelation extends BaseRelation with PrunedFilteredScan {

  @transient val sc = sqlContext.sparkContext

  val parameters: Map[String, String]

  private val indexer = new ZOrderCurveIndexer()

  private val autoIndex: Boolean = parameters.getOrElse("magellan.index", "false").toBoolean

  private val precision = parameters.getOrElse("magellan.index.precision", "30").toInt

  private val indexSchema = ArrayType(new StructType()
    .add("curve", new ZOrderCurveUDT, false)
    .add("relation", StringType, false))

  override val schema = StructType(schemaWithHandlers().map(_._1))

  protected def schemaWithHandlers(): List[(StructField, (Int, Array[Any]) => Option[Any])] = {

    def extractShape = {
      (fieldIndex: Int, row: Array[Any]) =>
        val shape = row(0).asInstanceOf[Shape]
        val shapeType = shape.getType()
        val fieldSchema = schema(fieldIndex)
        val expectedShapeType = fieldSchema.dataType.asInstanceOf[GeometricUDT].geometryType
        if (expectedShapeType == shapeType) Some(shape) else None
    }

    def extractMetadata = {
      (fieldIndex: Int, row: Array[Any]) =>
        val meta = row(1).asInstanceOf[Option[Map[String, String]]]
        Some(meta.fold(Map[String, String]())(identity))
    }

    def valid = {
      (fieldIndex: Int, row: Array[Any]) =>
        val shape = row(0).asInstanceOf[Shape]
        Some(shape.isValid())
    }

    def index = {
      (fieldIndex: Int, row: Array[Any]) =>
        val shape = row(0).asInstanceOf[Shape]
        Some(this.index(shape))
    }

    val base = List(
      (StructField("point", new PointUDT(), true), extractShape),
      (StructField("polyline", new PolyLineUDT(), true), extractShape),
      (StructField("polygon", new PolygonUDT(), true), extractShape),
      (StructField("metadata", MapType(StringType, StringType, true), true), extractMetadata),
      (StructField("valid", BooleanType, true), valid)
    )

    if (autoIndex) {
      base ++ List((StructField("index", indexSchema, false), index))
    } else {
      base
    }
  }

  protected def _buildScan(): RDD[Array[Any]]

  private def index(shape: Shape) = {
    val indices = indexer.indexWithMeta(shape, precision) map {
      case (index: ZOrderCurve, relation: Relate) => {
        (index, relation.name())
      }
    }

    indices
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]) = {
    val handlers = schemaWithHandlers()
    val indices = requiredColumns.map(attr => schema.fieldIndex(attr))
    val numFields = indices.size
    _buildScan().mapPartitions { iter =>
      val row = new Array[Any](numFields)
      iter.flatMap { record  =>

        (0 until numFields).foreach(i => row(i) = null)

        indices.zipWithIndex.foreach { case (index, i) =>
          // get the appropriate handler
          val handler = handlers(index)._2
          val v = handler(index, record)
          v.foreach(x => row(i) = x)
          }
        Some(Row.fromSeq(row))
      }
    }
  }
}
