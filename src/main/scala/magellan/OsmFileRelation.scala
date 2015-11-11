package magellan

import com.google.common.base.Objects
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import magellan.io._
import magellan.mapreduce._

case class OsmFileRelation(path: String)(@transient val sqlContext: SQLContext)
  extends SpatialRelation {
  
  protected override def _buildScan(): RDD[(Shape, Option[Map[String, String]])] = {

    val osmRdd = sqlContext.sparkContext.newAPIHadoopFile(
      path,
      classOf[OsmInputFormat],
      classOf[Tuple2[String, String]],
      classOf[OsmShape]
    )
    
    osmRdd.map { case (k, v) => (v.toSubType.shape, Option(v.tags))}
  }
  
  override def hashCode(): Int = Objects.hashCode(path, schema)
}
