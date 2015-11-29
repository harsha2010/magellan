package magellan.io

import org.apache.spark.SerializableWritable
import java.io.{DataInput, DataOutput, ByteArrayOutputStream}
import org.apache.hadoop.io.{Writable, Text, FloatWritable, MapWritable, ArrayWritable}
import magellan.{Shape, Point}
import collection.JavaConversions._

case class OsmKey(val shapeType: String, val id: String) extends Serializable { }

abstract class OsmShape(val id: String, val tags: Map[String, String]) extends Serializable { }

case class OsmNode(
    override val id: String,
    val lat: Double,
    val lon: Double,
    override val tags: Map[String, String])
  extends OsmShape(id, tags) {
  
  def point: Point = Point(lon, lat)
}

case class OsmWay(
    override val id: String,
    val nodeIds: Seq[String],
    override val tags: Map[String, String])
  extends OsmShape(id, tags) { }

case class OsmRelation(
    override val id: String,
    val wayIds: Seq[String],
    override val tags: Map[String, String])
  extends OsmShape(id, tags) { }
