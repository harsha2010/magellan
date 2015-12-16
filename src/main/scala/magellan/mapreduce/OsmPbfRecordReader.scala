package magellan.mapreduce

import java.util

import crosby.binary.osmosis.OsmosisReader
import magellan.io.{OsmKey, OsmNode, OsmRelation, OsmShape, OsmWay}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer
import org.openstreetmap.osmosis.core.domain.v0_6._
import org.openstreetmap.osmosis.core.task.v0_6.Sink
import scala.collection.JavaConversions._

private[magellan] class OsmPbfRecordReader
  extends RecordReader[OsmKey, OsmShape] {
 
  val definedNodeLabels = Set("node", "way", "relation")
  var nodes : Seq[Entity] = _
  
  var current : Int = 0
  lazy val total = nodes.length
  
  override def initialize(genericSplit: InputSplit, context: TaskAttemptContext) : Unit = {
    val split: FileSplit = genericSplit.asInstanceOf[FileSplit]
    val job = MapReduceUtils.getConfigurationFromContext(context)
    
    val file = split.getPath
    val fs = file.getFileSystem(job)
    val fileIn = fs.open(file)

    val osmosisReader = new OsmosisReader(fileIn)
    osmosisReader.setSink(new Sink {
      override def process(entityContainer: EntityContainer): Unit = {
        if (entityContainer.getEntity.getType != EntityType.Bound) {
          nodes = nodes:+entityContainer.getEntity
        }
      }

      override def initialize(map: util.Map[String, AnyRef]): Unit = {
        nodes = List()
      }

      override def complete(): Unit = {}

      override def release(): Unit = {}
    })
    osmosisReader.run()
  }
  
  override def nextKeyValue() : Boolean = {
    if (nodes.nonEmpty) {
      if (current != 0) nodes = nodes.tail
      current += 1
    }
    nodes.nonEmpty
  }
  
  override def getCurrentKey: OsmKey = {
    val current = nodes.head
    new OsmKey(current.getType.name(), current.getId.toString)
  }
  
  def getTags(entity: Entity) = {
    entity.getTags.to[Seq].map(tag => tag.getKey -> tag.getValue).toMap
  }
  
  def getOsmNode(osmosisNode: Node) = {
    new OsmNode(osmosisNode.getId.toString,
        osmosisNode.getLatitude,
        osmosisNode.getLongitude,
        getTags(osmosisNode))
  }
  
  def getOsmWay(shape: Way) = {
    new OsmWay(shape.getId.toString, shape.getWayNodes.to[Seq].map(node => node.getNodeId.toString), getTags(shape))
  }
  
  def getOsmRelation(shape: Relation) = {
    new OsmRelation(
        shape.getId.toString,
        shape.getMembers.to[Seq].map(members => members.getMemberId.toString), getTags(shape)
    )
  }
  
  override def getCurrentValue: OsmShape = {
    val current = nodes.head
    current match {
      case node : Node => getOsmNode(node)
      case way : Way => getOsmWay(way)
      case relation : Relation => getOsmRelation(relation)
    }
  }
  
  override def getProgress: Float = {
    current.toFloat / total
  }
  
  override def close() : Unit = { }
}
