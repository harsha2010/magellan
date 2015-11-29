package magellan.mapreduce

import magellan.io.{OsmKey, OsmShape, OsmNode, OsmWay, OsmRelation}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import scala.xml.{XML, Elem, Node}

private[magellan] class OsmRecordReader
  extends RecordReader[OsmKey, OsmShape] {
 
  val definedNodeLabels = Set("node", "way", "relation")
  var nodes : Seq[Node] = _
  
  var current : Int = 0
  lazy val total = nodes.length
  
  override def initialize(genericSplit: InputSplit, context: TaskAttemptContext) : Unit = {
    val split: FileSplit = genericSplit.asInstanceOf[FileSplit]
    val job = MapReduceUtils.getConfigurationFromContext(context)
    
    val file = split.getPath()
    val fs = file.getFileSystem(job)
    val fileIn = fs.open(file)
    
    val doc = XML.load(fileIn)
    fileIn.close()
    nodes = doc.child.filter(n => definedNodeLabels contains n.label)
  }
  
  override def nextKeyValue() : Boolean = {
    if (!nodes.isEmpty) { 
      if (current != 0) nodes = nodes.tail
      current += 1
    }
    !nodes.isEmpty
  }
  
  override def getCurrentKey() : OsmKey = {
    val current = nodes.head
    new OsmKey(current.label, (current \ "@id").text)
  }
  
  def getTags(shape: Node) = {
    (shape \ "tag").map(t => (t \ "@k").text -> (t \ "@v").text).toMap
  }
  
  def getOsmNode(shape: Node) = {
    new OsmNode(
        (shape \ "@id").text,
        (shape \ "@lat").text.toDouble,
        (shape \ "@lon").text.toDouble,
        getTags(shape))
  }
  
  def getOsmWay(shape: Node) = {
    new OsmWay((shape \ "@id").text, (shape \ "nd").map(w => (w \ "@ref").text), getTags(shape))
  }
  
  def getOsmRelation(shape: Node) = {
    new OsmRelation(
        (shape \ "@id").text,
        (shape \ "member").map(r => (r \ "@ref").text), getTags(shape)
    )
  }
  
  override def getCurrentValue() : OsmShape = {
    val current = nodes.head
    current.label match {
      case "node" => getOsmNode(current)
      case "way" => getOsmWay(current)
      case "relation" => getOsmRelation(current)
    }
  }
  
  override def getProgress() : Float = {
    current.toFloat / total
  }
  
  override def close() : Unit = { }
}
