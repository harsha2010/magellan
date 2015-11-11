package magellan.io

import java.io.{DataInput, DataOutput, ByteArrayOutputStream}
import org.apache.hadoop.io.Writable
import magellan.{Shape, Point}
import java.nio.ByteBuffer

class OsmShape extends Writable{
  var shapeId: Int = -1
  var id: String = _
  var bytes: Array[Byte] = _
  var tags: Map[String, String] = _
  
  protected def getBB: ByteBuffer = ByteBuffer.wrap(bytes)
  protected def intToByteArray(i: Int): Array[Byte] = ByteBuffer.allocate(4).putInt(i).array
  
  def shape = Point(0, 0)
  
  override def readFields(dataInput: DataInput): Unit = {
    shapeId = dataInput.readInt() // shape identifier
    id = dataInput.readUTF()
    val numBytes = dataInput.readInt()
    bytes = Array.ofDim[Byte](numBytes)
    dataInput.readFully(bytes)
    tags = readTags(dataInput)
  }
  
  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeInt(shapeId)
    dataOutput.writeUTF(id)
    dataOutput.writeInt(bytes.size)
    dataOutput.write(bytes)
    writeTags(dataOutput)
  }
  
  def readTags(dataInput: DataInput) = {
    val numTags = dataInput.readInt()
    (1 to numTags map { _ => (dataInput.readUTF, dataInput.readUTF)}).toMap
  }
  
  def writeTags(dataOutput: DataOutput) = {
    dataOutput.writeInt(tags.keys.size)
    tags foreach { case (k, v) =>
      dataOutput.writeUTF(k)
      dataOutput.writeUTF(v)
    }
  }
  
  def toSubType(): OsmShape = {
    shapeId match {
      case -1 => this
      case 0 => new OsmNode(this)
      case 1 => new OsmWay(this)
      case 2 => new OsmRelation(this)
    }
  }
}

case class OsmNode() extends OsmShape { 
  
  def this(id: String, lat: Float, lon: Float, tags: Map[String, String]) = {
    this()
    this.shapeId = 0
    this.id = id
    this.tags = tags
    this.bytes = Array.ofDim[Byte](8)
    this.setLatLon(lat, lon)
  }
  
  def this(osmShape: OsmShape) = {
    this()
    this.shapeId = 0
    this.id = osmShape.id
    this.tags = osmShape.tags
    this.bytes = osmShape.bytes
  }
  
  def lat(): Float = getBB.getFloat(0)
  
  def lon(): Float = getBB.getFloat(4)
  
  override def shape = Point(lon, lat)
 
  private def setLatLon(lat: Float, lon: Float): Unit = {
    val bb = getBB
    bb.putFloat(lat)
    bb.putFloat(lon)
  }
}

case class OsmWay() extends OsmShape {
  
  def this(id: String, nodeIds: Seq[String], tags: Map[String, String]) = {
    this()
    this.shapeId = 1
    this.id = id
    this.setNodeIds(nodeIds)
    this.tags = tags
  }
  
  def this(osmShape: OsmShape) = {
    this()
    this.shapeId = 1
    this.id = osmShape.id
    this.tags = osmShape.tags
    this.bytes = osmShape.bytes
  }
  
  def nodeIds(): Seq[String] = {
    val bb = getBB
    val numNodes = bb.getInt()

    1 to numNodes map { _ =>
      val numBytes = bb.getInt()
      val strBytes = Array.ofDim[Byte](numBytes)
      bb.get(strBytes)
      new String(strBytes)
    }
  }
  
  private def setNodeIds(nodeIds: Seq[String]): Unit = {
    val bStream = new ByteArrayOutputStream()
    bStream.write(intToByteArray(nodeIds.length))
    
    nodeIds foreach { n =>
      bStream.write(intToByteArray(n.length))
      bStream.write(n.getBytes)
    }
    bytes = bStream.toByteArray()
  }
}

case class OsmRelation() extends OsmShape {
  
  def this(id: String, wayIds: Seq[String], tags: Map[String, String]) = {
    this()
    this.shapeId = 2
    this.id = id
    this.setWayIds(wayIds)
    this.tags = tags
  }
  
  def this(osmShape: OsmShape) = {
    this()
    this.shapeId = 2
    this.id = osmShape.id
    this.tags = osmShape.tags
    this.bytes = osmShape.bytes
  }
  
  def wayIds(): Seq[String] = {
    val bb = getBB
    val numNodes = bb.getInt
    
    1 to numNodes map { n =>
      val numBytes = bb.getInt()
      val strBytes = Array.ofDim[Byte](numBytes)
      bb.get(strBytes)
      new String(strBytes)
    }
  }

  private def setWayIds(wayIds: Seq[String]): Unit = {
    val bStream = new ByteArrayOutputStream()
    bStream.write(intToByteArray(wayIds.length))
    
    wayIds foreach { w =>
      bStream.write(intToByteArray(w.length))
      bStream.write(w.getBytes)
    }
    bytes = bStream.toByteArray()
  }
}
