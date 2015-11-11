package magellan

import org.scalatest.FunSuite
import org.apache.spark.sql.magellan.MagellanContext
import magellan.io.{OsmShape, OsmNode, OsmWay, OsmRelation}
import java.io.{DataInput, DataOutput, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

class OsmSuite extends FunSuite with TestSparkContext {
  
  def sqlCtx = new MagellanContext(sc)
  
  test("OsmNode creation") {
    val node = new OsmNode("ID1", 45.4214.toFloat, 75.6919.toFloat, Map("foo" -> "bar", "baz" -> "qux"))
    assert(node.id == "ID1")
    assert(node.lat == 45.4214.toFloat)
    assert(node.lon == 75.6919.toFloat)
    assert(node.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
  
  test("OsmNode read/write") {
    val shape = new OsmNode("ID1", 45.4214.toFloat, 75.6919.toFloat, Map("foo" -> "bar", "baz" -> "qux"))
    val dataOutBytes = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(dataOutBytes)
    shape.write(dataOut)
    
    val dataInBytes = new ByteArrayInputStream(dataOutBytes.toByteArray())
    val dataIn = new DataInputStream(dataInBytes)
    val shape2 = new OsmNode()
    shape2.readFields(dataIn)
    
    assert(shape2.id == "ID1")
    assert(shape2.lat == 45.4214.toFloat)
    assert(shape2.lon == 75.6919.toFloat)
    assert(shape2.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
  
  test("OsmWay creation") {
    val way = new OsmWay("ID1", List("1", "2", "3"), Map("foo" -> "bar", "baz" -> "qux"))
    assert(way.id == "ID1")
    assert(way.nodeIds == List("1", "2", "3"))
    assert(way.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
  
  test("OsmWay read/write") {
    val shape = new OsmWay("ID1", List("1", "2", "3"), Map("foo" -> "bar", "baz" -> "qux"))
    val dataOutBytes = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(dataOutBytes)
    shape.write(dataOut)
    
    val dataInBytes = new ByteArrayInputStream(dataOutBytes.toByteArray())
    val dataIn = new DataInputStream(dataInBytes)
    val shape2 = new OsmWay()
    shape2.readFields(dataIn)
    
    assert(shape2.id == "ID1")
    assert(shape2.nodeIds == List("1", "2", "3"))
    assert(shape2.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
  
  test("OsmRelation creation") {
    val rel = new OsmRelation("ID1", List("1", "2", "3"), Map("foo" -> "bar", "baz" -> "qux"))
    assert(rel.id == "ID1")
    assert(rel.wayIds == List("1", "2", "3"))
    assert(rel.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
  
  test("OsmRelation read/write") {
    val shape = new OsmRelation("ID1", List("1", "2", "3"), Map("foo" -> "bar", "baz" -> "qux"))
    val dataOutBytes = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(dataOutBytes)
    shape.write(dataOut)
    
    val dataInBytes = new ByteArrayInputStream(dataOutBytes.toByteArray())
    val dataIn = new DataInputStream(dataInBytes)
    val shape2 = new OsmRelation()
    shape2.readFields(dataIn)
    
    assert(shape2.id == "ID1")
    assert(shape2.wayIds == List("1", "2", "3"))
    assert(shape2.tags == Map("foo" -> "bar", "baz" -> "qux"))
  }
}
