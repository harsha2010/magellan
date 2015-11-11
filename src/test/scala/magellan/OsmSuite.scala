package magellan

import org.scalatest.FunSuite
import org.apache.spark.sql.magellan.MagellanContext
import magellan.io.{OsmKey, OsmShape, OsmNode, OsmWay, OsmRelation}
import java.io.{DataInput, DataOutput, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrameNaFunctions}

class OsmSuite extends FunSuite with TestSparkContext {
  
  def sqlCtx = new MagellanContext(sc)
  
  def osmRdd() : RDD[(OsmKey, OsmShape)] = {
    val rows = List(
        (
          new OsmKey("node", "ID1"),
          new OsmNode("ID1", 45.4214, 75.6919, Map("foo" -> "bar"))
        ), (
          new OsmKey("node", "ID2"),
          new OsmNode("ID2", 43.7000, 79.4000, Map("foo" -> "baz"))
        ), (
          new OsmKey("node", "ID3"),
          new OsmNode("ID3", 45.5017, 73.5673, Map("foo" -> "qux", "bar" -> "quux"))
        ), (
          new OsmKey("way", "ID4"),
          new OsmWay("ID4", List("ID2", "ID1", "ID3"), Map("type" -> "road"))
        )
    )
    sc.parallelize(rows, 2)
  }
  
  def fileRelation = new OsmFileRelation("/test")(sqlContext)
  
  test("nodesRdd filters and casts") {
    val nodes = fileRelation.nodesRdd(osmRdd).collect().sortBy({ node => node.id })
    assert(nodes.length == 3)
    println(nodes(0).toString())
    assert(nodes(0).id == "ID1")
    assert(nodes(0).point == Point(75.6919, 45.4214))
  }
  
  test("waysRdd filters and casts") {
    val ways = fileRelation.waysRdd(osmRdd).collect()
    assert(ways.length == 1)
    assert(ways(0).id == "ID4")
  }
  
  test("joinedNodesWays joins nodes and ways") {
    val nodes = fileRelation.nodesRdd(osmRdd)
    val ways = fileRelation.waysRdd(osmRdd)
    val joined = fileRelation
      .joinedNodesWays(nodes, ways)
      .collect()
      .sortBy({ case (key, _) => (key.id, key.index) })
    assert(joined.length == 3)
    assert(joined(0)._1 == new WayKey("ID4", 0))
    assert(joined(1)._2._1 == Point(75.6919, 45.4214))
  }
  
  test("wayShapes generates polyline") {
    val nodes = fileRelation.nodesRdd(osmRdd)
    val ways = fileRelation.waysRdd(osmRdd)
    val shapes = fileRelation.wayShapes(nodes, ways).collect()
    assert(shapes.length == 1)
    assert(shapes(0)._1 ==
      new PolyLine(
        List().toIndexedSeq,
        List(Point(79.4000, 43.7000),
        Point(75.6919, 45.4214),
        Point(73.5673, 45.5017)).toIndexedSeq))
  }
  
  test("read point") {
    val path = this.getClass.getClassLoader.getResource("osm/point").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "osm")
      .load(path)
    assert(df.count() === 1)
    
    val p = df.select("point").map { case Row(p: Point) => p}.first()
    assert(p.equals(new Point(-75.6470109, 45.4187480)))
  }
  
  test("read linestring") {
    val path = this.getClass.getClassLoader.getResource("osm/linestring").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "osm")
      .load(path)
    assert(df.count() === 5)
    assert(df.filter(df("polyline").isNotNull).count() === 1)
    
    val p = df
      .filter(df("polyline").isNotNull)
      .select("polyline")
      .map { case Row(p: PolyLine) => p}.first()
    
    assert(p.points.size == 4)
    assert(p.points(0) == new Point(-75.6362879, 45.4188896))
    assert(p.points(1) == new Point(-75.6378443, 45.4191178))
    assert(p.points(2) == new Point(-75.6382141, 45.4191290))
    assert(p.points(3) == new Point(-75.6390858, 45.4190782))
  }
  
  test("read polygon") {
    val path = this.getClass.getClassLoader.getResource("osm/polygon").getPath
    val df = sqlCtx.read
      .format("magellan")
      .option("type", "osm")
      .load(path)

    assert(df.count() === 4)
    
    val polygons = df.select("polygon")
      .filter(df("polygon").isNotNull)
      .map({ case Row(p: Polygon) => p})
      .collect()
    
    assert(polygons.length == 1)
    val p = polygons(0)
    
    val expectedPoints = Vector(
        new Point(-75.6419079, 45.4200638),
        new Point(-75.6421911, 45.4217868),
        new Point(-75.6420795, 45.4220880),
        new Point(-75.6419079, 45.4200638))
    val expected = new Polygon(Vector(), expectedPoints)
    
    assert(p == expected)
  }
  
  test("read complete file") {
    val path = this.getClass
      .getClassLoader
      .getResource("osm/combination")
      .getPath

    val df = sqlCtx.read
      .format("magellan")
      .option("type", "osm")
      .load(path)
      
    val points = df.select("point")
      .filter(df("point").isNotNull)
    
    val linestrings = df.select("polyline")
      .filter(df("polyline").isNotNull)
    
    val polygons = df.select("polygon")
      .filter(df("polygon").isNotNull)
    
    assert(df.count() == 1288)
    assert(points.count() == 1134)
    assert(linestrings.count() == 90)
    assert(polygons.count() == 64)
  }
}
