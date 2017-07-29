package magellan

import java.util.Objects

import magellan.io._
import magellan.mapreduce._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

private[magellan] case class WayKey(val id: String, val index: Int)

private[magellan] object WayKey {
  implicit def orderingByIdIndex[A <: WayKey]: Ordering[A] = {
    Ordering.by(fk => (fk.id, fk.index))
  }
}

private case class WayValue(val id: String, val index: Int, val tags: Map[String, String])

private class WayPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")
  
  override def numPartitions: Int = partitions
  
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[WayKey]
    k.id.hashCode.abs % numPartitions
  }
}

case class OsmFileRelation(
    path: String,
    parameters: Map[String, String])
    (@transient val sqlContext: SQLContext)
  extends SpatialRelation {
  
  private def expandWayValues(way: OsmWay) : Seq[(String, WayValue)] = {
    way.nodeIds.zipWithIndex.map({
      case (nodeId, index) => (nodeId, WayValue(way.id, index, way.tags))
    })
  }
  
  private def keyedPointsRdd(nodes: RDD[OsmNode]): RDD[(String, Point)] = {
    nodes.map({ node => (node.id, node.point) })
  }
  
  private[magellan] def nodesRdd(osmRdd: RDD[(OsmKey, OsmShape)]): RDD[OsmNode] = {
    osmRdd
      .values
      .filter({ shape => shape.isInstanceOf[OsmNode] })
      .map({ shape => shape.asInstanceOf[OsmNode] })
  }
  
  private[magellan] def waysRdd(osmRdd: RDD[(OsmKey, OsmShape)]): RDD[OsmWay] = {
    osmRdd
      .values
      .filter({ shape => shape.isInstanceOf[OsmWay] })
      .map({ shape => shape.asInstanceOf[OsmWay] })
  }
  
  private[magellan] def joinedNodesWays(nodes: RDD[OsmNode], ways: RDD[OsmWay]) = {
    val wayValueTuples = ways.flatMap({ way => expandWayValues(way) })
    
    wayValueTuples
      .join(keyedPointsRdd(nodes))
      .values
      .map({
        case (wayValue, point) => (new WayKey(wayValue.id, wayValue.index), (point, wayValue.tags))
      })
  }
  
  private def wayShapeIsArea(head: Point, tail: Point, tags: Map[String, String]) : Boolean = {
    head == tail && {
      tags.getOrElse("area", "no") == "yes" ||
        !(tags.contains("barrier") ||
        tags.contains("highway"))
    }
  }
  
  private def createWayShape(currentPoints: List[Point], tags: Map[String, String]) : Shape = {
    if (currentPoints.length == 0) {
      NullShape
    } else if (currentPoints.length == 1) {
      currentPoints(0)
    } else {
      val reversedPoints = currentPoints.reverse
      if (wayShapeIsArea(reversedPoints.head, currentPoints.head, tags)) {
        Polygon(Array(), reversedPoints.toArray)
      } else {
        PolyLine(new Array[Int](reversedPoints.toArray.size), reversedPoints.toArray)
      }
    }
  }
  
  private def createWayShapes(
      i: Iterator[(WayKey, (Point, Map[String, String]))],
      currentId: String,
      currentPoints: List[Point],
      currentTags: Map[String, String]): Stream[(Shape, Map[String, String])] = {
    if (i.hasNext) {
      val (key: WayKey, (point: Point, tags: Map[String, String])) = i.next()
      if (currentId == "" || key.id == currentId) {
        createWayShapes(i, key.id, point :: currentPoints, tags)
      } else {
        (createWayShape(currentPoints, tags), tags) #::
          createWayShapes(i, key.id, List(point), tags)
      }
    } else {
      if (currentPoints.isEmpty) {
        Stream.Empty
      } else {
        Stream((createWayShape(currentPoints, currentTags), currentTags))
      }
    }
  }

  private[magellan] def wayShapes(
      nodes: RDD[OsmNode],
      ways: RDD[OsmWay]): RDD[(Shape, Option[Map[String, String]])] = {
    joinedNodesWays(nodes, ways)
      .repartitionAndSortWithinPartitions(new WayPartitioner(sc.defaultParallelism))
      .mapPartitions({ i => createWayShapes(i, "", List(), Map()).toIterator })
      .map({ case (shape, tags) => (shape, Option(tags))})
  }

  protected override def _buildScan(): RDD[Array[Any]] = {

    val osmRdd = sqlContext.sparkContext.newAPIHadoopFile(
      path,
      classOf[OsmInputFormat],
      classOf[OsmKey],
      classOf[OsmShape]
    )
    
    val nodes = nodesRdd(osmRdd).persist
    val ways = waysRdd(osmRdd)

    wayShapes(nodes, ways)
      .union(nodes.map({ node => (node.point, Some(node.tags))}))
        .map {
          case (shape: Shape, meta: Option[Map[String, String]]) =>
            Array(shape, meta)
        }
  }

  override def hashCode(): Int = Objects.hash(path, schema)
}
