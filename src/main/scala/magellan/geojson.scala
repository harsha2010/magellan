package magellan

import magellan.geometry.Curve
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer

case class Geometry(`type`: String, coordinates: JValue) {

  def coordinatesToPoint(coordinates: JValue) = {
    coordinates match {
      case JArray(List(JDouble(x), JDouble(y))) => Point(x, y)
      case JArray(List(JDouble(x), JInt(y)))    => Point(x, y.toDouble)
      case JArray(List(JInt(x), JDouble(y)))    => Point(x.toDouble, y)
      case JArray(List(JInt(x), JInt(y)))       => Point(x.toDouble, y.toDouble)
    }
  }

  def extractPoints(p: List[JValue]) = p.map(coordinatesToPoint)

  def extractPolygon(coordinates: JArray): Polygon = {
    val JArray(p) = coordinates
    val rings = p.map { case JArray(q) => extractPoints(q) }
    val indices = rings.scanLeft(0)((running, current) => running + current.size).dropRight(1)
    Polygon(indices.toArray, rings.flatten.toArray)
  }

  val shapes = {
    `type` match {
      case "Point" => { List(coordinatesToPoint(coordinates)) }
      case "LineString" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        val points = extractPoints(p)
        //val indices = points.scanLeft(0)((running, current) => running + points.size-1).dropRight(1)
        val indices = new Array[Int](points.size)
        List(PolyLine(indices, points.toArray))
      }
      case "Polyline" | "MultiLineString" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        val lineSegments = p.map { case JArray(q) => extractPoints(q) }
        val indices = lineSegments.scanLeft(0)((running, current) => running + current.size).dropRight(1)
        List(PolyLine(indices.toArray, lineSegments.flatten.toArray))
      }
      case "Polygon" => {
        val p = coordinates.asInstanceOf[JArray]
        List(extractPolygon(p))
      }
      case "MultiPolygon" => {
        val JArray(p) = coordinates.asInstanceOf[JArray]
        // array of polygon coordinates
        p.map { case polygon @ JArray(q: List[JArray]) => extractPolygon(polygon)}
      }
    }
  }

}

case class Feature(
    `type`: String,
    properties: Option[Map[String, String]],
    geometry: Geometry)

case class CRS(`type`: String, properties: Option[Map[String, String]])

case class GeoJSON(`type`: String, crs: Option[CRS], features: List[Feature])

object GeoJSON {

  def write(shape: Shape): Geometry = {
    val (t, jvalue) = shape match {
      case point: Point => ("Point", asJValue(point))
      case polygon: Polygon =>
        val coords: JValue = polygon.loops.map(asJValue(_)).toList
        ("Polygon", coords)
      case polyline: PolyLine =>
        val coords: JValue = polyline.curves.map(asJValue(_)).toList
        ("MultiLineString", coords)
    }
    Geometry(`type` = t, coordinates = jvalue)
  }

  def writeJson(shape: Shape): String = {
    val geometry = write(shape)
    val json =
      ("type" -> geometry.`type`) ~
        ("coordinates" -> geometry.coordinates)
    compact(render(json))
  }

  private [magellan] def asJValue(point: Point): JValue = {
    List(JDouble(point.getX()), JDouble(point.getY()))
  }

  private [magellan] def asJValue(curve: Curve): JValue = {
    val iter = curve.iterator()
    var end: Point = null
    val coords = new ListBuffer[JValue]
    while (iter.hasNext) {
      val line = iter.next()
      val start =  line.getStart()
      end = line.getEnd()
      coords.append(asJValue(start))
    }
    coords.append(asJValue(end))
    coords.toList
  }
}
