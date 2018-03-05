package magellan.esri

import com.esri.core.geometry.{Geometry => ESRIGeometry, Point => ESRIPoint, Polygon => ESRIPolygon, Polyline => ESRIPolyLine}
import magellan._
import scala.collection.mutable.ArrayBuffer

object ESRIUtil {

  def fromESRIGeometry(geometry: ESRIGeometry): Shape = {
    geometry match {
      case esriPoint: ESRIPoint => fromESRI(esriPoint)
      case esriPolyline: ESRIPolyLine => fromESRI(esriPolyline)
      case esriPolygon: ESRIPolygon => fromESRI(esriPolygon)
    }
  }

  def toESRIGeometry(shape: Shape): ESRIGeometry = {
    shape match {
      case point: Point => toESRI(point)
      case polyline: PolyLine => toESRI(polyline)
      case polygon: Polygon => toESRI(polygon)
    }
  }

  /**
   * Convert ESRI 2D Point to Magellan Point.
   *
   * @param esriPoint
   * @return
   */
  def fromESRI(esriPoint: ESRIPoint): Point = {
    Point(esriPoint.getX, esriPoint.getY)
  }

  /**
   * Convert Magellan Point to ESRI 2D Point
   *
   * @param point
   * @return
   */
  def toESRI(point: Point): ESRIPoint = {
    val esriPoint = new ESRIPoint()
    esriPoint.setXY(point.getX(), point.getY())
    esriPoint
  }

  /**
   * Convert ESRI PolyLine to Magellan PolyLine.
   *
   * @param esriPolyLine
   * @return
   */
  def fromESRI(esriPolyLine: ESRIPolyLine): PolyLine = {
    val length = esriPolyLine.getPointCount
    if (length == 0) {
      PolyLine(Array[Int](), Array[Point]())
    } else {
      val indices = ArrayBuffer[Int]()
      indices.+=(0)
      val points = ArrayBuffer[Point]()
      var start = esriPolyLine.getPoint(0)
      var currentRingIndex = 0
      points.+=(Point(start.getX(), start.getY()))

      for (i <- (1 until length)) {
        val p = esriPolyLine.getPoint(i)
        val j = esriPolyLine.getPathEnd(currentRingIndex)
        if (j < length) {
          val end = esriPolyLine.getPoint(j)
          if (p.getX == end.getX && p.getY == end.getY) {
            indices.+=(i)
            currentRingIndex += 1
            // add start point
            points.+= (Point(start.getX(), start.getY()))
            start = end
          }
        }
        points.+=(Point(p.getX(), p.getY()))
      }
      PolyLine(indices.toArray, points.toArray)
    }
  }

  /**
   * Convert Magellan PolyLine to ESRI PolyLine.
   *
   * @param polyline
   * @return
   */
  def toESRI(polyline: PolyLine): ESRIPolyLine = {
    val l = new ESRIPolyLine()
    val indices = polyline.getRings()
    val length = polyline.length
    if (length > 0) {
      var startIndex = 0
      var endIndex = 1
      var currentRingIndex = 0
      val startVertex = polyline.getVertex(startIndex)
      l.startPath(
        startVertex.getX(),
        startVertex.getY())

      while (endIndex < length) {
        val endVertex = polyline.getVertex(endIndex)
        l.lineTo(endVertex.getX(), endVertex.getY())
        startIndex += 1
        endIndex += 1
        // if we reach a ring boundary skip it
        val nextRingIndex = currentRingIndex + 1
        if (nextRingIndex < indices.length) {
          val nextRing = indices(nextRingIndex)
          if (endIndex == nextRing) {
            startIndex += 1
            endIndex += 1
            currentRingIndex = nextRingIndex
            val startVertex = polyline.getVertex(startIndex)
            l.startPath(
              startVertex.getX(),
              startVertex.getY())
          }
        }
      }
    }
    l
  }

  /**
   * Convert ESRI Polygon to Magellan Polygon.
   *
   * @param esriPolygon
   * @return
   */
  def fromESRI(esriPolygon: ESRIPolygon): Polygon = {
    val length = esriPolygon.getPointCount
    if (length == 0) {
      Polygon(Array[Int](), Array[Point]())
    } else {
      val indices = ArrayBuffer[Int]()
      indices.+=(0)
      val points = ArrayBuffer[Point]()
      var start = esriPolygon.getPoint(0)
      var currentRingIndex = 0
      points.+=(Point(start.getX(), start.getY()))

      for (i <- (1 until length)) {
        val p = esriPolygon.getPoint(i)
        val j = esriPolygon.getPathEnd(currentRingIndex)
        if (j < length) {
          val end = esriPolygon.getPoint(j)
          if (p.getX == end.getX && p.getY == end.getY) {
            indices.+=(i)
            currentRingIndex += 1
            // add start point
            points.+= (Point(start.getX(), start.getY()))
            start = end
          }
        }
        points.+=(Point(p.getX(), p.getY()))
      }
      Polygon(indices.toArray, points.toArray)
    }
  }

  /**
   * Convert Magellan Polygon to ESRI Polygon.
   *
   * @param polygon
   * @return
   */
  def toESRI(polygon: Polygon): ESRIPolygon = {
    val p = new ESRIPolygon()
    val indices = polygon.getRings()
    val length = polygon.length
    if (length > 0) {
      var startIndex = 0
      var endIndex = 1
      var currentRingIndex = 0
      val startVertex = polygon.getVertex(startIndex)
      p.startPath(
        startVertex.getX(),
        startVertex.getY())

      while (endIndex < length) {
        val endVertex = polygon.getVertex(endIndex)
        p.lineTo(endVertex.getX(), endVertex.getY())
        startIndex += 1
        endIndex += 1
        // if we reach a ring boundary skip it
        val nextRingIndex = currentRingIndex + 1
        if (nextRingIndex < indices.length) {
          val nextRing = indices(nextRingIndex)
          if (endIndex == nextRing) {
            startIndex += 1
            endIndex += 1
            currentRingIndex = nextRingIndex
            val startVertex = polygon.getVertex(startIndex)
            p.startPath(
              startVertex.getX(),
              startVertex.getY())
          }
        }
      }
    }
    p
  }
}
