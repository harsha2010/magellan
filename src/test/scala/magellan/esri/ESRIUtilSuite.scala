package magellan.esri

import com.esri.core.geometry.{Point => ESRIPoint, Polygon => ESRIPolygon, Polyline => ESRIPolyline}
import org.scalatest.FunSuite
import magellan._
import magellan.TestingUtils._

class ESRIUtilSuite extends FunSuite {

  test("to esri-point") {
    val esriPoint = ESRIUtil.toESRI(Point(0.0, 1.0))
    assert(esriPoint.getX === 0.0)
    assert(esriPoint.getY === 1.0)
  }

  test("from esri-point") {
    val esriPoint = new ESRIPoint(0.0, 1.0)
    val point = ESRIUtil.fromESRI(esriPoint)
    assert(point.getX() == 0.0)
    assert(point.getY() == 1.0)
  }

  test("to esri-polygon") {
    // no hole
    var ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))
    var polygon = Polygon(Array(0), ring)
    var esriPolygon = ESRIUtil.toESRI(polygon)
    assert(esriPolygon.calculateRingArea2D(0) ~== 4.0 absTol 0.001)
    assert(esriPolygon.getPathCount === 1)
    assert(esriPolygon.getPoint(0).getX === 1.0)
    assert(esriPolygon.getPoint(0).getY === 1.0)
    assert(esriPolygon.getPoint(1).getX === 1.0)
    assert(esriPolygon.getPoint(1).getY === -1.0)
    assert(esriPolygon.getPoint(3).getX === -1.0)
    assert(esriPolygon.getPoint(3).getY === 1.0)
  }

  test("from esri-polygon") {
    val esriPolygon = new ESRIPolygon()
    // outer ring1
    esriPolygon.startPath(-200, -100)
    esriPolygon.lineTo(200, -100)
    esriPolygon.lineTo(200, 100)
    esriPolygon.lineTo(-190, 100)
    esriPolygon.lineTo(-190, 90)
    esriPolygon.lineTo(-200, 90)

    // hole
    esriPolygon.startPath(-100, 50)
    esriPolygon.lineTo(100, 50)
    esriPolygon.lineTo(100, -40)
    esriPolygon.lineTo(90, -40)
    esriPolygon.lineTo(90, -50)
    esriPolygon.lineTo(-100, -50)

    // island
    esriPolygon.startPath(-10, -10)
    esriPolygon.lineTo(10, -10)
    esriPolygon.lineTo(10, 10)
    esriPolygon.lineTo(-10, 10)

    esriPolygon.reverseAllPaths()

    val polygon = ESRIUtil.fromESRI(esriPolygon)
    assert(polygon.getRings() === Array(0, 6, 12))
    assert(polygon.getVertex(6) === Point(-200.0, -100.0))
    assert(polygon.getVertex(13) === Point(-100.0, 50.0))
  }

  test("to esri-polyline") {
    var ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0))
    var polyline = PolyLine(Array(0), ring)
    var esriPolyline = ESRIUtil.toESRI(polyline)
    assert(esriPolyline.getPoint(0).getX === 1.0)
    assert(esriPolyline.getPoint(0).getY === 1.0)
    assert(esriPolyline.getPoint(1).getX === 1.0)
    assert(esriPolyline.getPoint(1).getY === -1.0)
    assert(esriPolyline.getPoint(3).getX === -1.0)
    assert(esriPolyline.getPoint(3).getY === 1.0)
  }

  test("from esri-polyline") {
    val esriPolyline = new ESRIPolyline()
    // outer ring1
    esriPolyline.startPath(-200, -100)
    esriPolyline.lineTo(200, -100)
    esriPolyline.lineTo(200, 100)
    esriPolyline.lineTo(-190, 100)
    esriPolyline.lineTo(-190, 90)
    esriPolyline.lineTo(-200, 90)

    esriPolyline.startPath(-100, 50)
    esriPolyline.lineTo(100, 50)
    esriPolyline.lineTo(100, -40)
    esriPolyline.lineTo(90, -40)
    esriPolyline.lineTo(90, -50)
    esriPolyline.lineTo(-100, -50)


    esriPolyline.reverseAllPaths()

    val polyline = ESRIUtil.fromESRI(esriPolyline)
    assert(polyline.getRings() === Array(0, 6))
    assert(polyline.getVertex(2) === Point(-190.0, 100.0))
    assert(polyline.getVertex(7) === Point(-100.0, -50.0))
  }
}
