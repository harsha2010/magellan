package magellan.encoders

import magellan.{Point, PolyLine, Polygon, TestSparkContext}
import magellan.encoders.Encoders._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class EncodersSuite extends FunSuite with TestSparkContext {

  test("point encoder") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._
    val points = sc.parallelize(Seq(
      Point(-1.0, -0.9),
      Point(1.1, -0.8),
      Point(1.2, 0.9),
      Point(-0.8, 0.9)
    )).toDS()

    assert(points.filter(_.getX() == -1.0).count() === 1)
  }

  test("polygon encoder") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._
    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))

    val ring2 = Array(Point(5.0, 5.0), Point(5.0, 4.0),
      Point(4.0, 4.0), Point(4.0, 5.0), Point(5.0, 5.0))

    val polygon1 = Polygon(Array(0),ring1)
    val polygon2 = Polygon(Array(0), ring2)

    val polygons = sc.parallelize(Seq(
      polygon1,
      polygon2)).toDS()

    assert(polygons.filter(_.contains(Point(0.0, 0.0))).count() === 1)
  }

  test("join") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._

    val ring1 = Array(Point(1.0, 1.0), Point(1.0, -1.0),
      Point(-1.0, -1.0), Point(-1.0, 1.0), Point(1.0, 1.0))

    val ring2 = Array(Point(5.0, 5.0), Point(5.0, 4.0),
      Point(4.0, 4.0), Point(4.0, 5.0), Point(5.0, 5.0))

    val polygon1 = Polygon(Array(0),ring1)
    val polygon2 = Polygon(Array(0), ring2)

    val polygons = sc.parallelize(Seq(
      polygon1,
      polygon2)).toDS()

    val points = sc.parallelize(Seq(
      Point(0.0, 0.0),
      Point(1.1, 1.1),
      Point(4.4, 4.4)
    )).toDS()

    val joined = points.join(polygons,
      points.col("type") within polygons.col("type"))

    assert(joined.count() === 2)
  }

  test("polyline encoder") {
    val sqlContext = this.sqlContext
    import sqlContext.implicits._

    val ring = Array(Point(-1.0, 1.0), Point(1.0, 1.0),
      Point(1.0, -1.0), Point(-1.0, -1.0))
    val polyline = PolyLine(Array(0), ring)
    val polylines = sc.parallelize(Seq(polyline)).toDS()

    assert(polylines.filter(_.getVertex(0) == Point(-1.0, 1.0)).count() === 1)
  }
}
