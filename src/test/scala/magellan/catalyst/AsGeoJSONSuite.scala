package magellan.catalyst

import magellan.{Geometry, Point, TestSparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.magellan.dsl.expressions._
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.FunSuite

class AsGeoJSONSuite extends FunSuite with TestSparkContext {

  test("as geojson") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val point = Point(35.7, -122.3)
    val points = Seq((1, point))
    val df = sc.parallelize(points).toDF("id", "point")
    val withJson = df.withColumn("json", $"point" asGeoJSON)
    val json = withJson.select("json").map { case Row(s: String) =>  s}.take(1)(0)
    implicit val formats = org.json4s.DefaultFormats
    val result = parse(json).extract[Geometry]
    val shapes = result.shapes
    // expect a single point
    assert(shapes.size == 1)
    assert(shapes.head.asInstanceOf[Point] === point)
  }
}
