package magellan.catalyst

import magellan.{Point, Polygon, TestSparkContext}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.scalatest.FunSuite

class BufferSuite extends FunSuite with TestSparkContext {

  test("buffer point") {
    val sqlCtx = this.sqlContext
    import sqlCtx.implicits._
    val point = Point(0.0, 1.0)
    val points = Seq((1, point))
    val df = sc.parallelize(points).toDF("id", "point")
    val buffered = df.withColumn("buffered", $"point" buffer 0.5)
    val polygon = buffered.select($"buffered").take(1)(0).get(0).asInstanceOf[Polygon]
    assert(polygon.getNumRings() === 1)
    // check that [0.0, 0.75] is within this polygon
    assert(polygon.contains(Point(0.0, 0.75)))
    // check that [0.4, 1.0] is within this polygon
    assert(polygon.contains(Point(0.4, 1.0)))
    // check that [0.6, 1.0] is outside this polygon
    assert(!polygon.contains(Point(0.6, 1.0)))
  }
}
