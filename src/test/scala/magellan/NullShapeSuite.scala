package magellan

import org.apache.spark.sql.types.NullShapeUDT
import org.codehaus.jackson.map.ObjectMapper
import org.scalatest.FunSuite

class NullShapeSuite extends FunSuite with TestSparkContext {

  test("serialization") {
    val nullShape = NullShape
    val nullShapeUDT = new NullShapeUDT()
    val row = nullShapeUDT.serialize(nullShape)
    assert(row.numFields == 1)
    assert(row.getInt(0) == nullShape.getType())
    val deserializedNullShape = nullShapeUDT.deserialize(row)
    assert(nullShape.equals(deserializedNullShape))
  }

  test("nullshape udf") {
    val sQLContext = this.sqlContext
    import sQLContext.implicits._

  }

  test("jackson serialization") {
    val s = new ObjectMapper().writeValueAsString(NullShape)
    assert(s.contains("type"))
  }

}
