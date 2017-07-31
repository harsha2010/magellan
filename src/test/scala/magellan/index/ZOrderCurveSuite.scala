/**
 * Copyright 2015 Ram Sriharsha
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package magellan.index

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import magellan.{BoundingBox, Point, Polygon, PolygonDeserializer}
import magellan.TestingUtils._
import org.scalatest.FunSuite

class ZOrderCurveSuite extends FunSuite {

  test("equals") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.4517249, 37.765315), 5)
    val anotherIndex = indexer.index(Point(-122.4517249, 37.765315), 5)

    assert(index === anotherIndex)
    assert(index !== indexer.index(Point(-122.4517249, 37.765315), 7))
    println(indexer.index(Point(-122.45, 37.76), 25).boundingBox)
    assert(indexer.index(Point(-122.45, 37.76), 25) !== indexer.index(Point(-122.45, 37.8), 25))
  }

  test("GeoHash globe") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.4517249, 37.765315), 5)
    assert(index.bits === (9L << 59))
    val child = index.children().filter(_.code() === "0100110")
    assert(!child.isEmpty)
    val c = child.head
    assert(c.bits === 5476377146882523136L)
  }

  test("GeoHash Point") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.3959313, 37.7912976), 25)
    assert(index.toBase32() === "9q8yy")
    assert(index.code() === "0100110110010001111011110")
    val BoundingBox(xmin, ymin, xmax, ymax) = index.boundingBox
    assert(xmin ~== -122.41 absTol 0.5)
    assert(xmax ~== -122.41 absTol 0.5)
    assert(ymin ~== 37.771 absTol 0.5)
    assert(ymax ~== 37.771 absTol 0.5)

    val anotherIndex = indexer.index(Point(116.6009234, 40.0798573), 30)
    assert(anotherIndex.toBase32() === "wx4uj2")
  }

  test("GeoHash children") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    val index = indexer.index(Point(-122.3959313, 37.7912976), 23)
    val children = index.children()
    assert(children.filter(_.toBase32() === "9q8yy").nonEmpty)
  }

  test("cover") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-4, -4, 4, 4))
    var cover = indexer.cover(BoundingBox(1.0, 1.0, 3.0, 3.0), 2)
    assert(cover.size === 1)
    val Seq(hash) = cover
    assert(hash.code() === "11")

    cover = indexer.cover(BoundingBox(1.0, 1.0, 3.0, 3.0), 4)
    assert(cover.size === 4)

    assert(cover.map(_.code()).sorted === Seq("1100", "1101", "1110", "1111"))

    cover = indexer.cover(BoundingBox(1.0, 1.0, 3.0, 3.0), 6)
    assert(cover.map(_.code).sorted === Seq("110011", "110110", "111001", "111100"))
  }

  test("index Polygon") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-4, -4, 4, 4))
    var ring = Array(Point(2.0, 1.0),
        Point(3.5, 1.0),
        Point(3.5, 1.5),
        Point(2.5, 1.5),
        Point(2.5, 2.5),
        Point(2, 2.5),
        Point(2.0, 1.0)
      )

    var polygon = Polygon(Array(0), ring)

    var hashes = indexer.index(polygon, 2)
    assert(hashes.map(_.code()) === Seq("11"))

    hashes = indexer.index(polygon, 4)
    assert(hashes.map(_.code()).sorted === Seq("1110", "1111"))

    hashes = indexer.index(polygon, 6)
    assert(hashes.map(_.code()).sorted === Seq("111001", "111011", "111100"))
  }

  test("geohash polygon") {
    val path = this.getClass.getClassLoader.getResource("testindex/testpolygon.json").getPath
    val mapper = new ObjectMapper()
    val module = new SimpleModule()
    module.addDeserializer(classOf[Polygon], new PolygonDeserializer())
    mapper.registerModule(module)
    val polygon: Polygon = mapper.readerFor(classOf[Polygon]).readValue(new File(path))
    val indexer = new ZOrderCurveIndexer(BoundingBox(-180, -90, 180, 90))
    var index = indexer.index(polygon, 20)
    assert(index.map(_.toBase32()).sorted === Seq("dr5n", "dr5q"))

    index = indexer.index(polygon, 25)
    assert(index.map(_.toBase32()).contains("dr5nx"))
  }

  test("index equality") {
    val indexer = new ZOrderCurveIndexer(BoundingBox(-4, -4, 4, 4))
    val index1 = indexer.index(Point(0.5, 0.5), 4)
    val index2 = indexer.index(Point(0.25, 0.25), 4)
    val index3 = indexer.index(Point(2.05, 2.05), 4)
    assert(index1 === index2)
    assert(index1.hashCode() === index2.hashCode())
    assert(index1 !== index3)
  }
}

