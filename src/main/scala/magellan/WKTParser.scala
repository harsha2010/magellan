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

package magellan

import fastparse.all._
import fastparse.core.Parsed.{Failure, Success}

import scala.collection.mutable.ListBuffer

object WKTParser {

  def whitespace: P[String] = P(" ") map {
    _.toString
  }

  val posInt: P[String] = P(CharIn('0' to '9').rep(1).!)

  val negInt: P[String] = P("-" ~ posInt) map {
    "-" + _
  }

  val int: P[String] = P(posInt | negInt)

  val float: P[String] = P(int ~ P(".") ~ posInt) map { case (x, y) => (x + "." + y) }

  val number = P(float | int) map {
    _.toDouble
  }

  def multi0: P[String] = P("""MULTI""") map {
    _.toString
  }

  def point0: P[String] = P("""POINT""") map {
    _.toString
  }

  def empty0: P[String] = P("""EMPTY""") map {
    _.toString
  }

  def comma: P[String] = P(",") map {
    _.toString
  }

  def leftBrace: P[String] = P("(") map {
    _.toString
  }

  def rightBrace: P[String] = P(")") map {
    _.toString
  }

  def coords: P[Point] = P(number ~ whitespace ~ number) map {
    case (x, _, y) => Point(x, y)
  }

  def ring: P[Array[Point]] = P(leftBrace ~ coords.rep(1, (comma ~ whitespace | comma)) ~ rightBrace) map {
    case (_, x, _) => x.toArray
  }

  def pointCoords: P[Point] = P(leftBrace ~ coords ~ rightBrace) map {
    case (_, x, _) => x
  }

  def point: P[Point] = P(point0 ~ whitespace.? ~ pointCoords) map {
    case (_, _, p) => p
  }

  def multipoint: P[Array[Point]] = P(multi0 ~ point0 ~ whitespace.? ~ leftBrace ~ (pointCoords.rep(1, (comma ~ whitespace | comma)) | coords.rep(1, (comma ~ whitespace | comma))) ~ rightBrace) map {
    case (_, _, _, _, p, _) => p.toArray
  }

  def pointEmpty: P[Shape] = P(point0 ~ whitespace ~ empty0) map { _ => NullShape }

  def linestring0: P[String] = P("""LINESTRING""") map {
    _.toString
  }

  def linestring: P[PolyLine] = P(linestring0 ~ whitespace.? ~ ring) map {
    case (_, _, x) => PolyLine(Array(0), x)
  }

  def multilinestring: P[Array[PolyLine]] = P(multi0 ~ linestring0 ~ whitespace.? ~ leftBrace ~ ring.rep(1, (comma ~ whitespace | comma)) ~ rightBrace) map {
    case (_, _, _, _, p, _) => p.map(points => PolyLine(Array(0), points)).toArray
  }

  def polygon0: P[String] = P("""POLYGON""") map {
    _.toString
  }

  def polygonWithoutHoles: P[Polygon] = polygon


  def polygonWithHoles: P[Polygon] = polygon

  def polygon: P[Polygon] =
    P(polygon0 ~ whitespace.? ~ polygonCoords) map {
      case (_, _, x) => x
    }

  def polygonCoords: P[Polygon] =
    P(P("(") ~ ring.rep(1, (comma ~ whitespace | comma)) ~ P(")")) map {
      case (x) =>
        val indices = ListBuffer[Int]()
        val points = ListBuffer[Point]()
        var prev = 0
        var i = 0
        val numRings = x.size
        while (i < numRings) {
          indices.+=(prev)
          prev += x(i).length
          points.++=(x(i))
          i += 1
        }
        Polygon(indices.toArray, points.toArray)
    }


  def multipolygon: P[Array[Polygon]] = P(multi0 ~ polygon0 ~ whitespace.? ~ leftBrace ~ polygonCoords.rep(1, (comma ~ whitespace | comma)) ~ rightBrace) map {
    case (_, _, _, _, p, _) => p.toArray
  }

  def expr: P[Shape] = P(point | pointEmpty | linestring | polygon ~ End)

  def singleShapeArray: P[Array[Shape]] = P(point | pointEmpty | linestring | polygon) map {
    case (p) => Array(p)
  }


  def exprArray: P[Array[_ <: Shape]] = P(singleShapeArray | multipoint | multilinestring | multipolygon ~ End)

  def parseAll(text: String): Shape = {
    expr.parse(text) match {
      case Success(value, _) => value
      case Failure(parser, index, stack) => throw new RuntimeException(stack.toString)
    }
  }

  def parseAllArray(text: String): Array[_ <: Shape] = {
    exprArray.parse(text) match {
      case Success(value, _) => value
      case Failure(parser, index, stack) => throw new RuntimeException(stack.toString)
    }
  }

}
