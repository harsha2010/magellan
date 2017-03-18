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

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.combinator.RegexParsers

object WKTParser extends RegexParsers {

  override def skipWhitespace = false

  def whitespace: Parser[String] = " "

  def int: Parser[Int] = """[1-9][0-9]*""".r^^{_.toInt}

  def float: Parser[Double] = """-?(\d+(\.\d*)?|\d*\.\d+)([eE][+-]?\d+)?[fFdD]?""".r^^ (_.toDouble)

  def point0: Parser[String] = """POINT"""

  def empty0: Parser[String] = """EMPTY"""

  def comma: Parser[String] = ","

  def coords: Parser[Point] =  float ~ whitespace ~ float ^^ {
    case x ~ " " ~ y => Point(x.asInstanceOf[Double], y.asInstanceOf[Double])
  }

  def ring: Parser[Array[Point]] = "(" ~ repsep(coords, (comma~whiteSpace | comma)) ~ ")" ^^ {
    case "(" ~ x ~ ")" => x.toArray
  }

  def point = point0 ~ whitespace ~ "(" ~ coords ~ ")" ^^ {
    case _ ~ " " ~ "(" ~ p ~ ")" => p
  }

  def pointEmpty = point0 ~ whitespace ~ empty0 ^^ {_ => NullShape}

  def linestring0: Parser[String] = """LINESTRING"""

  def linestring = linestring0 ~ whitespace ~ ring ^^ {
    case _ ~ " " ~ x => PolyLine(Array(0), x)
  }

  def polygon0: Parser[String] = """POLYGON"""

  def polygonWithoutHoles = polygon0 ~ whitespace ~ "((" ~ repsep(coords, (comma~whiteSpace | comma)) ~ "))" ^^ {
    case _ ~ " " ~ "((" ~ x ~ "))" => Polygon(Array(0), x.toArray)
  }

  def polygonWithHoles = polygon0 ~ whitespace ~ "(" ~ repsep(ring, (comma~whiteSpace | comma)) ~ ")" ^^ {
    case _ ~ " " ~ "(" ~ x ~ ")" =>
      val indices = ListBuffer[Int]()
      val points = ListBuffer[Point]()
      var prev = 0
      var i = 0
      val numRings = x.size
      while (i < numRings) {
        indices.+= (prev)
        prev += x(i).length
        points.++=(x(i))
        i += 1
      }
      Polygon(indices.toArray, points.toArray)
  }

  def expr: Parser[Shape] = point | pointEmpty | linestring | polygonWithoutHoles | polygonWithHoles

}
