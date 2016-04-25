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

package magellan.coord

import magellan.Point

/**
 * Abstraction for a coordinate system.
 */
trait System extends Serializable {

  /**
   * Returns a transformer from `LongLat` to this system.
   * @return
   */
  def from(): Point => Point

  /**
   * Returns a transformer to `LongLat` from this system.
   * @return
   */
  def to(): Point => Point

}

/**
 * Default System for spatial analytics is the longlat system.
 * The x-coordinate represents the longitude, and the y-coordinate
 * represents the latitude in decimal notation.
 */
case object LongLat extends System {

  override def from() = identity

  override def to() = identity

}

class NAD83(params: Map[String, Any]) extends System {

  val RAD = 180d / Math.PI
  val ER  = 6378137.toDouble  // semi-major axis for GRS-80
  val RF  = 298.257222101  // reciprocal flattening for GRS-80
  val F   = 1.toDouble / RF  // flattening for GRS-80
  val ESQ = F + F - (F * F)
  val E   = StrictMath.sqrt(ESQ)

  private val ZONES =  Map(
    401 -> Array(122.toDouble, 2000000.0001016,
      500000.0001016001, 40.0,
      41.66666666666667, 39.33333333333333),
    403 -> Array(120.5, 2000000.0001016,
      500000.0001016001, 37.06666666666667,
      38.43333333333333, 36.5)
  )

  override def from() = {
    val zone = params("zone").asInstanceOf[Int]
    ZONES.get(zone) match {
      case Some(x) => if (x.length == 5) {
        toTransverseMercator(x)
      } else {
        toLambertConic(x)
      }
      case None => ???
    }
  }

  override def to() = {
    val zone = params("zone").asInstanceOf[Int]
    ZONES.get(zone) match {
      case Some(x) => if (x.length == 5) {
        fromTransverseMercator(x)
      } else {
        fromLambertConic(x)
      }
      case None => ???
    }
  }

  def qqq(e: Double, s: Double) = {
    (StrictMath.log((1 + s) / (1 - s)) - e *
      StrictMath.log((1 + e * s) / (1 - e * s))) / 2
  }

  def toLambertConic(params: Array[Double]) = {
    val cm = params(0) / RAD  // CENTRAL MERIDIAN (CM)
    val eo = params(1)  // FALSE EASTING VALUE AT THE CM (METERS)
    val nb = params(2)  // FALSE NORTHING VALUE AT SOUTHERMOST PARALLEL (METERS), (USUALLY ZERO)
    val fis = params(3) / RAD  // LATITUDE OF SO. STD. PARALLEL
    val fin = params(4) / RAD  // LATITUDE OF NO. STD. PARALLEL
    val fib = params(5) / RAD // LATITUDE OF SOUTHERNMOST PARALLEL
    val sinfs = StrictMath.sin(fis)
    val cosfs = StrictMath.cos(fis)
    val sinfn = StrictMath.sin(fin)
    val cosfn = StrictMath.cos(fin)
    val sinfb = StrictMath.sin(fib)
    val qs = qqq(E, sinfs)
    val qn = qqq(E, sinfn)
    val qb = qqq(E, sinfb)
    val w1 = StrictMath.sqrt(1.toDouble - ESQ * sinfs * sinfs)
    val w2 = StrictMath.sqrt(1.toDouble - ESQ * sinfn * sinfn)
    val sinfo = StrictMath.log(w2 * cosfs / (w1 * cosfn)) / (qn - qs)
    val k = ER * cosfs * StrictMath.exp(qs * sinfo) / (w1 * sinfo)
    val rb = k / StrictMath.exp(qb * sinfo)

    (point: Point) => {
      val (long, lat) = (point.getX(), point.getY())
      val l = - long / RAD
      val f = lat / RAD
      val q = qqq(E, StrictMath.sin(f))
      val r = k / StrictMath.exp(q * sinfo)
      val gam = (cm - l) * sinfo
      val n = rb + nb - (r * StrictMath.cos(gam))
      val e = eo + (r * StrictMath.sin(gam))
      Point(e, n)
    }
  }

  def toTransverseMercator(params: Array[Double]) = {
    (point: Point) => {
      point
    }
  }

  def fromLambertConic(params: Array[Double]) = {
    val cm = params(0) / RAD  // CENTRAL MERIDIAN (CM)
    val eo = params(1)  // FALSE EASTING VALUE AT THE CM (METERS)
    val nb = params(2)  // FALSE NORTHING VALUE AT SOUTHERMOST PARALLEL (METERS), (USUALLY ZERO)
    val fis = params(3) / RAD  // LATITUDE OF SO. STD. PARALLEL
    val fin = params(4) / RAD  // LATITUDE OF NO. STD. PARALLEL
    val fib = params(5) / RAD // LATITUDE OF SOUTHERNMOST PARALLEL
    val sinfs = StrictMath.sin(fis)
    val cosfs = StrictMath.cos(fis)
    val sinfn = StrictMath.sin(fin)
    val cosfn = StrictMath.cos(fin)
    val sinfb = StrictMath.sin(fib)

    val qs = qqq(E, sinfs)
    val qn = qqq(E, sinfn)
    val qb = qqq(E, sinfb)
    val w1 = StrictMath.sqrt(1.toDouble - ESQ * sinfs * sinfs)
    val w2 = StrictMath.sqrt(1.toDouble - ESQ * sinfn * sinfn)
    val sinfo = StrictMath.log(w2 * cosfs / (w1 * cosfn)) / (qn - qs)
    val k = ER * cosfs * StrictMath.exp(qs * sinfo) / (w1 * sinfo)
    val rb = k / StrictMath.exp(qb * sinfo)
    (point: Point) => {
      val easting = point.getX()
      val northing = point.getY()
      val npr = rb - northing + nb
      val epr = easting - eo
      val gam = StrictMath.atan(epr / npr)
      val lon = cm - (gam / sinfo)
      val rpt = StrictMath.sqrt(npr * npr + epr * epr)
      val q = StrictMath.log(k / rpt) / sinfo
      val temp = StrictMath.exp(q + q)
      var sine = (temp - 1.toDouble) / (temp + 1.toDouble)
      var f1, f2 = 0.0
      for (i <- 0 until 2) {
        f1 = ((StrictMath.log((1.toDouble + sine) / (1.toDouble - sine)) - E *
          StrictMath.log((1.toDouble + E * sine) / (1.toDouble - E * sine))) / 2.toDouble) - q
        f2 = 1.toDouble / (1.toDouble - sine * sine) - ESQ / (1.toDouble - ESQ * sine * sine)
        sine -= (f1/ f2)
      }
      Point(StrictMath.toDegrees(lon) * -1, StrictMath.toDegrees(StrictMath.asin(sine)))
    }
  }

  def fromTransverseMercator(params: Array[Double]) = {
    val cm = params(0)  // CENTRAL MERIDIAN (CM)
    val fe = params(1)  // FALSE EASTING VALUE AT THE CM (METERS)
    val or = params(2) / RAD  // origin latitude
    val sf = 1.0 - (1.0 / params(3)) // scale factor
    val fn = params(4)  // false northing
    // translated from TCONPC subroutine
    val eps = ESQ / (1.0 - ESQ)
    val pr = (1.0 - F) * ER
    val en = (ER - pr) / (ER + pr)
    val en2 = en * en
    val en3 = en * en * en
    val en4 = en2 * en2

    var c2 = -3.0 * en / 2.0 + 9.0 * en3 / 16.0
    var c4 = 15.0d * en2 / 16.0d - 15.0d * en4 /32.0
    var c6 = -35.0 * en3 / 48.0
    var c8 = 315.0 * en4 / 512.0
    val u0 = 2.0 * (c2 - 2.0 * c4 + 3.0 * c6 - 4.0 * c8)
    val u2 = 8.0 * (c4 - 4.0 * c6 + 10.0 * c8)
    val u4 = 32.0 * (c6 - 6.0 * c8)
    val u6 = 129.0 * c8

    c2 = 3.0 * en / 2.0 - 27.0 * en3 / 32.0
    c4 = 21.0 * en2 / 16.0 - 55.0 * en4 / 32.0d
    c6 = 151.0 * en3 / 96.0
    c8 = 1097.0d * en4 / 512.0
    val v0 = 2.0 * (c2 - 2.0 * c4 + 3.0 * c6 - 4.0 * c8)
    val v2 = 8.0 * (c4 - 4.0 * c6 + 10.0 * c8)
    val v4 = 32.0 * (c6 - 6.0 * c8)
    val v6 = 128.0 * c8

    val r = ER * (1.0 - en) * (1.0 - en * en) * (1.0 + 2.25 * en * en + (225.0 / 64.0) * en4)
    val cosor = StrictMath.cos(or)
    val omo = or + StrictMath.sin(or) * cosor *
      (u0 + u2 * cosor * cosor + u4 * StrictMath.pow(cosor, 4) + u6 * StrictMath.pow(cosor, 6))
    val so = sf * r * omo

    (point: Point) => {
      val easting = point.getX()
      val northing = point.getY()
      // translated from TMGEOD subroutine
      val om = (northing - fn + so) / (r * sf)
      val cosom = StrictMath.cos(om)
      val foot = om + StrictMath.sin(om) * cosom *
        (v0 + v2 * cosom * cosom + v4 * StrictMath.pow(cosom, 4) + v6 * StrictMath.pow(cosom, 6))
      val sinf = StrictMath.sin(foot)
      val cosf = StrictMath.cos(foot)
      val tn = sinf / cosf
      val ts = tn * tn
      val ets = eps * cosf * cosf
      val rn = ER * sf / StrictMath.sqrt(1.0 - ESQ * sinf * sinf)
      val q = (easting - fe) / rn
      val qs = q * q
      val b2 = -tn * (1.0 + ets) / 2.0
      val b4 = -(5.0 + 3.0 * ts + ets * (1.0 - 9.0 * ts) - 4.0 * ets * ets) / 12.0
      val b6 = (61.0 + 45.0 * ts * (2.0 + ts) + ets * (46.0 - 252.0 * ts -60.0 * ts * ts)) / 360.0
      val b1 = 1.0
      val b3 = -(1.0 + ts + ts + ets) / 6.0
      val b5 = (5.0 + ts * (28.0 + 24.0 * ts) + ets * (6.0 + 8.0 * ts)) / 120.0
      val b7 = -(61.0 + 662.0 * ts + 1320.0 * ts * ts + 720.0 * StrictMath.pow(ts, 3)) / 5040.0
      val lat = foot + b2 * qs * (1.0 + qs * (b4 + b6 * qs))
      val l = b1 * q * (1.0 + qs * (b3 + qs * (b5 + b7 * qs)))
      val lon = -l / cosf + cm
      Point(StrictMath.toDegrees(lon) * -1, StrictMath.toDegrees(lat))
    }
  }
}
