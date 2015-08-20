#
# Copyright 2015 Ram Sriharsha
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import math
from pyspark import SparkContext
from magellan.types import Point

__all__ = ["NAD83"]

class System(object):

    def frm(self):
        raise NotImplementedError()

    def to(self):
        raise NotImplementedError()


class NAD83(System):

    RAD = 180 / math.pi
    ER  = float(6378137)  # semi-major axis for GRS-80
    RF  = 298.257222101  # reciprocal flattening for GRS-80
    F   = float(1) / RF  # flattening for GRS-80
    ESQ = F + F - (F * F)
    E   = math.sqrt(ESQ)

    ZONES =  {
        401 : [float(122), 2000000.0001016,500000.0001016001, 40.0,
               41.66666666666667, 39.33333333333333],
        403 : [float(120.5), 2000000.0001016,500000.0001016001, 37.06666666666667,
               38.43333333333333, 36.5]}

    def __init__(self, params):
        self.params = NAD83.ZONES[params['zone']]


    def frm(self):
        return self.to_lambert_conic()

    def qqq(self, e, s):
        return (math.log((1 + s) / (1 - s)) - e *
         math.log((1 + e * s) / (1 - e * s))) / 2

    def extract_point(self, p):
        if isinstance(p, Point):
            return p

        assert p[0] == 1, "Point should have type = 1"
        return Point(p[1], p[2])

    def to_lambert_conic(self):
        cm = self.params[0] / NAD83.RAD  # CENTRAL MERIDIAN (CM)
        eo = self.params[1]  # FALSE EASTING VALUE AT THE CM (METERS)
        nb = self.params[2]  # FALSE NORTHING VALUE AT SOUTHERMOST PARALLEL (METERS), (USUALLY ZERO)
        fis = self.params[3] / NAD83.RAD  # LATITUDE OF SO. STD. PARALLEL
        fin = self.params[4] / NAD83.RAD  # LATITUDE OF NO. STD. PARALLEL
        fib = self.params[5] / NAD83.RAD # LATITUDE OF SOUTHERNMOST PARALLEL
        sinfs = math.sin(fis)
        cosfs = math.cos(fis)
        sinfn = math.sin(fin)
        cosfn = math.cos(fin)
        sinfb = math.sin(fib)
        qs = self.qqq(NAD83.E, sinfs)
        qn = self.qqq(NAD83.E, sinfn)
        qb = self.qqq(NAD83.E, sinfb)
        w1 = math.sqrt(1 - NAD83.ESQ * sinfs * sinfs)
        w2 = math.sqrt(1 - NAD83.ESQ * sinfn * sinfn)
        sinfo = math.log(w2 * cosfs / (w1 * cosfn)) / (qn - qs)
        k = NAD83.ER * cosfs * math.exp(qs * sinfo) / (w1 * sinfo)
        rb = k / math.exp(qb * sinfo)

        def _(point):
            # TODO: Fix BUG in Pyspark Serialization code that returns tuples here instead of Points
            long = point.x
            lat = point.y
            l = - long / NAD83.RAD
            f = lat / NAD83.RAD
            q = self.qqq(NAD83.E, math.sin(f))
            r = k / math.exp(q * sinfo)
            gam = (cm - l) * sinfo
            n = rb + nb - (r * math.cos(gam))
            e = eo + (r * math.sin(gam))
            return Point(e, n)

        return _

