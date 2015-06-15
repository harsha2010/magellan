#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark.sql.types import UserDefinedType, StructField, StructType,\
    ArrayType, DoubleType, IntegerType

__all__ = ['Point', 'Polygon']


class PointUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: SpatialSDK Internal Use Only
    """

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        StructType([
            StructField("type", IntegerType(), False),
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True)])

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        "spatialsdk.types"

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        "org.apache.spatialsdk.PointUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, Point):
            return (1, obj.x, obj.y)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        assert len(datum) == 3, \
            "PointUDT.deserialize given row with length %d but requires 3" % len(datum)
        tpe = datum[0]
        assert tpe == 1, "Point should have type = 1"
        return Point(datum[1], datum[2])

    def simpleString(self):
        return 'point'


class Point(object):
    """
    A point is a zero dimensional shape.
    The coordinates of a point can be in linear units such as feet or meters,
    or they can be in angular units such as degrees or radians.
    The associated spatial reference specifies the units of the coordinates.
    In the case of a geographic coordinate system, the x-coordinate is the longitude
    and the y-coordinate is the latitude.

    >>> v = Point(1.0, 2.0)
    Point([1.0, 2.0])
    """
    def __init__(self, x=0.0, y=0.0):
        self.x = x
        self.y = y

    def toShapely(self):
        from shapely.geometry import Point as SPoint
        return SPoint(self.x, self.y)


class PolygonUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: SpatialSDK Internal Use Only
    """
    pointUDT = PointUDT()

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        StructType([
            StructField("type", IntegerType(), False),
            StructField("indices", ArrayType(IntegerType(), False), True),
            StructField("points", ArrayType(PointUDT(), False), True)])


    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        "spatialsdk.types"

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        "org.apache.spatialsdk.PolygonUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, Polygon):
            indices = [int(index) for index in obj.indices]
            points = [self.pointUDT.serialize(point) for point in obj.points]
            return (5, indices, points)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        assert len(datum) == 3, \
            "PolygonUDT.deserialize given row with length %d but requires 4" % len(datum)
        tpe = datum[0]
        assert tpe == 5, "Polygon should have type = 5"
        return Polygon(datum[1], [self.pointUDT.deserialize(point) for point in datum[2]])

    def simpleString(self):
        return 'polygon'

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "(" + ",".join((str(self.size), inds, pts)) + ")"

    def __getitem__(self, index):
        points = self.points
        if not isinstance(index, int):
            raise TypeError(
                "Indices must be of type integer, got type %s" % type(index))
        if index < 0:
            index += self.size
        if index >= self.size or index < 0:
            raise ValueError("Index %d out of bounds." % index)

        return points[index]


class Polygon(object):
    """
    A polygon consists of one or more rings. A ring is a connected sequence of four or more points
    that form a closed, non-self-intersecting loop. A polygon may contain multiple outer rings.
    The order of vertices or orientation for a ring indicates which side of the ring is the interior
    of the polygon. The neighborhood to the right of an observer walking along the ring
    in vertex order is the neighborhood inside the polygon.
    Vertices of rings defining holes in polygons are in a counterclockwise direction.
    Vertices for a single, ringed polygon are, therefore, always in clockwise order.
    The rings of a polygon are referred to as its parts.

    >>> v = Polygon([0], [Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 1.0))
    Point([-1.0,-1.0, 1.0, 1.0], [0], Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 1.0))
    """

    def __init__(self, indices, points):
        self.indices = indices
        self.points = points
        # compute the bounding box
        MAX_VALUE = sys.float_info.max
        MIN_VALUE = sys.float_info.min
        xmin = MAX_VALUE
        ymin = MAX_VALUE
        xmax = MIN_VALUE
        ymax = MIN_VALUE
        for point in points:
            x = point.x
            y = point.y
            if x < xmin:
                xmin = x
            if x > xmax:
                xmax = x
            if y < ymin:
                ymin = y
            if y > ymax:
                ymax = y

        self.box = (xmin, ymin, xmax, ymax)

    def toShapely(self):
        from shapely.geometry import Polygon as SPolygon
        from itertools import izip
        points = []
        l = []
        l.extend(self.indices)
        l.append(len(self.points))

        for i,j in zip(l, l[1:]):
            spoints = [(point.x, point.y) for point in self.points[i:j - 1]]
            points.append(spoints)

        shell = points[0]
        holes = points[1:]
        return SPolygon(shell=shell, holes=holes)



