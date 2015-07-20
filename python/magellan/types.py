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

import sys

from shapely.geometry import Point as SPoint
from shapely.geometry import Polygon as SPolygon
from shapely.geometry import LineString, MultiLineString
from itertools import izip
from pyspark.sql.types import UserDefinedType, StructField, StructType, \
    ArrayType, DoubleType, IntegerType

__all__ = ['Point', 'Polygon']

class ShapelyGeometry(object):

    _shape_type = None

    def toShapely(self):
        raise NotImplementedError()

    def contains(self, other):
        return self.toShapely().contains(other.toShapely())

    def intersects(self, other):
        return self.toShapely().intersects(other.toShapely())

    def area(self):
        return self.toShapely().area()

    def distance(self, other):
        return self.toShapely().distance(other.toShapely())

    def convex_hull(self):
        return self.toShapely().convex_hull()


class PointUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: SpatialSDK Internal Use Only
    """

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        return StructType([
            StructField("type", IntegerType(), False),
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True)])

    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        return "magellan.types"

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        return "magellan.PointUDT"

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


class Point(ShapelyGeometry):
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
    def __init__(self, x, y):
        self._shape_type = 1
        self.x = x
        self.y = y

    def __str__(self):
        return "Point (" + str(self.x) + "," + str(self.y) + ")"

    def __repr__(self):
        return self.__str__()

    def __unicode__(self):
        return self.__str__()

    def __reduce__(self):
        return (Point, (self.x, self.y))

    def toShapely(self):
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
        return StructType([
            StructField("type", IntegerType(), False),
            StructField("indices", ArrayType(IntegerType(), False), True),
            StructField("points", ArrayType(PointUDT(), False), True)])


    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        return "magellan.types"

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        return "magellan.PolygonUDT"

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
        return "(" + ",".join((inds, pts)) + ")"

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


class Polygon(ShapelyGeometry):
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

    def __init__(self, indices = [], points = []):
        self._shape_type = 5
        self.indices = indices
        self.points = points

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "(" + ",".join((inds, pts)) + ")"

    def __repr__(self):
        return self.__str__()

    def toShapely(self):
        l = []
        l.extend(self.indices)
        l.append(len(self.points))
        p = []
        for i,j in zip(l, l[1:]):
            spoints = [(point.x, point.y) for point in self.points[i:j - 1]]
            p.append(spoints)

        shell = p[0]
        holes = p[1:]
        return SPolygon(shell=shell, holes=holes)


class PolyLineUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: SpatialSDK Internal Use Only
    """
    pointUDT = PointUDT()

    @classmethod
    def sqlType(cls):
        """
        Underlying SQL storage type for this UDT.
        """
        return StructType([
            StructField("type", IntegerType(), False),
            StructField("indices", ArrayType(IntegerType(), False), True),
            StructField("points", ArrayType(PointUDT(), False), True)])


    @classmethod
    def module(cls):
        """
        The Python module of the UDT.
        """
        return "magellan.types"

    @classmethod
    def scalaUDT(cls):
        """
        The class name of the paired Scala UDT.
        """
        return "magellan.PolyLineUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, PolyLine):
            indices = [int(index) for index in obj.indices]
            points = [self.pointUDT.serialize(point) for point in obj.points]
            return (3, indices, points)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        assert len(datum) == 3, \
            "PolyLineUDT.deserialize given row with length %d but requires 4" % len(datum)
        tpe = datum[0]
        assert tpe == 3, "PolyLine should have type = 5"
        return Polygon(datum[1], [self.pointUDT.deserialize(point) for point in datum[2]])

    def simpleString(self):
        return 'polyline'

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "(" + ",".join((inds, pts)) + ")"

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


class PolyLine(ShapelyGeometry):
    """
    A PolyLine is an ordered set of vertices that consists of one or more parts.
    A part is a connected sequence of two or more points.
    Parts may or may not be connected to one another.
    Parts may or may not intersect one another

    >>> v = PolyLine([0], [Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 0.0))
    Point([0], Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 0.0))
    """

    def __init__(self, indices = [], points = []):
        self._shape_type = 3
        self.indices = indices
        self.points = points

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "(" + ",".join((inds, pts)) + ")"

    def __repr__(self):
        return self.__str__()

    def toShapely(self):
        l = []
        l.extend(self.indices)
        l.append(len(self.points))
        p = []
        for i,j in zip(l, l[1:]):
            spoints = [(point.x, point.y) for point in self.points[i:j - 1]]
            p.append(LineString(spoints))
        return MultiLineString(p)

