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

import json
import sys

from itertools import izip, repeat

from pyspark import SparkContext
from pyspark.sql.types import DataType, UserDefinedType, Row, StructField, StructType, \
    ArrayType, DoubleType, IntegerType

__all__ = ['Point']

try:
    from shapely.geometry import Point as SPoint
    from shapely.geometry import Polygon as SPolygon
    from shapely.geometry import LineString, MultiLineString
    _have_shapely = True
except:
    # No Shapely in environment, but that's okay
    _have_shapely = False


class Shape(DataType):

    def convert(self):
        raise NotImplementedError()

    def toShapely(self):
        if _have_shapely:
            return self.convert()
        else:
            raise TypeError("Cannot convert to Shapely type")


class PointUDT(UserDefinedType):
    """User-defined type (UDT).

    .. note:: WARN: SpatialSDK Internal Use Only
    """

    @classmethod
    def sqlType(cls):
        return Point()

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
        return "org.apache.spark.sql.types.PointUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, Point):
            #pnt = Row(IntegerType(), DoubleType(),DoubleType(),DoubleType(),DoubleType(),DoubleType(),DoubleType() )
            #return pnt("udt",obj.x,obj.y,obj.x,obj.y,obj.x,obj.y)
            return 1,obj.x, obj.y,obj.x, obj.y,obj.x, obj.y
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        if isinstance(datum, Point):
            return datum
        else:
            assert len(datum) == 7, \
                "PointUDT.deserialize given row with length %d but requires 7" % len(datum)
            return Point(datum[5], datum[6])

    def simpleString(self):
        return 'point'

    @classmethod
    def fromJson(cls, json):
        return Point(json['x'], json['y'])


class Point(Shape):
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

    __UDT__ = PointUDT()

    def __init__(self, x = 0.0, y = 0.0):
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

    def __eq__(self, other):
        return isinstance(other, Point) and self.x == other.x and self.y == other.y

    @classmethod
    def fromJson(cls, json):
        return Point(json['x'], json['y'])

    def jsonValue(self):
        return {"type": "udt",
                "pyClass": "magellan.types.PointUDT",
                "class": "magellan.PointUDT",
                "sqlType": "magellan.Point"}

    def convert(self):
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
        return Polygon()

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
        return "org.apache.spark.sql.types.PolygonUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, Polygon):
            x_list = []
            y_list = []
            for p in obj.points:
                x_list.append(p.x)
                y_list.append(p.y)
            return 5, min(x_list), min(y_list), max(x_list), max(y_list), obj.indices, x_list, y_list
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        if isinstance(datum, Polygon):
            return datum
        else:
            assert len(datum) == 8, \
                "PolygonUDT.deserialize given row with length %d but requires 2" % len(datum)
            return Polygon(datum[5], [self.pointUDT.deserialize(point) for point in zip(repeat(1), datum[6], datum[7], datum[6], datum[7], datum[6], datum[7])])

    def simpleString(self):
        return 'polygon'

    @classmethod
    def fromJson(cls, json):
        indices = json["indices"]
        points = [PointUDT.fromJson(point) for point in json["points"]]
        return Polygon(indices, points)


class Polygon(Shape):
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

    __UDT__ = PolygonUDT()

    def __init__(self, indices = [], points = []):
        self.indices = indices
        self.xcoordinates = [p.x for p in points]
        self.ycoordinates = [p.y for p in points]
        if points:
            self.xmin = min(self.xcoordinates)
            self.ymin = min(self.ycoordinates)
            self.xmax = max(self.xcoordinates)
            self.ymax = max(self.ycoordinates)
        else:
            self.xmin = None
            self.ymin = None
            self.xmax = None
            self.ymax = None
        self.boundingBox = BoundingBox(self.xmin, self.ymin, self.xmax, self.ymax)
        self.size = len(points)
        self.points = points

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "Polygon (" + ",".join((inds, pts)) + ")"

    def __repr__(self):
        return self.__str__()

    def __reduce__(self):
        return (Polygon, (self.indices, self.points))

    @classmethod
    def fromJson(cls, json):
        indices = json["indices"]
        points = [PointUDT.fromJson(point) for point in json["points"]]
        return Polygon(indices, points)

    def jsonValue(self):
        return {"type": "udt",
                "pyClass": "magellan.types.PolygonUDT",
                "class": "magellan.Polygon",
                "sqlType": "magellan.Polygon"}

    def convert(self):
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
        return PolyLine()

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
        return "org.apache.spark.sql.types.PolyLineUDT"

    def serialize(self, obj):
        """
        Converts the a user-type object into a SQL datum.
        """
        if isinstance(obj, PolyLine):
            x_list = []
            y_list = []
            for p in obj.points:
                x_list.append(p.x)
                y_list.append(p.y)
            return 3, min(x_list), min(y_list), max(x_list), max(y_list), obj.indices, x_list, y_list
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(self, datum):
        """
        Converts a SQL datum into a user-type object.
        """
        if isinstance(datum, PolyLine):
            return datum
        else:
            assert len(datum) == 2, \
                "PolyLineUDT.deserialize given row with length %d but requires 2" % len(datum)
            return PolyLine(datum[0], [self.pointUDT.deserialize(point) for point in datum[1]])

    def simpleString(self):
        return 'polyline'

    @classmethod
    def fromJson(cls, json):
        indices = json["indices"]
        points = [PointUDT.fromJson(point) for point in json["points"]]
        return PolyLine(indices, points)


class PolyLine(Shape):
    """
    A PolyLine is an ordered set of vertices that consists of one or more parts.
    A part is a connected sequence of two or more points.
    Parts may or may not be connected to one another.
    Parts may or may not intersect one another
    >>> v = PolyLine([0], [Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 0.0))
    Point([0], Point(1.0, 1.0), Point(1.0, -1.0), Point(1.0, 0.0))
    """

    __UDT__ = PolyLineUDT()

    def __init__(self, indices = [], points = []):
        self.indices = indices
        self.xcoordinates = [p.x for p in points]
        self.ycoordinates = [p.y for p in points]
        if points:
            self.xmin = min(self.xcoordinates)
            self.ymin = min(self.ycoordinates)
            self.xmax = max(self.xcoordinates)
            self.ymax = max(self.ycoordinates)
        else:
            self.xmin = None
            self.ymin = None
            self.xmax = None
            self.ymax = None
        self.boundingBox = BoundingBox(self.xmin, self.ymin, self.xmax, self.ymax)
        self.size = len(points)
        self.points = points

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        pts = "[" + ",".join([str(v) for v in self.points]) + "]"
        return "Polygon (" + ",".join((inds, pts)) + ")"

    def __repr__(self):
        return self.__str__()

    def __reduce__(self):
        return (PolyLine, (self.indices, self.points))

    @classmethod
    def fromJson(cls, json):
        indices = json["indices"]
        points = [PointUDT.fromJson(point) for point in json["points"]]
        return PolyLine(indices, points)

    def jsonValue(self):
        return {"type": "udt",
                "pyClass": "magellan.types.PolyLineUDT",
                "class": "magellan.PolyLine",
                "sqlType": "magellan.PolyLine"}

    def convert(self):
        l = []
        l.extend(self.indices)
        l.append(len(self.points))
        p = []
        for i,j in zip(l, l[1:]):
            spoints = [(point.x, point.y) for point in self.points[i:j - 1]]
            p.append(LineString(spoints))
        return MultiLineString(p)


def _inbound_shape_converter(json_string):
    j = json.loads(json_string)
    shapeType = str(j["pyClass"])  # convert unicode to str
    split = shapeType.rfind(".")
    module = shapeType[:split]
    shapeClass = shapeType[split+1:]
    m = __import__(module, globals(), locals(), [shapeClass])
    UDT = getattr(m, shapeClass)
    return UDT.fromJson(j)

# This is used to unpickle a Row from JVM
def _create_row_inbound_converter(dataType):
    return lambda *a: dataType.fromInternal(a)

class BoundingBox(object):

    def __init__(self,xmin,ymin,xmax,ymax):
        self.xmin = xmin
        self.ymin = ymin
        self.xmax = xmax
        self.ymax = ymax

    def intersects(self, other):
        if not other.xmin >= self.xmax and other.ymax >= self.min and other.ymax <= self.ymin and other.xmax <= self.xmin:
            return True
        else:
            return False