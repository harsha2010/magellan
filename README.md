# Magellan: Geospatial Analytics Using Spark

[![Build Status](https://travis-ci.org/harsha2010/magellan.svg?branch=master)](https://travis-ci.org/harsha2010/magellan)
[![codecov.io](http://codecov.io/github/harsha2010/magellan/coverage.svg?branch=master)](http://codecov.io/github/harsha2010/magellan?branch=maste)


Geospatial data is pervasive, and spatial context is a very rich signal of user intent and relevance
in search and targeted advertising and an important variable in many predictive analytics applications.
For example when a user searches for “canyon hotels”, without location awareness the top result
or sponsored ads might be for hotels in the town “Canyon, TX”.
However, if they are are near the Grand Canyon, the top results or ads should be for nearby hotels.
Thus a search term combined with location context allows for much more relevant results and ads.
Similarly a variety of other predictive analytics problems can leverage location as a context.

To leverage spatial context in a predictive analytics application requires us to be able
to parse these datasets at scale, join them with target datasets that contain point in space information,
and answer geometrical queries efficiently.

Magellan is an open source library Geospatial Analytics using Spark as the underlying engine.
We leverage Catalyst’s pluggable optimizer to efficiently execute spatial joins, SparkSQL’s powerful operators to express geometric queries in a natural DSL, and Pyspark’s Python integration to provide Python bindings.

# Linking

You can link against this library using the following coordinates:

	groupId: org.apache
	artifactId: magellan
	version: 1.0.0

# Requirements

This library requires Spark 1.3+

# Capabilities

The library currently supports the [ESRI](https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf) format files.

We aim to support the full suite of [OpenGIS Simple Features for SQL ](http://www.opengeospatial.org/standards/sfs) spatial predicate functions and operators together with additional topological functions.

capabilities include:

**Geometries**: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection
	
**Predicates**: Intersects, Touches, Disjoint, Crosses, Within, Contains, Overlaps, Equals, Covers
	
**Operations**: Union, Distance, Intersection, Symmetric Difference, Convex Hull, Envelope, Buffer, Simplify, Valid, Area, Length
	
**Scala and Python API**


# Examples

## Reading Data

You can read data as follows:


	val df = sqlCtx.load("org.apache.magellan", path)
	df.show()
	
	+-----+--------+--------------------+--------------------+-----+
	|point|polyline|             polygon|            metadata|valid|
	+-----+--------+--------------------+--------------------+-----+
	| null|    null|Polygon(5, Vector...|Map(neighborho ->...| true|
	| null|    null|Polygon(5, Vector...|Map(neighborho ->...| true|
	| null|    null|Polygon(5, Vector...|Map(neighborho ->...| true|
	| null|    null|Polygon(5, Vector...|Map(neighborho ->...| true|
	+-----+--------+--------------------+--------------------+-----+
	
	df.select(df.metadata['neighborho']).show()
	
	+--------------------+
	|metadata[neighborho]|
	+--------------------+
	|Twin Peaks       ...|
	|Pacific Heights  ...|
	|Visitacion Valley...|
	|Potrero Hill     ...|
	+--------------------+
	


# Operations

## Expressions

### point

This is a convenience function for creating a point from two expressions, or two literals as the case may be.

	from magellan.types import Point
	from pyspark.sql import Row, SQLContext

	Record = Row("id", "point")
	df = sc.parallelize([(0, Point(1.0, 1.0)),
                         (1, Point(1.0, 2.0)),
                         (2, Point(-1.5, 1.5)),
                         (3, Point(3.5, 0.0))]) \
    .map(lambda x: Record(*x)).toDF()

	df.show()
	
	+---+----------+
	| id|     point|
	+---+----------+
	|  0| [1.0,1.0]|
	|  1| [1.0,2.0]|
	|  2|[-1.5,1.5]|
	|  3| [3.5,0.0]|
	+---+----------+

Similar UDFs exist for line, polygon, polyline etc.


## Predicates

### within

	
	PointRecord = Row("id", "point")
	pointdf = sc.parallelize([(0, Point(1.0, 1.0)),
                     		   (1, Point(1.0, 2.0)),
                     		   (2, Point(-1.5, 1.5)),
                     		   (3, Point(3.5, 0.0))]) \
    			.map(lambda x: PointRecord(*x)).toDF()
	
	PolygonRecord = Row("id", "polygon")
	polygondf = sc.parallelize([
					(0, Polygon([0], 	[Point(2.5, 2.5), Point(2.5, 2.75), Point(2.75, 2.75), Point(2.5, 2.5)])),
                    (1, Polygon([0], [Point(0.5, 0.5), Point(0.5, 0.75), Point(0.75, 0.75), Point(0.5, 0.5)])),
                    (2, Polygon([0], [Point(0.0, 0.0), Point(0.0, 1.0), Point(1.0, 1.0), Point(1.0, 0.0), Point(0.0, 0.0)]))]) \
   					 .map(lambda x: PolygonRecord(*x)).toDF()
    
	from magellan.column import within
	from pyspark.sql.functions import col
												pointdf.join(polygondf).where(col("point").within(col("polygon"))).show()
												
	+--+-----+--+-------+
	|id|point|id|polygon|
	+--+-----+--+-------+
	+--+-----+--+-------+

This agrees with our expectation: The point 1,1 is on the boundary of one of the polygons, but that is not the same as being within the polygon.


### intersects

		pointdf.join(polygondf).where(col("point").intersects(col("polygon"))).show()
		
	+--+-----------+--+--------------------+
	|id|      point|id|             polygon|
	+--+-----------+--+--------------------+
	| 0|[1,1.0,1.0]| 2|[5,ArrayBuffer(0)...|
	+--+-----------+--+--------------------+