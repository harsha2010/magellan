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

	groupId: harsha2010
	artifactId: magellan
	version: 1.0.3-s_2.10

# Requirements

This library requires Spark 1.4+

# Capabilities

The library currently supports the [ESRI](https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf) format files as well as [GeoJSON](http://geojson.org).

We aim to support the full suite of [OpenGIS Simple Features for SQL ](http://www.opengeospatial.org/standards/sfs) spatial predicate functions and operators together with additional topological functions.

capabilities we aim to support include (ones currently available are highlighted):

**Geometries**: **Point**, **LineString**, **Polygon**, **MultiPoint**, MultiLineString, MultiPolygon, GeometryCollection
	
**Predicates**: **Intersects**, Touches, Disjoint, Crosses, **Within**, **Contains**, Overlaps, Equals, Covers
	
**Operations**: Union, Distance, **Intersection**, Symmetric Difference, Convex Hull, Envelope, Buffer, Simplify, Valid, Area, Length
	
**Scala and Python API**



# Reading Data

You can read Shapefile formatted data as follows:


	val df = sqlCtx.read.
	  format("magellan").
	  load(path)
	  
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
	

To read GeoJSON format pass in the type as geojson during load as follows:

	val df = sqlCtx.read.
	  format("magellan").
	  option("type", "geojson").
	  load(path)
	  

# Scala API

Magellan is hosted on [Spark Packages](http://spark-packages.org/package/harsha2010/magellan)

When launching the Spark Shell, Magellan can be included like any other spark package using the --packages option:

	> $SPARK_HOME/bin/spark-shell --packages harsha2010:magellan:1.0.2-s_2.10

A few common packages you might want to import within Magellan
	
	import magellan.{Point, Polygon}
	import org.apache.spark.sql.magellan.dsl.expressions._
	import org.apache.spark.sql.types._

## Data Structures

### Point

	val points = sc.parallelize(Seq((-1.0, -1.0), (-1.0, 1.0), (1.0, -1.0))).toDF("x", "y").select(point($"x", $"y").as("point"))
	
	points.show()
	
	+-----------------+
	|            point|
	+-----------------+
	|Point(-1.0, -1.0)|
	| Point(-1.0, 1.0)|
	| Point(1.0, -1.0)|
	+-----------------+
	
### Polygon

	case class PolygonRecord(polygon: Polygon)
	
	val ring = Array(new Point(1.0, 1.0), new Point(1.0, -1.0),
      new Point(-1.0, -1.0), new Point(-1.0, 1.0),
      new Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
        PolygonRecord(new Polygon(Array(0), ring))
      )).toDF()
      
    polygons.show()
    
    +--------------------+
	|             polygon|
	+--------------------+
	|Polygon(5, Vector...|
	+--------------------+

## Predicates

### within

	polygons.select(point(0.5, 0.5) within $"polygon").count()

### intersects

	points.join(polygons).where($"point" intersects $"polygon").show()
	
	+-----------------+--------------------+
	|            point|             polygon|
	+-----------------+--------------------+
	|Point(-1.0, -1.0)|Polygon(5, Vector...|
	| Point(-1.0, 1.0)|Polygon(5, Vector...|
	| Point(1.0, -1.0)|Polygon(5, Vector...|
	+-----------------+--------------------+


