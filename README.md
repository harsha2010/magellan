# Magellan: Geospatial Analytics Using Spark
[![Gitter chat](https://badges.gitter.im/Magellan-dev/Lobby.png)](https://gitter.im/Magellan-dev/Lobby)
[![Build Status](https://travis-ci.org/harsha2010/magellan.svg?branch=master)](https://travis-ci.org/harsha2010/magellan)
[![codecov.io](http://codecov.io/github/harsha2010/magellan/coverage.svg?branch=master)](http://codecov.io/github/harsha2010/magellan?branch=maste)


Magellan is a distributed execution engine for geospatial analytics on big data. It is implemented on top of Apache Spark and deeply leverages modern database techniques like efficient data layout, code generation and query optimization in order to optimize geospatial queries.

The application developer writes standard sql or data frame queries to evaluate geometric expressions while the execution engine takes care of efficiently laying data out in memory during query processing, picking the right query plan, optimizing the query execution with cheap and efficient spatial indices while presenting a declarative abstraction to the developer.

Magellan is the first library to extend Spark SQL to provide a relational abstraction for geospatial analytics. I see it as an evolution of geospatial analytics engines into the emerging world of big data by providing abstractions that are developer friendly, can be leveraged by anyone who understands or uses Apache Spark while simultaneously showcasing an execution engine that is state of the art for geospatial analytics on big data.

# Version Release Notes

You can find notes on the various released versions [here](https://github.com/harsha2010/magellan/releases)

# Linking

You can link against the latest release using the following coordinates:

	groupId: harsha2010
	artifactId: magellan
	version: 1.0.5-s_2.11

# Requirements

v1.0.5 requires Spark 2.1+ and Scala 2.11

# Capabilities

The library currently supports reading the following formats:
  
  * [ESRI](https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf) 
  * [GeoJSON](http://geojson.org)
  * [OSM-XML](http://wiki.openstreetmap.org/wiki/OSM_XML)
  * [WKT](https://en.wikipedia.org/wiki/Well-known_text).

We aim to support the full suite of [OpenGIS Simple Features for SQL ](http://www.opengeospatial.org/standards/sfs) spatial predicate functions and operators together with additional topological functions.

The following geometries are currently supported:

**Geometries**:

  * Point
  * LineString
  * Polygon
  * MultiPoint
  * MultiPolygon (treated as a collection of Polygons and read in as a row per polygon by the GeoJSON reader)
	
The following predicates are currently supported:

  * Intersects
  * Contains
  * Within

The following languages are currently supported:

  * Scala




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

	> $SPARK_HOME/bin/spark-shell --packages harsha2010:magellan:1.0.4-s_2.11

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
	
	val ring = Array(Point(1.0, 1.0), Point(1.0, -1.0),
     Point(-1.0, -1.0), Point(-1.0, 1.0),
     Point(1.0, 1.0))
    val polygons = sc.parallelize(Seq(
        PolygonRecord(Polygon(Array(0), ring))
      )).toDF()
      
    polygons.show()
    
    +--------------------+
	|             polygon|
	+--------------------+
	|Polygon(5, Vector...|
	+--------------------+

## Predicates

### within

	points.join(polygons).where($"point" within $"polygon").show()

### intersects

	points.join(polygons).where($"point" intersects $"polygon").show()
	
	+-----------------+--------------------+
	|            point|             polygon|
	+-----------------+--------------------+
	|Point(-1.0, -1.0)|Polygon(5, Vector...|
	| Point(-1.0, 1.0)|Polygon(5, Vector...|
	| Point(1.0, -1.0)|Polygon(5, Vector...|
	+-----------------+--------------------+

### contains

Since contains is an overloaded expression (contains is used for checking String containment by Spark SQL), Magellan uses the Binary Expression ```>?``` for checking shape containment.

	points.join(polygons).where($"polygon" >? $"polygon").show()


	
A Databricks notebook with similar examples is published [here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/137058993011870/882779309834027/6891974485343070/latest.html) for convenience.

# Spatial indexes

Starting v1.0.5, Magellan support spatial indexes.
Spatial indexes supported the so called [ZOrderCurves](https://en.wikipedia.org/wiki/Z-order_curve).


Given a column of shapes, one can index the shapes to a given precision using a geohash indexer by doing the following:

```scala
df.withColumn("index", $"polygon" index 30)
```

This produces a new column called ```index``` which is a list of ZOrder Curves of precision ```30``` that taken together cover the polygon.

# Creating Indexes while loading data

The Spatial Relations (GeoJSON, Shapefile, OSM-XML) all have the ability to automatically index the geometries while loading them.

To turn this feature on, pass in the parameter ```magellan.index = true``` and optionally a value for ```magellan.index.precision``` (default = 30) while loading the data as follows:

```scala
spark.read.format("magellan")
  .option("magellan.index", "true")
  .option("magellan.index.precision", "25")
  .load(s"$path")
```

This creates an additional column called ```index``` which holds the list of ZOrder Curves of the given precision that cover each geometry in the dataset.

# Spatial Joins

Magellan leverages Spark SQL and has support for joins by default. However, these joins are by default not aware that the columns are geometric so a join of the form

```scala
  points.join(polygons).where($"point" within $"polygon")
```

will be treated as a Cartesian Join followed by a predicate. 
In some cases (especially when the polygon dataset is small (O(100-10000) polygons) this is fast enough.
However, when the number of polygons is much larger than that, you will need spatial joins to allow you to scale this computation

To enable spatial joins in Magellan, add a spatial join rule to Spark by injecting the following code before the join:

```scala
  magellan.Utils.injectRules(spark)
```


Furthermore, during the join, you will need to provide Magellan a hint of the precision at which to create indices for the join

You can do this by annotating either of the dataframes involved in the join by providing a Spatial Join Hint as follows:

```scala
var df = df.index(30) //after load or
val df =spark.read.format(...).load(..).index(30) //during load
```

Then a join of the form

```scala
  points.join(polygons).where($"point" within $"polygon") // or
  
  points.join(polygons index 30).where($"point" within $"polygon")
```

automatically uses indexes to speed up the join


# Developer Channel

Please visit [Gitter](https://gitter.im/magellan-dev/Lobby?source=orgpage) to discuss Magellan, obtain help from developers or report issues.
# Magellan Blog

For more details on Magellan and thoughts around Geospatial Analytics and the optimizations chosen for this project, please visit my [blog](https://magellan.ghost.io)
