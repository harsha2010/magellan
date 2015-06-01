# Geospatial Analytics Using Spark

This package allows one to read Geospatial data formats as Spark Data Sources. It also provides a set of UDFS and utility functions that allows one to execute efficient geometric algorithms on this dataset.

# Linking

You can link against this library using the following coordinates:

	groupId: com.hortonworks
	artifactId: spark-spatialsdk_2.10
	version: 1.0.0

# Requirements

This library requires Spark 1.3+

# Features

The library currently supports the [ESRI](https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf) format files.

An ESRI Shapefile is typically packaged as a directory containing .shp, .dbf files. We don't currently parse the .dbf (DBIndex) files. As a result, we expect that the path to the shapefile directory contains only .shf files.

The following data structures are parsed properly:

	1. Point
	2. NullShape
	3. Polygon

We will be adding support for other datastructures as necessary.

# Examples

## Reading Data

You can read data by instantiating the [SpatialContext](https://github.com/harsha2010/spatialsdk/blob/master/src/main/scala/com/hortonworks/spatialsdk/SpatialContext.scala) as below:

	import com.hortonworks.spatialsdk.SpatialContext
	
	val sqlCtx = new SpatialContext(sc)
    val df = sqlCtx.shapeFile(path)
    // this parses out the given shapefile
    // the schema is of the form point, polygon
    df.select($"polygon")...

You can import implicits to make this a bit simpler:

	import com.hortonworks.spatialsdk._
	
	val sqlCtx = new SQLContext(sc)
	val df = sqlCtx.shapeFile(path)
   	
   	
You can also use Datasource.load as follows:

	val df = sqlCtx.load("com.hortonworks.spatialsdk", path)


## Operations

### within

	
	val pdf = sqlCtx.shapeFile(pointsPath).as("pdf")
	val sdf = sqlCtx.shapeFile(polygonsPath).as("sdf")
	
	pdf.join(sdf).where($"pdf.point" within $"sdf.polygon")

