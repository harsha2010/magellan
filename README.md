# Magellan: Geospatial Analytics Using Spark

This package allows one to read Geospatial data formats as Spark Data Sources. It also provides a set of UDFS and utility functions that allows one to execute efficient geometric algorithms on this dataset.

# Linking

You can link against this library using the following coordinates:

	groupId: org.apache
	artifactId: magellan
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
	4. PolyLine

We will be adding support for other datastructures as necessary.

# Examples

## Reading Data

You can read data as follows:


	val df = sqlCtx.load("org.apache.magellan", path)


# Operations

## Expressions

### point

### line

### polygon

### polyline


## Predicates

### within

	
	val pdf = sqlCtx.shapeFile(pointsPath).as("pdf")
	val sdf = sqlCtx.shapeFile(polygonsPath).as("sdf")
	
	pdf.join(sdf).where($"pdf.point" within $"sdf.polygon")

### intersects

	val sdf = sqlCtx.shapeFile(polygonsPath).as("sdf")
	val line = new Line(new Point(0.0), new Point(1.0, 1.0))
	sdf.where(line within $"sdf.polygon")