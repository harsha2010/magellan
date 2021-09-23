name := "magellan"

version := "1.0.6-SNAPSHOT"

organization := "harsha2010"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12")

sparkVersion := "2.4.3"

scalacOptions += "-optimize"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

val testHadoopVersion = settingKey[String]("The version of Hadoop to test against.")

testHadoopVersion := sys.props.getOrElse("hadoop.testVersion", "2.7.7")

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.6",
  "com.google.guava" % "guava" % "27.1-jre",
  "org.slf4j" % "slf4j-api" % "1.7.16" % "provided",
  "com.lihaoyi" %% "fastparse" % "0.4.4" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.vividsolutions" % "jts" % "1.13" % "test",
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.8",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-base" % "2.9.8",
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.9.8",
  "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.9.8",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"

)

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value % "provided"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" exclude("org.apache.hadoop", "hadoop-client")
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/harsha2010/magellan</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:harsha2010/magellan.git</url>
    <connection>scm:git:git@github.com:harsha2010/magellan.git</connection>
  </scm>
  <developers>
    <developer>
      <id>harsha2010</id>
      <name>Ram Sriharsha</name>
      <url>www.linkedin.com/in/harsha340</url>
    </developer>
  </developers>)

spName := "harsha2010/magellan"

parallelExecution in Test := false


scoverage.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

