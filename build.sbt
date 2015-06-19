name := "magellan"

version := "1.0.0"

organization := "org.apache"

scalaVersion := "2.10.4"

parallelExecution in Test := false

crossScalaVersions := Seq("2.10.4", "2.11.6")

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" % "provided"

libraryDependencies += "com.vividsolutions" % "jts" % "1.13"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

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
  <url>https://github.com/databricks/spark-csv</url>
  <licenses>
    <license>
      <name>Apache License, Verision 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:harsha2010/spatialsdk.git</url>
    <connection>scm:git:git@github.com:harsha2010/magellan.git</connection>
  </scm>
  <developers>
    <developer>
      <id>harsha2010</id>
      <name>Ram Sriharsha</name>
      <url>www.linkedin.com/in/harsha340</url>
    </developer>
  </developers>)

spName := "org.apache/magellan"

sparkVersion := "1.3.1"

sparkComponents += "sql"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"
