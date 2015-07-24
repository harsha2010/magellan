name := "magellan"

version := "1.0.2"

organization := "harsha2010"

scalaVersion := "2.10.4"

parallelExecution in Test := false

crossScalaVersions := Seq("2.10.4", "2.11.6")

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.5" % "provided"

libraryDependencies += "com.esri.geometry" % "esri-geometry-api" % "1.2.1"

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

sparkVersion := "1.4.0"

sparkComponents += "sql"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test"

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0") 
