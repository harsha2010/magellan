//resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

//addSbtPlugin("com.typesafe.sbteclipse" %% "sbteclipse-plugin" % "2.2.0")
//
  //addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")


addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")


libraryDependencies += "org.scalariform" %% "scalariform" % "0.2.10"

//addSbtPlugin("com.alpinenow" % "junit_xml_listener" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.3.0")
