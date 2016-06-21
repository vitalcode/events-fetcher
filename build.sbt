import sbtassembly.AssemblyPlugin.autoImport._

name := """events-fetcher"""
organization := "Vitaliy Kuznetsov & Lesia Mirchenko"
version := "0.0.1"
scalaVersion := "2.10.6"

//dockerBaseImage := "dockerfile/java:oracle-java8"

resolvers ++= Seq(
    "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/public/"
)

libraryDependencies ++= {
    val joddV = "3.6.7"
    val clouderaV = "cdh5.4.7"
    val zooV = s"3.4.5-$clouderaV"
    val hadoopV = s"2.6.0-$clouderaV"
    val hbaseV = s"1.0.0-$clouderaV"
    val sparkV = s"1.3.0-$clouderaV"
    val scalaTestV = "2.2.5"
    val scalaLoggingV = "3.1.0"
    val scalamockV = "3.2"
    val macwireV = "2.2.2"
    val esV = "2.1.1"
    val configV = "1.3.0"
    val scalaHttpV = "2.2.1"
    val eventsModelV = "0.0.1"

    Seq(
        "org.apache.zookeeper" % "zookeeper" % zooV, //% "provided",
        "org.apache.hbase" % "hbase" % hbaseV,
        "org.apache.hbase" % "hbase-client" % hbaseV,
        "org.apache.hbase" % "hbase-server" % hbaseV excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
        "org.apache.hbase" % "hbase-common" % hbaseV,
        "org.apache.hbase" % "hbase-protocol" % hbaseV,
        "org.apache.hbase" % "hbase-hadoop-compat" % hbaseV,
        "org.apache.hadoop" % "hadoop-common" % hadoopV excludeAll ExclusionRule(organization = "javax.servlet"),
        "org.apache.hadoop" % "hadoop-client" % hadoopV excludeAll ExclusionRule(organization = "javax.servlet") exclude("com.google.guava", "guava"),
        "org.apache.spark" %% "spark-core" % sparkV,

        "org.elasticsearch" %% "elasticsearch-spark" % "2.1.0.Beta4",

        "org.apache.htrace" % "htrace-core" % "3.1.0-incubating",
        "com.google.guava" % "guava" % "12.0.1",
        "com.google.protobuf" % "protobuf-java" % "2.5.0",

        "org.jodd" % "jodd-lagarto" % joddV,
        "org.jodd" % "jodd-core" % joddV,
        "org.jodd" % "jodd-log" % joddV,
        "com.typesafe" % "config" % configV,
        "org.scalaj" %% "scalaj-http" % scalaHttpV,

        "org.scalatest" %% "scalatest" % scalaTestV % "test",
        "org.scalamock" %% "scalamock-scalatest-support" % scalamockV % "test",

        "vitalcode" %% "events-model" % eventsModelV
    )
}

enablePlugins(JavaAppPackaging)

assemblyMergeStrategy in assembly := {
    case PathList("reference.conf") => MergeStrategy.concat
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case "META-INF/jersey-module-version" => MergeStrategy.first
    case _ => MergeStrategy.first
}

parallelExecution in Test := false
assemblyJarName in assembly := "fetcher.jar"
mainClass in assembly := Some("uk.vitalcode.events.fetcher.Client")

