name := """events-fetcher"""
organization := "Vitaliy Kuznetsov & Lesia Mirchenko"
version := "0.0.1"
scalaVersion := "2.10.6"

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
    val shapelessV = "2.2.5"

    Seq(
        "org.apache.zookeeper" % "zookeeper" % zooV,

        "org.apache.hbase" % "hbase" % hbaseV,
        "org.apache.hbase" % "hbase-client" % hbaseV,
        "org.apache.hbase" % "hbase-server" % hbaseV excludeAll ExclusionRule(organization = "org.mortbay.jetty"),
        "org.apache.hbase" % "hbase-common" % hbaseV,
        "org.apache.hbase" % "hbase-protocol" % hbaseV,
        "org.apache.hbase" % "hbase-hadoop-compat" % hbaseV,

        "org.apache.htrace" % "htrace-core" % "3.1.0-incubating",
        "com.google.guava" % "guava" % "12.0.1",
        "com.google.protobuf" % "protobuf-java" % "2.5.0",

        "org.apache.hadoop" % "hadoop-common" % hadoopV excludeAll ExclusionRule(organization = "javax.servlet"),
        "org.apache.hadoop" % "hadoop-client" % hadoopV excludeAll ExclusionRule(organization = "javax.servlet") exclude("com.google.guava", "guava"),

        "org.apache.spark" %% "spark-core" % sparkV, // excludeAll ExclusionRule(organization = "org.eclipse.jetty.orbit"), // % "provided",

        "org.jodd" % "jodd-lagarto" % joddV,
        "org.jodd" % "jodd-core" % joddV,
        "org.jodd" % "jodd-log" % joddV,

        "com.chuusai" %% "shapeless" % shapelessV,

        "org.scalatest" %% "scalatest" % scalaTestV % "test",
        "org.scalamock" %% "scalamock-scalatest-support" % scalamockV % "test"

        // "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
        //
        // "com.softwaremill.macwire" %% "macros" % macwireV % "provided",
        // "com.softwaremill.macwire" %% "util" % macwireV,
        // "com.softwaremill.macwire" %% "proxy" % macwireV,
    )
}

mergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
    case "META-INF/jersey-module-version" => MergeStrategy.first
    case _ => MergeStrategy.first
}
