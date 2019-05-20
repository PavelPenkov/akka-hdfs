ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ru.ozon"
ThisBuild / organizationName := "akka_hdfs"

name := "Akka HDFS"

enablePlugins(JavaAppPackaging)

libraryDependencies := Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.22",
  ("com.lightbend.akka" %% "akka-stream-alpakka-hdfs" % "1.0.1").excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "org.apache.hadoop" % "hadoop-common" % "2.7.3",
  "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.3"
)
