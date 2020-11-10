ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "ru.ozon"
ThisBuild / organizationName := "akka_hdfs"

name := "Akka HDFS"

enablePlugins(JavaAppPackaging)

scalaVersion := "2.13.3"

val akkaVersion = "2.6.8"
val alpakkaKafkaVersion = "2.0.4"
val alpakkaHdfsVersion = "2.0.1"

maintainer := "ppenkov@ozon.ru"

libraryDependencies := Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.6.8",
  ("com.lightbend.akka" %% "akka-stream-alpakka-hdfs" % alpakkaHdfsVersion).excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
  "org.apache.hadoop" % "hadoop-client" % "2.9.1",
  "org.apache.hadoop" % "hadoop-common" % "2.9.1",
  "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.1.2",
  "commons-codec" % "commons-codec" % "1.12"
)

javaOptions in Universal ++= Seq(
  "-Dcom.sun.management.jmxremote",
  "-Dcom.sun.management.jmxremote.port=9010",
  "-Dcom.sun.management.jmxremote.authenticate=false",
  "-Dcom.sun.management.jmxremote.ssl=false",
  "-J-Xmx2g"
)